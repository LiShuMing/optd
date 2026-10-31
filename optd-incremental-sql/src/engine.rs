//! DataFusion logical operators connected by typed, signed relation changes.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Column;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, TableScan};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::accumulator_state::AccumulatorState;
use crate::bag::Bag;
use crate::executor::{self, cache_scan, Bindings, Relation};
use crate::graph::Node;
use crate::group_state::{supported_key, GroupState, KeyPolicy};
use crate::keyed_state::KeyedState;
use crate::planner::plan::StateSpec;
use crate::planner::{IncrementalOptimizer, IncrementalPlan, Strategy};
use crate::row_blocks::RowBlocks;
use crate::source_state::SourceState;
use crate::store::Snapshot;
use crate::{Error, Result};

static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

/// Signed changes to one named table. All entries in a prepare call are atomic.
#[derive(Clone, Debug)]
pub struct TableChange {
    /// Name used when registering the table for SQL planning.
    pub table: String,
    /// Complete typed row images, retaining duplicates and NULLs.
    pub rows: RecordBatch,
    /// One signed multiplicity per physical row.
    pub weights: Vec<i64>,
}

pub use crate::metrics::{GroupWork, KeyedWork, NodeStats, RefreshStats};

/// Immutable candidate; only acknowledgement changes the owning view.
#[must_use = "a prepared refresh must be acknowledged to advance the view"]
pub struct PreparedSqlRefresh {
    owner: u64,
    before: u64,
    tables: BTreeMap<String, SourceState>,
    states: Vec<Relation>,
    groups: Vec<Option<GroupState>>,
    keyed: Vec<Option<KeyedState>>,
    accumulators: Vec<Option<AccumulatorState>>,
    delta: Bag,
    output: RecordBatch,
    stats: RefreshStats,
}
impl PreparedSqlRefresh {
    /// Candidate full SQL result, preserving the root's ordering if present.
    pub fn contents(&self) -> &RecordBatch {
        &self.output
    }
    /// Complete row images followed by signed `__optd_weight` multiplicities.
    pub fn delta(&self) -> Result<RecordBatch> {
        self.delta.weighted_batch()
    }
    /// Actual execution strategies for the prepared batch.
    pub fn stats(&self) -> &RefreshStats {
        &self.stats
    }
    /// Source epoch that will become visible on acknowledgement.
    pub fn epoch(&self) -> u64 {
        self.stats.epoch
    }
}

/// A typed SQL definition with operator-local maintained relations.
pub struct SqlView {
    sql: String,
    owner: u64,
    epoch: u64,
    nodes: Vec<Node>,
    incremental_plan: IncrementalPlan,
    states: Vec<Relation>,
    groups: Vec<Option<GroupState>>,
    keyed: Vec<Option<KeyedState>>,
    accumulators: Vec<Option<AccumulatorState>>,
    tables: BTreeMap<String, SourceState>,
    ctx: SessionContext,
}

pub(crate) fn context() -> SessionContext {
    // One physical input partition preserves ordered cache sequences; execution
    // parallelism is a separate performance qualification, not a correctness knob.
    let mut config = SessionConfig::new().with_target_partitions(1);
    // A physical expression subquery could read an original registered table
    // outside this graph. Force DataFusion's scalar-to-join lowering instead.
    config
        .options_mut()
        .optimizer
        .enable_physical_uncorrelated_scalar_subquery = false;
    SessionContext::new_with_config(config)
}

impl SqlView {
    /// Rebuild exactly the committed sources and validate its result before
    /// restoring the logical epoch. Source provenance is validated by its owner.
    pub(crate) async fn restore_checkpoint_with_optimizer(
        snapshot: &Snapshot,
        optimizer: IncrementalOptimizer,
    ) -> Result<Self> {
        let mut view =
            Self::new_with_optimizer(&snapshot.sql, snapshot.tables.clone(), optimizer).await?;
        let rebuilt = view.contents()?;
        let ordered = view.nodes.last().is_some_and(|node| node.ordered);
        if (ordered && rebuilt != snapshot.output)
            || Bag::from_batch(&rebuilt)? != Bag::from_batch(&snapshot.output)?
        {
            return Err(Error::Invalid(
                "recovered SQL result differs from committed output".into(),
            ));
        }
        view.epoch = snapshot.epoch;
        Ok(view)
    }

    /// Bind a source-aware publication to this owner's exact prepared state.
    pub(crate) fn checkpoint_for(&self, candidate: &PreparedSqlRefresh) -> Result<Snapshot> {
        if candidate.owner != self.owner
            || candidate.before != self.epoch
            || self.epoch.checked_add(1) != Some(candidate.epoch())
        {
            return Err(Error::Invalid(
                "checkpoint candidate owner or predecessor differs".into(),
            ));
        }
        Ok(Snapshot {
            sql: self.sql.clone(),
            epoch: candidate.epoch(),
            tables: candidate
                .tables
                .iter()
                .map(|(name, bag)| Ok((name.clone(), bag.snapshot()?.bag.to_batch()?)))
                .collect::<Result<_>>()?,
            output: candidate.contents().clone(),
            delta: candidate.delta()?,
        })
    }

    /// Encode the initial view without manufacturing a source transition.
    pub(crate) fn bootstrap_snapshot(&self) -> Result<Snapshot> {
        if self.epoch != 0 {
            return Err(Error::Invalid("bootstrap requires epoch zero".into()));
        }
        Ok(Snapshot {
            sql: self.sql.clone(),
            epoch: 0,
            tables: self.tables()?,
            output: self.contents()?,
            delta: Bag::from_batch(&self.contents()?)?.weighted_batch()?,
        })
    }

    /// Analyze deterministic read-only SQL and initialize every operator once.
    pub async fn new(sql: &str, tables: BTreeMap<String, RecordBatch>) -> Result<Self> {
        Self::new_with_optimizer(sql, tables, IncrementalOptimizer::default()).await
    }

    /// Compile against schemas without executing source rows or initializing state.
    ///
    /// The report includes every refusal in an admitted SQL graph, including final
    /// physical replacements. SQL binding/normalization errors remain typed errors.
    /// The SQL optimizer still folds immutable constants. Eligibility does not
    /// guarantee runtime arithmetic or resource success.
    pub async fn preflight(
        sql: &str,
        schemas: BTreeMap<String, datafusion::arrow::datatypes::SchemaRef>,
        optimizer: IncrementalOptimizer,
    ) -> Result<crate::PlanPreflight> {
        let ctx = context();
        for (name, schema) in schemas {
            ctx.register_batch(&name, RecordBatch::new_empty(schema))?;
        }
        Ok(crate::planning::compile(&ctx, sql, &optimizer)
            .await?
            .report)
    }

    /// Compile a caller-selected rule registry, then initialize its verified plan.
    ///
    /// Rules control actual maintenance expressions. They are trusted semantic
    /// extensions and must be tested against independent full-query results.
    pub async fn new_with_optimizer(
        sql: &str,
        tables: BTreeMap<String, RecordBatch>,
        optimizer: IncrementalOptimizer,
    ) -> Result<Self> {
        let ctx = context();
        for (name, batch) in &tables {
            ctx.register_batch(name, batch.clone())?;
        }
        let compilation = crate::planning::compile(&ctx, sql, &optimizer).await?;
        let (nodes, incremental_plan) = compilation.into_plan(optimizer.requires_incremental())?;
        // Reject the complete plan before encoding source rows or creating any state.
        let source = tables
            .into_iter()
            .map(|(name, batch)| Ok((name, SourceState::new(Bag::from_batch(&batch)?)?)))
            .collect::<Result<_>>()?;
        let group_slots = vec![None; nodes.len()];
        let mut out = Self {
            sql: sql.into(),
            owner: NEXT_OWNER.fetch_add(1, Ordering::Relaxed),
            epoch: 0,
            nodes,
            incremental_plan,
            states: vec![],
            keyed: vec![None; group_slots.len()],
            accumulators: vec![None; group_slots.len()],
            groups: group_slots,
            tables: source,
            ctx,
        };
        for id in 0..out.nodes.len() {
            let batch = if let LogicalPlan::TableScan(scan) = &out.nodes[id].plan {
                let table = out
                    .source_for(scan, &out.tables)?
                    .snapshot()?
                    .bag
                    .to_batch()?;
                out.eval_scan(scan, table).await?
            } else {
                out.eval_full(id, &out.states).await?
            };
            out.states.push(Relation::new(batch)?);
            match &out.incremental_plan.programs()[id].state {
                Some(StateSpec::Accumulator {
                    input, operator, ..
                }) => {
                    out.accumulators[id] = Some(AccumulatorState::new(
                        &out.ctx,
                        operator,
                        &out.states[*input].rows.materialize()?.batch,
                    )?);
                }
                Some(StateSpec::Group {
                    input, projection, ..
                }) => {
                    let input_batch = out.states[*input].rows.materialize()?.batch;
                    let input = &input_batch;
                    let keys =
                        executor::apply(&out.ctx, projection, std::slice::from_ref(input)).await?;
                    let index = GroupState::try_new(
                        input,
                        &keys,
                        &out.states[id].rows.materialize()?.batch,
                        KeyPolicy {
                            binary_strings: true,
                        },
                    )?
                    .ok_or_else(|| {
                        Error::Invalid("qualified group index could not initialize".into())
                    })?;
                    out.groups[id] = Some(index);
                }
                Some(StateSpec::Keyed(spec)) => {
                    let inputs = spec
                        .inputs
                        .iter()
                        .map(|i| Ok(out.states[*i].rows.materialize()?.batch))
                        .collect::<Result<Vec<_>>>()?;
                    let mut projected = Vec::with_capacity(inputs.len());
                    for (input, projection) in inputs.iter().zip(&spec.projections) {
                        projected.push(
                            executor::apply(&out.ctx, projection, std::slice::from_ref(input))
                                .await?,
                        );
                    }
                    let output_keys = if let Some(projection) = &spec.output_projection {
                        Some(
                            executor::apply(
                                &out.ctx,
                                projection,
                                std::slice::from_ref(&out.states[id].rows.materialize()?.batch),
                            )
                            .await?,
                        )
                    } else {
                        None
                    };
                    let output_batch = out.states[id].rows.materialize()?.batch;
                    let output = output_keys.as_ref().map(|keys| (&output_batch, keys));
                    let mut index = KeyedState::new(&inputs, &projected, output)?;
                    index.initialize_matches(&out.ctx, spec)?;
                    out.keyed[id] = Some(index);
                }
                None => {}
            }
        }
        Ok(out)
    }

    /// Current source epoch; bootstrap is epoch zero.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Maintained SQL result; ORDER BY order is preserved at the root.
    pub fn contents(&self) -> Result<RecordBatch> {
        self.states
            .last()
            .ok_or_else(|| Error::Invalid("empty plan graph".into()))?
            .rows
            .materialize()
            .map(|m| m.batch)
    }
    /// Canonical source snapshots at this epoch, suitable for a separate oracle.
    pub fn tables(&self) -> Result<BTreeMap<String, RecordBatch>> {
        self.tables
            .iter()
            .map(|(n, b)| Ok((n.clone(), b.snapshot()?.bag.to_batch()?)))
            .collect()
    }
    /// SQL definition bound to this generation.
    pub fn sql(&self) -> &str {
        &self.sql
    }
    /// Immutable executable maintenance plan, including selected rules and expressions.
    pub fn incremental_plan(&self) -> &IncrementalPlan {
        &self.incremental_plan
    }
    /// Planned strategies; a dirty ordered chain is explicitly recomputed.
    pub fn strategies(&self) -> Vec<NodeStats> {
        self.nodes
            .iter()
            .enumerate()
            .map(|(id, n)| NodeStats {
                id,
                operator: n.plan.display().to_string(),
                strategy: self.node_strategy(id),
                rule: self.incremental_plan.nodes()[id].rule.clone(),
                delta_rows: 0,
                group_work: None,
                keyed_work: None,
                accumulator_work: None,
            })
            .collect()
    }

    fn node_strategy(&self, id: usize) -> Strategy {
        self.incremental_plan.programs()[id].strategy()
    }

    pub(crate) fn admits_group_index(ctx: &SessionContext, plan: &LogicalPlan) -> Result<bool> {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return Ok(false);
        };
        if aggregate.group_expr.is_empty()
            || aggregate
                .group_expr
                .iter()
                .any(|e| matches!(e, Expr::GroupingSet(_)))
        {
            return Ok(false);
        }
        for key in &aggregate.group_expr {
            if !supported_key(
                &key.get_type(aggregate.input.schema())?,
                KeyPolicy {
                    binary_strings: true,
                },
            ) {
                return Ok(false);
            }
        }
        let session = ctx.state();
        for expr in &aggregate.aggr_expr {
            let mut expr = expr;
            while let Expr::Alias(alias) = expr {
                expr = &alias.expr;
            }
            let Expr::AggregateFunction(function) = expr else {
                return Ok(false);
            };
            let name = function.func.name();
            // Check implementation identity against this fixed builtin registry,
            // not merely a name that a custom UDAF could reuse.
            if !matches!(name, "count" | "sum" | "avg" | "min" | "max")
                || session.aggregate_functions().get(name) != Some(&function.func)
                || !function.params.order_by.is_empty()
            {
                return Ok(false);
            }
            for arg in &function.params.args {
                if !matches!(
                    arg.get_type(aggregate.input.schema())?,
                    DataType::Null
                        | DataType::Boolean
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Decimal128(_, _)
                        | DataType::Decimal256(_, _)
                        | DataType::Utf8
                        | DataType::Utf8View
                        | DataType::LargeUtf8
                        | DataType::Date32
                        | DataType::Date64
                ) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn source_for<'a>(
        &self,
        scan: &TableScan,
        tables: &'a BTreeMap<String, SourceState>,
    ) -> Result<&'a SourceState> {
        tables
            .get(&scan.table_name.to_string())
            .or_else(|| tables.get(scan.table_name.table()))
            .ok_or_else(|| Error::Invalid(format!("unbound table {}", scan.table_name)))
    }

    async fn execute(&self, plan: LogicalPlan) -> Result<RecordBatch> {
        executor::execute(&self.ctx, plan).await
    }

    async fn eval_scan(&self, scan: &TableScan, batch: RecordBatch) -> Result<RecordBatch> {
        let provider = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?);
        let mut builder =
            LogicalPlanBuilder::scan(scan.table_name.clone(), provider_as_source(provider), None)?;
        // A TableScan's pushed expressions are still part of its semantics.
        for filter in &scan.filters {
            builder = builder.filter(filter.clone())?;
        }
        if let Some(indices) = &scan.projection {
            let fields = builder.schema();
            let cols = indices
                .iter()
                .map(|i| {
                    let (qualifier, field) = fields.qualified_field(*i);
                    Expr::Column(Column::new(qualifier.cloned(), field.name()))
                })
                .collect::<Vec<_>>();
            builder = builder.project(cols)?;
        }
        if let Some(fetch) = scan.fetch {
            builder = builder.limit(0, Some(fetch))?;
        }
        self.execute(builder.build()?).await
    }

    fn cached_input(&self, id: usize, states: &[Relation]) -> Result<LogicalPlan> {
        let node = &self.nodes[id];
        if node.ordered {
            // Keep Sort and its unary chain in the local physical plan, so a
            // Limit cannot lose its ordering at a MemTable boundary.
            let inputs = node
                .inputs
                .iter()
                .map(|i| self.cached_input(*i, states))
                .collect::<Result<Vec<_>>>()?;
            return Ok(node.plan.with_new_exprs(node.plan.expressions(), inputs)?);
        }
        cache_scan(node.plan.schema(), states[id].rows.materialize()?.batch, id)
    }

    async fn eval_full(&self, id: usize, states: &[Relation]) -> Result<RecordBatch> {
        let node = &self.nodes[id];
        let inputs = node
            .inputs
            .iter()
            .map(|i| self.cached_input(*i, states))
            .collect::<Result<Vec<_>>>()?;
        self.execute(node.plan.with_new_exprs(node.plan.expressions(), inputs)?)
            .await
            .map_err(|source| Error::Operator {
                id,
                plan: node.plan.display().to_string(),
                source: Box::new(source),
            })
    }

    /// Prepare an atomic multi-table epoch, without mutating any visible state.
    pub async fn prepare(&self, changes: &[TableChange]) -> Result<PreparedSqlRefresh> {
        let epoch = self.epoch.checked_add(1).ok_or(Error::Overflow)?;
        let mut stats = RefreshStats {
            epoch,
            ..Default::default()
        };
        let mut source_deltas = BTreeMap::<String, Bag>::new();
        for change in changes {
            let old = self
                .tables
                .get(&change.table)
                .ok_or_else(|| Error::Invalid(format!("unknown changed table {}", change.table)))?;
            let delta = Bag::from_weighted(&change.rows, &change.weights)?;
            let staged = source_deltas.get(&change.table);
            old.check_staged_addition(staged, &delta, &mut stats.work.cache_work.source_state)?;
            let combined = match staged {
                Some(previous) => previous.add(&delta)?,
                None => delta,
            };
            source_deltas.insert(change.table.clone(), combined);
        }
        let mut tables = self.tables.clone();
        for (name, delta) in &source_deltas {
            if delta.is_empty() {
                continue;
            }
            // Every name was bound above; apply the atomic net change once,
            // sharing the committed source unchanged when the net change is zero.
            let old = &self.tables[name];
            let next = old.apply(
                delta,
                &mut stats.work.sources,
                &mut stats.work.cache_work.source_state,
            )?;
            tables.insert(name.clone(), next);
        }
        // State candidates share immutable directory roots; mutations account path copies.
        let mut states = self.states.clone();
        let mut groups = self.groups.clone();
        let mut keyed = self.keyed.clone();
        let mut accumulators = self.accumulators.clone();
        let mut deltas: Vec<Bag> = Vec::with_capacity(self.nodes.len());
        // A sorted relation can reorder without changing its bag (for example,
        // projecting away a modified sort key). LIMIT must still be refreshed.
        let mut sequence_changed: Vec<bool> = Vec::with_capacity(self.nodes.len());
        for (id, node) in self.nodes.iter().enumerate() {
            let compiled = &self.incremental_plan.nodes()[id];
            let selected_strategy = self.incremental_plan.programs()[id].strategy();
            let mut group_work = None;
            let mut keyed_work = None;
            let mut accumulator_work = None;
            let schema = self.states[id].rows.schema();
            // Scans own a raw-source dependency even though they have no graph
            // children. This binding is independent of the selected rule.
            let source_delta = if let LogicalPlan::TableScan(scan) = &node.plan {
                Some(
                    source_deltas
                        .get(&scan.table_name.to_string())
                        .or_else(|| source_deltas.get(scan.table_name.table()))
                        .cloned()
                        .unwrap_or_else(|| Bag::empty(scan.source.schema())),
                )
            } else {
                None
            };
            let dirty = match &source_delta {
                Some(delta) => !delta.is_empty(),
                None => {
                    node.inputs.iter().any(|i| !deltas[*i].is_empty())
                        || (selected_strategy == Strategy::OperatorRecompute
                            && node.inputs.iter().any(|i| sequence_changed[*i]))
                }
            };
            let (delta, next) = if !dirty {
                (Bag::empty(schema), None)
            } else {
                let result = executor::run(
                    &self.ctx,
                    &self.incremental_plan.programs()[id].steps,
                    Bindings {
                        deltas: &deltas,
                        before: &self.states,
                        after: &states,
                        source_deltas: &source_deltas,
                        source_after: &tables,
                        keyed: &self.keyed,
                        groups: &self.groups,
                        accumulators: &self.accumulators,
                    },
                    &mut stats.work,
                )
                .await
                .map_err(|source| Error::Operator {
                    id,
                    plan: node.plan.display().to_string(),
                    source: Box::new(source),
                })?;
                if let Some(index) = result.keyed {
                    keyed[id] = Some(index);
                }
                if let Some(index) = result.groups {
                    groups[id] = Some(index);
                }
                if let Some(index) = result.accumulator {
                    accumulators[id] = Some(index);
                }
                accumulator_work = result.accumulator_work;
                keyed_work = result.keyed_work;
                group_work = result.group_work;
                let replacement = if let Some(batch) = result.replacement {
                    let imported = RowBlocks::from_relation_batch(batch)?;
                    stats
                        .work
                        .cache_work
                        .replacement_bootstrap
                        .absorb(&imported.work)?;
                    stats
                        .work
                        .cache_work
                        .leaf_validation
                        .absorb(&imported.leaf_validation)?;
                    Some(Relation {
                        rows: imported.rows,
                    })
                } else {
                    None
                };
                (result.delta, replacement)
            };
            if dirty {
                states[id] = match next {
                    Some(next) => next,
                    None if delta.is_empty() => self.states[id].clone(),
                    None => {
                        let update = self.states[id].rows.apply(&delta)?;
                        stats.work.caches.absorb(&update.work.delta_update)?;
                        stats
                            .work
                            .cache_work
                            .leaf_validation
                            .absorb(&update.work.leaf_validation)?;
                        stats
                            .work
                            .cache_work
                            .cache_blocks
                            .absorb(&update.work.block_update)?;
                        Relation { rows: update.rows }
                    }
                };
                stats.nodes.push(NodeStats {
                    id,
                    operator: node.plan.display().to_string(),
                    strategy: selected_strategy,
                    rule: compiled.rule.clone(),
                    delta_rows: delta.len(),
                    group_work,
                    keyed_work,
                    accumulator_work,
                });
            }
            let changed_sequence = if node.ordered {
                let after = states[id].rows.materialize_recorded(
                    &mut stats.work.cache_work.complete_caches,
                    &mut stats.work.cache_work.demanded_leaves,
                )?;
                let before = self.states[id].rows.materialize_recorded(
                    &mut stats.work.cache_work.complete_caches,
                    &mut stats.work.cache_work.demanded_leaves,
                )?;
                after != before
            } else {
                false
            };
            sequence_changed.push(changed_sequence);
            deltas.push(delta);
        }
        let delta = deltas
            .pop()
            .ok_or_else(|| Error::Invalid("empty graph".into()))?;
        // Complete root output is an explicit API demand; intermediates remain lazy.
        let output = states
            .last()
            .ok_or_else(|| Error::Invalid("empty graph".into()))?
            .rows
            .materialize_recorded(
                &mut stats.work.cache_work.complete_caches,
                &mut stats.work.cache_work.demanded_leaves,
            )?;
        Ok(PreparedSqlRefresh {
            owner: self.owner,
            before: self.epoch,
            tables,
            states,
            groups,
            keyed,
            accumulators,
            delta,
            output,
            stats,
        })
    }

    /// Publish a candidate already made durable, or use this for volatile runs.
    pub fn acknowledge(&mut self, candidate: PreparedSqlRefresh) -> Result<()> {
        if candidate.owner != self.owner || candidate.before != self.epoch {
            return Err(Error::Invalid("stale or foreign SQL refresh".into()));
        }
        self.epoch = candidate.stats.epoch;
        self.tables = candidate.tables;
        self.states = candidate.states;
        self.groups = candidate.groups;
        self.keyed = candidate.keyed;
        self.accumulators = candidate.accumulators;
        Ok(())
    }
}
