//! DataFusion logical operators connected by typed, signed relation changes.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::Column;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{
    Distinct, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, TableScan, Volatility,
};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::bag::Bag;
use crate::executor::{self, cache_scan, Bindings, Relation};
use crate::group_state::{supported_key, GroupState, KeyPolicy};
use crate::keyed_state::KeyedState;
use crate::planner::plan::{self, StateSpec};
use crate::planner::{IncrementalOptimizer, IncrementalPlan, RuleContext, Strategy};
use crate::store::{CheckpointStore, PublishPoint, Snapshot};
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

#[derive(Clone)]
struct Node {
    plan: LogicalPlan,
    inputs: Vec<usize>,
    ordered: bool,
}

// DISTINCT is set membership, represented by the existing indexed group state.
fn normalize_distinct(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(plan
        .transform_up(|plan| {
            if let LogicalPlan::Distinct(Distinct::All(input)) = &plan {
                if !input.schema().fields().is_empty() {
                    let mut grouped = LogicalPlanBuilder::from(input.as_ref().clone())
                        .aggregate(
                            input
                                .schema()
                                .columns()
                                .into_iter()
                                .map(Expr::Column)
                                .collect::<Vec<_>>(),
                            Vec::<Expr>::new(),
                        )?
                        .build()?;
                    if grouped.schema().as_arrow() != plan.schema().as_arrow()
                        || grouped.schema().iter().ne(plan.schema().iter())
                    {
                        return Err(datafusion::common::DataFusionError::Plan(
                            "DISTINCT normalization changed fields or qualifiers".into(),
                        ));
                    }
                    // Grouping strengthens uniqueness dependencies. Retaining the original
                    // weaker dependencies preserves the exact parent schema contract.
                    if let LogicalPlan::Aggregate(aggregate) = &mut grouped {
                        aggregate.schema = plan.schema().clone();
                    }
                    return Ok(Transformed::yes(grouped));
                }
            }
            Ok(Transformed::no(plan))
        })?
        .data)
}

/// Immutable candidate; only acknowledgement changes the owning view.
pub struct PreparedSqlRefresh {
    owner: u64,
    before: u64,
    tables: BTreeMap<String, Bag>,
    states: Vec<Relation>,
    groups: Vec<Option<GroupState>>,
    keyed: Vec<Option<KeyedState>>,
    delta: Bag,
    stats: RefreshStats,
}
impl PreparedSqlRefresh {
    /// Candidate full SQL result, preserving the root's ordering if present.
    pub fn contents(&self) -> &RecordBatch {
        &self.states[self.states.len() - 1].batch
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
    tables: BTreeMap<String, Bag>,
    ctx: SessionContext,
}

fn context() -> SessionContext {
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

fn check_expr(expr: &Expr) -> datafusion::common::Result<()> {
    expr.apply(|e| {
        let forbidden = matches!(e, Expr::ScalarSubquery(_) | Expr::Exists(_) |
            Expr::InSubquery(_) | Expr::OuterReferenceColumn(_, _) | Expr::Placeholder(_))
            || matches!(e, Expr::ScalarFunction(f) if f.func.signature().volatility != Volatility::Immutable);
        if forbidden {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "expression cannot be maintained: {e}")));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn add_node(
    plan: &LogicalPlan,
    nodes: &mut Vec<Node>,
    known: &mut HashMap<LogicalPlan, usize>,
) -> Result<usize> {
    if let Some(id) = known.get(plan) {
        return Ok(*id);
    }
    match plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Unnest(_) => {}
        _ => {
            return Err(Error::Invalid(format!(
                "unsupported SQL operator: {}",
                plan.display()
            )))
        }
    }
    for e in plan.expressions() {
        check_expr(&e)?;
    }
    let inputs = plan
        .inputs()
        .iter()
        .map(|p| add_node(p, nodes, known))
        .collect::<Result<Vec<_>>>()?;
    let ordered = matches!(plan, LogicalPlan::Sort(_))
        || (inputs.len() == 1
            && nodes[inputs[0]].ordered
            && matches!(
                plan,
                LogicalPlan::Projection(_)
                    | LogicalPlan::Filter(_)
                    | LogicalPlan::SubqueryAlias(_)
                    | LogicalPlan::Limit(_)
                    | LogicalPlan::Repartition(_)
            ));
    let id = nodes.len();
    nodes.push(Node {
        plan: plan.clone(),
        inputs,
        ordered,
    });
    known.insert(plan.clone(), id);
    Ok(id)
}

impl SqlView {
    /// Rebuild exactly the committed sources and validate its result before
    /// restoring the logical epoch. Source provenance is validated by its owner.
    #[cfg(feature = "iceberg")]
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
    #[cfg(feature = "iceberg")]
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
                .map(|(name, bag)| Ok((name.clone(), bag.to_batch()?)))
                .collect::<Result<_>>()?,
            output: candidate.contents().clone(),
            delta: candidate.delta()?,
        })
    }

    /// Encode the initial view without manufacturing a source transition.
    #[cfg(feature = "iceberg")]
    pub(crate) fn bootstrap_snapshot(&self) -> Result<Snapshot> {
        if self.epoch != 0 {
            return Err(Error::Invalid("bootstrap requires epoch zero".into()));
        }
        Ok(Snapshot {
            sql: self.sql.clone(),
            epoch: 0,
            tables: self.tables()?,
            output: self.contents()?,
            delta: self
                .states
                .last()
                .ok_or_else(|| Error::Invalid("empty SQL graph".into()))?
                .bag
                .weighted_batch()?,
        })
    }

    /// Analyze deterministic read-only SQL and initialize every operator once.
    pub async fn new(sql: &str, tables: BTreeMap<String, RecordBatch>) -> Result<Self> {
        Self::new_with_optimizer(sql, tables, IncrementalOptimizer::default()).await
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
        let mut source = BTreeMap::new();
        for (name, batch) in tables {
            ctx.register_batch(&name, batch.clone())?;
            source.insert(name, Bag::from_batch(&batch)?);
        }
        let analyzed = ctx.state().create_logical_plan(sql).await?;
        // Check volatility before optimizations can constant-fold a stable
        // function such as current_date into an apparently immutable literal.
        analyzed.apply_with_subqueries(|p| {
            for e in p.expressions() {
                e.apply(|e| {
                    if matches!(e, Expr::ScalarFunction(f) if f.func.signature().volatility != Volatility::Immutable) {
                        return Err(datafusion::common::DataFusionError::Plan("non-immutable SQL function".into()));
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let plan = normalize_distinct(ctx.state().optimize(&analyzed)?)?;
        let mut nodes = Vec::new();
        let root = add_node(&plan, &mut nodes, &mut HashMap::new())?;
        if root + 1 != nodes.len() {
            return Err(Error::Invalid(
                "query root was not last in postorder".into(),
            ));
        }
        let compiled = nodes
            .iter()
            .enumerate()
            .map(|(id, node)| {
                let group_eligible = Self::admits_group_index(&ctx, &node.plan)?;
                optimizer.rewrite_node(&RuleContext::new(
                    id,
                    &node.plan,
                    &node.inputs,
                    node.ordered,
                    group_eligible,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut incremental_plan = IncrementalPlan::new(compiled)?;
        let mut after_bindings: Vec<plan::IncrementalRel> = Vec::with_capacity(nodes.len());
        let mut programs = Vec::with_capacity(nodes.len());
        for (id, node) in nodes.iter().enumerate() {
            let children = node
                .inputs
                .iter()
                .map(|i| after_bindings[*i].clone())
                .collect::<Vec<_>>();
            let context = RuleContext::new(
                id,
                &node.plan,
                &node.inputs,
                node.ordered,
                Self::admits_group_index(&ctx, &node.plan)?,
            );
            let program = crate::planner::lower::compile(
                &context,
                &incremental_plan.nodes()[id].maintenance,
                children.clone(),
                optimizer.plan_rules(),
            )?;
            if optimizer.requires_incremental() && program.strategy() == Strategy::OperatorRecompute
            {
                return Err(Error::Invalid(
                    "strict incremental planning forbids physical replacement recomputation".into(),
                ));
            }
            programs.push(program);
            after_bindings.push(crate::planner::lower::after_input(
                id,
                &node.plan,
                node.ordered,
                children,
            )?);
        }
        incremental_plan.install_programs(programs)?;
        let group_slots = vec![None; nodes.len()];
        let mut out = Self {
            sql: sql.into(),
            owner: NEXT_OWNER.fetch_add(1, Ordering::Relaxed),
            epoch: 0,
            nodes,
            incremental_plan,
            states: vec![],
            keyed: vec![None; group_slots.len()],
            groups: group_slots,
            tables: source,
            ctx,
        };
        for id in 0..out.nodes.len() {
            let batch = if let LogicalPlan::TableScan(scan) = &out.nodes[id].plan {
                let table = out.source_for(scan, &out.tables)?.to_batch()?;
                out.eval_scan(scan, table).await?
            } else {
                out.eval_full(id, &out.states).await?
            };
            out.states.push(Relation::new(batch)?);
            match &out.incremental_plan.programs()[id].state {
                Some(StateSpec::Group {
                    input, projection, ..
                }) => {
                    let input = &out.states[*input].batch;
                    let keys =
                        executor::apply(&out.ctx, projection, std::slice::from_ref(input)).await?;
                    let index = GroupState::try_new(
                        input,
                        &keys,
                        &out.states[id].batch,
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
                        .map(|i| out.states[*i].batch.clone())
                        .collect::<Vec<_>>();
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
                                std::slice::from_ref(&out.states[id].batch),
                            )
                            .await?,
                        )
                    } else {
                        None
                    };
                    let output = output_keys
                        .as_ref()
                        .map(|keys| (&out.states[id].batch, keys));
                    out.keyed[id] = Some(KeyedState::new(&inputs, &projected, output)?);
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
            .map(|s| s.batch.clone())
            .ok_or_else(|| Error::Invalid("empty plan graph".into()))
    }
    /// Canonical source snapshots at this epoch, suitable for a separate oracle.
    pub fn tables(&self) -> Result<BTreeMap<String, RecordBatch>> {
        self.tables
            .iter()
            .map(|(n, b)| Ok((n.clone(), b.to_batch()?)))
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
            })
            .collect()
    }

    fn node_strategy(&self, id: usize) -> Strategy {
        self.incremental_plan.programs()[id].strategy()
    }

    fn admits_group_index(ctx: &SessionContext, plan: &LogicalPlan) -> Result<bool> {
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
        tables: &'a BTreeMap<String, Bag>,
    ) -> Result<&'a Bag> {
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
        cache_scan(node.plan.schema(), states[id].batch.clone(), id)
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
        let mut tables = self.tables.clone();
        for change in changes {
            let old = tables
                .get(&change.table)
                .ok_or_else(|| Error::Invalid(format!("unknown changed table {}", change.table)))?;
            let delta = Bag::from_weighted(&change.rows, &change.weights)?;
            let next = old.add(&delta)?;
            tables.insert(change.table.clone(), next);
        }
        for table in tables.values() {
            table.validate_relation()?;
        }
        let mut states = self.states.clone();
        let mut groups = self.groups.clone();
        let mut keyed = self.keyed.clone();
        let mut deltas: Vec<Bag> = Vec::with_capacity(self.nodes.len());
        // A sorted relation can reorder without changing its bag (for example,
        // projecting away a modified sort key). LIMIT must still be refreshed.
        let mut sequence_changed: Vec<bool> = Vec::with_capacity(self.nodes.len());
        let mut stats = RefreshStats {
            epoch,
            nodes: vec![],
        };
        for (id, node) in self.nodes.iter().enumerate() {
            let compiled = &self.incremental_plan.nodes()[id];
            let selected_strategy = self.incremental_plan.programs()[id].strategy();
            let mut group_work = None;
            let mut keyed_work = None;
            let schema = self.states[id].bag.schema();
            // Scans own a raw-source dependency even though they have no graph
            // children. This binding is independent of the selected rule.
            let source_delta = if let LogicalPlan::TableScan(scan) = &node.plan {
                Some(
                    self.source_for(scan, &tables)?
                        .difference(self.source_for(scan, &self.tables)?)?,
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
                        source_before: &self.tables,
                        source_after: &tables,
                        keyed: &self.keyed,
                        groups: &self.groups,
                    },
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
                keyed_work = result.keyed_work;
                group_work = result.group_work;
                (
                    result.delta,
                    result.replacement.map(Relation::new).transpose()?,
                )
            };
            if dirty {
                states[id] = match next {
                    Some(next) => next,
                    None => Relation::from_bag(self.states[id].bag.add(&delta)?)?,
                };
                stats.nodes.push(NodeStats {
                    id,
                    operator: node.plan.display().to_string(),
                    strategy: selected_strategy,
                    rule: compiled.rule.clone(),
                    delta_rows: delta.len(),
                    group_work,
                    keyed_work,
                });
            }
            sequence_changed.push(node.ordered && states[id].batch != self.states[id].batch);
            deltas.push(delta);
        }
        let delta = deltas
            .pop()
            .ok_or_else(|| Error::Invalid("empty graph".into()))?;
        Ok(PreparedSqlRefresh {
            owner: self.owner,
            before: self.epoch,
            tables,
            states,
            groups,
            keyed,
            delta,
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
        Ok(())
    }
}

/// A SQL view whose input vector and output changes commit in one local epoch.
pub struct DurableSqlView {
    view: SqlView,
    store: CheckpointStore,
}
impl DurableSqlView {
    /// Bootstrap a new store or recover its exact committed source vector.
    /// Initial tables are used only when no checkpoint has been published.
    pub async fn open(
        path: impl AsRef<Path>,
        sql: &str,
        initial: BTreeMap<String, RecordBatch>,
    ) -> Result<Self> {
        Self::open_with_optimizer(path, sql, initial, IncrementalOptimizer::default()).await
    }

    /// Bootstrap or rebuild committed sources using the supplied rule registry.
    ///
    /// Rule implementations are not persisted. Supply the desired registry again
    /// on reopen; committed output is still checked against the rebuilt query.
    pub async fn open_with_optimizer(
        path: impl AsRef<Path>,
        sql: &str,
        initial: BTreeMap<String, RecordBatch>,
        optimizer: IncrementalOptimizer,
    ) -> Result<Self> {
        let store = CheckpointStore::open(path)?;
        let view = match store.load()? {
            Some(snapshot) => {
                if snapshot.sql != sql {
                    return Err(Error::Invalid(
                        "SQL definition differs from checkpoint".into(),
                    ));
                }
                let mut view = SqlView::new_with_optimizer(sql, snapshot.tables, optimizer).await?;
                let rebuilt = view.contents()?;
                let ordered = view.nodes.last().is_some_and(|n| n.ordered);
                if (ordered && rebuilt != snapshot.output)
                    || Bag::from_batch(&rebuilt)? != Bag::from_batch(&snapshot.output)?
                {
                    return Err(Error::Invalid(
                        "recovered SQL result differs from committed output".into(),
                    ));
                }
                view.epoch = snapshot.epoch;
                view
            }
            None => {
                let view = SqlView::new_with_optimizer(sql, initial, optimizer).await?;
                let bootstrap = Snapshot {
                    sql: sql.into(),
                    epoch: 0,
                    tables: view.tables()?,
                    output: view.contents()?,
                    delta: view
                        .states
                        .last()
                        .ok_or_else(|| Error::Invalid("empty graph".into()))?
                        .bag
                        .weighted_batch()?,
                };
                store.commit(None, &bootstrap)?;
                view
            }
        };
        Ok(Self { view, store })
    }
    /// Current committed SQL view.
    pub fn view(&self) -> &SqlView {
        &self.view
    }
    /// Persist and then acknowledge one atomic multi-table batch.
    pub async fn refresh(&mut self, changes: &[TableChange]) -> Result<RefreshStats> {
        self.refresh_with_observer(changes, |_| Ok(())).await
    }
    /// Refresh with deterministic durability barriers for process-kill acceptance.
    pub async fn refresh_with_observer(
        &mut self,
        changes: &[TableChange],
        observer: impl FnMut(PublishPoint) -> Result<()>,
    ) -> Result<RefreshStats> {
        // Reconcile a prior committed-but-unacknowledged attempt before accepting
        // a new batch. Callers recover rather than replay an uncertain epoch.
        if self
            .store
            .load()?
            .is_none_or(|s| s.epoch != self.view.epoch)
        {
            return Err(Error::Invalid(
                "durable epoch differs; reopen before submitting another batch".into(),
            ));
        }
        let candidate = self.view.prepare(changes).await?;
        let snapshot = Snapshot {
            sql: self.view.sql.clone(),
            epoch: candidate.epoch(),
            tables: candidate
                .tables
                .iter()
                .map(|(n, b)| Ok((n.clone(), b.to_batch()?)))
                .collect::<Result<_>>()?,
            output: candidate.contents().clone(),
            delta: candidate.delta()?,
        };
        self.store
            .commit_with_observer(Some(self.view.epoch), &snapshot, observer)?;
        let stats = candidate.stats.clone();
        self.view.acknowledge(candidate)?;
        Ok(stats)
    }
}

#[cfg(test)]
mod normalization_tests {
    use super::*;
    use datafusion::logical_expr::{col, Distinct, EmptyRelation};

    #[test]
    fn remaining_distinct_normalizes_with_exact_qualifiers_and_zero_columns_stay_distinct(
    ) -> Result<()> {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("k", DataType::Int64, true),
        ]));
        let scan = cache_scan(
            &Arc::new(datafusion::common::DFSchema::try_from_qualified_schema(
                "t", &schema,
            )?),
            RecordBatch::new_empty(schema),
            0,
        )?;
        let input = LogicalPlanBuilder::from(scan)
            .project(vec![col("t.k")])?
            .build()?;
        let distinct = LogicalPlan::Distinct(Distinct::All(Arc::new(input)));
        let normalized = normalize_distinct(distinct.clone())?;
        assert!(matches!(normalized, LogicalPlan::Aggregate(_)));
        assert_eq!(normalized.schema(), distinct.schema());
        let zero = LogicalPlan::Distinct(Distinct::All(Arc::new(LogicalPlan::EmptyRelation(
            EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(datafusion::common::DFSchema::empty()),
            },
        ))));
        assert_eq!(normalize_distinct(zero.clone())?, zero);
        Ok(())
    }
}
