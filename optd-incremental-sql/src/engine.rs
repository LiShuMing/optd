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
use crate::executor::{self, cache_scan, Bindings, ProgramTemplates, Relation};
use crate::graph::Node;
use crate::group_state::{supported_key, GroupState, KeyPolicy};
use crate::keyed_state::KeyedState;
use crate::planner::plan::StateSpec;
use crate::planner::{IncrementalOptimizer, IncrementalPlan, Strategy};
use crate::rank_state::RankState;
use crate::row_blocks::RowBlocks;
use crate::source_state::SourceState;
use crate::state_codec::{OperatorStateSnapshot, ViewStateSnapshot};
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
    states: Vec<Option<Relation>>,
    groups: Vec<Option<GroupState>>,
    keyed: Vec<Option<KeyedState>>,
    accumulators: Vec<Option<AccumulatorState>>,
    ranks: Vec<Option<RankState>>,
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
    /// Candidate maintained output blocks, sharing unchanged leaves with the prior revision.
    pub(crate) fn output_blocks(&self) -> Result<RowBlocks> {
        Ok(self
            .states
            .last()
            .and_then(|rel| rel.as_ref())
            .ok_or_else(|| Error::Invalid("empty plan graph or missing root relation".into()))?
            .rows
            .clone())
    }
    /// Export this prepared candidate's complete derived state before acknowledging it.
    pub(crate) fn export_state(
        &self,
        view: &SqlView,
        schemas: &BTreeMap<String, datafusion::arrow::datatypes::SchemaRef>,
    ) -> Result<ViewStateSnapshot> {
        export_view_parts(
            view,
            self.epoch(),
            schemas,
            &self.states,
            &self.accumulators,
            &self.keyed,
            &self.groups,
            &self.ranks,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn export_view_parts(
    view: &SqlView,
    view_epoch: u64,
    schemas: &BTreeMap<String, datafusion::arrow::datatypes::SchemaRef>,
    states: &[Option<Relation>],
    accumulators: &[Option<AccumulatorState>],
    keyed: &[Option<KeyedState>],
    groups: &[Option<GroupState>],
    ranks: &[Option<RankState>],
) -> Result<ViewStateSnapshot> {
    let plan_fingerprint = view.incremental_plan.fingerprint(&view.sql, schemas)?;
    let node_relations = states
        .iter()
        .map(|rel| rel.as_ref().map(|r| r.rows.clone()))
        .collect();
    let mut operators = Vec::with_capacity(view.nodes.len());
    for id in 0..view.nodes.len() {
        let op = match &view.incremental_plan.programs()[id].state {
            Some(StateSpec::Accumulator { .. }) => {
                let state = accumulators[id]
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("missing accumulator state on export".into()))?;
                Some(OperatorStateSnapshot::Accumulator(
                    state.export_persisted()?,
                ))
            }
            Some(StateSpec::Keyed(_)) => {
                let state = keyed[id]
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("missing keyed state on export".into()))?;
                Some(OperatorStateSnapshot::Keyed(state.export_persisted()?))
            }
            Some(StateSpec::Group { .. }) => {
                let state = groups[id]
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("missing group state on export".into()))?;
                Some(OperatorStateSnapshot::Group(state.export_persisted()?))
            }
            Some(StateSpec::Rank { .. }) => {
                let state = ranks[id]
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("missing rank state on export".into()))?;
                Some(OperatorStateSnapshot::Rank(state.export_persisted()?))
            }
            None => None,
        };
        operators.push(op);
    }
    Ok(ViewStateSnapshot {
        view_epoch,
        plan_fingerprint,
        node_relations,
        operators,
    })
}

/// A typed SQL definition with operator-local maintained relations.
pub struct SqlView {
    sql: String,
    owner: u64,
    epoch: u64,
    nodes: Vec<Node>,
    node_schemas: Vec<datafusion::arrow::datatypes::SchemaRef>,
    retained_relations: Vec<bool>,
    incremental_plan: IncrementalPlan,
    templates: Vec<ProgramTemplates>,
    states: Vec<Option<Relation>>,
    groups: Vec<Option<GroupState>>,
    keyed: Vec<Option<KeyedState>>,
    accumulators: Vec<Option<AccumulatorState>>,
    ranks: Vec<Option<RankState>>,
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

/// Schema-only diagnostic explanation for an arbitrary SQL query.
///
/// Always includes the full `PlanPreflight` (including any blockers) and carries
/// the compiled `IncrementalPlan` whenever compilation succeeded, even when
/// strict mode rejects one or more nodes for complete recomputation.
#[derive(Clone, Debug)]
pub struct QueryExplain {
    /// Complete schema-bound node qualification and blockers.
    pub preflight: crate::PlanPreflight,
    /// Compiled logical/physical incremental plan when compilation succeeded.
    pub plan: Option<IncrementalPlan>,
}

impl QueryExplain {
    /// Render the query's incremental plan as a readable Unicode DAG tree.
    pub fn explain_tree(&self) -> String {
        crate::planner::explain::format_query_explain(
            &self.preflight,
            self.plan.as_ref(),
            crate::planner::ExplainStyle::Tree,
        )
    }

    /// Render the DAG tree followed by lowered physical step DAGs for each node.
    pub fn explain_detailed(&self) -> String {
        crate::planner::explain::format_query_explain(
            &self.preflight,
            self.plan.as_ref(),
            crate::planner::ExplainStyle::Detailed,
        )
    }
}

/// Pre-applied shared source revision and optional single-attribution work counts.
pub(crate) struct SharedSourceUpdate {
    pub(crate) source: SourceState,
    pub(crate) work: Option<(crate::RelationWork, crate::SourceStateWork)>,
}

impl SqlView {
    /// Floating result positions with the explicitly documented stable numerical contract.
    /// Source images, other columns and signed-delta replay retain exact identity.
    pub fn numerical_columns(&self) -> Result<Vec<usize>> {
        crate::planning::numerical_columns(&self.nodes)
    }

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

    /// Compile a SQL query against schemas only and return both preflight diagnostics
    /// and the compiled incremental plan for text EXPLAIN rendering.
    pub async fn explain_query(
        sql: &str,
        schemas: BTreeMap<String, datafusion::arrow::datatypes::SchemaRef>,
        optimizer: IncrementalOptimizer,
    ) -> Result<QueryExplain> {
        let ctx = context();
        for (name, schema) in schemas {
            ctx.register_batch(&name, RecordBatch::new_empty(schema))?;
        }
        let (preflight, plan) = crate::planning::compile(&ctx, sql, &optimizer)
            .await?
            .into_explain();
        Ok(QueryExplain { preflight, plan })
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
        let shared = tables
            .into_iter()
            .map(|(name, batch)| Ok((name, SourceState::new(Bag::from_batch(&batch)?)?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        Self::new_with_shared_sources(sql, shared, optimizer).await
    }

    /// Initialize a view directly from shared `SourceState` handles so multiple views
    /// referencing the same base table share one `Arc<SourceSnapshot>` per table.
    pub(crate) async fn new_with_shared_sources(
        sql: &str,
        tables: BTreeMap<String, SourceState>,
        optimizer: IncrementalOptimizer,
    ) -> Result<Self> {
        let ctx = context();
        for (name, source) in &tables {
            ctx.register_batch(name, RecordBatch::new_empty(source.schema()))?;
        }
        let compilation = crate::planning::compile(&ctx, sql, &optimizer).await?;
        let (nodes, incremental_plan) = compilation.into_plan(optimizer.requires_incremental())?;
        let templates = incremental_plan
            .programs()
            .iter()
            .map(|program| ProgramTemplates::compile(&ctx, program))
            .collect::<Result<Vec<_>>>()?;
        let source = tables;
        let node_schemas: Vec<datafusion::arrow::datatypes::SchemaRef> = nodes
            .iter()
            .map(|n| Arc::new(n.plan.schema().as_arrow().clone()))
            .collect();
        let retained_relations = incremental_plan.retained_relations();
        let group_slots = vec![None; nodes.len()];
        let mut out = Self {
            sql: sql.into(),
            owner: NEXT_OWNER.fetch_add(1, Ordering::Relaxed),
            epoch: 0,
            nodes,
            node_schemas,
            retained_relations: retained_relations.clone(),
            incremental_plan,
            templates,
            states: Vec::new(),
            keyed: vec![None; group_slots.len()],
            accumulators: vec![None; group_slots.len()],
            ranks: vec![None; group_slots.len()],
            groups: group_slots,
            tables: source,
            ctx,
        };
        let mut bootstrap_states: Vec<Relation> = Vec::with_capacity(out.nodes.len());
        for id in 0..out.nodes.len() {
            let batch = if let LogicalPlan::TableScan(scan) = &out.nodes[id].plan {
                let table = out
                    .source_for(scan, &out.tables)?
                    .snapshot()?
                    .bag
                    .to_batch()?;
                out.eval_scan(scan, table).await?
            } else {
                out.eval_full(id, &bootstrap_states).await?
            };
            bootstrap_states.push(Relation::new(batch)?);
            match &out.incremental_plan.programs()[id].state {
                Some(StateSpec::Rank {
                    input, operator, ..
                }) => {
                    out.ranks[id] = Some(RankState::new(
                        &out.ctx,
                        operator,
                        &bootstrap_states[*input].rows.materialize()?.batch,
                    )?);
                }
                Some(StateSpec::Accumulator {
                    input, operator, ..
                }) => {
                    let state = AccumulatorState::new(
                        &out.ctx,
                        operator,
                        &bootstrap_states[*input].rows.materialize()?.batch,
                    )?;
                    bootstrap_states[id] = Relation::new(state.contents()?)?;
                    out.accumulators[id] = Some(state);
                }
                Some(StateSpec::Group {
                    input, projection, ..
                }) => {
                    let input_batch = bootstrap_states[*input].rows.materialize()?.batch;
                    let input = &input_batch;
                    let keys =
                        executor::apply(&out.ctx, projection, std::slice::from_ref(input)).await?;
                    let index = GroupState::try_new(
                        input,
                        &keys,
                        &bootstrap_states[id].rows.materialize()?.batch,
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
                        .map(|i| Ok(bootstrap_states[*i].rows.materialize()?.batch))
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
                                std::slice::from_ref(
                                    &bootstrap_states[id].rows.materialize()?.batch,
                                ),
                            )
                            .await?,
                        )
                    } else {
                        None
                    };
                    let output_batch = bootstrap_states[id].rows.materialize()?.batch;
                    let output = output_keys.as_ref().map(|keys| (&output_batch, keys));
                    let mut index = KeyedState::new(&inputs, &projected, output)?;
                    index.initialize_matches(&out.ctx, spec)?;
                    out.keyed[id] = Some(index);
                }
                None => {}
            }
        }
        // Drop dead intermediate relation caches once operator states are initialized.
        out.states = bootstrap_states
            .into_iter()
            .enumerate()
            .map(|(id, rel)| retained_relations[id].then_some(rel))
            .collect();
        Ok(out)
    }

    /// Number of graph nodes that retain a maintained full relation cache at runtime.
    pub fn retained_relation_count(&self) -> usize {
        self.states.iter().filter(|rel| rel.is_some()).count()
    }

    /// Rebind matching dependency tables to session-shared `SourceState` handles.
    pub(crate) fn bind_shared_sources(&mut self, shared: &BTreeMap<String, SourceState>) {
        for (name, slot) in &mut self.tables {
            if let Some(shared_source) = shared.get(name) {
                *slot = shared_source.clone();
            }
        }
    }

    /// True when both views share the exact same `Arc<SourceSnapshot>` for `table`.
    pub(crate) fn source_state_ptr_eq(&self, other: &Self, table: &str) -> bool {
        match (self.tables.get(table), other.tables.get(table)) {
            (Some(a), Some(b)) => a.ptr_eq(b),
            _ => false,
        }
    }

    /// Current source epoch; bootstrap is epoch zero.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Maintained SQL result; ORDER BY order is preserved at the root.
    pub fn contents(&self) -> Result<RecordBatch> {
        self.states
            .last()
            .and_then(|rel| rel.as_ref())
            .ok_or_else(|| Error::Invalid("empty plan graph or missing root relation".into()))?
            .rows
            .materialize()
            .map(|m| m.batch)
    }
    /// Whether the root node retains a presentation ordering.
    pub(crate) fn is_ordered(&self) -> bool {
        self.nodes.last().is_some_and(|node| node.ordered)
    }
    /// Shared maintained root output blocks for block-level database persistence.
    pub(crate) fn output_blocks(&self) -> Result<RowBlocks> {
        Ok(self
            .states
            .last()
            .and_then(|rel| rel.as_ref())
            .ok_or_else(|| Error::Invalid("empty plan graph or missing root relation".into()))?
            .rows
            .clone())
    }

    /// Export this view's committed derived state for operator-state checkpointing.
    pub(crate) fn export_state(
        &self,
        schemas: &BTreeMap<String, datafusion::arrow::datatypes::SchemaRef>,
    ) -> Result<ViewStateSnapshot> {
        export_view_parts(
            self,
            self.epoch,
            schemas,
            &self.states,
            &self.accumulators,
            &self.keyed,
            &self.groups,
            &self.ranks,
        )
    }

    /// Compile the strict incremental plan against schemas only and return its fingerprint.
    pub(crate) async fn compile_fingerprint(
        sql: &str,
        schemas: &BTreeMap<String, datafusion::arrow::datatypes::SchemaRef>,
        optimizer: IncrementalOptimizer,
    ) -> Result<String> {
        let ctx = context();
        for (name, schema) in schemas {
            ctx.register_batch(name, RecordBatch::new_empty(Arc::clone(schema)))?;
        }
        let compilation = crate::planning::compile(&ctx, sql, &optimizer).await?;
        let (_, plan) = compilation.into_plan(optimizer.requires_incremental())?;
        plan.fingerprint(sql, schemas)
    }

    /// Restore a `SqlView` directly from persisted operator states without full SQL evaluation.
    pub(crate) async fn restore_from_state(
        sql: &str,
        source_bags: BTreeMap<String, Bag>,
        optimizer: IncrementalOptimizer,
        snapshot: ViewStateSnapshot,
    ) -> Result<Self> {
        let ctx = context();
        let schemas: BTreeMap<String, datafusion::arrow::datatypes::SchemaRef> = source_bags
            .iter()
            .map(|(name, bag)| (name.clone(), bag.schema()))
            .collect();
        for (name, schema) in &schemas {
            ctx.register_batch(name, RecordBatch::new_empty(Arc::clone(schema)))?;
        }
        let compilation = crate::planning::compile(&ctx, sql, &optimizer).await?;
        let (nodes, incremental_plan) = compilation.into_plan(optimizer.requires_incremental())?;
        let actual_fingerprint = incremental_plan.fingerprint(sql, &schemas)?;
        if actual_fingerprint != snapshot.plan_fingerprint {
            return Err(Error::Invalid(
                "persisted view state fingerprint differs from compiled plan".into(),
            ));
        }
        if snapshot.node_relations.len() != nodes.len() || snapshot.operators.len() != nodes.len() {
            return Err(Error::Invalid(
                "persisted view state node count differs from compiled plan".into(),
            ));
        }
        let node_schemas: Vec<datafusion::arrow::datatypes::SchemaRef> = nodes
            .iter()
            .map(|n| Arc::new(n.plan.schema().as_arrow().clone()))
            .collect();
        let retained_relations = incremental_plan.retained_relations();
        for (id, (&required, persisted)) in retained_relations
            .iter()
            .zip(&snapshot.node_relations)
            .enumerate()
        {
            if required && persisted.is_none() {
                return Err(Error::Invalid(format!(
                    "persisted view state missing required relation for node #{id}"
                )));
            }
        }
        let templates = incremental_plan
            .programs()
            .iter()
            .map(|program| ProgramTemplates::compile(&ctx, program))
            .collect::<Result<Vec<_>>>()?;
        let states: Vec<Option<Relation>> = snapshot
            .node_relations
            .into_iter()
            .enumerate()
            .map(|(id, rows)| {
                retained_relations[id]
                    .then_some(rows)
                    .flatten()
                    .map(|rows| Relation { rows })
            })
            .collect();
        let mut accumulators = vec![None; nodes.len()];
        let mut keyed = vec![None; nodes.len()];
        let mut groups = vec![None; nodes.len()];
        let mut ranks = vec![None; nodes.len()];
        for (id, op_snap) in snapshot.operators.into_iter().enumerate() {
            match (&incremental_plan.programs()[id].state, op_snap) {
                (
                    Some(StateSpec::Accumulator {
                        input, operator, ..
                    }),
                    Some(OperatorStateSnapshot::Accumulator(snap)),
                ) => {
                    accumulators[id] = Some(AccumulatorState::from_persisted(
                        &ctx,
                        operator,
                        Arc::clone(&node_schemas[*input]),
                        snap,
                    )?);
                }
                (Some(StateSpec::Keyed(spec)), Some(OperatorStateSnapshot::Keyed(snap))) => {
                    let input_schemas = spec
                        .inputs
                        .iter()
                        .map(|i| Arc::clone(&node_schemas[*i]))
                        .collect();
                    let output_schema = spec
                        .output_projection
                        .as_ref()
                        .map(|_| Arc::clone(&node_schemas[id]));
                    keyed[id] = Some(KeyedState::from_persisted(
                        &ctx,
                        spec,
                        input_schemas,
                        output_schema,
                        snap,
                    )?);
                }
                (
                    Some(StateSpec::Group { input, .. }),
                    Some(OperatorStateSnapshot::Group(snap)),
                ) => {
                    groups[id] = Some(GroupState::from_persisted(
                        Arc::clone(&node_schemas[*input]),
                        Arc::clone(&node_schemas[id]),
                        snap,
                    )?);
                }
                (
                    Some(StateSpec::Rank {
                        input, operator, ..
                    }),
                    Some(OperatorStateSnapshot::Rank(snap)),
                ) => {
                    ranks[id] = Some(RankState::from_persisted(
                        &ctx,
                        operator,
                        Arc::clone(&node_schemas[*input]),
                        snap,
                    )?);
                }
                (None, None) => {}
                _ => {
                    return Err(Error::Invalid(
                        "persisted operator state variant differs from compiled StateSpec".into(),
                    ));
                }
            }
        }
        let tables = source_bags
            .into_iter()
            .map(|(name, bag)| Ok((name, SourceState::new(bag)?)))
            .collect::<Result<_>>()?;
        Ok(Self {
            sql: sql.into(),
            owner: NEXT_OWNER.fetch_add(1, Ordering::Relaxed),
            epoch: snapshot.view_epoch,
            nodes,
            node_schemas,
            retained_relations,
            incremental_plan,
            templates,
            states,
            groups,
            keyed,
            accumulators,
            ranks,
            tables,
            ctx,
        })
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
                rank_work: None,
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
        self.prepare_with_shared_sources(changes, None).await
    }

    /// Prepare an atomic multi-table epoch while reusing pre-applied shared `SourceState`
    /// handles across sibling views in the same session.
    pub(crate) async fn prepare_with_shared_sources(
        &self,
        changes: &[TableChange],
        shared_next: Option<&BTreeMap<String, SharedSourceUpdate>>,
    ) -> Result<PreparedSqlRefresh> {
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
            if let Some(shared) = shared_next.and_then(|m| m.get(name)) {
                if shared.source.schema() != delta.schema() {
                    return Err(Error::Invalid(
                        "shared source schema differs from delta".into(),
                    ));
                }
                if let Some((rel_work, src_work)) = &shared.work {
                    stats.work.sources.absorb(rel_work)?;
                    stats.work.cache_work.source_state.absorb(src_work)?;
                }
                tables.insert(name.clone(), shared.source.clone());
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
        let mut ranks = self.ranks.clone();
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
            let mut rank_work = None;
            let schema = Arc::clone(&self.node_schemas[id]);
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
                    &self.templates[id],
                    Bindings {
                        deltas: &deltas,
                        before: &self.states,
                        after: &states,
                        source_deltas: &source_deltas,
                        source_after: &tables,
                        keyed: &self.keyed,
                        groups: &self.groups,
                        accumulators: &self.accumulators,
                        ranks: &self.ranks,
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
                if let Some(index) = result.rank {
                    ranks[id] = Some(index);
                }
                rank_work = result.rank_work;
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
                if self.retained_relations[id] {
                    states[id] = match next {
                        Some(next) => Some(next),
                        None if delta.is_empty() => self.states[id].clone(),
                        None => {
                            let existing = self.states[id].as_ref().ok_or_else(|| {
                                Error::Invalid(format!(
                                    "missing retained relation cache for node #{id}"
                                ))
                            })?;
                            let update = existing.rows.apply(&delta)?;
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
                            Some(Relation { rows: update.rows })
                        }
                    };
                }
                stats.nodes.push(NodeStats {
                    id,
                    operator: node.plan.display().to_string(),
                    strategy: selected_strategy,
                    rule: compiled.rule.clone(),
                    delta_rows: delta.len(),
                    group_work,
                    keyed_work,
                    accumulator_work,
                    rank_work,
                });
            }
            let changed_sequence = if node.ordered {
                let after = states[id]
                    .as_ref()
                    .ok_or_else(|| {
                        Error::Invalid(format!("missing retained ordered relation for node #{id}"))
                    })?
                    .rows
                    .materialize_recorded(
                        &mut stats.work.cache_work.complete_caches,
                        &mut stats.work.cache_work.demanded_leaves,
                    )?;
                let before = self.states[id]
                    .as_ref()
                    .ok_or_else(|| {
                        Error::Invalid(format!("missing retained ordered relation for node #{id}"))
                    })?
                    .rows
                    .materialize_recorded(
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
            .and_then(|rel| rel.as_ref())
            .ok_or_else(|| Error::Invalid("empty graph or missing root relation".into()))?
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
            ranks,
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
        self.ranks = candidate.ranks;
        Ok(())
    }
}
