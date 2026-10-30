//! DataFusion logical operators connected by typed, signed relation changes.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{
    Distinct, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, TableScan, Volatility,
};
use datafusion::physical_plan::collect;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};
use serde::Serialize;

use crate::bag::Bag;
use crate::group_state::{supported_key, GroupState, KeyPolicy};
use crate::keyed_state::{key_expressions, KeyedState};
use crate::planner::{
    DeltaExpr, IncrementalOptimizer, IncrementalPlan, KeyScope, Maintenance, RuleContext,
    RuleIdentity, SnapshotVersion, Strategy,
};
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

/// Work observed for one operator, separate from performance claims.
#[derive(Clone, Debug, Serialize)]
pub struct NodeStats {
    /// Stable postorder identity within this query definition.
    pub id: usize,
    /// Logical operator description.
    pub operator: String,
    /// Actual refresh path.
    pub strategy: Strategy,
    /// Rule whose compiled expression was executed for this operator.
    pub rule: RuleIdentity,
    /// Number of changed distinct output rows.
    pub delta_rows: usize,
    /// Indexed aggregate work, present only for changed-group maintenance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_work: Option<GroupWork>,
    /// Actual affected-bucket snapshot work, excluding full cache/source I/O.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyed_work: Option<KeyedWork>,
}

/// Actual input work at an indexed aggregate, independent of elapsed time.
#[derive(Clone, Debug, Serialize)]
pub struct GroupWork {
    /// Number of installed groups before this refresh.
    pub total_groups_before: usize,
    /// Number of installed groups after this refresh.
    pub total_groups_after: usize,
    /// Keys whose complete input multiset changed, including removed groups.
    pub touched_groups: usize,
    /// Expanded insertions plus retractions supplied by the child delta.
    pub changed_input_rows: usize,
    /// Surviving rows actually read by the local aggregate.
    pub aggregate_input_rows: usize,
    /// Rows produced by recomputing affected groups.
    pub aggregate_output_rows: usize,
}

/// Observed operator-local indexed work; not an end-to-end complexity claim.
#[derive(Clone, Debug, Serialize)]
pub struct KeyedWork {
    /// Closure declared by the selected expression.
    pub scope: KeyScope,
    /// Installed input bucket counts before this refresh, one per port.
    pub total_keys_before: Vec<usize>,
    /// Candidate input bucket counts, one per port.
    pub total_keys_after: Vec<usize>,
    /// Union of changed keys across the input ports.
    pub touched_keys: usize,
    /// Expanded insertions and retractions received from children.
    pub changed_input_rows: usize,
    /// Rows supplied per port to a before operator evaluation.
    pub snapshot_input_rows_before: Vec<usize>,
    /// Rows supplied per port to an after operator evaluation.
    pub snapshot_input_rows_after: Vec<usize>,
    /// Actual before operator calls; windows use cached output instead.
    pub snapshot_evaluations_before: usize,
    /// Actual after operator calls.
    pub snapshot_evaluations_after: usize,
    /// Committed window output rows read for the before snapshot.
    pub cached_before_output_rows: usize,
    /// Complete child scans used to find affected buckets (zero on this path).
    /// All buckets may still be affected; source and result I/O are excluded.
    pub full_input_scans: usize,
}

/// Actual dirty nodes and source epoch for a prepared refresh.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RefreshStats {
    /// Candidate source epoch.
    pub epoch: u64,
    /// Operators that were evaluated; unchanged ancestors are omitted.
    pub nodes: Vec<NodeStats>,
}

#[derive(Clone)]
struct Node {
    plan: LogicalPlan,
    inputs: Vec<usize>,
    ordered: bool,
}

#[derive(Clone)]
struct Relation {
    bag: Bag,
    // Preserve physical sequence for ordering-sensitive consumers. Native
    // delta operators otherwise materialize in a deterministic canonical order.
    batch: RecordBatch,
}
impl Relation {
    fn new(batch: RecordBatch) -> Result<Self> {
        Ok(Self {
            bag: Bag::from_batch(&batch)?,
            batch,
        })
    }
    fn from_bag(bag: Bag) -> Result<Self> {
        bag.validate_relation()?;
        let batch = bag.to_batch()?;
        Ok(Self { bag, batch })
    }
}

#[derive(Clone, Copy)]
struct DeltaBindings<'a> {
    deltas: &'a [Bag],
    after: &'a [Relation],
    source: Option<&'a Bag>,
    scoped: Option<&'a ScopedSnapshots>,
}

struct ScopedSnapshots {
    before: Option<Bag>,
    after: Option<Bag>,
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

fn cache_scan(schema: &DFSchemaRef, batch: RecordBatch, id: usize) -> Result<LogicalPlan> {
    let provider = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?);
    // The graph's columns retain their original qualifiers, including joins
    // whose fields come from multiple tables. Materialization changes storage,
    // not the expression's binding to a column index. Construct directly because
    // TableScanBuilder first assigns ONE qualifier to every field, rejecting
    // valid join schemas such as a.id, b.id before we can restore qualifiers.
    Ok(LogicalPlan::TableScan(TableScan {
        table_name: format!("__optd_cache_{id}").into(),
        source: provider_as_source(provider),
        projection: None,
        projected_schema: schema.clone(),
        filters: vec![],
        fetch: None,
        statistics_requests: Default::default(),
    }))
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
        let incremental_plan = IncrementalPlan::new(compiled)?;
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
            if matches!(
                out.incremental_plan.nodes()[id].maintenance,
                Maintenance::ChangedGroups
            ) {
                let child = out.nodes[id].inputs[0];
                let input = &out.states[child].batch;
                let keys = out.group_keys(id, input.clone()).await?;
                let index = GroupState::try_new(
                    input,
                    &keys,
                    &out.states[id].batch,
                    KeyPolicy {
                        binary_strings: true,
                    },
                )?
                .ok_or_else(|| Error::Rewrite {
                    node: id,
                    rule: out.incremental_plan.nodes()[id].rule.name.clone(),
                    phase: "verify",
                    source: Box::new(Error::Invalid(
                        "qualified grouped plan could not initialize its index".into(),
                    )),
                })?;
                out.groups[id] = Some(index);
            }
            if let Some(scope) = out.incremental_plan.nodes()[id].maintenance.key_scope() {
                let keys = key_expressions(&out.nodes[id].plan, scope)?
                    .ok_or_else(|| Error::Invalid("missing qualified scope keys".into()))?;
                let mut inputs = vec![];
                let mut projected = vec![];
                for (port, expressions) in keys.iter().enumerate() {
                    let child = out.nodes[id].inputs[port];
                    let batch = out.states[child].batch.clone();
                    projected.push(out.project_keys(child, batch.clone(), expressions).await?);
                    inputs.push(batch);
                }
                let output_keys = if scope == KeyScope::WindowPartitions {
                    Some(
                        out.project_keys(id, out.states[id].batch.clone(), &keys[0])
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
        self.incremental_plan.nodes()[id].maintenance.strategy()
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

    async fn group_keys(&self, id: usize, input: RecordBatch) -> Result<RecordBatch> {
        let LogicalPlan::Aggregate(aggregate) = &self.nodes[id].plan else {
            return Err(Error::Invalid(
                "group projection requires an aggregate".into(),
            ));
        };
        let child = self.nodes[id].inputs[0];
        let plan =
            LogicalPlanBuilder::from(cache_scan(self.nodes[child].plan.schema(), input, child)?)
                .project(aggregate.group_expr.clone())?
                .build()?;
        self.execute(plan).await
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
        let schema = Arc::new(plan.schema().as_arrow().clone());
        // The graph has already been optimized. Re-optimizing a local node can
        // push LIMIT across a materialization boundary or discard qualifiers.
        let physical = DefaultPhysicalPlanner::default()
            .create_physical_plan(&plan, &self.ctx.state())
            .await?;
        let batches = collect(physical, self.ctx.task_ctx()).await?;
        Ok(concat_batches(&schema, &batches)?)
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

    async fn eval_inputs(&self, id: usize, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
        let node = &self.nodes[id];
        let inputs = node
            .inputs
            .iter()
            .zip(batches)
            .map(|(i, b)| cache_scan(self.nodes[*i].plan.schema(), b, *i))
            .collect::<Result<Vec<_>>>()?;
        self.execute(node.plan.with_new_exprs(node.plan.expressions(), inputs)?)
            .await
            .map_err(|source| Error::Operator {
                id,
                plan: node.plan.display().to_string(),
                source: Box::new(source),
            })
    }

    async fn project_keys(
        &self,
        id: usize,
        batch: RecordBatch,
        expressions: &[Expr],
    ) -> Result<RecordBatch> {
        let scan = cache_scan(self.nodes[id].plan.schema(), batch, id)?;
        // Aliases avoid duplicate expression names for repeated/composite keys.
        let projection = expressions
            .iter()
            .enumerate()
            .map(|(i, e)| e.clone().alias(format!("__key_{i}")))
            .collect::<Vec<_>>();
        self.execute(
            LogicalPlanBuilder::from(scan)
                .project(projection)?
                .build()?,
        )
        .await
    }

    async fn eval_scoped_delta(
        &self,
        id: usize,
        expr: &DeltaExpr,
        deltas: &[Bag],
        states: &[Relation],
    ) -> Result<(Bag, KeyedState, KeyedWork)> {
        let node = &self.nodes[id];
        let scope = expr
            .key_scope()
            .ok_or_else(|| Error::Invalid("missing scope declaration".into()))?;
        let keys = key_expressions(&node.plan, scope)?
            .ok_or_else(|| Error::Invalid("missing qualified keys".into()))?;
        let index = self.keyed[id]
            .as_ref()
            .ok_or_else(|| Error::Invalid("missing compiled key index".into()))?;
        let mut positives = vec![];
        let mut negatives = vec![];
        let mut positive_keys = vec![];
        let mut negative_keys = vec![];
        let mut changed_input_rows = 0usize;
        for (port, child) in node.inputs.iter().enumerate() {
            let (pos, neg) = deltas[*child].signed_parts()?;
            changed_input_rows = changed_input_rows
                .checked_add(pos.num_rows())
                .and_then(|n| n.checked_add(neg.num_rows()))
                .ok_or(Error::Overflow)?;
            positive_keys.push(self.project_keys(*child, pos.clone(), &keys[port]).await?);
            negative_keys.push(self.project_keys(*child, neg.clone(), &keys[port]).await?);
            positives.push(pos);
            negatives.push(neg);
        }
        let need_before = expr.reads_scoped(SnapshotVersion::Before);
        let need_after = expr.reads_scoped(SnapshotVersion::After);
        let mut update = index.prepare(
            &positives,
            &positive_keys,
            &negatives,
            &negative_keys,
            need_before,
            need_after,
        )?;
        let mut work = KeyedWork {
            scope,
            total_keys_before: index.totals(),
            total_keys_after: update.state.totals(),
            touched_keys: update.touched(),
            changed_input_rows,
            snapshot_input_rows_before: vec![0; node.inputs.len()],
            snapshot_input_rows_after: vec![0; node.inputs.len()],
            snapshot_evaluations_before: 0,
            snapshot_evaluations_after: 0,
            cached_before_output_rows: 0,
            full_input_scans: 0,
        };
        let mut snapshots = ScopedSnapshots {
            before: None,
            after: None,
        };
        for version in [SnapshotVersion::Before, SnapshotVersion::After] {
            if !expr.reads_scoped(version) {
                continue;
            }
            let value = if update.touched() == 0 {
                Bag::empty(self.states[id].bag.schema())
            } else if version == SnapshotVersion::Before && scope == KeyScope::WindowPartitions {
                let cached = update
                    .cached_before()
                    .ok_or_else(|| Error::Invalid("missing committed window output".into()))?;
                work.cached_before_output_rows = cached.num_rows();
                Bag::from_batch(cached)?
            } else {
                let inputs = update.inputs(version);
                let rows = inputs.iter().map(RecordBatch::num_rows).collect();
                match version {
                    SnapshotVersion::Before => {
                        work.snapshot_input_rows_before = rows;
                        work.snapshot_evaluations_before += 1;
                    }
                    SnapshotVersion::After => {
                        work.snapshot_input_rows_after = rows;
                        work.snapshot_evaluations_after += 1;
                    }
                }
                Bag::from_batch(&self.eval_inputs(id, inputs.to_vec()).await?)?
            };
            match version {
                SnapshotVersion::Before => snapshots.before = Some(value),
                SnapshotVersion::After => snapshots.after = Some(value),
            }
        }
        let delta = self
            .eval_delta_expr(
                id,
                expr,
                DeltaBindings {
                    deltas,
                    after: states,
                    source: None,
                    scoped: Some(&snapshots),
                },
                self.states[id].bag.schema(),
            )
            .await?;
        if scope == KeyScope::WindowPartitions {
            let (pos, neg) = delta.signed_parts()?;
            let pos_keys = self.project_keys(id, pos.clone(), &keys[0]).await?;
            let neg_keys = self.project_keys(id, neg.clone(), &keys[0]).await?;
            update.apply_window_delta(&pos, &pos_keys, &neg, &neg_keys)?;
        }
        Ok((delta, update.state, work))
    }

    // Expressions are verified and frozen before bootstrap. Boxing only the
    // bounded expression recursion avoids adding another async dependency.
    fn eval_delta_expr<'a>(
        &'a self,
        id: usize,
        expr: &'a DeltaExpr,
        bindings: DeltaBindings<'a>,
        expected_schema: SchemaRef,
    ) -> Pin<Box<dyn Future<Output = Result<Bag>> + Send + 'a>> {
        Box::pin(async move {
            let node = &self.nodes[id];
            match expr {
                DeltaExpr::ScopedSnapshot { version, .. } => {
                    let snapshots = bindings
                        .scoped
                        .ok_or_else(|| Error::Invalid("missing scoped snapshot binding".into()))?;
                    match version {
                        SnapshotVersion::Before => &snapshots.before,
                        SnapshotVersion::After => &snapshots.after,
                    }
                    .clone()
                    .ok_or_else(|| {
                        Error::Invalid("missing requested scoped snapshot version".into())
                    })
                }
                DeltaExpr::InputDelta(port) => Ok(bindings.deltas[node.inputs[*port]].clone()),
                DeltaExpr::InputSnapshot { input, version } => {
                    let states = match version {
                        SnapshotVersion::Before => &self.states,
                        SnapshotVersion::After => bindings.after,
                    };
                    Ok(states[node.inputs[*input]].bag.clone())
                }
                DeltaExpr::SourceDelta => bindings.source.cloned().ok_or_else(|| {
                    Error::Invalid("compiled source expression has no source binding".into())
                }),
                DeltaExpr::Empty => Ok(Bag::empty(expected_schema)),
                DeltaExpr::Sum(terms) => {
                    let mut result = Bag::empty(expected_schema.clone());
                    for term in terms {
                        let value = self
                            .eval_delta_expr(id, term, bindings, expected_schema.clone())
                            .await?;
                        result = result.add(&value)?;
                    }
                    Ok(result)
                }
                DeltaExpr::Negate(inner) => {
                    let value = self
                        .eval_delta_expr(id, inner, bindings, expected_schema.clone())
                        .await?;
                    Bag::empty(expected_schema).difference(&value)
                }
                DeltaExpr::Linear(inputs) => {
                    let schemas = match &node.plan {
                        LogicalPlan::TableScan(scan) => vec![scan.source.schema()],
                        _ => node
                            .inputs
                            .iter()
                            .map(|input| self.states[*input].bag.schema())
                            .collect(),
                    };
                    let mut positive = Vec::with_capacity(inputs.len());
                    let mut negative = Vec::with_capacity(inputs.len());
                    for (input, schema) in inputs.iter().zip(schemas) {
                        let value = self.eval_delta_expr(id, input, bindings, schema).await?;
                        let (pos, neg) = value.signed_parts()?;
                        positive.push(pos);
                        negative.push(neg);
                    }
                    let (pos, neg) = if let LogicalPlan::TableScan(scan) = &node.plan {
                        // The verifier establishes the scan's one synthetic raw-source port.
                        let pos = positive.pop().ok_or_else(|| {
                            Error::Invalid("compiled scan is missing its raw-source port".into())
                        })?;
                        let neg = negative.pop().ok_or_else(|| {
                            Error::Invalid("compiled scan is missing its raw-source port".into())
                        })?;
                        (
                            self.eval_scan(scan, pos).await?,
                            self.eval_scan(scan, neg).await?,
                        )
                    } else {
                        (
                            self.eval_inputs(id, positive).await?,
                            self.eval_inputs(id, negative).await?,
                        )
                    };
                    Bag::from_parts(&pos, &neg)
                }
                DeltaExpr::Join { left, right } => {
                    // The verifier requires an inner join and its two typed input ports.
                    let l = self
                        .eval_delta_expr(
                            id,
                            left,
                            bindings,
                            self.states[node.inputs[0]].bag.schema(),
                        )
                        .await?;
                    let r = self
                        .eval_delta_expr(
                            id,
                            right,
                            bindings,
                            self.states[node.inputs[1]].bag.schema(),
                        )
                        .await?;
                    self.eval_signed_join(id, &l, &r).await
                }
            }
        })
    }

    async fn eval_signed_join(&self, id: usize, left: &Bag, right: &Bag) -> Result<Bag> {
        let (lp, ln) = left.signed_parts()?;
        let (rp, rn) = right.signed_parts()?;
        let mut result = Bag::empty(self.states[id].bag.schema());
        // Bilinear lifting includes negative x negative with a positive sign.
        // Empty inner-join branches have no output and need no physical execution.
        for (l, r, negative) in [
            (&lp, &rp, false),
            (&lp, &rn, true),
            (&ln, &rp, true),
            (&ln, &rn, false),
        ] {
            if l.num_rows() == 0 || r.num_rows() == 0 {
                continue;
            }
            let batch = self.eval_inputs(id, vec![l.clone(), r.clone()]).await?;
            let value = Bag::from_batch(&batch)?;
            result = if negative {
                result.difference(&value)?
            } else {
                result.add(&value)?
            };
        }
        Ok(result)
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
            let selected_strategy = compiled.maintenance.strategy();
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
                match &compiled.maintenance {
                    Maintenance::Delta(expr) if compiled.maintenance.key_scope().is_some() => {
                        let (delta, index, work) =
                            self.eval_scoped_delta(id, expr, &deltas, &states).await?;
                        keyed[id] = Some(index);
                        keyed_work = Some(work);
                        (delta, None)
                    }
                    Maintenance::Delta(expr) => {
                        let delta = self
                            .eval_delta_expr(
                                id,
                                expr,
                                DeltaBindings {
                                    deltas: &deltas,
                                    after: &states,
                                    source: source_delta.as_ref(),
                                    scoped: None,
                                },
                                schema,
                            )
                            .await?;
                        (delta, None)
                    }
                    Maintenance::ChangedGroups => {
                        let index = self.groups[id].as_ref().ok_or_else(|| {
                            Error::Invalid("compiled grouped plan has no group index".into())
                        })?;
                        let child = node.inputs[0];
                        let (positive, negative) = deltas[child].signed_parts()?;
                        let positive_keys = self.group_keys(id, positive.clone()).await?;
                        let negative_keys = self.group_keys(id, negative.clone()).await?;
                        let update =
                            index.prepare(&positive, &positive_keys, &negative, &negative_keys)?;
                        let touched_groups = update.touched_groups();
                        let aggregate_input_rows = update.aggregate_input_rows();
                        let output = self.eval_inputs(id, vec![update.input_after()?]).await?;
                        let aggregate_output_rows = output.num_rows();
                        let (next_index, delta) = update.finish(&output)?;
                        group_work = Some(GroupWork {
                            total_groups_before: index.total_groups(),
                            total_groups_after: next_index.total_groups(),
                            touched_groups,
                            changed_input_rows: positive
                                .num_rows()
                                .checked_add(negative.num_rows())
                                .ok_or(Error::Overflow)?,
                            aggregate_input_rows,
                            aggregate_output_rows,
                        });
                        groups[id] = Some(next_index);
                        (delta, None)
                    }
                    Maintenance::Recompute => {
                        let batch = if let LogicalPlan::TableScan(scan) = &node.plan {
                            self.eval_scan(scan, self.source_for(scan, &tables)?.to_batch()?)
                                .await?
                        } else {
                            self.eval_full(id, &states).await?
                        };
                        let next = Relation::new(batch)?;
                        (next.bag.difference(&self.states[id].bag)?, Some(next))
                    }
                    Maintenance::Constant => (Bag::empty(schema), None),
                }
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
