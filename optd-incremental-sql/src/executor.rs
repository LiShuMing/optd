//! Physical incremental operations. No rule selection or original SQL graph access.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchemaRef, TableReference};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{LogicalPlan, TableScan};
use datafusion::physical_plan::collect;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::accumulator_state::AccumulatorState;
use crate::bag::Bag;
use crate::group_state::{GroupState, GroupUpdate};
use crate::keyed_state::{KeyedState, KeyedUpdate};
use crate::metrics::{AccumulatorWork, GroupWork, KeyedWork};
use crate::planner::plan::{ApplyMode, Operation, PhysicalStep, RelationOperator};
use crate::planner::SnapshotVersion;
use crate::{Error, Result};

pub(crate) async fn execute(ctx: &SessionContext, plan: LogicalPlan) -> Result<RecordBatch> {
    let schema = Arc::new(plan.schema().as_arrow().clone());
    // These operators have already been planned across materialization boundaries.
    let physical = DefaultPhysicalPlanner::default()
        .create_physical_plan(&plan, &ctx.state())
        .await?;
    Ok(concat_batches(
        &schema,
        &collect(physical, ctx.task_ctx()).await?,
    )?)
}

pub(crate) async fn apply(
    ctx: &SessionContext,
    operator: &RelationOperator,
    batches: &[RecordBatch],
) -> Result<RecordBatch> {
    if batches.len() != operator.inputs.len() {
        return Err(Error::Invalid(
            "physical operator input arity differs".into(),
        ));
    }
    for (batch, schema) in batches.iter().zip(&operator.inputs) {
        if batch.schema().as_ref() != schema.as_arrow() {
            return Err(Error::Invalid(
                "physical operator input schema differs".into(),
            ));
        }
    }
    let mut port = 0;
    let plan = operator
        .template
        .as_ref()
        .clone()
        .transform_up(|plan| {
            if matches!(plan, LogicalPlan::EmptyRelation(_)) {
                let batch = batches.get(port).ok_or_else(|| {
                    datafusion::common::DataFusionError::Plan(
                        "unbound physical operator port".into(),
                    )
                })?;
                let input = cache_scan(&operator.inputs[port], batch.clone(), port)
                    .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))?;
                port += 1;
                return Ok(Transformed::yes(input));
            }
            Ok(Transformed::no(plan))
        })?
        .data;
    if port != batches.len() {
        return Err(Error::Invalid("unused physical operator input".into()));
    }
    execute(ctx, plan).await
}

/// Immutable versioned bindings supplied by refresh orchestration.
pub(crate) struct Bindings<'a> {
    pub(crate) deltas: &'a [Bag],
    pub(crate) before: &'a [Relation],
    pub(crate) after: &'a [Relation],
    pub(crate) source_deltas: &'a BTreeMap<String, Bag>,
    pub(crate) source_after: &'a BTreeMap<String, Bag>,
    pub(crate) keyed: &'a [Option<KeyedState>],
    pub(crate) groups: &'a [Option<GroupState>],
    pub(crate) accumulators: &'a [Option<AccumulatorState>],
}

pub(crate) struct ExecutionResult {
    pub(crate) delta: Bag,
    pub(crate) replacement: Option<RecordBatch>,
    pub(crate) keyed: Option<KeyedState>,
    pub(crate) groups: Option<GroupState>,
    pub(crate) keyed_work: Option<KeyedWork>,
    pub(crate) group_work: Option<GroupWork>,
    pub(crate) accumulator: Option<AccumulatorState>,
    pub(crate) accumulator_work: Option<AccumulatorWork>,
}
struct ProjectedChange {
    positive: RecordBatch,
    negative: RecordBatch,
    positive_keys: RecordBatch,
    negative_keys: RecordBatch,
}
struct RelationValue {
    bag: Bag,
    batch: Option<RecordBatch>,
}
impl RelationValue {
    fn new(batch: RecordBatch) -> Result<Self> {
        Ok(Self {
            bag: Bag::from_batch(&batch)?,
            batch: Some(batch),
        })
    }
    fn from_signed(bag: Bag) -> Result<Self> {
        Ok(Self { bag, batch: None })
    }
    fn batch(&self) -> Result<RecordBatch> {
        self.batch
            .clone()
            .map(Ok)
            .unwrap_or_else(|| self.bag.to_batch())
    }
}
impl Clone for RelationValue {
    fn clone(&self) -> Self {
        Self {
            bag: self.bag.clone(),
            batch: self.batch.clone(),
        }
    }
}
enum Value {
    Relation(RelationValue),
    Projected(ProjectedChange),
    Index(usize),
    Groups(usize),
}
impl Value {
    fn relation(&self) -> Result<&RelationValue> {
        match self {
            Self::Relation(r) => Ok(r),
            _ => Err(Error::Invalid("physical input is not a relation".into())),
        }
    }
    fn projected(&self) -> Result<&ProjectedChange> {
        match self {
            Self::Projected(p) => Ok(p),
            _ => Err(Error::Invalid(
                "physical input is not a projected change".into(),
            )),
        }
    }
}
fn source<'a>(tables: &'a BTreeMap<String, Bag>, name: &str) -> Result<&'a Bag> {
    let reference = TableReference::from(name);
    tables
        .get(name)
        .or_else(|| tables.get(reference.table()))
        .ok_or_else(|| Error::Invalid(format!("unbound physical source {name}")))
}
async fn project_change(
    ctx: &SessionContext,
    projection: &RelationOperator,
    bag: &Bag,
) -> Result<ProjectedChange> {
    let (positive, negative) = bag.signed_parts()?;
    let positive_keys = apply(ctx, projection, std::slice::from_ref(&positive)).await?;
    let negative_keys = apply(ctx, projection, std::slice::from_ref(&negative)).await?;
    Ok(ProjectedChange {
        positive,
        negative,
        positive_keys,
        negative_keys,
    })
}
async fn signed_apply(
    ctx: &SessionContext,
    operator: &RelationOperator,
    mode: ApplyMode,
    inputs: &[&RelationValue],
) -> Result<RelationValue> {
    if mode == ApplyMode::Positive {
        return RelationValue::new(
            apply(
                ctx,
                operator,
                &inputs
                    .iter()
                    .map(|r| r.batch())
                    .collect::<Result<Vec<_>>>()?,
            )
            .await?,
        );
    }
    let parts = inputs
        .iter()
        .map(|r| r.bag.signed_parts())
        .collect::<Result<Vec<_>>>()?;
    if mode == ApplyMode::SignedLinear {
        let positive = parts.iter().map(|p| p.0.clone()).collect::<Vec<_>>();
        let negative = parts.iter().map(|p| p.1.clone()).collect::<Vec<_>>();
        return RelationValue::from_signed(Bag::from_parts(
            &apply(ctx, operator, &positive).await?,
            &apply(ctx, operator, &negative).await?,
        )?);
    }
    let mut result = Bag::empty(operator.schema());
    let (lp, ln) = &parts[0];
    let (rp, rn) = &parts[1];
    for (left, right, negative) in [
        (lp, rp, false),
        (lp, rn, true),
        (ln, rp, true),
        (ln, rn, false),
    ] {
        if left.num_rows() == 0 || right.num_rows() == 0 {
            continue;
        }
        let value = Bag::from_batch(&apply(ctx, operator, &[left.clone(), right.clone()]).await?)?;
        result = if negative {
            result.difference(&value)?
        } else {
            result.add(&value)?
        };
    }
    RelationValue::from_signed(result)
}

/// Execute only the selected physical program; candidate state is returned privately.
pub(crate) async fn run(
    ctx: &SessionContext,
    steps: &[PhysicalStep],
    bindings: Bindings<'_>,
) -> Result<ExecutionResult> {
    let mut values: Vec<Value> = Vec::with_capacity(steps.len());
    let mut indexes: BTreeMap<usize, KeyedUpdate> = BTreeMap::new();
    let mut group_updates: BTreeMap<usize, GroupUpdate> = BTreeMap::new();
    let mut keyed_work: Option<KeyedWork> = None;
    let mut group_work: Option<GroupWork> = None;
    let mut next_keyed = None;
    let mut next_groups = None;
    let mut next_accumulator = None;
    let mut accumulator_work = None;
    let mut replacement = None;
    for step in steps {
        let inputs = step.inputs.iter().map(|i| &values[*i]).collect::<Vec<_>>();
        let value = match &step.operation {
            Operation::UpdateAccumulator { state, .. } => {
                let index = bindings.accumulators[*state]
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("missing aggregate accumulator state".into()))?;
                let (positive, negative) = inputs[0].relation()?.bag.signed_parts()?;
                let (next, delta, work) = index.update(&positive, &negative)?;
                next_accumulator = Some(next);
                accumulator_work = Some(work);
                Value::Relation(RelationValue::from_signed(delta)?)
            }
            Operation::DeltaRead { input, .. } => {
                Value::Relation(RelationValue::from_signed(bindings.deltas[*input].clone())?)
            }
            Operation::SnapshotRead { input, version, .. } => {
                Value::Relation(RelationValue::new(match version {
                    SnapshotVersion::Before => bindings.before[*input].batch.clone(),
                    SnapshotVersion::After => bindings.after[*input].batch.clone(),
                })?)
            }
            Operation::SourceRead { name, delta, .. } => {
                let after = source(bindings.source_after, name)?;
                Value::Relation(RelationValue::from_signed(if *delta {
                    source(bindings.source_deltas, name)?.clone()
                } else {
                    after.clone()
                })?)
            }
            Operation::Empty(schema) => {
                Value::Relation(RelationValue::from_signed(Bag::empty(schema.clone()))?)
            }
            Operation::Sum => {
                let mut bag = Bag::empty(inputs[0].relation()?.bag.schema());
                for input in &inputs {
                    bag = bag.add(&input.relation()?.bag)?;
                }
                Value::Relation(RelationValue::from_signed(bag)?)
            }
            Operation::Negate => {
                let bag = &inputs[0].relation()?.bag;
                Value::Relation(RelationValue::from_signed(
                    Bag::empty(bag.schema()).difference(bag)?,
                )?)
            }
            Operation::Apply { operator, mode } => Value::Relation(
                signed_apply(
                    ctx,
                    operator,
                    *mode,
                    &inputs
                        .iter()
                        .map(|v| v.relation())
                        .collect::<Result<Vec<_>>>()?,
                )
                .await?,
            ),
            Operation::ProjectKeys(projection) => {
                Value::Projected(project_change(ctx, projection, &inputs[0].relation()?.bag).await?)
            }
            Operation::PrepareIndex(spec) => {
                let changes = inputs
                    .iter()
                    .map(|v| v.projected())
                    .collect::<Result<Vec<_>>>()?;
                let index = bindings.keyed[spec.id]
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("missing physical index".into()))?;
                let update = index.prepare(
                    &changes
                        .iter()
                        .map(|p| p.positive.clone())
                        .collect::<Vec<_>>(),
                    &changes
                        .iter()
                        .map(|p| p.positive_keys.clone())
                        .collect::<Vec<_>>(),
                    &changes
                        .iter()
                        .map(|p| p.negative.clone())
                        .collect::<Vec<_>>(),
                    &changes
                        .iter()
                        .map(|p| p.negative_keys.clone())
                        .collect::<Vec<_>>(),
                    spec.before,
                    spec.after,
                )?;
                let changed_input_rows = changes.iter().try_fold(0usize, |n, p| {
                    n.checked_add(p.positive.num_rows())
                        .and_then(|n| n.checked_add(p.negative.num_rows()))
                        .ok_or(Error::Overflow)
                })?;
                keyed_work = Some(KeyedWork {
                    scope: spec.scope,
                    total_keys_before: index.totals(),
                    total_keys_after: update.state.totals(),
                    touched_keys: update.touched(),
                    changed_input_rows,
                    snapshot_input_rows_before: vec![0; spec.inputs.len()],
                    snapshot_input_rows_after: vec![0; spec.inputs.len()],
                    snapshot_evaluations_before: 0,
                    snapshot_evaluations_after: 0,
                    cached_before_output_rows: 0,
                    full_input_scans: 0,
                });
                indexes.insert(spec.id, update);
                Value::Index(spec.id)
            }
            Operation::IndexLookup {
                state,
                port,
                version,
                ..
            } => {
                if !matches!(inputs[0],Value::Index(id) if id == state) {
                    return Err(Error::Invalid("physical index token differs".into()));
                }
                let update = indexes
                    .get(state)
                    .ok_or_else(|| Error::Invalid("missing prepared index".into()))?;
                let batch = update.lookup_input(*port, *version)?;
                if let Some(work) = &mut keyed_work {
                    let rows = match version {
                        SnapshotVersion::Before => &mut work.snapshot_input_rows_before,
                        SnapshotVersion::After => &mut work.snapshot_input_rows_after,
                    };
                    rows[*port] = rows[*port]
                        .checked_add(batch.num_rows())
                        .ok_or(Error::Overflow)?;
                }
                Value::Relation(RelationValue::new(batch)?)
            }
            Operation::CachedOutput { state, schema } => {
                let update = indexes
                    .get(state)
                    .ok_or_else(|| Error::Invalid("missing prepared output index".into()))?;
                let batch = if update.touched() == 0 {
                    RecordBatch::new_empty(schema.clone())
                } else {
                    update.lookup_output()?
                };
                if let Some(work) = &mut keyed_work {
                    work.cached_before_output_rows = batch.num_rows();
                }
                Value::Relation(RelationValue::new(batch)?)
            }
            Operation::EvaluateSnapshot {
                state,
                version,
                operator,
            } => {
                let update = indexes
                    .get(state)
                    .ok_or_else(|| Error::Invalid("missing snapshot index".into()))?;
                if update.touched() == 0 {
                    Value::Relation(RelationValue::from_signed(Bag::empty(operator.schema()))?)
                } else {
                    let relations = inputs
                        .iter()
                        .map(|v| v.relation())
                        .collect::<Result<Vec<_>>>()?;
                    if let Some(work) = &mut keyed_work {
                        match version {
                            SnapshotVersion::Before => {
                                work.snapshot_evaluations_before += 1;
                            }
                            SnapshotVersion::After => {
                                work.snapshot_evaluations_after += 1;
                            }
                        }
                    }
                    Value::Relation(
                        signed_apply(ctx, operator, ApplyMode::Positive, &relations).await?,
                    )
                }
            }
            Operation::CompleteIndex(spec) => {
                let mut update = indexes
                    .remove(&spec.id)
                    .ok_or_else(|| Error::Invalid("missing completed index".into()))?;
                let output = inputs[1].relation()?;
                if let Some(projection) = &spec.output_projection {
                    let projected = project_change(ctx, projection, &output.bag).await?;
                    update.apply_window_delta(
                        &projected.positive,
                        &projected.positive_keys,
                        &projected.negative,
                        &projected.negative_keys,
                    )?;
                }
                next_keyed = Some(update.state);
                Value::Relation(output.clone())
            }
            Operation::PrepareGroups { state } => {
                let p = inputs[0].projected()?;
                let index = bindings.groups[*state]
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("missing group index".into()))?;
                let update =
                    index.prepare(&p.positive, &p.positive_keys, &p.negative, &p.negative_keys)?;
                group_work = Some(GroupWork {
                    total_groups_before: index.total_groups(),
                    total_groups_after: 0,
                    touched_groups: update.touched_groups(),
                    changed_input_rows: p
                        .positive
                        .num_rows()
                        .checked_add(p.negative.num_rows())
                        .ok_or(Error::Overflow)?,
                    aggregate_input_rows: update.aggregate_input_rows(),
                    aggregate_output_rows: 0,
                });
                group_updates.insert(*state, update);
                Value::Groups(*state)
            }
            Operation::GroupLookup { state, .. } => {
                if !matches!(inputs[0],Value::Groups(id) if id == state) {
                    return Err(Error::Invalid("physical group token differs".into()));
                }
                let update = group_updates
                    .get(state)
                    .ok_or_else(|| Error::Invalid("missing prepared groups".into()))?;
                Value::Relation(RelationValue::new(update.input_after()?.clone())?)
            }
            Operation::CompleteGroups { state } => {
                let update = group_updates
                    .remove(state)
                    .ok_or_else(|| Error::Invalid("missing completed groups".into()))?;
                let output = inputs[1].relation()?.batch()?;
                let (index, delta) = update.finish(&output)?;
                if let Some(work) = &mut group_work {
                    work.total_groups_after = index.total_groups();
                    work.aggregate_output_rows = output.num_rows();
                }
                next_groups = Some(index);
                Value::Relation(RelationValue::from_signed(delta)?)
            }
            Operation::Replace => {
                let after = inputs[0].relation()?;
                let before = inputs[1].relation()?;
                replacement = Some(after.batch()?);
                Value::Relation(RelationValue::from_signed(
                    after.bag.difference(&before.bag)?,
                )?)
            }
        };
        values.push(value);
    }
    let output = values
        .pop()
        .ok_or_else(|| Error::Invalid("empty physical program".into()))?;
    let Value::Relation(output) = output else {
        return Err(Error::Invalid("physical root is not a relation".into()));
    };
    Ok(ExecutionResult {
        delta: output.bag,
        replacement,
        keyed: next_keyed,
        groups: next_groups,
        keyed_work,
        group_work,
        accumulator: next_accumulator,
        accumulator_work,
    })
}

#[derive(Clone)]
pub(crate) struct Relation {
    pub(crate) bag: Bag,
    // Preserve physical sequence for ordering-sensitive consumers. Native
    // delta operators otherwise materialize in a deterministic canonical order.
    pub(crate) batch: RecordBatch,
}
impl Relation {
    pub(crate) fn new(batch: RecordBatch) -> Result<Self> {
        Ok(Self {
            bag: Bag::from_batch(&batch)?,
            batch,
        })
    }
    pub(crate) fn from_bag(bag: Bag) -> Result<Self> {
        bag.validate_relation()?;
        let batch = bag.to_batch()?;
        Ok(Self { bag, batch })
    }
}

pub(crate) fn cache_scan(
    schema: &DFSchemaRef,
    batch: RecordBatch,
    id: usize,
) -> Result<LogicalPlan> {
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
