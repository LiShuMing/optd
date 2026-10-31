//! Retractable builtin aggregate state. No historical group inputs are retained.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;

use crate::bag::Bag;
use crate::group_state::{encode_keys, supported_key, KeyPolicy};
use crate::metrics::AccumulatorWork;
use crate::planner::plan::RelationOperator;
use crate::{Error, Result};

type Key = Vec<u8>;
type PartitionRows = BTreeMap<Key, (Vec<ScalarValue>, Vec<u32>)>;

fn unalias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    expr
}

/// Admit only exact arithmetic and known retractable builtin implementations.
pub(crate) fn eligible(ctx: &SessionContext, plan: &LogicalPlan) -> Result<bool> {
    let LogicalPlan::Aggregate(agg) = plan else {
        return Ok(false);
    };
    if agg.aggr_expr.is_empty() {
        return Ok(false);
    }
    for key in &agg.group_expr {
        if matches!(key, Expr::GroupingSet(_))
            || !supported_key(
                &key.get_type(agg.input.schema())?,
                KeyPolicy {
                    binary_strings: true,
                },
            )
        {
            return Ok(false);
        }
    }
    let session = ctx.state();
    for expr in &agg.aggr_expr {
        let Expr::AggregateFunction(f) = unalias(expr) else {
            return Ok(false);
        };
        if !matches!(f.func.name(), "count" | "sum" | "avg")
            || session.aggregate_functions().get(f.func.name()) != Some(&f.func)
            || f.params.distinct
            || f.params.filter.is_some()
            || !f.params.order_by.is_empty()
            || f.params.null_treatment.is_some()
            || f.params.args.is_empty()
        {
            return Ok(false);
        }
        if f.func.name() != "count" {
            for arg in &f.params.args {
                if !matches!(
                    arg.get_type(agg.input.schema())?,
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Decimal128(_, _)
                        | DataType::Decimal256(_, _)
                ) {
                    return Ok(false);
                }
            }
            // Floating averages can change rounding when split into refresh batches.
            if f.func.name() == "avg"
                && !matches!(
                    expr.get_type(agg.input.schema())?,
                    DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
                )
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// One group's cardinality, mergeable accumulator states and last emitted row.
/// Cardinality controls group removal; it differs from COUNT over nullable values.
#[derive(Clone, Debug)]
struct Entry {
    rows: i64,
    keys: Vec<ScalarValue>,
    states: Vec<Vec<ScalarValue>>,
    output: Vec<ScalarValue>,
}

/// Exact retractable aggregates, retaining states rather than historical input rows.
///
/// Candidates share untouched entries through `Arc`; cloning still copies the
/// complete map directory. Only changed groups rebuild accumulator values.
#[derive(Clone, Debug)]
pub(crate) struct AccumulatorState {
    schema: SchemaRef,
    keys: Vec<Arc<dyn PhysicalExpr>>,
    functions: Vec<AggregateFunctionExpr>,
    entries: BTreeMap<Key, Arc<Entry>>,
}

impl AccumulatorState {
    /// Bootstrap a planner-qualified aggregate from its complete positive input.
    /// The planner must apply `eligible` first; this is not a general aggregate
    /// interpreter. A global aggregate retains its output even for empty input.
    pub(crate) fn new(
        ctx: &SessionContext,
        operator: &RelationOperator,
        input: &RecordBatch,
    ) -> Result<Self> {
        let LogicalPlan::Aggregate(agg) = operator.template.as_ref() else {
            return Err(Error::Invalid("accumulator requires aggregate".into()));
        };
        let session = ctx.state();
        let keys = agg
            .group_expr
            .iter()
            .map(|e| session.create_physical_expr(e.clone(), &operator.inputs[0]))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut functions = Vec::new();
        for (i, expr) in agg.aggr_expr.iter().enumerate() {
            let Expr::AggregateFunction(f) = unalias(expr) else {
                return Err(Error::Invalid("invalid accumulator expression".into()));
            };
            let args = f
                .params
                .args
                .iter()
                .map(|e| session.create_physical_expr(e.clone(), &operator.inputs[0]))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let fun = AggregateExprBuilder::new(f.func.clone(), args)
                .schema(input.schema())
                .alias(operator.schema().field(agg.group_expr.len() + i).name())
                .build()?;
            if !fun.create_sliding_accumulator()?.supports_retract_batch() {
                return Err(Error::Invalid("aggregate cannot retract".into()));
            }
            functions.push(fun);
        }
        let mut state = Self {
            schema: operator.schema(),
            keys,
            functions,
            entries: BTreeMap::new(),
        };
        if state.keys.is_empty() {
            let entry = state.empty_entry(Vec::new())?;
            state.entries.insert(Vec::new(), Arc::new(entry));
        }
        let empty = RecordBatch::new_empty(input.schema());
        let (state, _, _) = state.update(input, &empty)?;
        Ok(state)
    }

    fn empty_entry(&self, keys: Vec<ScalarValue>) -> Result<Entry> {
        let mut states = Vec::new();
        let mut output = keys.clone();
        for f in &self.functions {
            let mut acc = f.create_sliding_accumulator()?;
            output.push(acc.evaluate()?);
            states.push(acc.state()?);
        }
        Ok(Entry {
            rows: 0,
            keys,
            states,
            output,
        })
    }

    fn partition(&self, batch: &RecordBatch) -> Result<PartitionRows> {
        let arrays = self
            .keys
            .iter()
            .map(|e| e.evaluate(batch)?.into_array(batch.num_rows()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let encoded = if arrays.is_empty() {
            vec![Vec::new(); batch.num_rows()]
        } else {
            encode_keys(&RecordBatch::try_from_iter(
                arrays
                    .iter()
                    .enumerate()
                    .map(|(i, a)| (format!("k{i}"), a.clone())),
            )?)?
        };
        let mut groups = BTreeMap::new();
        for (i, key) in encoded.into_iter().enumerate() {
            let values = arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a.as_ref(), i))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let entry = groups.entry(key).or_insert_with(|| (values, Vec::new()));
            entry.1.push(u32::try_from(i).map_err(|_| Error::Overflow)?);
        }
        Ok(groups)
    }

    fn output(&self, entries: impl Iterator<Item = Vec<ScalarValue>>) -> Result<Bag> {
        let rows = entries.collect::<Vec<_>>();
        if rows.is_empty() {
            return Ok(Bag::empty(self.schema.clone()));
        }
        let columns = (0..self.schema.fields().len())
            .map(|i| ScalarValue::iter_to_array(rows.iter().map(|r| r[i].clone())))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Bag::from_batch(&RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    /// Return an isolated candidate, signed output difference and local work counts.
    ///
    /// Retract old contributions before adding new ones. Callers must supply valid
    /// full-row changes: without historical inputs, this state cannot establish
    /// that each retracted row existed. Any error leaves `self` unchanged.
    /// Grouped empty states disappear; the empty global aggregate remains present.
    pub(crate) fn update(
        &self,
        positive: &RecordBatch,
        negative: &RecordBatch,
    ) -> Result<(Self, Bag, AccumulatorWork)> {
        let pos = self.partition(positive)?;
        let mut neg = self.partition(negative)?;
        let mut changes = BTreeMap::new();
        for (key, (values, indices)) in pos {
            let minus = neg.remove(&key).map(|x| x.1).unwrap_or_default();
            changes.insert(key, (values, indices, minus));
        }
        for (key, (values, indices)) in neg {
            changes.insert(key, (values, Vec::new(), indices));
        }
        let mut next = self.clone();
        let mut old = Vec::new();
        let mut new = Vec::new();
        let arguments = |batch: &RecordBatch| -> Result<Vec<Vec<ArrayRef>>> {
            self.functions
                .iter()
                .map(|f| {
                    f.expressions()
                        .iter()
                        .map(|e| Ok(e.evaluate(batch)?.into_array(batch.num_rows())?))
                        .collect()
                })
                .collect()
        };
        let pa = arguments(positive)?;
        let na = arguments(negative)?;
        let touched = changes.len();
        for (key, (values, plus, minus)) in changes {
            let previous = self.entries.get(&key);
            if let Some(entry) = previous {
                old.push(entry.output.clone());
            }
            let mut entry = match previous {
                Some(entry) => entry.as_ref().clone(),
                None => self.empty_entry(values)?,
            };
            entry.rows = entry
                .rows
                .checked_sub(i64::try_from(minus.len()).map_err(|_| Error::Overflow)?)
                .and_then(|n| n.checked_add(i64::try_from(plus.len()).ok()?))
                .ok_or(Error::Overflow)?;
            if entry.rows < 0 {
                return Err(Error::Invalid(
                    "negative aggregate group cardinality".into(),
                ));
            }
            if entry.rows == 0 && !self.keys.is_empty() {
                next.entries.remove(&key);
                continue;
            }
            entry.output = entry.keys.clone();
            for (i, f) in self.functions.iter().enumerate() {
                let mut acc = f.create_sliding_accumulator()?;
                acc.merge_batch(
                    &entry.states[i]
                        .iter()
                        .map(|v| v.to_array_of_size(1))
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                )?;
                let select = |arrays: &[ArrayRef], indices: &[u32]| -> Result<Vec<ArrayRef>> {
                    let indices = UInt32Array::from(indices.to_vec());
                    Ok(arrays
                        .iter()
                        .map(|a| take(a.as_ref(), &indices, None))
                        .collect::<std::result::Result<Vec<_>, _>>()?)
                };
                if !minus.is_empty() {
                    acc.retract_batch(&select(&na[i], &minus)?)?;
                }
                if !plus.is_empty() {
                    acc.update_batch(&select(&pa[i], &plus)?)?;
                }
                entry.output.push(acc.evaluate()?);
                entry.states[i] = acc.state()?;
            }
            new.push(entry.output.clone());
            next.entries.insert(key, Arc::new(entry));
        }
        let delta = self
            .output(new.into_iter())?
            .difference(&self.output(old.into_iter())?)?;
        let work = AccumulatorWork {
            touched_groups: touched,
            delta_input_rows: positive
                .num_rows()
                .checked_add(negative.num_rows())
                .ok_or(Error::Overflow)?,
            historical_input_rows: 0,
            groups_before: self.entries.len(),
            groups_after: next.entries.len(),
        };
        Ok((next, delta, work))
    }
}
