//! Qualified aggregates choose native, counted extrema or DISTINCT membership cells.

use std::sync::Arc;

use datafusion::arrow::array::{new_empty_array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;

use super::value_index::{Extremum, OrderedValues};
use crate::error::invalid;
use crate::metrics::{DistinctWork, ValueIndexWork};
use crate::{Error, Result};

/// Aggregate SQL input multiplicity, qualified before physical state is constructed.
pub(super) enum ArgumentMode {
    All,
    Distinct(DataType),
}

/// Exclusive representations prevent applying ordinary deltas to DISTINCT native state.
#[derive(Clone, Debug)]
pub(super) enum Cell {
    Native(Vec<ScalarValue>),
    Values(OrderedValues),
    Distinct {
        values: OrderedValues,
        states: Vec<ScalarValue>,
    },
}

/// Index order and exact argument types are shared across all groups of one function.
#[derive(Clone, Debug)]
enum Maintenance {
    Native,
    Extremum {
        direction: Extremum,
        converter: Arc<RowConverter>,
        empty: ScalarValue,
    },
    Distinct {
        converter: Arc<RowConverter>,
        argument_type: DataType,
    },
}

/// Physical expressions always use ordinary native state; DISTINCT is handled by membership.
#[derive(Clone, Debug)]
pub(super) struct AggregateSpec {
    pub(super) expression: AggregateFunctionExpr,
    maintenance: Maintenance,
}

/// Preserve native empty output coercion while installing the matching empty cell.
pub(super) struct InitialCell {
    pub(super) state: Cell,
    pub(super) output: ScalarValue,
}

/// Candidate-local work domains remain separate for extrema and DISTINCT functions.
#[derive(Default)]
pub(super) struct CellWork {
    pub(super) extrema: ValueIndexWork,
    pub(super) distinct: DistinctWork,
}

impl AggregateSpec {
    /// Caller proves canonical builtin identity, modifiers, argument and result eligibility.
    pub(super) fn new(
        expression: AggregateFunctionExpr,
        builtin_name: &str,
        mode: ArgumentMode,
    ) -> Result<Self> {
        let maintenance = match builtin_name {
            "min" | "max" => {
                let empty = expression.create_accumulator()?.evaluate()?;
                let converter =
                    Arc::new(RowConverter::new(vec![SortField::new(empty.data_type())])?);
                Maintenance::Extremum {
                    direction: if builtin_name == "min" {
                        Extremum::Minimum
                    } else {
                        Extremum::Maximum
                    },
                    converter,
                    empty,
                }
            }
            _ => {
                if !expression
                    .create_sliding_accumulator()?
                    .supports_retract_batch()
                {
                    return Err(invalid("aggregate cannot retract"));
                }
                match mode {
                    ArgumentMode::All => Maintenance::Native,
                    ArgumentMode::Distinct(argument_type) => Maintenance::Distinct {
                        converter: Arc::new(RowConverter::new(vec![SortField::new(
                            argument_type.clone(),
                        )])?),
                        argument_type,
                    },
                }
            }
        };
        Ok(Self {
            expression,
            maintenance,
        })
    }

    /// Identify nodes that publish the optional extrema work domain.
    pub(super) fn is_indexed(&self) -> bool {
        matches!(&self.maintenance, Maintenance::Extremum { .. })
    }

    /// Identify functions whose counts and native state have separate work counters.
    pub(super) fn is_distinct(&self) -> bool {
        matches!(&self.maintenance, Maintenance::Distinct { .. })
    }

    /// Empty native states have identical outputs for ordinary and DISTINCT inputs.
    pub(super) fn initial(&self) -> Result<InitialCell> {
        Ok(match &self.maintenance {
            Maintenance::Extremum { empty, .. } => InitialCell {
                state: Cell::Values(OrderedValues::default()),
                output: empty.clone(),
            },
            Maintenance::Native | Maintenance::Distinct { .. } => {
                let mut accumulator = self.expression.create_sliding_accumulator()?;
                let output = accumulator.evaluate()?;
                let states = accumulator.state()?;
                let state = if self.is_distinct() {
                    Cell::Distinct {
                        values: OrderedValues::default(),
                        states,
                    }
                } else {
                    Cell::Native(states)
                };
                InitialCell { state, output }
            }
        })
    }

    /// Update a private cloned cell. Any failure discards the enclosing candidate.
    pub(super) fn apply(
        &self,
        cell: &mut Cell,
        positive: &[ArrayRef],
        negative: &[ArrayRef],
        work: &mut CellWork,
    ) -> Result<ScalarValue> {
        match (&self.maintenance, cell) {
            (Maintenance::Native, Cell::Native(states)) => {
                self.apply_native(states, positive, negative)
            }
            (
                Maintenance::Extremum {
                    direction,
                    converter,
                    empty,
                },
                Cell::Values(values),
            ) => {
                let ([positive], [negative]) = (positive, negative) else {
                    return Err(invalid("indexed extremum requires one argument"));
                };
                values.apply(converter, positive, negative, &mut work.extrema)?;
                Ok(values
                    .value(*direction, &mut work.extrema)?
                    .cloned()
                    .unwrap_or_else(|| empty.clone()))
            }
            (
                Maintenance::Distinct {
                    converter,
                    argument_type,
                },
                Cell::Distinct { values, states },
            ) => {
                let ([positive], [negative]) = (positive, negative) else {
                    return Err(invalid("DISTINCT membership requires one argument"));
                };
                let mut index_work = ValueIndexWork::default();
                let changes =
                    values.apply_membership(converter, positive, negative, &mut index_work)?;
                add_count(&mut work.distinct.input_rows, index_work.input_rows)?;
                add_count(&mut work.distinct.value_updates, index_work.value_updates)?;
                add_count(
                    &mut work.distinct.directory_entries_copied,
                    index_work.directory_entries_copied,
                )?;
                add_count(&mut work.distinct.entering_values, changes.entering.len())?;
                add_count(&mut work.distinct.leaving_values, changes.leaving.len())?;
                self.apply_native(
                    states,
                    &[membership_array(changes.entering, argument_type)?],
                    &[membership_array(changes.leaving, argument_type)?],
                )
            }
            _ => Err(invalid(
                "aggregate cell does not match its qualified maintenance mode",
            )),
        }
    }

    /// Retract before adding; DISTINCT callers supply only membership zero crossings.
    fn apply_native(
        &self,
        states: &mut Vec<ScalarValue>,
        positive: &[ArrayRef],
        negative: &[ArrayRef],
    ) -> Result<ScalarValue> {
        let mut accumulator = self.expression.create_sliding_accumulator()?;
        accumulator.merge_batch(
            &states
                .iter()
                .map(|v| v.to_array_of_size(1))
                .collect::<std::result::Result<Vec<_>, _>>()?,
        )?;
        if negative.first().is_some_and(|a| !a.is_empty()) {
            accumulator.retract_batch(negative)?;
        }
        if positive.first().is_some_and(|a| !a.is_empty()) {
            accumulator.update_batch(positive)?;
        }
        let output = accumulator.evaluate()?;
        *states = accumulator.state()?;
        Ok(output)
    }
}

/// Empty crossings must keep the exact qualified argument type, including decimal scale.
fn membership_array(values: Vec<ScalarValue>, data_type: &DataType) -> Result<ArrayRef> {
    if values.is_empty() {
        Ok(new_empty_array(data_type))
    } else {
        Ok(ScalarValue::iter_to_array(values)?)
    }
}

/// Counter failure belongs to the same private candidate as state failure.
fn add_count(counter: &mut usize, amount: usize) -> Result<()> {
    *counter = counter.checked_add(amount).ok_or(Error::Overflow)?;
    Ok(())
}
