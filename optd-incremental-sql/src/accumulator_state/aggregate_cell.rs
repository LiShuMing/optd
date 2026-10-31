//! One qualified aggregate chooses a native retractable cell or an ordered-value cell.

use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;

use super::value_index::{Extremum, OrderedValues};
use crate::error::invalid;
use crate::metrics::ValueIndexWork;
use crate::Result;

/// A mutually exclusive cell representation keeps native and indexed state aligned.
#[derive(Clone, Debug)]
pub(super) enum Cell {
    Native(Vec<ScalarValue>),
    Values(OrderedValues),
}

/// Index order belongs to the qualified function and is shared across its groups.
#[derive(Clone, Debug)]
enum Maintenance {
    Native,
    Extremum {
        direction: Extremum,
        converter: Arc<RowConverter>,
        empty: ScalarValue,
    },
}

/// Physical expression metadata and the only permitted maintenance algorithm for its cells.
#[derive(Clone, Debug)]
pub(super) struct AggregateSpec {
    pub(super) expression: AggregateFunctionExpr,
    maintenance: Maintenance,
}

/// Preserve native initial output coercion while installing the matching empty state.
pub(super) struct InitialCell {
    pub(super) state: Cell,
    pub(super) output: ScalarValue,
}

impl AggregateSpec {
    /// Caller must first apply accumulator eligibility and builtin identity checks.
    pub(super) fn new(expression: AggregateFunctionExpr, builtin_name: &str) -> Result<Self> {
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
                Maintenance::Native
            }
        };
        Ok(Self {
            expression,
            maintenance,
        })
    }

    /// Identify nodes that must publish the optional value-index work domain.
    pub(super) fn is_indexed(&self) -> bool {
        matches!(&self.maintenance, Maintenance::Extremum { .. })
    }

    /// Install an empty cell with exactly the native aggregate result type.
    pub(super) fn initial(&self) -> Result<InitialCell> {
        Ok(match &self.maintenance {
            Maintenance::Native => {
                let mut accumulator = self.expression.create_sliding_accumulator()?;
                let output = accumulator.evaluate()?;
                InitialCell {
                    state: Cell::Native(accumulator.state()?),
                    output,
                }
            }
            Maintenance::Extremum { empty, .. } => InitialCell {
                state: Cell::Values(OrderedValues::default()),
                output: empty.clone(),
            },
        })
    }

    /// Update a private cloned cell. Any failure discards the enclosing candidate.
    pub(super) fn apply(
        &self,
        cell: &mut Cell,
        positive: &[ArrayRef],
        negative: &[ArrayRef],
        work: &mut ValueIndexWork,
    ) -> Result<ScalarValue> {
        match (&self.maintenance, cell) {
            (Maintenance::Native, Cell::Native(states)) => {
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
                values.apply(converter, positive, negative, work)?;
                Ok(values
                    .value(*direction, work)?
                    .cloned()
                    .unwrap_or_else(|| empty.clone()))
            }
            _ => Err(invalid(
                "aggregate cell does not match its qualified maintenance mode",
            )),
        }
    }
}
