//! Qualified aggregates choose native, counted extrema or DISTINCT membership cells.

use std::sync::Arc;

use datafusion::arrow::array::{new_empty_array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;

use super::moments::{MomentOutput, Moments};
use super::value_index::{Extremum, OrderedValues};
use crate::error::invalid;
use crate::metrics::{DistinctWork, NumericWork, ValueIndexWork};
use crate::state_codec::{
    batch_to_counted_values, batch_to_scalar_rows, counted_values_to_batch, scalar_rows_to_batch,
    FunctionCellsSnapshot,
};
use crate::{Error, Result};

/// Aggregate SQL input multiplicity, qualified before physical state is constructed.
pub(super) enum ArgumentMode {
    All,
    ExactIntegerAverage,
    Moments(MomentOutput),
    Distinct(DataType),
}

/// Exclusive representations prevent applying ordinary deltas to DISTINCT native state.
#[derive(Clone, Debug)]
pub(super) enum Cell {
    Native(Vec<ScalarValue>),
    ExactIntegerAverage {
        states: Vec<ScalarValue>,
        mass: u64,
    },
    Moments(Moments),
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
    ExactIntegerAverage,
    Moments(MomentOutput),
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
    pub(super) numeric: NumericWork,
}

impl AggregateSpec {
    /// Caller proves canonical builtin identity, modifiers, argument and result eligibility.
    pub(super) fn new(
        expression: AggregateFunctionExpr,
        builtin_name: &str,
        mode: ArgumentMode,
    ) -> Result<Self> {
        let maintenance = if let ArgumentMode::Moments(kind) = mode {
            Maintenance::Moments(kind)
        } else {
            match builtin_name {
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
                        ArgumentMode::Moments(_) => {
                            return Err(invalid("moment mode escaped numerical dispatch"))
                        }
                        ArgumentMode::All => Maintenance::Native,
                        ArgumentMode::ExactIntegerAverage => Maintenance::ExactIntegerAverage,
                        ArgumentMode::Distinct(argument_type) => Maintenance::Distinct {
                            converter: Arc::new(RowConverter::new(vec![SortField::new(
                                argument_type.clone(),
                            )])?),
                            argument_type,
                        },
                    }
                }
            }
        };
        Ok(Self {
            expression,
            maintenance,
        })
    }

    /// Identify qualified stable moment cells for explicit numerical accounting.
    pub(super) fn is_numeric(&self) -> bool {
        matches!(self.maintenance, Maintenance::Moments(_))
    }

    /// Identify nodes that publish the optional extrema work domain.
    pub(super) fn is_indexed(&self) -> bool {
        matches!(&self.maintenance, Maintenance::Extremum { .. })
    }

    /// Identify functions whose counts and native state have separate work counters.
    pub(super) fn is_distinct(&self) -> bool {
        matches!(&self.maintenance, Maintenance::Distinct { .. })
    }

    /// Export one function's cells across all groups in deterministic group order.
    pub(super) fn export_cells<'a>(
        &self,
        cells: impl IntoIterator<Item = &'a Cell>,
    ) -> Result<FunctionCellsSnapshot> {
        let cells: Vec<&Cell> = cells.into_iter().collect();
        match &self.maintenance {
            Maintenance::Native => {
                let mut rows = Vec::with_capacity(cells.len());
                for cell in cells {
                    let Cell::Native(states) = cell else {
                        return Err(invalid("aggregate cell mode mismatch on export"));
                    };
                    rows.push(states.clone());
                }
                let states_batch = if rows.is_empty() {
                    None
                } else {
                    Some(scalar_rows_to_batch(None, &rows)?)
                };
                Ok(FunctionCellsSnapshot::Native { states_batch })
            }
            Maintenance::ExactIntegerAverage => {
                let mut rows = Vec::with_capacity(cells.len());
                let mut masses = Vec::with_capacity(cells.len());
                for cell in cells {
                    let Cell::ExactIntegerAverage { states, mass } = cell else {
                        return Err(invalid("aggregate cell mode mismatch on export"));
                    };
                    rows.push(states.clone());
                    masses.push(*mass);
                }
                let states_batch = if rows.is_empty() {
                    None
                } else {
                    Some(scalar_rows_to_batch(None, &rows)?)
                };
                Ok(FunctionCellsSnapshot::ExactIntegerAverage {
                    states_batch,
                    masses,
                })
            }
            Maintenance::Moments(_) => {
                let mut entries = Vec::with_capacity(cells.len());
                let mut values = Vec::new();
                for (group_idx, cell) in cells.into_iter().enumerate() {
                    let Cell::Moments(moments) = cell else {
                        return Err(invalid("aggregate cell mode mismatch on export"));
                    };
                    let (meta, vals) = moments.export_persisted();
                    entries.push(meta);
                    for (v, c) in vals {
                        values.push((group_idx as u64, v, c));
                    }
                }
                Ok(FunctionCellsSnapshot::Moments {
                    entries,
                    values_batch: counted_values_to_batch(&values)?,
                })
            }
            Maintenance::Extremum { .. } => {
                let mut values = Vec::new();
                for (group_idx, cell) in cells.into_iter().enumerate() {
                    let Cell::Values(index) = cell else {
                        return Err(invalid("aggregate cell mode mismatch on export"));
                    };
                    for (v, c) in index.entries() {
                        values.push((group_idx as u64, v.clone(), c as i64));
                    }
                }
                Ok(FunctionCellsSnapshot::Values {
                    values_batch: counted_values_to_batch(&values)?,
                })
            }
            Maintenance::Distinct { .. } => {
                let mut values = Vec::new();
                let mut rows = Vec::with_capacity(cells.len());
                for (group_idx, cell) in cells.into_iter().enumerate() {
                    let Cell::Distinct {
                        values: index,
                        states,
                    } = cell
                    else {
                        return Err(invalid("aggregate cell mode mismatch on export"));
                    };
                    for (v, c) in index.entries() {
                        values.push((group_idx as u64, v.clone(), c as i64));
                    }
                    rows.push(states.clone());
                }
                let states_batch = if rows.is_empty() {
                    None
                } else {
                    Some(scalar_rows_to_batch(None, &rows)?)
                };
                Ok(FunctionCellsSnapshot::Distinct {
                    values_batch: counted_values_to_batch(&values)?,
                    states_batch,
                })
            }
        }
    }

    /// Restore one function's cells across `group_count` groups.
    pub(super) fn restore_cells(
        &self,
        group_count: usize,
        snapshot: FunctionCellsSnapshot,
    ) -> Result<Vec<Cell>> {
        match (&self.maintenance, snapshot) {
            (Maintenance::Native, FunctionCellsSnapshot::Native { states_batch }) => {
                let rows = match states_batch {
                    Some(batch) => batch_to_scalar_rows(&batch)?,
                    None => Vec::new(),
                };
                if rows.len() != group_count {
                    return Err(invalid("persisted native cell count differs"));
                }
                Ok(rows.into_iter().map(Cell::Native).collect())
            }
            (
                Maintenance::ExactIntegerAverage,
                FunctionCellsSnapshot::ExactIntegerAverage {
                    states_batch,
                    masses,
                },
            ) => {
                let rows = match states_batch {
                    Some(batch) => batch_to_scalar_rows(&batch)?,
                    None => Vec::new(),
                };
                if rows.len() != group_count || masses.len() != group_count {
                    return Err(invalid("persisted exact-avg cell count differs"));
                }
                Ok(rows
                    .into_iter()
                    .zip(masses)
                    .map(|(states, mass)| Cell::ExactIntegerAverage { states, mass })
                    .collect())
            }
            (
                Maintenance::Moments(_),
                FunctionCellsSnapshot::Moments {
                    entries,
                    values_batch,
                },
            ) => {
                if entries.len() != group_count {
                    return Err(invalid("persisted moment cell count differs"));
                }
                let mut per_group: Vec<Vec<(ScalarValue, i64)>> = vec![Vec::new(); group_count];
                if let Some(batch) = values_batch {
                    for (idx, v, c) in batch_to_counted_values(&batch)? {
                        let slot = per_group
                            .get_mut(idx as usize)
                            .ok_or_else(|| invalid("moment value group index out of bounds"))?;
                        slot.push((v, c));
                    }
                }
                entries
                    .into_iter()
                    .zip(per_group)
                    .map(|(meta, vals)| Ok(Cell::Moments(Moments::from_persisted(meta, vals)?)))
                    .collect()
            }
            (
                Maintenance::Extremum { converter, .. },
                FunctionCellsSnapshot::Values { values_batch },
            ) => {
                let mut per_group: Vec<Vec<(ScalarValue, i64)>> = vec![Vec::new(); group_count];
                if let Some(batch) = values_batch {
                    for (idx, v, c) in batch_to_counted_values(&batch)? {
                        let slot = per_group
                            .get_mut(idx as usize)
                            .ok_or_else(|| invalid("extremum value group index out of bounds"))?;
                        slot.push((v, c));
                    }
                }
                per_group
                    .into_iter()
                    .map(|vals| {
                        Ok(Cell::Values(OrderedValues::from_persisted(
                            converter, vals,
                        )?))
                    })
                    .collect()
            }
            (
                Maintenance::Distinct { converter, .. },
                FunctionCellsSnapshot::Distinct {
                    values_batch,
                    states_batch,
                },
            ) => {
                let rows = match states_batch {
                    Some(batch) => batch_to_scalar_rows(&batch)?,
                    None => Vec::new(),
                };
                if rows.len() != group_count {
                    return Err(invalid("persisted distinct state count differs"));
                }
                let mut per_group: Vec<Vec<(ScalarValue, i64)>> = vec![Vec::new(); group_count];
                if let Some(batch) = values_batch {
                    for (idx, v, c) in batch_to_counted_values(&batch)? {
                        let slot = per_group
                            .get_mut(idx as usize)
                            .ok_or_else(|| invalid("distinct value group index out of bounds"))?;
                        slot.push((v, c));
                    }
                }
                rows.into_iter()
                    .zip(per_group)
                    .map(|(states, vals)| {
                        Ok(Cell::Distinct {
                            values: OrderedValues::from_persisted(converter, vals)?,
                            states,
                        })
                    })
                    .collect()
            }
            _ => Err(invalid(
                "persisted aggregate cell variant does not match compiled function mode",
            )),
        }
    }

    /// Empty native states have identical outputs for ordinary and DISTINCT inputs.
    pub(super) fn initial(&self) -> Result<InitialCell> {
        Ok(match &self.maintenance {
            Maintenance::Moments(_) => InitialCell {
                state: Cell::Moments(Moments::default()),
                output: ScalarValue::Float64(None),
            },
            Maintenance::Extremum { empty, .. } => InitialCell {
                state: Cell::Values(OrderedValues::default()),
                output: empty.clone(),
            },
            Maintenance::Native
            | Maintenance::ExactIntegerAverage
            | Maintenance::Distinct { .. } => {
                let mut accumulator = self.expression.create_sliding_accumulator()?;
                let output = accumulator.evaluate()?;
                let states = accumulator.state()?;
                let state = if self.is_distinct() {
                    Cell::Distinct {
                        values: OrderedValues::default(),
                        states,
                    }
                } else if matches!(self.maintenance, Maintenance::ExactIntegerAverage) {
                    Cell::ExactIntegerAverage { states, mass: 0 }
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
            (Maintenance::Moments(kind), Cell::Moments(moments)) => {
                let ([positive], [negative]) = (positive, negative) else {
                    return Err(invalid("moments require one argument"));
                };
                moments.apply(positive, negative, *kind, &mut work.numeric)
            }
            (Maintenance::ExactIntegerAverage, Cell::ExactIntegerAverage { states, mass }) => {
                let ([positive_array], [negative_array]) = (positive, negative) else {
                    return Err(invalid("exact integer average requires one argument"));
                };
                let removed = integer_mass(negative_array)?;
                let added = integer_mass(positive_array)?;
                let next = mass
                    .checked_sub(removed)
                    .and_then(|n| n.checked_add(added))
                    .filter(|n| *n <= (1u64 << 53))
                    .ok_or_else(|| Error::ExactIntegerAverage)?;
                let output = self.apply_native(states, positive, negative)?;
                *mass = next;
                Ok(output)
            }
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

/// Absolute integral mass bounds every partial native sum, regardless of batch order.
fn integer_mass(array: &ArrayRef) -> Result<u64> {
    let mut mass = 0u64;
    for index in 0..array.len() {
        match ScalarValue::try_from_array(array, index)? {
            ScalarValue::Float64(None) => {}
            ScalarValue::Float64(Some(value))
                if value.is_finite()
                    && value.fract() == 0.0
                    && value.abs() <= (1u64 << 53) as f64 =>
            {
                mass = mass
                    .checked_add(value.abs() as u64)
                    .filter(|n| *n <= (1u64 << 53))
                    .ok_or_else(|| Error::ExactIntegerAverage)?;
            }
            _ => return Err(Error::ExactIntegerAverage),
        }
    }
    Ok(mass)
}
