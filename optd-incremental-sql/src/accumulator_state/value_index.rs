//! Extrema and DISTINCT retain counted values, never historical complete rows.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::row::RowConverter;
use datafusion::common::ScalarValue;

use crate::error::invalid;
use crate::metrics::ValueIndexWork;
use crate::state_directory::StateDirectory;
use crate::{Error, Result};

/// Minimum and maximum use the same counted representation and opposite directory ends.
#[derive(Clone, Copy, Debug)]
pub(super) enum Extremum {
    Minimum,
    Maximum,
}

/// Exact typed argument identity and its positive surviving multiplicity.
#[derive(Debug)]
struct ValueCount {
    value: ScalarValue,
    count: i64,
}

/// Consolidation is statement-local; zero projected changes never copy index paths.
struct ValueDelta {
    value: ScalarValue,
    weight: i64,
}

/// Non-NULL values entering or leaving a DISTINCT aggregate's native input.
#[derive(Default)]
pub(super) struct MembershipChanges {
    pub(super) entering: Vec<ScalarValue>,
    pub(super) leaving: Vec<ScalarValue>,
}

/// Cloning shares immutable value branches and resets candidate-local copy work.
#[derive(Clone, Debug, Default)]
pub(super) struct OrderedValues {
    values: StateDirectory<Arc<ValueCount>>,
}

impl OrderedValues {
    /// Update one private cloned cell; callers discard it after any error.
    /// Full-row deletion validity belongs to the graph; this index checks value counts.
    pub(super) fn apply(
        &mut self,
        converter: &RowConverter,
        positive: &ArrayRef,
        negative: &ArrayRef,
        work: &mut ValueIndexWork,
    ) -> Result<()> {
        self.update(converter, positive, negative, work, None)
    }

    /// Update counts and return only native membership zero crossings.
    pub(super) fn apply_membership(
        &mut self,
        converter: &RowConverter,
        positive: &ArrayRef,
        negative: &ArrayRef,
        work: &mut ValueIndexWork,
    ) -> Result<MembershipChanges> {
        let mut membership = MembershipChanges::default();
        self.update(converter, positive, negative, work, Some(&mut membership))?;
        Ok(membership)
    }

    /// Consolidate and validate counts once, optionally observing membership changes.
    fn update(
        &mut self,
        converter: &RowConverter,
        positive: &ArrayRef,
        negative: &ArrayRef,
        work: &mut ValueIndexWork,
        mut membership: Option<&mut MembershipChanges>,
    ) -> Result<()> {
        let mut delta = BTreeMap::<Vec<u8>, ValueDelta>::new();
        for (array, weight) in [(positive, 1i64), (negative, -1i64)] {
            work.input_rows = work
                .input_rows
                .checked_add(array.len())
                .ok_or(Error::Overflow)?;
            if array.is_empty() || array.null_count() == array.len() {
                continue;
            }
            let encoded = converter.convert_columns(&[Arc::clone(array)])?;
            for index in 0..array.len() {
                let value = ScalarValue::try_from_array(array, index)?;
                if value.is_null() {
                    continue;
                }
                let entry = delta
                    .entry(encoded.row(index).as_ref().to_vec())
                    .or_insert_with(|| ValueDelta {
                        value: value.clone(),
                        weight: 0,
                    });
                if entry.value != value {
                    return Err(invalid(
                        "counted value sort encoding does not preserve scalar identity",
                    ));
                }
                entry.weight = entry.weight.checked_add(weight).ok_or(Error::Overflow)?;
            }
        }
        let before_copies = self.values.copied_entries()?;
        for (key, delta) in delta {
            if delta.weight == 0 {
                continue;
            }
            work.value_updates = work.value_updates.checked_add(1).ok_or(Error::Overflow)?;
            let previous = self.values.get(&key);
            if previous.is_some_and(|old| old.value != delta.value) {
                return Err(invalid(
                    "counted value sort encoding differs from retained scalar identity",
                ));
            }
            let old_count = previous.map(|old| old.count).unwrap_or_default();
            let next = old_count.checked_add(delta.weight).ok_or(Error::Overflow)?;
            if next < 0 {
                return Err(invalid("negative value reference count"));
            }
            if let Some(changes) = &mut membership {
                if old_count == 0 && next > 0 {
                    changes.entering.push(delta.value.clone());
                } else if old_count > 0 && next == 0 {
                    changes.leaving.push(delta.value.clone());
                }
            }
            if next == 0 {
                self.values.remove(&key);
            } else {
                self.values.insert(
                    key,
                    Arc::new(ValueCount {
                        value: delta.value,
                        count: next,
                    }),
                );
            }
        }
        let copies = self
            .values
            .copied_entries()?
            .checked_sub(before_copies)
            .ok_or(Error::Overflow)?;
        work.directory_entries_copied = work
            .directory_entries_copied
            .checked_add(copies)
            .ok_or(Error::Overflow)?;
        Ok(())
    }

    /// One logarithmic endpoint lookup; an empty index represents native NULL output.
    pub(super) fn value(
        &self,
        direction: Extremum,
        work: &mut ValueIndexWork,
    ) -> Result<Option<&ScalarValue>> {
        work.extremum_lookups = work
            .extremum_lookups
            .checked_add(1)
            .ok_or(Error::Overflow)?;
        let entry = match direction {
            Extremum::Minimum => self.values.first_key_value(),
            Extremum::Maximum => self.values.last_key_value(),
        };
        Ok(entry.map(|(_, entry)| &entry.value))
    }
}

#[cfg(test)]
mod tests;
