//! Positive weighted ranges; projection precedes expanded Arrow allocation.

use std::sync::Arc;

use datafusion::arrow::array::new_empty_array;
use datafusion::arrow::record_batch::RecordBatch;

use super::{batch_with_rows, materialize, Bag, DistinctRows};
use crate::work::RowReadWork;
use crate::{Error, Result};

impl Bag {
    /// Emit a canonical positive multiplicity range, without expanding its prefix.
    /// Sorting still encodes all distinct rows in this bounded leaf and is measured.
    pub(crate) fn range_batch(
        &self,
        offset: usize,
        length: usize,
        projection: &[usize],
        work: &mut RowReadWork,
    ) -> Result<RecordBatch> {
        let schema = Arc::new(self.schema.project(projection)?);
        self.validate_relation()?;
        if offset.checked_add(length).ok_or(Error::Overflow)? > self.total()? {
            return Err(Error::Invalid("row range exceeds relation".into()));
        }
        if length == 0 {
            return batch_with_rows(
                Arc::clone(&schema),
                schema
                    .fields()
                    .iter()
                    .map(|f| new_empty_array(f.data_type()))
                    .collect(),
                0,
            );
        }
        let entries = self.sorted_entries()?;
        // sorted_entries performs one distinct-row encoding only for nontrivial orders.
        // Its temporary buffer bytes are excluded, as at other Bag sort boundaries.
        if entries.len() > 1 && !self.schema.fields().is_empty() {
            work.ordering_rows = work
                .ordering_rows
                .checked_add(entries.len())
                .ok_or(Error::Overflow)?;
        }
        let mut skip = offset;
        let mut remaining = length;
        let mut selected = Vec::new();
        for (row, weight) in entries {
            work.stored_entries_visited = work
                .stored_entries_visited
                .checked_add(1)
                .ok_or(Error::Overflow)?;
            let count = usize::try_from(weight).map_err(|_| Error::Overflow)?;
            if skip >= count {
                skip -= count;
                continue;
            }
            let count = (count - skip).min(remaining);
            skip = 0;
            let projected = projection
                .iter()
                .map(|&index| row[index].clone())
                .collect::<Vec<_>>();
            selected.push((
                projected,
                i64::try_from(count).map_err(|_| Error::Overflow)?,
            ));
            remaining -= count;
            if remaining == 0 {
                break;
            }
        }
        let borrowed = selected
            .iter()
            .map(|(row, weight)| (row, *weight))
            .collect::<Vec<_>>();
        let batch = materialize(schema, &borrowed, true)?;
        work.materialization.record_materialization(&batch)?;
        Ok(batch)
    }

    /// Exact expanded logical NULL counts from bounded distinct Arrow rows.
    /// Arrow defines dictionary/union/nested logical validity, avoiding scalar guesses.
    pub(crate) fn distinct_rows(&self) -> Result<DistinctRows> {
        let entries = self.sorted_entries()?;
        let batch = materialize(self.schema(), &entries, false)?;
        let null_counts = batch
            .columns()
            .iter()
            .map(|array| {
                let Some(nulls) = array.logical_nulls() else {
                    return Ok(0);
                };
                entries
                    .iter()
                    .enumerate()
                    .try_fold(0usize, |count, (index, (_, weight))| {
                        if nulls.is_null(index) {
                            count
                                .checked_add(usize::try_from(*weight).map_err(|_| Error::Overflow)?)
                                .ok_or(Error::Overflow)
                        } else {
                            Ok(count)
                        }
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(DistinctRows { batch, null_counts })
    }
}
