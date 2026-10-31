//! Candidate-local accounting for full relation caches and cloned state directories.

use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Relation-map rebuilding and cache materialization at the owning scope.
/// Refresh uses complete source/cache maps; DML block maintenance uses leaf maps.
///
/// Row entries count distinct complete rows, not expanded multiplicities. Array
/// bytes are Arrow's reported buffer sizes for materialized batches, not measured
/// allocator traffic, retained unique bytes or disk IO. Temporary expressions and
/// affected-bucket payload updates are outside this counter's scope. Complete
/// replacement batches from physical expressions are also excluded.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationWork {
    /// Nonempty signed updates that rebuild the owning complete or leaf map.
    pub map_rebuilds: usize,
    /// Existing distinct row entries copied into those rebuilt maps.
    pub old_row_entries_copied: usize,
    /// Cache batches materialized from updated Bags at the owning scope.
    pub materializations: usize,
    /// Expanded rows emitted by those materializations, including duplicates.
    pub materialized_rows: usize,
    /// Sum of `get_array_memory_size()` for those batches.
    pub materialized_array_bytes: usize,
}

impl RelationWork {
    /// Record an actual full-map rebuild, independent of the size of its delta.
    pub(crate) fn record_rebuild(&mut self, old_entries: usize) -> Result<()> {
        add(&mut self.map_rebuilds, 1)?;
        add(&mut self.old_row_entries_copied, old_entries)
    }

    /// Record the newly materialized cache batch, excluding shared batch clones.
    pub(crate) fn record_materialization(&mut self, batch: &RecordBatch) -> Result<()> {
        add(&mut self.materializations, 1)?;
        add(&mut self.materialized_rows, batch.num_rows())?;
        add(
            &mut self.materialized_array_bytes,
            batch.get_array_memory_size(),
        )
    }
}

/// Actual directory entries cloned while preparing one candidate refresh.
///
/// Counts include eager candidate copies and subsequent copies made by state
/// preparation. Each clone counts separately. Shared `Arc` bucket contents,
/// container capacity, pointer bytes and copied vector headers are excluded.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct StateCopyWork {
    /// Group input/output directory entries cloned, with shared bucket payloads.
    pub group_entries: usize,
    /// Per-input keyed-index entries cloned, with shared input buckets.
    pub keyed_input_entries: usize,
    /// Window output directory entries cloned, with shared output Bags.
    pub window_output_entries: usize,
    /// Retractable aggregate group entries cloned, with shared accumulator cells.
    pub accumulator_entries: usize,
}

impl StateCopyWork {
    /// Count one group-directory clone at its operation boundary.
    pub(crate) fn record_groups(&mut self, entries: usize) -> Result<()> {
        add(&mut self.group_entries, entries)
    }

    /// Count one keyed input-directory clone without traversing bucket payloads.
    pub(crate) fn record_keyed_input(&mut self, entries: usize) -> Result<()> {
        add(&mut self.keyed_input_entries, entries)
    }

    /// Count one window output-directory clone.
    pub(crate) fn record_window_output(&mut self, entries: usize) -> Result<()> {
        add(&mut self.window_output_entries, entries)
    }

    /// Count one accumulator-directory clone without serializing its states.
    pub(crate) fn record_accumulators(&mut self, entries: usize) -> Result<()> {
        add(&mut self.accumulator_entries, entries)
    }
}

/// Measured refresh-orchestration work beyond local operator bucket counters.
///
/// These are candidate-local logical work counts, not end-to-end IO or performance
/// measurements. SQL selection/DML, external sources, DataFusion internals, local
/// bucket payload updates, physical subexpression materialization, checkpoint IO
/// and recovery are excluded, as are source-name maps and graph-vector headers.
/// Missing work in older serialized statistics is
/// unknown; it must not be interpreted as measured zero.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RefreshWork {
    /// Full raw source Bag rebuilds; sources are not Arrow-materialized here.
    pub sources: RelationWork,
    /// Full delta-update relation-map rebuilds and their Arrow materializations.
    /// Complete replacement batches from physical expressions are excluded.
    pub caches: RelationWork,
    /// State-directory copies across candidate and physical-state preparation.
    pub state_copies: StateCopyWork,
}

/// Checked addition keeps diagnostic counters from silently wrapping.
fn add(counter: &mut usize, amount: usize) -> Result<()> {
    *counter = counter.checked_add(amount).ok_or(Error::Overflow)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_reject_overflow_without_wrapping() {
        let mut counter = usize::MAX;
        assert!(matches!(add(&mut counter, 1), Err(Error::Overflow)));
        assert_eq!(counter, usize::MAX);
    }
}
