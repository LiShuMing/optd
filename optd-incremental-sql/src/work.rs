//! Candidate-local accounting for relation updates, materialization and state copies.

use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Relation-map rebuilding and cache materialization at the owning scope.
/// Raw sources use hash-collision bucket maps; maintained caches and SQL targets use leaf maps.
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
/// Counts include each actual shared tree-node clone on modified paths, including
/// rotation siblings and delete successors. Root clones and new entries are zero.
/// Shared `Arc` bucket contents,
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
    /// Record actual group-directory path copies after candidate updates.
    pub(crate) fn record_groups(&mut self, entries: usize) -> Result<()> {
        add(&mut self.group_entries, entries)
    }

    /// Record actual keyed input path copies without traversing payloads.
    pub(crate) fn record_keyed_input(&mut self, entries: usize) -> Result<()> {
        add(&mut self.keyed_input_entries, entries)
    }

    /// Record actual window output path copies after the output delta.
    pub(crate) fn record_window_output(&mut self, entries: usize) -> Result<()> {
        add(&mut self.window_output_entries, entries)
    }

    /// Record actual accumulator path copies without serializing its states.
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
/// Complete node replacement collection/leaf bootstrap is measured separately.
/// Missing work in older serialized statistics is
/// unknown; it must not be interpreted as measured zero.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RefreshWork {
    /// Actual raw source collision-bucket map rebuilds; no Arrow expansion.
    pub sources: RelationWork,
    /// Changed cache-leaf map rebuilds and their Arrow materializations.
    /// Complete concatenations and replacement bootstrap have separate counters.
    pub caches: RelationWork,
    /// State-directory copies across candidate and physical-state preparation.
    pub state_copies: StateCopyWork,
    /// Extended source/cache diagnostics share one allocation, keeping existing public
    /// refresh-enum variants compact without changing their field types.
    #[serde(flatten)]
    pub cache_work: Box<CacheRelationWork>,
}

/// Actual maintained relation update, demand and replacement boundaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct CacheRelationWork {
    /// Actual graph leaf expansions demanded by complete snapshots/root outputs.
    pub demanded_leaves: RelationWork,
    /// Distinct-row Arrow batches used for lazy graph leaf identity/boundary checks.
    /// Temporary scalar/sort allocations inside Bag conversion remain excluded.
    pub leaf_validation: RowEncodingWork,
    /// Raw-source validation, shared updates and demanded complete Bag collection.
    /// Kept in this existing boxed group to preserve public refresh-enum sizes.
    pub source_state: SourceStateWork,
    /// Actual maintained node row-leaf update and directory work.
    pub cache_blocks: RowBlockWork,
    /// Complete node concatenations demanded by snapshots, ordering or root output.
    pub complete_caches: RowMaterializationWork,
    /// Arrow collection and leaf bootstrap for complete physical replacements.
    pub replacement_bootstrap: RowBootstrapWork,
}

/// Distinct-row validation/encoding at a retained graph leaf, without weight expansion.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RowEncodingWork {
    /// Successful distinct leaf boundary batches encoded.
    pub batches: usize,
    /// Distinct complete rows in those batches, independent of multiplicity.
    pub rows: usize,
    /// Arrow buffer sizes of the distinct-row boundary batches.
    pub array_bytes: usize,
}
impl RowEncodingWork {
    /// Record the actual distinct Arrow batch; internal temporary allocations are excluded.
    pub(crate) fn record(&mut self, batch: &RecordBatch) -> Result<()> {
        add(&mut self.batches, 1)?;
        add(&mut self.rows, batch.num_rows())?;
        add(&mut self.array_bytes, batch.get_array_memory_size())
    }
    /// Merge independently observed leaf validation boundaries.
    pub(crate) fn absorb(&mut self, other: &Self) -> Result<()> {
        add(&mut self.batches, other.batches)?;
        add(&mut self.rows, other.rows)?;
        add(&mut self.array_bytes, other.array_bytes)
    }
}

/// Actual weighted source work, excluding bootstrap and external snapshot/export IO.
/// Hash collisions enlarge a touched bucket; their historical copies appear in sources.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct SourceStateWork {
    /// Nonempty consolidated source updates, regardless of touched bucket count.
    pub updates: usize,
    /// Exact incoming rows probed for ordered-prefix validation.
    pub validation_probes: usize,
    /// Collision buckets updated once per consolidated source change.
    pub touched_buckets: usize,
    /// Shared source-directory tree nodes actually copied along changed paths.
    pub directory_entries_copied: usize,
    /// Complete Bag cache misses demanded by graph SourceRead expressions.
    pub snapshot_collections: usize,
    /// Distinct rows copied into those complete Bags (not expanded multiplicities).
    pub snapshot_entries_collected: usize,
}
impl SourceStateWork {
    /// Account for one successful incoming-row prefix probe.
    pub(crate) fn record_validation_probe(&mut self) -> Result<()> {
        add(&mut self.validation_probes, 1)
    }
    /// Account for one rebuilt exact collision bucket.
    pub(crate) fn record_touched_bucket(&mut self) -> Result<()> {
        add(&mut self.touched_buckets, 1)
    }
    /// Record a consolidated update and measured path copies.
    pub(crate) fn record_update(&mut self, copies: usize) -> Result<()> {
        add(&mut self.updates, 1)?;
        add(&mut self.directory_entries_copied, copies)
    }
    /// Record only actual snapshot misses; cache hits pass zeroes.
    pub(crate) fn record_collection(&mut self, misses: usize, entries: usize) -> Result<()> {
        add(&mut self.snapshot_collections, misses)?;
        add(&mut self.snapshot_entries_collected, entries)
    }
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

/// Changed-leaf and actual directory path-copy work, distinct from checkpoint IO.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowBlockWork {
    /// Installed nonempty leaves before this statement (size, not access work).
    pub total_blocks_before: usize,
    /// Shared old tree-node entries actually cloned on modified paths.
    pub directory_entries_copied: usize,
    /// Leaf ranges visited, including insertion into an empty table.
    pub touched_blocks: usize,
    /// Existing leaves replaced or removed by this statement.
    pub touched_existing_blocks: usize,
    /// Unchanged existing leaf payloads shared by the candidate.
    pub reused_blocks: usize,
    /// Changed leaves split into multiple bounded replacements.
    pub split_blocks: usize,
    /// New distinct entries copied to create split Bag subsets, beyond old-map copying.
    pub split_row_entries_copied: usize,
    /// Distinct signed entries encoded for directory lookup.
    pub delta_key_rows: usize,
    /// Arrow buffer sizes of those temporary encoding batches.
    pub delta_key_array_bytes: usize,
}

/// Complete-relation cache misses at consumers that require one Arrow batch.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowMaterializationWork {
    /// Successful concatenations (zero for shared cache hits).
    pub misses: u64,
    /// Expanded rows emitted by those concatenations.
    pub rows: u64,
    /// Sum of Arrow array buffer sizes in the resulting batches.
    pub array_bytes: u64,
}

/// Actual Arrow-to-block work during import, replacement and history restoration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowBootstrapWork {
    /// Expanded table rows collected into a positive Bag.
    pub collected_rows: u64,
    /// Distinct complete rows in the collected Bags.
    pub distinct_entries: u64,
    /// Nonempty ordered leaf caches materialized during import.
    pub leaf_materializations: u64,
    /// Expanded rows in those leaf caches.
    pub materialized_rows: u64,
    /// Sum of Arrow array buffer sizes in those leaf caches.
    pub materialized_array_bytes: u64,
}

/// Actual leaf-update work, independent of SQL statement or source ownership.
#[derive(Clone, Debug, Default)]
pub(crate) struct RowUpdateWork {
    pub(crate) delta_update: RelationWork,
    pub(crate) block_update: RowBlockWork,
    pub(crate) leaf_validation: RowEncodingWork,
}

impl RelationWork {
    /// Merge independently measured leaf work without inferring complete-map copies.
    pub(crate) fn absorb(&mut self, other: &Self) -> Result<()> {
        add(&mut self.map_rebuilds, other.map_rebuilds)?;
        add(
            &mut self.old_row_entries_copied,
            other.old_row_entries_copied,
        )?;
        add(&mut self.materializations, other.materializations)?;
        add(&mut self.materialized_rows, other.materialized_rows)?;
        add(
            &mut self.materialized_array_bytes,
            other.materialized_array_bytes,
        )
    }
}
impl RowBlockWork {
    /// Sum actual block operations; total_blocks_before remains a summed size diagnostic.
    pub(crate) fn absorb(&mut self, other: &Self) -> Result<()> {
        add(&mut self.total_blocks_before, other.total_blocks_before)?;
        add(
            &mut self.directory_entries_copied,
            other.directory_entries_copied,
        )?;
        add(&mut self.touched_blocks, other.touched_blocks)?;
        add(
            &mut self.touched_existing_blocks,
            other.touched_existing_blocks,
        )?;
        add(&mut self.reused_blocks, other.reused_blocks)?;
        add(&mut self.split_blocks, other.split_blocks)?;
        add(
            &mut self.split_row_entries_copied,
            other.split_row_entries_copied,
        )?;
        add(&mut self.delta_key_rows, other.delta_key_rows)?;
        add(&mut self.delta_key_array_bytes, other.delta_key_array_bytes)?;
        Ok(())
    }
}
impl RowMaterializationWork {
    /// Sum actual complete-cache misses, not estimated available relation sizes.
    pub(crate) fn absorb(&mut self, other: &Self) -> Result<()> {
        self.misses = self
            .misses
            .checked_add(other.misses)
            .ok_or(Error::Overflow)?;
        self.rows = self.rows.checked_add(other.rows).ok_or(Error::Overflow)?;
        self.array_bytes = self
            .array_bytes
            .checked_add(other.array_bytes)
            .ok_or(Error::Overflow)?;
        Ok(())
    }
}

impl RowBootstrapWork {
    pub(crate) fn absorb(&mut self, other: &Self) -> Result<()> {
        self.collected_rows = self
            .collected_rows
            .checked_add(other.collected_rows)
            .ok_or(Error::Overflow)?;
        self.distinct_entries = self
            .distinct_entries
            .checked_add(other.distinct_entries)
            .ok_or(Error::Overflow)?;
        self.leaf_materializations = self
            .leaf_materializations
            .checked_add(other.leaf_materializations)
            .ok_or(Error::Overflow)?;
        self.materialized_rows = self
            .materialized_rows
            .checked_add(other.materialized_rows)
            .ok_or(Error::Overflow)?;
        self.materialized_array_bytes = self
            .materialized_array_bytes
            .checked_add(other.materialized_array_bytes)
            .ok_or(Error::Overflow)?;
        Ok(())
    }
}
