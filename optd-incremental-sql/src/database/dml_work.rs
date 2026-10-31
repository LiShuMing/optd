//! Optional, backward-readable diagnostics for one executed DML statement.

use serde::{Deserialize, Serialize};

use crate::RelationWork;

/// Actual batches delivered at physical source boundaries and selector output.
///
/// Counts include repeated scan executions, after source projection/fetch and
/// before downstream filtering. They are neither disk reads nor CPU row visits.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectorWork {
    /// Successful source batches actually yielded, including zero-row batches.
    pub source_batches: usize,
    /// Expanded rows in those source batches.
    pub source_rows: usize,
    /// Sum of source Arrow array buffer sizes, counting repeated delivery.
    /// This does not measure allocations or uniquely retained memory.
    pub source_array_bytes: usize,
    /// Result batches produced by the complete selector.
    pub output_batches: usize,
    /// Result rows before old/new image extraction and assignment casts.
    pub output_rows: usize,
}

/// Changed-leaf and actual directory path-copy work, distinct from checkpoint IO.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableBlockWork {
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

/// Actual target collection and complete-map or changed-leaf replacement work.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DmlTargetWork {
    /// Expanded input rows collected from the statement's target snapshot.
    pub collected_rows: usize,
    /// Distinct complete row entries resulting from that collection.
    pub distinct_entries: usize,
    /// Actual map copies and Arrow materialization at complete-map/leaf boundaries.
    pub delta_update: RelationWork,
    /// Measured leaf maintenance, absent in receipts predating block snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_update: Option<TableBlockWork>,
}

/// Measured statement work, distinct from view maintenance and durable storage.
///
/// Present only for measured execution. Historical receipts omit this diagnostic;
/// callers must retain that absence as unknown rather than synthesize zeros.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DmlExecutionWork {
    /// Physical selector source consumption and result sizes.
    pub selector: SelectorWork,
    /// Target conversion, map copying and final Arrow materialization.
    pub target: DmlTargetWork,
    /// Complete old row images selected for DELETE/UPDATE, including duplicates.
    pub old_image_rows: usize,
    /// Complete new row images selected for INSERT/UPDATE, including duplicates.
    pub new_image_rows: usize,
}
