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

/// Complete target-table conversion and replacement work for a DML statement.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DmlTargetWork {
    /// Expanded input rows collected from the statement's target snapshot.
    pub collected_rows: usize,
    /// Distinct complete row entries resulting from that collection.
    pub distinct_entries: usize,
    /// Actual complete map copies and final Arrow materialization.
    pub delta_update: RelationWork,
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
