//! Optional, backward-readable diagnostics for one executed DML statement.

use serde::{Deserialize, Serialize};

use crate::RelationWork;

/// Actual batches delivered at physical source boundaries and selector output.
///
/// Counts include repeated scan executions, after source projection/fetch and
/// before downstream filtering. They are neither disk reads nor CPU row visits.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectorWork {
    /// Actual weighted base-table leaf demand, unknown in historical receipts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_reads: Option<crate::RowReadWork>,
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

pub use crate::work::RowBlockWork as TableBlockWork;

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
    /// Distinct-row identity and boundary encoding, without multiplicity expansion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leaf_validation: Option<crate::RowEncodingWork>,
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

impl From<crate::work::RowUpdateWork> for DmlTargetWork {
    fn from(work: crate::work::RowUpdateWork) -> Self {
        Self {
            delta_update: work.delta_update,
            block_update: Some(work.block_update),
            leaf_validation: Some(work.leaf_validation),
            ..Default::default()
        }
    }
}
