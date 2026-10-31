//! SQL results and durable commit statistics, independent of session execution.

use crate::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use super::dml_work::DmlExecutionWork;

/// DML selection and emitted changes, distinct from incremental operator work.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DmlWork {
    /// Expanded target rows inserted/deleted/updated by this statement.
    pub affected_rows: usize,
    /// Rows in referenced source relations; not a physical scan measurement.
    pub source_relation_rows: usize,
    /// Distinct nonzero full-row changes after consolidation.
    pub net_changed_rows: usize,
    /// Actual selector delivery and target replacement work, when measured.
    /// Missing in historical receipts means unknown; retries retain the original.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<DmlExecutionWork>,
}

/// Durable outcome of one database transaction; retained for identified retries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitReport {
    /// Database epoch published by this transaction.
    pub epoch: u64,
    /// Sum of affected row counts across DML statements.
    pub affected_rows: usize,
    /// Statement-local source and change accounting.
    pub dml: Vec<DmlWork>,
    /// Existing structured refresh statistics, indexed by view name.
    pub maintenance: BTreeMap<String, Value>,
}

/// Result of a SQL statement or an identified write transaction.
pub struct SqlResponse {
    /// Actual base-table chunk demand for SELECT; excludes views and other SQL work.
    pub table_reads: Option<crate::RowReadWork>,
    /// Committed epoch (or the original outcome's epoch for a replayed request).
    pub epoch: u64,
    /// SELECT output, read from a single database/session snapshot.
    pub rows: Option<RecordBatch>,
    /// Durable commit information, absent for reads and uncommitted statements.
    pub commit: Option<CommitReport>,
    /// True only when a persisted request outcome was returned without reexecution.
    pub replayed: bool,
}

impl SqlResponse {
    pub(super) fn empty(epoch: u64) -> Self {
        Self {
            epoch,
            table_reads: None,
            rows: None,
            commit: None,
            replayed: false,
        }
    }
    pub(super) fn committed(report: CommitReport, replayed: bool) -> Self {
        Self {
            epoch: report.epoch,
            table_reads: None,
            rows: None,
            commit: Some(report),
            replayed,
        }
    }
}

/// Render typed rows for JSON CLI inspection; durable values remain Arrow IPC.
pub fn display_rows(batch: &RecordBatch) -> Result<Vec<Vec<String>>> {
    (0..batch.num_rows())
        .map(|i| {
            batch
                .columns()
                .iter()
                .map(|a| Ok(ScalarValue::try_from_array(a, i)?.to_string()))
                .collect()
        })
        .collect()
}
