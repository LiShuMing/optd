//! Typed, multi-table SQL maintenance and atomic host-local recovery.
//!
//! Linear operators and inner joins propagate signed row changes; nonlinear
//! operators explicitly report local recomputation. No whole-query fallback is
//! hidden behind the incremental API. See `docs/src/incremental/sql_execution.md`.

mod bag;
mod engine;
mod group_state;
pub mod store;

pub use bag::MAX_MATERIALIZED_ROWS;
pub use engine::{
    DurableSqlView, GroupWork, NodeStats, PreparedSqlRefresh, RefreshStats, SqlView, Strategy,
    TableChange,
};

/// Failure before a candidate can become visible or a checkpoint is accepted.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Rejected SQL, input, state transition or resource requirement.
    #[error("{0}")]
    Invalid(String),
    /// Signed row multiplicity cannot be represented.
    #[error("signed row multiplicity overflow")]
    Overflow,
    /// An identified graph operator failed before publication.
    #[error("operator {id} ({plan}): {source}")]
    Operator {
        /// Stable graph node identity.
        id: usize,
        /// Logical operator description.
        plan: String,
        /// Original typed failure.
        #[source]
        source: Box<Error>,
    },
    /// SQL planning or local operator execution failed.
    #[error(transparent)]
    DataFusion(#[from] datafusion::common::DataFusionError),
    /// Arrow representation or IPC operation failed.
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    /// Local durability operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Checkpoint metadata could not be decoded.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Result returned by the typed SQL backend.
pub type Result<T> = std::result::Result<T, Error>;
