//! Typed failures shared by SQL planning, maintenance and recovery.

/// Failure before a candidate can become visible or a checkpoint is accepted.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Rejected SQL, input, state transition or resource requirement.
    #[error("{0}")]
    Invalid(String),
    /// An invalid or conflicting rule registry definition.
    #[error("incremental rule registry: {0}")]
    RuleRegistry(String),
    /// A rule failed while matching, rewriting, or verifying a logical node.
    #[error("incremental node {node}, rule {rule}, {phase}: {source}")]
    Rewrite {
        /// Stable logical node identity.
        node: usize,
        /// Registered rule identity, or the unmatched-request description.
        rule: String,
        /// Planning phase in which the error occurred.
        phase: &'static str,
        /// Original failure, retained for diagnostics.
        #[source]
        source: Box<Error>,
    },
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
    /// An admitted typed Iceberg source could not be loaded or scanned.
    #[cfg(feature = "iceberg")]
    #[error(transparent)]
    Iceberg(#[from] ::iceberg::Error),
}

/// Result returned by the typed SQL backend.
pub type Result<T> = std::result::Result<T, Error>;

/// Construct an input/state rejection without erasing underlying IO errors.
pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
