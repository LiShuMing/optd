//! Typed, multi-table SQL maintenance and atomic host-local recovery.
//!
//! Linear operators and inner joins propagate signed row changes; nonlinear
//! operators explicitly report local recomputation. No whole-query fallback is
//! hidden behind the incremental API. See `docs/src/incremental/sql_execution.md`.

mod accumulator_state;
mod bag;
mod engine;
mod executor;
mod group_state;
mod keyed_state;
mod metrics;
pub mod planner;
pub mod source_vector;
pub mod store;

#[cfg(feature = "iceberg")]
pub mod coordinator;
#[cfg(feature = "iceberg")]
pub mod iceberg;

pub use bag::MAX_MATERIALIZED_ROWS;
pub use engine::{
    DurableSqlView, GroupWork, KeyedWork, NodeStats, PreparedSqlRefresh, RefreshStats, SqlView,
    TableChange,
};
pub use metrics::AccumulatorWork;
pub use planner::Strategy;

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

#[cfg(all(test, feature = "iceberg"))]
mod dependency_contract {
    #[test]
    fn iceberg_and_sql_use_the_same_arrow_schema_type() -> Result<(), Box<dyn std::error::Error>> {
        use ::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};

        let source = Schema::builder()
            .with_schema_id(1)
            .with_fields([std::sync::Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()?;
        let arrow: ArrowSchema = ::iceberg::arrow::schema_to_arrow_schema(&source)?;
        assert_eq!(arrow.field(0).data_type(), &DataType::Int32);
        Ok(())
    }
}
