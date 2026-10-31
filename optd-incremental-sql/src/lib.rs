//! Typed, multi-table SQL maintenance and atomic host-local recovery.
//!
//! Linear operators and inner joins propagate signed row changes; nonlinear
//! operators explicitly report local recomputation. No whole-query fallback is
//! hidden behind the incremental API. See `docs/src/incremental/sql_execution.md`.

#![deny(missing_docs)]

mod accumulator_state;
mod bag;
pub mod database;
mod durable_sql_view;
mod engine;
mod error;
mod executor;
mod graph;
mod group_state;
mod keyed_state;
mod metrics;
pub mod planner;
mod row_blocks;
mod source_state;
pub mod source_vector;
mod state_directory;
pub mod store;
mod work;

#[cfg(feature = "iceberg")]
pub mod coordinator;
#[cfg(feature = "iceberg")]
pub mod iceberg;

pub use bag::MAX_MATERIALIZED_ROWS;
pub use durable_sql_view::DurableSqlView;
pub use engine::{
    GroupWork, KeyedWork, NodeStats, PreparedSqlRefresh, RefreshStats, SqlView, TableChange,
};
pub use error::{Error, Result};
pub use metrics::{AccumulatorWork, DistinctWork, ValueIndexWork};
pub use planner::Strategy;
pub use work::{
    CacheRelationWork, RefreshWork, RelationWork, RowEncodingWork, RowReadWork, SourceStateWork,
    StateCopyWork,
};

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
