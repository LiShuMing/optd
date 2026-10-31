//! SQL writes and synchronous materialized views sharing one durable database root.
//!
//! One session owns the host-local writer lock. SQL writes are compiled into row
//! images and delegated to the existing incremental planner and executor.

mod dml_compiler;
mod response;
mod sql;
mod sql_session;
mod store;
mod transaction_workspace;

pub use response::{display_rows, CommitReport, DmlWork, SqlResponse};
pub use sql_session::SqlSession;
