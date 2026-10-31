//! SQL writes and synchronous materialized views sharing one durable database root.
//!
//! One session owns the host-local writer lock. SQL writes are compiled into row
//! images and delegated to the existing incremental planner and executor.

mod dml_compiler;
mod dml_work;
mod response;
mod selector_work;
mod sql;
mod sql_session;
mod store;
mod store_work;
mod table_provider;
mod table_rows;
mod transaction_workspace;

pub use dml_work::{DmlExecutionWork, DmlTargetWork, SelectorWork, TableBlockWork};
pub use response::{display_rows, CommitReport, DmlWork, SqlResponse};
pub use sql_session::SqlSession;
pub use store_work::{
    FileWork, PayloadIoWork, PayloadWork, RecoveryWork, StoreWork, TableBootstrapWork,
    TableMaterializationWork,
};
