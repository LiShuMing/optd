//! Planning-only compilation and complete qualification; never owns runtime state.

mod compiler;
mod numerical;
mod preflight;
pub(crate) use numerical::columns as numerical_columns;
mod restrictions;

pub(crate) use compiler::compile;
pub use preflight::{NodePreflight, PlanBlocker, PlanPreflight};
