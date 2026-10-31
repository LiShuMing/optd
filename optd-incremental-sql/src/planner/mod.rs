//! SQL maintenance rules compiled into verified Optd logical/physical plans.
//!
//! Rules construct signed-expression programs rather than assigning diagnostic
//! strategy labels. Custom rules are trusted algebraic extensions: the verifier
//! checks their shape and types, while independent SQL comparisons must establish
//! their semantic identities.

mod capability;
mod expr;
pub(crate) mod lower;
mod optd_rules;
pub mod plan;
mod rules;
mod verify_plan;

mod incremental_optimizer;

pub use expr::{DeltaExpr, KeyScope, Maintenance, SnapshotVersion, MAX_EXPR_DEPTH, MAX_EXPR_NODES};
pub use incremental_optimizer::{
    IncrementalOptimizer, IncrementalPlan, IncrementalRule, MaintenanceNode, RuleContext,
    RuleIdentity, RuleRewrite, Strategy,
};

#[cfg(test)]
mod tests;
