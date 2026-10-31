//! Read-only qualification records, independent of executable or persisted programs.

use std::fmt;

use serde::Serialize;

use crate::planner::{RuleIdentity, Strategy};

/// One normalized logical node and the physical behavior actually selected for it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodePreflight {
    /// Stable postorder identity; repeated graph inputs retain their original ports.
    pub id: usize,
    /// Typed logical operator, including expressions and casts.
    pub operator: String,
    /// Original dependency identities.
    pub inputs: Vec<usize>,
    /// Selected transformer; absent when matching/qualification failed.
    pub rule: Option<RuleIdentity>,
    /// Final physical behavior; absent when compilation failed.
    pub strategy: Option<Strategy>,
}

/// One blocked node; multiple independent nodes are never collapsed to the first error.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PlanBlocker {
    /// Stable identity matching `planned_nodes`.
    pub id: usize,
    /// Complete typed logical operator.
    pub operator: String,
    /// Selected/failing rule name, or a named compilation phase.
    pub rule: String,
    /// Matching, verification, lowering or strict-policy failure boundary.
    pub phase: String,
    /// Concrete selected-algorithm limitation or original compilation error.
    pub reason: String,
    /// Typed function/key/scope context for interpreting the refusal.
    pub details: Vec<String>,
}

/// Schema-bound qualification of every node in an admitted SQL graph.
///
/// This is a diagnostic, not executable input or runtime correctness evidence.
/// Parse/binding/normalization failures return errors before such a graph exists.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PlanPreflight {
    /// True exactly when compilation succeeds without complete-node replacements.
    pub strict_plan_eligible: bool,
    /// Nodes in deterministic dependency order, including successful siblings.
    pub planned_nodes: Vec<NodePreflight>,
    /// Every failed compilation or complete-node replacement in that order.
    pub blockers: Vec<PlanBlocker>,
}

impl fmt::Display for PlanPreflight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} blocking node(s)", self.blockers.len())?;
        for blocker in &self.blockers {
            write!(
                f,
                "; #{} {} [{} / {}]: {}",
                blocker.id, blocker.operator, blocker.rule, blocker.phase, blocker.reason
            )?;
            for detail in &blocker.details {
                write!(f, " ({detail})")?;
            }
        }
        Ok(())
    }
}
