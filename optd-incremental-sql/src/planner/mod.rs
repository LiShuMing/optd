//! Deterministic SQL maintenance rules and their verified executable plans.
//!
//! Rules construct signed-expression programs rather than assigning diagnostic
//! strategy labels. Custom rules are trusted algebraic extensions: the verifier
//! checks their shape and types, while independent SQL comparisons must establish
//! their semantic identities.

mod expr;
mod rules;

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use serde::Serialize;

use crate::{Error, Result};

pub use expr::{DeltaExpr, KeyScope, Maintenance, SnapshotVersion, MAX_EXPR_DEPTH, MAX_EXPR_NODES};

/// How an operator produces its output change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Strategy {
    /// Execute a checked signed-expression program.
    NativeDelta,
    /// Re-evaluate only groups whose indexed input multiset changed.
    ChangedGroupRecompute,
    /// Evaluate only indexed affected keys/partitions, subtracting their snapshots.
    KeyedRecompute,
    /// Re-evaluate this operator over maintained child relations, then diff.
    OperatorRecompute,
}

/// Read-only facts available when rewriting one logical node.
#[derive(Clone, Copy, Debug)]
pub struct RuleContext<'a> {
    node_id: usize,
    plan: &'a LogicalPlan,
    inputs: &'a [usize],
    ordered: bool,
    group_eligible: bool,
}

impl<'a> RuleContext<'a> {
    pub(crate) fn new(
        node_id: usize,
        plan: &'a LogicalPlan,
        inputs: &'a [usize],
        ordered: bool,
        group_eligible: bool,
    ) -> Self {
        Self {
            node_id,
            plan,
            inputs,
            ordered,
            group_eligible,
        }
    }

    /// The original typed operator whose maintenance is being compiled.
    pub fn logical_plan(&self) -> &'a LogicalPlan {
        self.plan
    }

    /// Stable postorder identity within this logical graph.
    pub fn node_id(&self) -> usize {
        self.node_id
    }

    /// Original graph-child identities, in logical input-port order.
    pub fn inputs(&self) -> &'a [usize] {
        self.inputs
    }

    /// Whether this node retains a sequence used by an ordered consumer.
    pub fn ordered(&self) -> bool {
        self.ordered
    }

    /// Whether the fixed SQL context has qualified this aggregate's group index.
    pub fn group_eligible(&self) -> bool {
        self.group_eligible
    }

    /// Whether this operator has a proven, typed dependency closure for a scope.
    pub fn supports_key_scope(&self, scope: KeyScope) -> Result<bool> {
        Ok(!self.ordered && crate::keyed_state::key_expressions(self.plan, scope)?.is_some())
    }
}

/// A separately registered transformer from a logical node to maintenance IR.
///
/// Matching and rewriting must use only the supplied immutable planning facts.
/// The methods must not mutate sources, cached relations or checkpoint storage.
pub trait IncrementalRule: Send + Sync {
    /// Stable, unique registry name used in explanations and failures.
    fn name(&self) -> &str;

    /// Version of this rule's implementation contract.
    fn version(&self) -> u32 {
        1
    }

    /// Higher priorities are tried first; equal priorities sort by stable name.
    fn priority(&self) -> i32 {
        0
    }

    /// Determine whether this rule applies without modifying the graph.
    fn matches(&self, context: &RuleContext<'_>) -> Result<bool>;

    /// Construct the actual executable replacement for a matched node.
    fn rewrite(&self, context: &RuleContext<'_>) -> Result<RuleRewrite>;
}

/// The declarative result of one matched transformer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RuleRewrite {
    /// Executable maintenance program, verified before bootstrap.
    pub maintenance: Maintenance,
    /// Human-readable explanation of the selected algorithm and its limits.
    pub reason: String,
}

/// Rule metadata captured immutably when a rule is registered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RuleIdentity {
    /// Unique stable registry name.
    pub name: String,
    /// Declared implementation-contract version.
    pub version: u32,
    /// Registration-time selection priority.
    pub priority: i32,
}

#[derive(Clone)]
struct RegisteredRule {
    identity: RuleIdentity,
    implementation: Arc<dyn IncrementalRule>,
}

impl RegisteredRule {
    fn new(implementation: Arc<dyn IncrementalRule>) -> Self {
        Self {
            identity: RuleIdentity {
                name: implementation.name().to_owned(),
                version: implementation.version(),
                priority: implementation.priority(),
            },
            implementation,
        }
    }
}

/// An immutable, deterministically ordered collection of maintenance rules.
#[derive(Clone)]
pub struct IncrementalOptimizer {
    rules: Vec<RegisteredRule>,
    require_incremental: bool,
}

impl std::fmt::Debug for IncrementalOptimizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalOptimizer")
            .field("require_incremental", &self.require_incremental)
            .field(
                "rules",
                &self
                    .rules
                    .iter()
                    .map(|rule| &rule.identity)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Default for IncrementalOptimizer {
    fn default() -> Self {
        let mut optimizer = Self {
            require_incremental: false,
            rules: rules::builtins()
                .into_iter()
                .map(RegisteredRule::new)
                .collect(),
        };
        optimizer.sort_rules();
        optimizer
    }
}

impl IncrementalOptimizer {
    /// Start without any implementation, so unmatched nodes are explicit errors.
    pub fn empty() -> Self {
        Self {
            rules: vec![],
            require_incremental: false,
        }
    }

    /// Reject complete-operator recomputation before bootstrap.
    /// This does not certify delta-sized source IO, caches or checkpoint writes.
    pub fn require_incremental(mut self) -> Self {
        self.require_incremental = true;
        self
    }

    /// Return a registry containing this additional uniquely named transformer.
    pub fn with_rule(mut self, rule: Arc<dyn IncrementalRule>) -> Result<Self> {
        let rule = RegisteredRule::new(rule);
        if rule.identity.name.trim().is_empty() {
            return Err(Error::RuleRegistry(
                "rule registration requires a nonempty stable name".into(),
            ));
        }
        if self
            .rules
            .iter()
            .any(|existing| existing.identity.name == rule.identity.name)
        {
            return Err(Error::RuleRegistry(format!(
                "duplicate rule name {:?}",
                rule.identity.name
            )));
        }
        self.rules.push(rule);
        self.sort_rules();
        Ok(self)
    }

    /// Return a registry without a named rule; misspellings are errors.
    pub fn without_rule(mut self, name: &str) -> Result<Self> {
        let index = self
            .rules
            .iter()
            .position(|rule| rule.identity.name == name)
            .ok_or_else(|| Error::RuleRegistry(format!("unknown rule name {name:?}")))?;
        self.rules.remove(index);
        Ok(self)
    }

    fn sort_rules(&mut self) {
        self.rules.sort_by(|left, right| {
            right
                .identity
                .priority
                .cmp(&left.identity.priority)
                .then_with(|| left.identity.name.cmp(&right.identity.name))
        });
    }

    pub(crate) fn rewrite_node(&self, context: &RuleContext<'_>) -> Result<MaintenanceNode> {
        for rule in &self.rules {
            let matched = rule.implementation.matches(context).map_err(|source| {
                rewrite_error(context.node_id, &rule.identity, "match", source)
            })?;
            if !matched {
                continue;
            }
            let replacement = rule.implementation.rewrite(context).map_err(|source| {
                rewrite_error(context.node_id, &rule.identity, "rewrite", source)
            })?;
            expr::verify(context, &replacement.maintenance).map_err(|source| {
                rewrite_error(context.node_id, &rule.identity, "verify", source)
            })?;
            if self.require_incremental && matches!(replacement.maintenance, Maintenance::Recompute)
            {
                return Err(rewrite_error(
                    context.node_id,
                    &rule.identity,
                    "policy",
                    Error::Invalid(
                        "strict incremental planning forbids complete-operator recomputation"
                            .into(),
                    ),
                ));
            }
            return Ok(MaintenanceNode {
                id: context.node_id,
                inputs: context.inputs.to_vec(),
                operator: context.plan.display().to_string(),
                ordered: context.ordered,
                rule: rule.identity.clone(),
                maintenance: replacement.maintenance,
                reason: replacement.reason,
            });
        }
        Err(Error::Rewrite {
            node: context.node_id,
            rule: "<unmatched>".into(),
            phase: "match",
            source: Box::new(Error::Invalid(format!(
                "no incremental rule matched operator {}",
                context.plan.display()
            ))),
        })
    }
}

fn rewrite_error(node: usize, rule: &RuleIdentity, phase: &'static str, source: Error) -> Error {
    Error::Rewrite {
        node,
        rule: rule.name.clone(),
        phase,
        source: Box::new(source),
    }
}

/// One verified executable implementation in a postorder maintenance graph.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MaintenanceNode {
    /// Stable identity matching its original logical node.
    pub id: usize,
    /// Original input nodes, preserving local-port order and repeated inputs.
    pub inputs: Vec<usize>,
    /// Diagnostic description of the original typed logical operator.
    pub operator: String,
    /// Whether sequence changes must invalidate ordering-sensitive consumers.
    pub ordered: bool,
    /// Transformer whose program actually executes for this node.
    pub rule: RuleIdentity,
    /// Verified executable replacement.
    pub maintenance: Maintenance,
    /// Why the rule selected this implementation.
    pub reason: String,
}

/// Immutable diagnostic and executable plan; serialized output is not executable input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct IncrementalPlan {
    nodes: Vec<MaintenanceNode>,
}

impl IncrementalPlan {
    pub(crate) fn new(nodes: Vec<MaintenanceNode>) -> Result<Self> {
        if nodes.is_empty() {
            return Err(Error::Rewrite {
                node: 0,
                rule: "<plan>".into(),
                phase: "verify",
                source: Box::new(Error::Invalid("empty incremental plan".into())),
            });
        }
        for (position, node) in nodes.iter().enumerate() {
            if node.id != position || node.inputs.iter().any(|input| *input >= position) {
                return Err(rewrite_error(
                    node.id,
                    &node.rule,
                    "verify",
                    Error::Invalid(
                        "maintenance nodes must have contiguous postorder ids and earlier inputs"
                            .into(),
                    ),
                ));
            }
        }
        // The nonempty check above proves that the final postorder node exists.
        let root = nodes.len() - 1;
        let mut reachable = BTreeSet::new();
        let mut pending = vec![root];
        while let Some(id) = pending.pop() {
            if reachable.insert(id) {
                pending.extend(nodes[id].inputs.iter().copied());
            }
        }
        if reachable.len() != nodes.len() {
            return Err(rewrite_error(
                root,
                &nodes[root].rule,
                "verify",
                Error::Invalid("incremental plan contains nodes unreachable from its root".into()),
            ));
        }
        Ok(Self { nodes })
    }

    /// Verified nodes in dependency-before-consumer order.
    pub fn nodes(&self) -> &[MaintenanceNode] {
        &self.nodes
    }

    /// Identity of the final postorder node.
    pub fn root(&self) -> usize {
        // Construction rejects empty plans and callers cannot mutate this vector.
        self.nodes.len() - 1
    }

    /// Explain selected rules and the expression programs that actually execute.
    pub fn explain(&self) -> String {
        let mut explanation = String::new();
        for node in &self.nodes {
            explanation.push_str(&format!(
                "#{} {} inputs={:?} ordered={} rule={}@{} priority={} strategy={:?}\n  {:?}\n  reason: {}\n",
                node.id,
                node.operator,
                node.inputs,
                node.ordered,
                node.rule.name,
                node.rule.version,
                node.rule.priority,
                node.maintenance.strategy(),
                node.maintenance,
                node.reason,
            ));
        }
        explanation
    }
}

#[cfg(test)]
mod tests;
