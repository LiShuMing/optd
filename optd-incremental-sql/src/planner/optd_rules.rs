//! Optd logical transformation and physical implementation rules.

use super::plan::{logical, IncrementalNode, IncrementalRel, Operation, PlanRule};
use super::{KeyScope, SnapshotVersion};
use crate::{Error, Result};
use optd_core::heuristics::{ApplyOrder, HeuristicsOptimizer, HeuristicsOptimizerOptions};
use optd_core::nodes::{PlanNode, PlanNodeOrGroup};
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};
use std::sync::Arc;

pub(super) struct ExpandScoped {
    pub(super) matcher: RuleMatcher<IncrementalNode>,
}
impl Rule<IncrementalNode, HeuristicsOptimizer<IncrementalNode>> for ExpandScoped {
    fn matcher(&self) -> &RuleMatcher<IncrementalNode> {
        &self.matcher
    }
    fn name(&self) -> &'static str {
        "incremental.expand_scoped_snapshot"
    }
    fn apply(
        &self,
        _: &HeuristicsOptimizer<IncrementalNode>,
        binding: IncrementalRel,
    ) -> Vec<PlanNodeOrGroup<IncrementalNode>> {
        let IncrementalNode::ScopedSnapshot {
            operator,
            scope,
            version,
            state,
        } = &binding.typ
        else {
            return vec![];
        };
        let token = binding.child_rel(0);
        let output = if *scope == KeyScope::WindowPartitions && *version == SnapshotVersion::Before
        {
            logical(
                Operation::CachedOutput {
                    state: *state,
                    schema: operator.schema(),
                },
                vec![token],
            )
        } else {
            let inputs = operator
                .inputs
                .iter()
                .enumerate()
                .map(|(port, schema)| {
                    logical(
                        Operation::IndexLookup {
                            state: *state,
                            port,
                            version: *version,
                            schema: Arc::new(schema.as_arrow().clone()),
                        },
                        vec![token.clone()],
                    )
                })
                .collect();
            logical(
                Operation::EvaluateSnapshot {
                    state: *state,
                    version: *version,
                    operator: operator.clone(),
                },
                inputs,
            )
        };
        vec![output.into()]
    }
}
pub(super) struct Implement {
    pub(super) matcher: RuleMatcher<IncrementalNode>,
}
impl Rule<IncrementalNode, HeuristicsOptimizer<IncrementalNode>> for Implement {
    fn matcher(&self) -> &RuleMatcher<IncrementalNode> {
        &self.matcher
    }
    fn name(&self) -> &'static str {
        "incremental.implement"
    }
    fn is_impl_rule(&self) -> bool {
        true
    }
    fn apply(
        &self,
        _: &HeuristicsOptimizer<IncrementalNode>,
        binding: IncrementalRel,
    ) -> Vec<PlanNodeOrGroup<IncrementalNode>> {
        let IncrementalNode::Logical(op) = &binding.typ else {
            return vec![];
        };
        vec![PlanNode {
            typ: IncrementalNode::Physical(op.clone()),
            children: binding.children.clone(),
            predicates: binding.predicates.clone(),
        }
        .into()]
    }
}
pub(super) fn matcher(typ: &IncrementalNode) -> RuleMatcher<IncrementalNode> {
    RuleMatcher::MatchDiscriminant {
        typ_discriminant: std::mem::discriminant(typ),
        children: vec![RuleMatcher::AnyMany],
    }
}
struct CheckedRule {
    inner: Arc<PlanRule>,
    failure: Arc<std::sync::Mutex<Option<String>>>,
}
fn materialized(root: &IncrementalRel) -> std::result::Result<(), String> {
    let mut pending = vec![(root, 0)];
    let mut count = 0;
    while let Some((node, depth)) = pending.pop() {
        count += 1;
        if count > 16384 || depth > 256 {
            return Err("rule result exceeds plan size/depth budget".into());
        }
        for child in &node.children {
            match child {
                PlanNodeOrGroup::PlanNode(child) => pending.push((child, depth + 1)),
                PlanNodeOrGroup::Group(_) => {
                    return Err("rule result contains an unresolved memo group".into())
                }
            }
        }
    }
    Ok(())
}
impl Rule<IncrementalNode, HeuristicsOptimizer<IncrementalNode>> for CheckedRule {
    fn matcher(&self) -> &RuleMatcher<IncrementalNode> {
        self.inner.matcher()
    }
    fn name(&self) -> &'static str {
        self.inner.name()
    }
    fn is_impl_rule(&self) -> bool {
        self.inner.is_impl_rule()
    }
    fn apply(
        &self,
        optimizer: &HeuristicsOptimizer<IncrementalNode>,
        binding: IncrementalRel,
    ) -> Vec<PlanNodeOrGroup<IncrementalNode>> {
        let output = self.inner.apply(optimizer, binding);
        let error = if output.len() > 1 {
            Some("heuristic rule returned multiple alternatives".into())
        } else {
            output.iter().find_map(|n| match n {
                PlanNodeOrGroup::PlanNode(node) => materialized(node).err(),
                PlanNodeOrGroup::Group(_) => Some("rule returned an unresolved memo group".into()),
            })
        };
        if let Some(error) = error {
            if let Ok(mut failure) = self.failure.lock() {
                *failure = Some(format!("{}: {error}", self.inner.name()));
            }
            vec![]
        } else {
            output
        }
    }
}
pub(super) fn optimize(
    root: IncrementalRel,
    rules: Vec<Arc<PlanRule>>,
    order: ApplyOrder,
) -> Result<IncrementalRel> {
    materialized(&root).map_err(Error::Invalid)?;
    let failure = Arc::new(std::sync::Mutex::new(None));
    let rules = rules
        .into_iter()
        .map(|inner| {
            Arc::new(CheckedRule {
                inner,
                failure: failure.clone(),
            }) as Arc<PlanRule>
        })
        .collect();
    let mut optimizer = HeuristicsOptimizer::new_with_rules(
        rules,
        HeuristicsOptimizerOptions {
            apply_order: order,
            enable_physical_prop_passthrough: false,
        },
        Arc::new([]),
        Arc::new([]),
    );
    let output = optimizer
        .optimize(root)
        .map_err(|e| Error::Invalid(format!("Optd incremental transformation: {e}")))?;
    let failure = failure
        .lock()
        .map_err(|_| Error::Invalid("Optd rule error channel poisoned".into()))?;
    if let Some(error) = failure.as_ref() {
        return Err(Error::Invalid(error.clone()));
    }
    Ok(output)
}
