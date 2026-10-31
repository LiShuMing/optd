//! The single schema-bound SQL/rule/Optd compiler shared by preflight and bootstrap.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, Volatility};
use datafusion::prelude::SessionContext;

use super::{NodePreflight, PlanBlocker, PlanPreflight};
use crate::graph::{self, Node};
use crate::planner::plan::{self, Operation};
use crate::planner::{
    IncrementalOptimizer, IncrementalPlan, RuleContext, SnapshotVersion, Strategy,
};
use crate::{Error, Result, SqlView};

/// A complete diagnostic graph plus an executable plan only if every node compiled.
pub(crate) struct Compilation {
    pub(crate) report: PlanPreflight,
    nodes: Vec<Node>,
    // Ready and rejected are mutually exclusive; no partial plan can be returned.
    plan: Result<IncrementalPlan>,
}

impl Compilation {
    /// Diagnostic continuation cannot escape as a partially executable plan.
    pub(crate) fn into_plan(self, strict: bool) -> Result<(Vec<Node>, IncrementalPlan)> {
        if strict && !self.report.strict_plan_eligible {
            // Compilation sets eligibility from blocker emptiness; keep the
            // error path fallible even if that invariant is changed later.
            let first =
                self.report.blockers.first().ok_or_else(|| {
                    Error::Invalid("ineligible plan has no recorded blocker".into())
                })?;
            // Keep the existing first-node Rewrite envelope for callers that
            // classify its rule/phase. Its source now carries the whole report.
            let phase = match first.phase.as_str() {
                "match" => "match",
                "rewrite" => "rewrite",
                "verify" => "verify",
                "policy" => "policy",
                _ => "compile",
            };
            return Err(Error::Rewrite {
                node: first.id,
                rule: first.rule.clone(),
                phase,
                source: Box::new(Error::PlanRejected {
                    report: Box::new(self.report),
                }),
            });
        }
        // Preserve existing non-strict typed rule failures for API callers.
        Ok((self.nodes, self.plan?))
    }
}

/// Gather all node failures without physical source-row execution or state updates.
pub(crate) async fn compile(
    ctx: &SessionContext,
    sql: &str,
    optimizer: &IncrementalOptimizer,
) -> Result<Compilation> {
    let analyzed = ctx.state().create_logical_plan(sql).await?;
    // Check before optimization can fold a stable function into a literal.
    analyzed.apply_with_subqueries(|plan| {
        for expression in plan.expressions() {
            expression.apply(|expression| {
                if matches!(expression, Expr::ScalarFunction(function)
                    if function.func.signature().volatility != Volatility::Immutable)
                {
                    return Err(datafusion::common::DataFusionError::Plan(
                        "non-immutable SQL function".into(),
                    ));
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let analyzed = graph::normalize_quantified_truth(analyzed)?;
    graph::validate_quantified_subqueries(&analyzed)?;
    let logical = graph::normalize_grouping_sets(graph::normalize_distinct(
        ctx.state().optimize(&analyzed)?,
    )?)?;
    let mut nodes = Vec::new();
    let root = graph::add_node(&logical, &mut nodes, &mut HashMap::new())?;
    if root + 1 != nodes.len() {
        return Err(Error::Invalid(
            "query root was not last in postorder".into(),
        ));
    }
    // Only policy is deferred. Failed matches/rewrites/verifications still stop
    // selection for that node; no later rule is tried to hide such a failure.
    let inspection = optimizer.for_preflight();
    let mut after_bindings: Vec<plan::IncrementalRel> = Vec::with_capacity(nodes.len());
    let mut maintenance = Vec::with_capacity(nodes.len());
    let mut programs = Vec::with_capacity(nodes.len());
    let mut report = PlanPreflight {
        strict_plan_eligible: false,
        planned_nodes: Vec::with_capacity(nodes.len()),
        blockers: Vec::new(),
    };
    let mut first_error = None;
    for (id, node) in nodes.iter().enumerate() {
        let children = node
            .inputs
            .iter()
            .map(|input| after_bindings[*input].clone())
            .collect::<Vec<_>>();
        let mut diagnostic = NodePreflight {
            id,
            inputs: node.inputs.clone(),
            operator: node.plan.display().to_string(),
            rule: None,
            strategy: None,
        };
        let result = (|| {
            let grouped = SqlView::admits_group_index(ctx, &node.plan)?;
            let accumulator = crate::accumulator_state::eligible(ctx, &node.plan)?;
            let context = RuleContext::new(id, &node.plan, &node.inputs, node.ordered, grouped)
                .with_accumulator_eligible(accumulator);
            let selected = inspection.rewrite_node(&context)?;
            diagnostic.rule = Some(selected.rule.clone());
            let program = crate::planner::lower::compile(
                &context,
                &selected.maintenance,
                children.clone(),
                optimizer.plan_rules(),
            )?;
            let strategy = program.strategy();
            diagnostic.strategy = Some(strategy);
            // Build the next symbolic binding before recording policy refusal;
            // a binding failure is the sole diagnostic for that failed node.
            let after =
                crate::planner::lower::after_input(id, &node.plan, node.ordered, children.clone())?;
            if strategy == Strategy::OperatorRecompute {
                report.blockers.push(PlanBlocker {
                    id,
                    operator: diagnostic.operator.clone(),
                    rule: selected.rule.name.clone(),
                    phase: "policy".into(),
                    reason: if selected.maintenance.strategy() == Strategy::OperatorRecompute {
                        selected.reason.clone()
                    } else {
                        "final physical program requires complete-operator replacement".into()
                    },
                    details: super::restrictions::details(&context)?,
                });
            }
            Ok::<_, Error>((selected, program, after))
        })();
        match result {
            Ok((selected, program, after)) => {
                maintenance.push(selected);
                programs.push(program);
                after_bindings.push(after);
            }
            Err(error) => {
                diagnostic.strategy = None;
                let (rule, phase) = match &error {
                    Error::Rewrite { rule, phase, .. } => (rule.clone(), (*phase).to_owned()),
                    _ => (
                        diagnostic
                            .rule
                            .as_ref()
                            .map_or("<compile>", |rule| &rule.name)
                            .to_owned(),
                        "compile".into(),
                    ),
                };
                report.blockers.push(PlanBlocker {
                    id,
                    operator: diagnostic.operator.clone(),
                    rule,
                    phase,
                    reason: error.to_string(),
                    details: vec![],
                });
                if first_error.is_none() {
                    first_error = Some(error);
                }
                // This typed placeholder permits parent/sibling diagnostics only.
                // first_error prevents installing or executing any partial graph.
                after_bindings.push(plan::logical(
                    Operation::SnapshotRead {
                        input: id,
                        schema: Arc::new(node.plan.schema().as_arrow().clone()),
                        version: SnapshotVersion::After,
                    },
                    vec![],
                ));
            }
        }
        report.planned_nodes.push(diagnostic);
    }
    report.strict_plan_eligible = report.blockers.is_empty();
    let plan = match first_error {
        Some(error) => Err(error),
        None => {
            let mut plan = IncrementalPlan::new(maintenance)?;
            plan.install_programs(programs)?;
            Ok(plan)
        }
    };
    Ok(Compilation {
        report,
        nodes,
        plan,
    })
}
