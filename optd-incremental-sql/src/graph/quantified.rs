//! Preserve observable SQL UNKNOWN before Boolean-only mark decorrelation.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};

use crate::Result;

const MAX_EXPRESSION_NODES: usize = 65_536;

mod truth_relations;
pub(crate) use truth_relations::normalize;

/// Only the conjunctive spine retains the existing NULL-aware anti-join path.
#[derive(Clone, Copy)]
enum FilterContext {
    Conjunctive,
    PositiveMembership,
}

fn quantified_truth_error() -> DataFusionError {
    DataFusionError::Plan(
        "quantified subquery requires three-valued mark semantics in this expression context (R04b/R06)".into(),
    )
}

/// Bound the additional validation walk independently of parser/binder limits.
fn consume_node_budget(remaining: &mut usize) -> DataFusionResult<()> {
    *remaining = remaining.checked_sub(1).ok_or_else(|| {
        DataFusionError::Plan("quantified expression validation exceeds node budget".into())
    })?;
    Ok(())
}

/// Ordinary expressions may observe FALSE vs UNKNOWN, unlike positive filters.
fn validate_observed_value(expression: &Expr, remaining: &mut usize) -> DataFusionResult<()> {
    expression.apply(|expression| {
        consume_node_budget(remaining)?;
        if matches!(expression, Expr::InSubquery(_)) {
            return Err(quantified_truth_error());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// TRUE-membership is preserved through positive AND/OR, but never through NOT.
fn validate_filter_membership(expression: &Expr) -> DataFusionResult<()> {
    let mut remaining = MAX_EXPRESSION_NODES;
    let mut pending = vec![(expression, FilterContext::Conjunctive)];
    while let Some((expression, context)) = pending.pop() {
        consume_node_budget(&mut remaining)?;
        match expression {
            Expr::Alias(alias) => pending.push((&alias.expr, context)),
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                pending.push((&binary.right, context));
                pending.push((&binary.left, context));
            }
            Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
                pending.push((&binary.right, FilterContext::PositiveMembership));
                pending.push((&binary.left, FilterContext::PositiveMembership));
            }
            Expr::InSubquery(subquery) => {
                if subquery.negated && matches!(context, FilterContext::PositiveMembership) {
                    return Err(quantified_truth_error());
                }
                // A compared scalar value cannot hide another quantified result.
                validate_observed_value(&subquery.expr, &mut remaining)?;
            }
            _ => validate_observed_value(expression, &mut remaining)?,
        }
    }
    Ok(())
}

/// Validate analyzed plans before optimization can erase quantified provenance.
///
/// Inspect nested subquery plans too. Refusal is shared by SQL reads/writes and
/// incremental preflight/bootstrap; it neither evaluates sources nor changes IR.
pub(crate) fn validate(plan: &LogicalPlan) -> Result<()> {
    plan.apply_with_subqueries(|plan| {
        if let LogicalPlan::Filter(predicate) = plan {
            validate_filter_membership(&predicate.predicate)?;
        } else {
            for expression in plan.expressions() {
                let mut remaining = MAX_EXPRESSION_NODES;
                validate_observed_value(&expression, &mut remaining)?;
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}
