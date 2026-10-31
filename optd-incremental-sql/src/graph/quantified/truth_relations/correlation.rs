//! Prove and extract typed equality correlations without crossing row-changing nodes.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DataFusionError, ExprSchema, NullEquality, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Operator};

use super::{comparison_type, RewriteContext};

const MAX_CORRELATION_DEPTH: usize = 64;
const MAX_CORRELATION_KEYS: usize = 16;

/// NULL policy belongs to each original correlation predicate, not the scalar IN.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Equality {
    Ordinary,
    NullSafe,
}

/// The outer lookup column and its corresponding inner relation column.
pub(super) struct CorrelationKey {
    pub(super) outer: Column,
    pub(super) inner: Column,
    pub(super) equality: Equality,
}

/// Position zero still denotes the original compared output; keys are carried
/// through projections as extra columns, never inferred from output names.
pub(super) struct PartitionInput {
    pub(super) plan: LogicalPlan,
    pub(super) keys: Vec<CorrelationKey>,
}

/// No outer reference may survive this proof or be pushed through an unproved scope.
pub(super) fn extract(
    plan: &LogicalPlan,
    outer_schema: &DFSchema,
    context: &mut RewriteContext,
) -> Result<PartitionInput> {
    let mut input = pull(plan, outer_schema, context, 0)?;
    if input.keys.is_empty() || contains_outer(&input.plan)? {
        return Err(refusal(
            "no complete equality-key proof for the remaining outer references",
        ));
    }
    if input
        .keys
        .iter()
        .any(|key| key.equality == Equality::NullSafe)
    {
        // These rows cannot satisfy an ordinary correlation against any outer
        // row. Removing them makes whole-vector NULL-safe equality sound even
        // for mixed policies, without inventing sentinel values or new state.
        let ordinary_valid = input
            .keys
            .iter()
            .filter(|key| key.equality == Equality::Ordinary)
            .map(|key| Expr::Column(key.inner.clone()).is_not_null())
            .reduce(Expr::and);
        if let Some(predicate) = ordinary_valid {
            input.plan = LogicalPlanBuilder::from(input.plan)
                .filter(predicate)?
                .build()?;
        }
    }
    Ok(input)
}

/// Members exclude NULL compared values; flags omit the comparison key entirely.
/// Thus enabling NULL equality for correlation never makes scalar NULL = NULL true.
pub(super) fn apply_join_policy(plan: LogicalPlan, keys: &[CorrelationKey]) -> Result<LogicalPlan> {
    let LogicalPlan::Join(mut join) = plan else {
        return Err(refusal(
            "expected a typed equality join for correlation policy",
        ));
    };
    if keys.iter().any(|key| key.equality == Equality::NullSafe) {
        join.null_equality = NullEquality::NullEqualsNull;
    }
    Ok(LogicalPlan::Join(join))
}

fn pull(
    plan: &LogicalPlan,
    outer_schema: &DFSchema,
    context: &mut RewriteContext,
    depth: usize,
) -> Result<PartitionInput> {
    if depth >= MAX_CORRELATION_DEPTH {
        return Err(refusal("correlation traversal exceeds the depth budget"));
    }
    // An uncorrelated subtree is unchanged. Only the outer-reference spine needs
    // row-preservation proofs, so independent inner joins/aggregates stay intact.
    if !contains_outer(plan)? {
        return Ok(PartitionInput {
            plan: plan.clone(),
            keys: vec![],
        });
    }
    match plan {
        LogicalPlan::Projection(projection) => {
            for expression in &projection.expr {
                if expression_contains_outer(expression)? {
                    return Err(refusal("outer reference in a subquery projection"));
                }
            }
            let mut input = pull(&projection.input, outer_schema, context, depth + 1)?;
            let mut expressions = projection.expr.clone();
            for key in &mut input.keys {
                let name = context.fresh()?;
                expressions.push(Expr::Column(key.inner.clone()).alias(&name));
                key.inner = Column::from_name(name);
            }
            input.plan = LogicalPlanBuilder::from(input.plan)
                .project(expressions)?
                .build()?;
            Ok(input)
        }
        LogicalPlan::Filter(filter) => {
            let mut input = pull(&filter.input, outer_schema, context, depth + 1)?;
            let mut pending = vec![filter.predicate.clone()];
            let mut inner_terms = Vec::new();
            while let Some(expression) = pending.pop() {
                if let Expr::BinaryExpr(binary) = &expression {
                    if binary.op == Operator::And {
                        pending.push(*binary.right.clone());
                        pending.push(*binary.left.clone());
                        continue;
                    }
                    let equality = match binary.op {
                        Operator::Eq => Some(Equality::Ordinary),
                        Operator::IsNotDistinctFrom => Some(Equality::NullSafe),
                        _ => None,
                    };
                    if let Some(equality) = equality {
                        let pair = match (binary.left.as_ref(), binary.right.as_ref()) {
                            (Expr::Column(inner), Expr::OuterReferenceColumn(field, outer))
                            | (Expr::OuterReferenceColumn(field, outer), Expr::Column(inner)) => {
                                Some((inner, field, outer))
                            }
                            _ => None,
                        };
                        if let Some((inner, field, outer)) = pair {
                            let inner_field = input.plan.schema().field_from_column(inner)?;
                            let outer_field = outer_schema.field_from_column(outer)?;
                            if field.data_type() != outer_field.data_type()
                                || inner_field.data_type() != outer_field.data_type()
                                || !comparison_type(inner_field.data_type())
                            {
                                return Err(refusal(
                                    "correlation keys require identical qualified equality types",
                                ));
                            }
                            if input.keys.len() >= MAX_CORRELATION_KEYS {
                                return Err(refusal("correlation key count exceeds the budget"));
                            }
                            input.keys.push(CorrelationKey {
                                outer: outer.clone(),
                                inner: inner.clone(),
                                equality,
                            });
                            continue;
                        }
                    }
                }
                if expression_contains_outer(&expression)? {
                    return Err(refusal("non-equality or computed correlation predicate"));
                }
                inner_terms.push(expression);
            }
            if let Some(predicate) = inner_terms.into_iter().reduce(Expr::and) {
                input.plan = LogicalPlanBuilder::from(input.plan)
                    .filter(predicate)?
                    .build()?;
            }
            Ok(input)
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let mut input = pull(&alias.input, outer_schema, context, depth + 1)?;
            input.plan = LogicalPlanBuilder::from(input.plan)
                .alias(alias.alias.clone())?
                .build()?;
            for key in &mut input.keys {
                key.inner.relation = Some(alias.alias.clone());
            }
            Ok(input)
        }
        _ => Err(refusal(&format!(
            "unproved correlation boundary: {}",
            plan.display()
        ))),
    }
}

fn refusal(reason: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "quantified three-valued correlation: {reason} (R04b/R06)"
    ))
}

/// Include expression subqueries, whose references belong to an unproved nested scope.
pub(super) fn contains_outer(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        for expression in node.expressions() {
            if expression_contains_outer(&expression)? {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

fn expression_contains_outer(expression: &Expr) -> Result<bool> {
    let mut found = false;
    expression.apply(|expression| {
        if matches!(expression, Expr::OuterReferenceColumn(_, _)) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(found)
}
