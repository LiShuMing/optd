//! Track numerical result lineage without weakening keys or exact source identities.

use crate::graph::Node;
use crate::Result;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use std::collections::BTreeSet;

fn affected(expression: &Expr, plan: &LogicalPlan, inputs: &[BTreeSet<usize>]) -> Result<bool> {
    let mut found = false;
    expression.apply(|expr| {
        if let Expr::Column(column) = expr {
            found |= plan.inputs().iter().zip(inputs).any(|(input, columns)| {
                input
                    .schema()
                    .index_of_column(column)
                    .is_ok_and(|index| columns.contains(&index))
            });
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

/// Only moment outputs and their same-argument means have a numerical contract.
/// An unrelated exact integer AVG in the same aggregate keeps exact comparison.
fn moment_output(expression: &Expr, aggregates: &[Expr]) -> bool {
    fn function(
        mut expression: &Expr,
    ) -> Option<&datafusion::logical_expr::expr::AggregateFunction> {
        while let Expr::Alias(alias) = expression {
            expression = &alias.expr;
        }
        match expression {
            Expr::AggregateFunction(function) => Some(function),
            _ => None,
        }
    }
    let Some(output) = function(expression) else {
        return false;
    };
    matches!(output.func.name(), "stddev" | "stddev_pop")
        || (output.func.name() == "avg"
            && aggregates.iter().filter_map(function).any(|peer| {
                matches!(peer.func.name(), "stddev" | "stddev_pop")
                    && peer.params.args == output.params.args
            }))
}

/// Numerical lineage concerns only floating result values; keys/counts remain exact.
pub(crate) fn columns(nodes: &[Node]) -> Result<Vec<usize>> {
    let mut states: Vec<BTreeSet<usize>> = Vec::with_capacity(nodes.len());
    for node in nodes {
        let plan = &node.plan;
        let inputs = node
            .inputs
            .iter()
            .map(|id| states[*id].clone())
            .collect::<Vec<_>>();
        let mut output = BTreeSet::new();
        match plan {
            LogicalPlan::Aggregate(aggregate) => {
                for (index, expr) in aggregate.aggr_expr.iter().enumerate() {
                    if (moment_output(expr, &aggregate.aggr_expr) || affected(expr, plan, &inputs)?)
                        && matches!(
                            expr.get_type(aggregate.input.schema())?,
                            DataType::Float32 | DataType::Float64
                        )
                    {
                        output.insert(aggregate.group_expr.len() + index);
                    }
                }
            }
            LogicalPlan::Projection(projection) => {
                for (index, expr) in projection.expr.iter().enumerate() {
                    if affected(expr, plan, &inputs)? {
                        output.insert(index);
                    }
                }
            }
            LogicalPlan::Union(_) | LogicalPlan::SubqueryAlias(_) => {
                for input in &inputs {
                    output.extend(input);
                }
            }
            _ => {
                for (index, column) in plan.schema().columns().into_iter().enumerate() {
                    if affected(&Expr::Column(column), plan, &inputs)? {
                        output.insert(index);
                    }
                }
            }
        }
        // Boolean/integer predicates and ranks never get a numerical comparison exemption.
        output.retain(|index| {
            matches!(
                plan.schema().field(*index).data_type(),
                DataType::Float32 | DataType::Float64
            )
        });
        states.push(output);
    }
    Ok(states.pop().unwrap_or_default().into_iter().collect())
}
