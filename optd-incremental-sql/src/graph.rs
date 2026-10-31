//! Normalize and validate the typed SQL graph before rule selection.

use crate::{Error, Result};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Distinct, Expr, LogicalPlan, LogicalPlanBuilder, Volatility};
use std::collections::HashMap;

#[derive(Clone)]
pub(crate) struct Node {
    pub(crate) plan: LogicalPlan,
    pub(crate) inputs: Vec<usize>,
    pub(crate) ordered: bool,
}

// DISTINCT is set membership, represented by the existing indexed group state.
pub(crate) fn normalize_distinct(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(plan
        .transform_up(|plan| {
            if let LogicalPlan::Distinct(Distinct::All(input)) = &plan {
                if !input.schema().fields().is_empty() {
                    let mut grouped = LogicalPlanBuilder::from(input.as_ref().clone())
                        .aggregate(
                            input
                                .schema()
                                .columns()
                                .into_iter()
                                .map(Expr::Column)
                                .collect::<Vec<_>>(),
                            Vec::<Expr>::new(),
                        )?
                        .build()?;
                    if grouped.schema().as_arrow() != plan.schema().as_arrow()
                        || grouped.schema().iter().ne(plan.schema().iter())
                    {
                        return Err(datafusion::common::DataFusionError::Plan(
                            "DISTINCT normalization changed fields or qualifiers".into(),
                        ));
                    }
                    // Grouping strengthens uniqueness dependencies. Retaining the original
                    // weaker dependencies preserves the exact parent schema contract.
                    if let LogicalPlan::Aggregate(aggregate) = &mut grouped {
                        aggregate.schema = plan.schema().clone();
                    }
                    return Ok(Transformed::yes(grouped));
                }
            }
            Ok(Transformed::no(plan))
        })?
        .data)
}

fn check_expr(expr: &Expr) -> datafusion::common::Result<()> {
    expr.apply(|e| {
        let forbidden = matches!(e, Expr::ScalarSubquery(_) | Expr::Exists(_) |
            Expr::InSubquery(_) | Expr::OuterReferenceColumn(_, _) | Expr::Placeholder(_))
            || matches!(e, Expr::ScalarFunction(f) if f.func.signature().volatility != Volatility::Immutable);
        if forbidden {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "expression cannot be maintained: {e}")));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

pub(crate) fn add_node(
    plan: &LogicalPlan,
    nodes: &mut Vec<Node>,
    known: &mut HashMap<LogicalPlan, usize>,
) -> Result<usize> {
    if let Some(id) = known.get(plan) {
        return Ok(*id);
    }
    match plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Unnest(_) => {}
        _ => {
            return Err(Error::Invalid(format!(
                "unsupported SQL operator: {}",
                plan.display()
            )))
        }
    }
    for e in plan.expressions() {
        check_expr(&e)?;
    }
    let inputs = plan
        .inputs()
        .iter()
        .map(|p| add_node(p, nodes, known))
        .collect::<Result<Vec<_>>>()?;
    let ordered = matches!(plan, LogicalPlan::Sort(_))
        || (inputs.len() == 1
            && nodes[inputs[0]].ordered
            && matches!(
                plan,
                LogicalPlan::Projection(_)
                    | LogicalPlan::Filter(_)
                    | LogicalPlan::SubqueryAlias(_)
                    | LogicalPlan::Limit(_)
                    | LogicalPlan::Repartition(_)
            ));
    let id = nodes.len();
    nodes.push(Node {
        plan: plan.clone(),
        inputs,
        ordered,
    });
    known.insert(plan.clone(), id);
    Ok(id)
}

#[cfg(test)]
mod normalization_tests {
    use super::*;
    use crate::executor::cache_scan;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{col, Distinct, EmptyRelation};
    use std::sync::Arc;

    #[test]
    fn remaining_distinct_normalizes_with_exact_qualifiers_and_zero_columns_stay_distinct(
    ) -> Result<()> {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("k", DataType::Int64, true),
        ]));
        let scan = cache_scan(
            &Arc::new(datafusion::common::DFSchema::try_from_qualified_schema(
                "t", &schema,
            )?),
            RecordBatch::new_empty(schema),
            0,
        )?;
        let input = LogicalPlanBuilder::from(scan)
            .project(vec![col("t.k")])?
            .build()?;
        let distinct = LogicalPlan::Distinct(Distinct::All(Arc::new(input)));
        let normalized = normalize_distinct(distinct.clone())?;
        assert!(matches!(normalized, LogicalPlan::Aggregate(_)));
        assert_eq!(normalized.schema(), distinct.schema());
        let zero = LogicalPlan::Distinct(Distinct::All(Arc::new(LogicalPlan::EmptyRelation(
            EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(datafusion::common::DFSchema::empty()),
            },
        ))));
        assert_eq!(normalize_distinct(zero.clone())?, zero);
        Ok(())
    }
}
