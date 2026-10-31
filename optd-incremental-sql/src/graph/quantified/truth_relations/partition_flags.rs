//! Partition counts separate missing comparison input from UNKNOWN comparisons.

use datafusion::common::{Column, Result, ScalarValue};
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{lit, when, Expr, JoinType, LogicalPlan, LogicalPlanBuilder};

use super::correlation::CorrelationKey;
use super::{RewriteContext, TruthRelation};

/// Only the correlation partition is the flag join's dependency key. Inserting
/// a NULL can change all comparison values there, while other partitions stay shared.
pub(super) fn append_truth(
    input: LogicalPlan,
    compared: LogicalPlan,
    keys: &[CorrelationKey],
    value_name: &str,
    left_value: Expr,
    matched: Expr,
    context: &mut RewriteContext,
) -> Result<TruthRelation> {
    let total_name = context.fresh()?;
    let count_name = context.fresh()?;
    let presence_name = context.fresh()?;
    let null_name = context.fresh()?;
    let groups = keys
        .iter()
        .map(|key| Expr::Column(key.inner.clone()))
        .collect::<Vec<_>>();
    let mut projection = Vec::new();
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    for key in keys {
        let name = context.fresh()?;
        projection.push(Expr::Column(key.inner.clone()).alias(&name));
        left_keys.push(Expr::Column(key.outer.clone()));
        right_keys.push(Expr::Column(Column::from_name(name)));
    }
    projection.push(lit(true).alias(&presence_name));
    projection.push(
        Expr::Column(Column::from_name(&total_name))
            .gt(Expr::Column(Column::from_name(&count_name)))
            .alias(&null_name),
    );
    let flags = LogicalPlanBuilder::from(compared)
        .aggregate(
            groups,
            vec![
                count(lit(1i64)).alias(&total_name),
                count(Expr::Column(Column::from_name(value_name))).alias(&count_name),
            ],
        )?
        .project(projection)?
        .build()?;
    let joined = LogicalPlanBuilder::from(input)
        .join_with_expr_keys(flags, JoinType::Left, (left_keys, right_keys), None)?
        .build()?;
    let empty = Expr::Column(Column::from_name(presence_name)).is_null();
    let unknown = left_value
        .is_null()
        .or(Expr::Column(Column::from_name(null_name)));
    let truth = when(matched, lit(true))
        .when(empty, lit(false))
        .when(unknown, Expr::Literal(ScalarValue::Boolean(None), None))
        .otherwise(lit(false))?;
    Ok(TruthRelation {
        input: joined,
        truth,
    })
}
