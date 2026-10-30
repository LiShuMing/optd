//! Planning-only qualification of typed dependency keys.

use super::KeyScope;
use crate::group_state::{supported_key, KeyPolicy};
use crate::Result;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Expr, ExprSchemable, JoinType, LogicalPlan};

/// Return only proven SQL-equality keys; None is a planning capability boundary.
pub(crate) fn key_expressions(
    plan: &LogicalPlan,
    scope: KeyScope,
) -> Result<Option<Vec<Vec<Expr>>>> {
    let expressions = match (scope, plan) {
        (KeyScope::JoinKeys, LogicalPlan::Join(join))
            if !join.on.is_empty()
                && matches!(
                    join.join_type,
                    JoinType::Inner
                        | JoinType::Left
                        | JoinType::Right
                        | JoinType::Full
                        | JoinType::LeftSemi
                        | JoinType::RightSemi
                        | JoinType::LeftAnti
                        | JoinType::RightAnti
                ) =>
        {
            vec![
                join.on.iter().map(|(l, _)| l.clone()).collect(),
                join.on.iter().map(|(_, r)| r.clone()).collect(),
            ]
        }
        (KeyScope::WindowPartitions, LogicalPlan::Window(window)) => {
            let mut common = None;
            for expression in &window.window_expr {
                let mut expression = expression;
                while let Expr::Alias(alias) = expression {
                    expression = &alias.expr;
                }
                let Expr::WindowFunction(function) = expression else {
                    return Ok(None);
                };
                let keys = &function.params.partition_by;
                if keys.is_empty() || common.as_ref().is_some_and(|common| common != keys) {
                    return Ok(None);
                }
                common = Some(keys.clone());
            }
            let Some(common) = common else {
                return Ok(None);
            };
            vec![common]
        }
        _ => return Ok(None),
    };
    let inputs = plan.inputs();
    let mut common_types: Option<Vec<DataType>> = None;
    for (keys, input) in expressions.iter().zip(inputs) {
        let types = keys
            .iter()
            .map(|key| key.get_type(input.schema()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if types.iter().any(|ty| {
            !supported_key(
                ty,
                KeyPolicy {
                    binary_strings: true,
                },
            )
        }) || common_types.as_ref().is_some_and(|common| common != &types)
        {
            return Ok(None);
        }
        common_types = Some(types);
    }
    Ok(Some(expressions))
}
