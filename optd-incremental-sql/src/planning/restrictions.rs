//! Typed diagnostic context; eligibility comes from the actual compiler predicates.

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};

use crate::planner::{KeyScope, RuleContext};
use crate::Result;

/// Explain the actual rejected node without guessing capability from its SQL text.
pub(super) fn details(context: &RuleContext<'_>) -> Result<Vec<String>> {
    let mut details = Vec::new();
    match context.logical_plan() {
        LogicalPlan::Aggregate(aggregate) => {
            details.push(format!(
                "qualified accumulator={}, affected-group scope={}",
                context.accumulator_eligible(),
                context.group_eligible()
            ));
            for expression in &aggregate.group_expr {
                details.push(format!(
                    "group key {expression}: {:?}",
                    expression.get_type(aggregate.input.schema())?
                ));
            }
            for expression in &aggregate.aggr_expr {
                let mut function = expression;
                while let Expr::Alias(alias) = function {
                    function = &alias.expr;
                }
                if let Expr::AggregateFunction(function) = function {
                    let types = function
                        .params
                        .args
                        .iter()
                        .map(|argument| argument.get_type(aggregate.input.schema()))
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    let output = expression.get_type(aggregate.input.schema())?;
                    if matches!(function.func.name(), "sum" | "avg")
                        && (types.iter().any(|ty| {
                            matches!(
                                ty,
                                DataType::Float16 | DataType::Float32 | DataType::Float64
                            )
                        }) || (function.func.name() == "avg"
                            && matches!(output, DataType::Float32 | DataType::Float64)))
                    {
                        details.push(format!(
                            "{} requires floating rounding/retraction qualification (R03e)",
                            function.func.name()
                        ));
                    }
                    if !matches!(
                        function.func.name(),
                        "count" | "sum" | "avg" | "min" | "max"
                    ) {
                        details.push(format!(
                            "{} has no qualified retractable or affected-group implementation",
                            function.func.name()
                        ));
                    }
                    details.push(format!("function={} args={types:?} output={output:?} distinct={} filter={} order_by={} null_treatment={:?}",
                        function.func.name(), function.params.distinct, function.params.filter.is_some(),
                        function.params.order_by.len(), function.params.null_treatment));
                }
            }
            if !context.accumulator_eligible() {
                details.push("accumulator admission requires canonical COUNT/SUM/AVG/MIN/MAX, qualified keys/modifiers and exact SUM/AVG arithmetic; floating numerical qualification is not enabled".into());
            }
            if !context.group_eligible() {
                details.push("affected-group admission requires nonempty supported keys and qualified aggregate functions/argument types".into());
            }
        }
        LogicalPlan::Join(join) => {
            details.push(format!(
                "join={:?}, equality_keys={}, qualified affected-key scope={}",
                join.join_type,
                join.on.len(),
                context.supports_key_scope(KeyScope::JoinKeys)?
            ));
            for (left, right) in &join.on {
                details.push(format!(
                    "join key {left}: {:?} / {right}: {:?}",
                    left.get_type(join.left.schema())?,
                    right.get_type(join.right.schema())?
                ));
            }
        }
        LogicalPlan::Window(window) => {
            details.push(format!(
                "qualified common nonempty partition scope={}",
                context.supports_key_scope(KeyScope::WindowPartitions)?
            ));
            details.extend(
                window
                    .window_expr
                    .iter()
                    .map(|expression| format!("window {expression}")),
            );
        }
        _ => {}
    }
    if context.ordered() {
        details.push("ordered sequence is consumed by a semantic ordering/row-bound path; ordered state is not yet qualified".into());
    }
    Ok(details)
}
