//! `quantified.truth_relations@1`: preserve SQL UNKNOWN with ordinary typed relations.
//!
//! This module only constructs logical expressions. Existing aggregate/join rules
//! own state and delta execution, including the real global impact of NULL flags.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::expr::InSubquery;
use datafusion::logical_expr::{
    lit, when, Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder, Projection,
};

use super::{validate_filter_membership, MAX_EXPRESSION_NODES};

mod correlation;
mod partition_flags;

/// Bound both relation fanout and name probes independently of binder limits.
const MAX_EXPANSIONS: usize = 64;

/// One plan-wide namespace and bounded expansion budget, including nested queries.
struct RewriteContext {
    used: BTreeSet<String>,
    next: usize,
    expansions: usize,
    remaining_generated_nodes: usize,
}

impl RewriteContext {
    fn collect(plan: &LogicalPlan) -> Result<Self> {
        let mut used = BTreeSet::new();
        let mut remaining = MAX_EXPRESSION_NODES;
        plan.apply_with_subqueries(|node| {
            super::consume_node_budget(&mut remaining)?;
            for (qualifier, field) in node.schema().iter() {
                super::consume_node_budget(&mut remaining)?;
                used.insert(field.name().clone());
                if let Some(qualifier) = qualifier {
                    used.insert(qualifier.to_string());
                }
            }
            for expression in node.expressions() {
                expression.apply(|expression| {
                    super::consume_node_budget(&mut remaining)?;
                    match expression {
                        Expr::Alias(alias) => {
                            used.insert(alias.name.clone());
                        }
                        Expr::Column(column) => {
                            used.insert(column.name.clone());
                        }
                        _ => {}
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(Self {
            used,
            next: 0,
            expansions: 0,
            remaining_generated_nodes: MAX_EXPRESSION_NODES,
        })
    }

    fn fresh(&mut self) -> Result<String> {
        while self.next < MAX_EXPRESSION_NODES {
            let name = format!("__optd_quantified_{}", self.next);
            self.next += 1;
            if self.used.insert(name.clone()) {
                return Ok(name);
            }
        }
        Err(DataFusionError::Plan(
            "quantified helper name budget exceeded".into(),
        ))
    }
}

/// Preserve already qualified positive-membership paths, lowering only observed truth.
pub(crate) fn normalize(plan: LogicalPlan) -> crate::Result<LogicalPlan> {
    let mut names = RewriteContext::collect(&plan)?;
    Ok(plan
        .transform_up_with_subqueries(|plan| match plan {
            LogicalPlan::Projection(projection) => {
                let mut input = projection.input.as_ref().clone();
                let mut changed = false;
                let expressions = projection
                    .expr
                    .into_iter()
                    .map(|expression| {
                        rewrite_expression(expression, &mut input, &mut names, &mut changed)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if !changed {
                    return Ok(Transformed::no(LogicalPlan::Projection(
                        Projection::try_new_with_schema(
                            expressions,
                            projection.input,
                            projection.schema,
                        )?,
                    )));
                }
                Ok(Transformed::yes(restore(
                    expressions,
                    input,
                    projection.schema,
                )?))
            }
            LogicalPlan::Filter(filter)
                if validate_filter_membership(&filter.predicate).is_err() =>
            {
                let schema = filter.input.schema().clone();
                let mut input = filter.input.as_ref().clone();
                let mut changed = false;
                let predicate =
                    rewrite_expression(filter.predicate, &mut input, &mut names, &mut changed)?;
                let filtered = LogicalPlanBuilder::from(input).filter(predicate)?.build()?;
                if changed {
                    Ok(Transformed::yes(restore(
                        schema.columns().into_iter().map(Expr::Column).collect(),
                        filtered,
                        schema,
                    )?))
                } else {
                    Ok(Transformed::no(filtered))
                }
            }
            _ => Ok(Transformed::no(plan)),
        })?
        .data)
}

/// Bottom-up scalar replacement lets an observed membership be compared again.
fn rewrite_expression(
    expression: Expr,
    input: &mut LogicalPlan,
    names: &mut RewriteContext,
    changed: &mut bool,
) -> Result<Expr> {
    Ok(expression
        .transform_up(|expression| {
            let Expr::InSubquery(subquery) = expression else {
                return Ok(Transformed::no(expression));
            };
            let relation = expand(input.clone(), subquery, names)?;
            *input = relation.input;
            *changed = true;
            Ok(Transformed::yes(relation.truth))
        })?
        .data)
}

/// Group equality and comparison equality must agree; coercion needs its own proof.
fn comparison_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// A scalar replacement bound only to columns in its expanded logical input.
struct TruthRelation {
    input: LogicalPlan,
    truth: Expr,
}

/// The right members are unique; the flags are exactly one row even for empty input.
fn expand(
    input: LogicalPlan,
    subquery: InSubquery,
    names: &mut RewriteContext,
) -> Result<TruthRelation> {
    let right = subquery.subquery.subquery.as_ref();
    right.apply_with_subqueries(|node| {
        // Nullable truth references S twice. Charge both before cloning its tree.
        super::consume_node_budget(&mut names.remaining_generated_nodes)?;
        super::consume_node_budget(&mut names.remaining_generated_nodes)?;
        for expression in node.expressions() {
            expression.apply(|_expression| {
                super::consume_node_budget(&mut names.remaining_generated_nodes)?;
                super::consume_node_budget(&mut names.remaining_generated_nodes)?;
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if right.schema().fields().len() != 1 || names.expansions >= MAX_EXPANSIONS {
        return Err(DataFusionError::Plan(
            "quantified relation width/expansion budget exceeded".into(),
        ));
    }
    names.expansions += 1;
    let data_type = subquery.expr.get_type(input.schema())?;
    if data_type != *right.schema().field(0).data_type() || !comparison_type(&data_type) {
        return Err(DataFusionError::Plan(
            "quantified three-valued comparison requires identical qualified types".into(),
        ));
    }
    let nullable = subquery.expr.nullable(input.schema())? || right.schema().field(0).is_nullable();
    let correlation::PartitionInput {
        plan: right,
        mut keys,
    } = if !subquery.subquery.outer_ref_columns.is_empty() || correlation::contains_outer(right)? {
        correlation::extract(right, input.schema(), names)?
    } else {
        correlation::PartitionInput {
            plan: right.clone(),
            keys: vec![],
        }
    };
    let value_name = names.fresh()?;
    let member_name = names.fresh()?;
    let matched_name = names.fresh()?;
    // Extraction preserves compared output position zero. All carried correlation
    // columns receive distinct names before members and flags branch independently.
    let mut compared_columns =
        vec![Expr::Column(right.schema().columns()[0].clone()).alias(&value_name)];
    for key in &mut keys {
        let name = names.fresh()?;
        compared_columns.push(Expr::Column(key.inner.clone()).alias(&name));
        key.inner = Column::from_name(name);
    }
    let compared = LogicalPlanBuilder::from(right)
        .project(compared_columns)?
        .build()?;
    let value = Expr::Column(Column::from_name(&value_name));
    let mut groups = keys
        .iter()
        .map(|key| Expr::Column(key.inner.clone()))
        .collect::<Vec<_>>();
    let mut member_columns = Vec::new();
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    for key in &keys {
        let name = names.fresh()?;
        member_columns.push(Expr::Column(key.inner.clone()).alias(&name));
        left_keys.push(Expr::Column(key.outer.clone()));
        right_keys.push(Expr::Column(Column::from_name(name)));
    }
    groups.push(value.clone());
    member_columns.extend([
        value.clone().alias(&member_name),
        lit(true).alias(&matched_name),
    ]);
    left_keys.push(*subquery.expr.clone());
    right_keys.push(Expr::Column(Column::from_name(&member_name)));
    let members = LogicalPlanBuilder::from(compared.clone())
        .filter(value.clone().is_not_null())?
        .aggregate(groups, Vec::<Expr>::new())?
        .project(member_columns)?
        .build()?;
    let joined = LogicalPlanBuilder::from(input)
        .join_with_expr_keys(members, JoinType::Left, (left_keys, right_keys), None)?
        .build()?;
    let matched = Expr::Column(Column::from_name(matched_name)).is_not_null();
    // Nonnullable comparisons can never produce UNKNOWN, including on empty S.
    let mut relation = if nullable && !keys.is_empty() {
        partition_flags::append_truth(
            joined,
            compared,
            &keys,
            &value_name,
            *subquery.expr,
            matched,
            names,
        )?
    } else if nullable {
        append_global_truth(
            joined,
            compared,
            &value_name,
            *subquery.expr,
            matched,
            names,
        )?
    } else {
        TruthRelation {
            input: joined,
            truth: matched,
        }
    };
    if subquery.negated {
        relation.truth = Expr::Not(Box::new(relation.truth));
    }
    Ok(relation)
}

/// Global flag changes really affect unchanged comparison keys; the equijoin
/// makes that fanout explicit rather than hiding it inside a scalar evaluator.
fn append_global_truth(
    input: LogicalPlan,
    compared: LogicalPlan,
    value_name: &str,
    left_value: Expr,
    matched: Expr,
    names: &mut RewriteContext,
) -> Result<TruthRelation> {
    let total_name = names.fresh()?;
    let count_name = names.fresh()?;
    let empty_name = names.fresh()?;
    let null_name = names.fresh()?;
    let left_key = names.fresh()?;
    let right_key = names.fresh()?;
    let flags = LogicalPlanBuilder::from(compared)
        .aggregate(
            Vec::<Expr>::new(),
            vec![
                count(lit(1i64)).alias(&total_name),
                count(Expr::Column(Column::from_name(value_name))).alias(&count_name),
            ],
        )?
        .project(vec![
            Expr::Column(Column::from_name(&total_name))
                .eq(lit(0i64))
                .alias(&empty_name),
            Expr::Column(Column::from_name(total_name))
                .gt(Expr::Column(Column::from_name(count_name)))
                .alias(&null_name),
            lit(0i64).alias(&right_key),
        ])?
        .build()?;
    let mut columns = input
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    columns.push(lit(0i64).alias(&left_key));
    let joined = LogicalPlanBuilder::from(input)
        .project(columns)?
        .join(
            flags,
            JoinType::Inner,
            (
                vec![Column::from_name(left_key)],
                vec![Column::from_name(right_key)],
            ),
            None,
        )?
        .build()?;
    let unknown = left_value
        .is_null()
        .or(Expr::Column(Column::from_name(null_name)));
    let truth = when(matched, lit(true))
        .when(Expr::Column(Column::from_name(empty_name)), lit(false))
        .when(unknown, Expr::Literal(ScalarValue::Boolean(None), None))
        .otherwise(lit(false))?;
    Ok(TruthRelation {
        input: joined,
        truth,
    })
}

/// Hide all helper fields and retain the original schema, including metadata.
fn restore(expressions: Vec<Expr>, input: LogicalPlan, schema: DFSchemaRef) -> Result<LogicalPlan> {
    if expressions.len() != schema.fields().len() {
        return Err(DataFusionError::Plan(
            "quantified normalization changed output width".into(),
        ));
    }
    let expressions = expressions
        .into_iter()
        .enumerate()
        .map(|(index, expression)| {
            let (qualifier, field) = schema.qualified_field(index);
            if expression.get_type(input.schema())? != *field.data_type()
                || expression.nullable(input.schema())? != field.is_nullable()
            {
                return Err(DataFusionError::Plan(
                    "quantified normalization changed output type/nullability".into(),
                ));
            }
            Ok(expression.alias_qualified_with_metadata(
                qualifier.cloned(),
                field.name(),
                Some(FieldMetadata::from(field.metadata().clone())),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        expressions,
        Arc::new(input),
        schema,
    )?))
}
