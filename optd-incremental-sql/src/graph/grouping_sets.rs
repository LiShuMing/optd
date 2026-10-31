//! Logical grouping-set expansion; ordinary rules retain all execution/state ownership.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::utils::{grouping_set_to_exprlist, powerset};
use datafusion::logical_expr::{
    lit, when, Aggregate, Expr, ExprSchemable, GroupingSet, LogicalPlan, Projection, Union,
};

use crate::{Error, Result};

/// Match the pinned planner's grouping-set fanout bound before allocating CUBE subsets.
const MAX_GROUPING_SETS: usize = 4096;

/// Bottom-up lowering preserves each replaced node's complete parent-facing schema.
pub(crate) fn normalize(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(plan
        .transform_up(|plan| {
            let LogicalPlan::Aggregate(aggregate) = &plan else {
                return Ok(Transformed::no(plan));
            };
            let [Expr::GroupingSet(grouping)] = aggregate.group_expr.as_slice() else {
                return Ok(Transformed::no(plan));
            };
            expand(aggregate, grouping)
                .map(Transformed::yes)
                .map_err(|error| {
                    DataFusionError::Plan(format!("grouping-set normalization: {error}"))
                })
        })?
        .data)
}

/// Stable schema positions determine semantic bits; occurrence ordinals preserve duplicates.
fn expand(aggregate: &Aggregate, grouping: &GroupingSet) -> Result<LogicalPlan> {
    let keys = grouping_set_to_exprlist(&aggregate.group_expr)?;
    if keys.len() > 64
        || aggregate.schema.fields().len() != keys.len() + 1 + aggregate.aggr_expr.len()
    {
        return Err(Error::Invalid(
            "unrepresentable grouping-set identity/schema".into(),
        ));
    }
    let sets = enumerate_sets(grouping)?;
    let mut ordinals = BTreeMap::<u64, u64>::new();
    let mut branches = Vec::with_capacity(sets.len());
    for set in sets {
        let included = keys.iter().map(|key| set.contains(key)).collect::<Vec<_>>();
        let mut mask = 0u64;
        for (index, present) in included.iter().enumerate() {
            if !present {
                mask |= 1u64
                    .checked_shl(
                        u32::try_from(keys.len() - index - 1).map_err(|_| Error::Overflow)?,
                    )
                    .ok_or(Error::Overflow)?;
            }
        }
        let ordinal = ordinals.entry(mask).or_default();
        let high = if *ordinal == 0 {
            0
        } else {
            let shift = u32::try_from(keys.len()).map_err(|_| Error::Overflow)?;
            if shift >= 64 || *ordinal > (u64::MAX >> shift) {
                return Err(Error::Overflow);
            }
            *ordinal << shift
        };
        let identity = typed_identity(mask | high, aggregate.schema.field(keys.len()).data_type())?;
        *ordinal = ordinal.checked_add(1).ok_or(Error::Overflow)?;
        let branch_keys = keys
            .iter()
            .zip(&included)
            .filter(|(_, present)| **present)
            .map(|(key, _)| (*key).clone())
            .collect::<Vec<_>>();
        let mut functions = aggregate.aggr_expr.clone();
        if branch_keys.is_empty() && functions.is_empty() {
            // An empty grouping set still emits one row on empty input.
            functions.push(
                datafusion::functions_aggregate::expr_fn::count(lit(1i64))
                    .alias("__optd_grouping_count"),
            );
        }
        let branch = Arc::new(LogicalPlan::Aggregate(Aggregate::try_new(
            aggregate.input.clone(),
            branch_keys.clone(),
            functions,
        )?));
        // Aggregate::try_new guarantees one output per branch key/function.
        // The positions below enumerate only those included keys and original functions;
        // an injected count belongs only to an empty-key, empty-function branch.
        let columns = branch.schema().columns();
        let mut output = Vec::with_capacity(aggregate.schema.fields().len());
        let mut position = 0;
        for (index, present) in included.iter().enumerate() {
            if *present {
                output.push(Expr::Column(columns[position].clone()));
                position += 1;
            } else {
                output.push(Expr::Literal(
                    ScalarValue::try_from(aggregate.schema.field(index).data_type())?,
                    None,
                ));
            }
        }
        output.push(Expr::Literal(identity, None));
        for index in 0..aggregate.aggr_expr.len() {
            output.push(Expr::Column(columns[branch_keys.len() + index].clone()));
        }
        for (index, expression) in output.iter_mut().enumerate() {
            let (qualifier, field) = aggregate.schema.qualified_field(index);
            if expression.get_type(branch.schema())? != *field.data_type() {
                return Err(Error::Invalid(
                    "grouping-set projection type differs".into(),
                ));
            }
            if field.is_nullable() && !expression.nullable(branch.schema())? {
                // Keep the nullable grouping-set schema even for a NOT NULL key.
                // No optimizer reruns after lowering to erase this schema witness.
                *expression = when(lit(true), expression.clone()).otherwise(Expr::Literal(
                    ScalarValue::try_from(field.data_type())?,
                    None,
                ))?;
            }
            if expression.nullable(branch.schema())? != field.is_nullable() {
                return Err(Error::Invalid(
                    "grouping-set projection nullability differs".into(),
                ));
            }
            *expression = expression.clone().alias_qualified_with_metadata(
                qualifier.cloned(),
                field.name(),
                Some(FieldMetadata::from(field.metadata().clone())),
            );
        }
        branches.push(Arc::new(LogicalPlan::Projection(
            Projection::try_new_with_schema(output, branch, aggregate.schema.clone())?,
        )));
    }
    if branches.is_empty() {
        return Err(Error::Invalid("empty grouping-set enumeration".into()));
    }
    if let [branch] = branches.as_slice() {
        // A single grouping-set occurrence needs no union; its projection already
        // carries the exact original schema and internal identity.
        return Ok(branch.as_ref().clone());
    }
    let mut union = Union::try_new(branches)?;
    // Keep the original weaker dependencies and qualifiers at the parent boundary.
    union.schema = aggregate.schema.clone();
    Ok(LogicalPlan::Union(union))
}

/// Enumerate semantic sets with checked fanout; never allocate an unbounded power set.
fn enumerate_sets(grouping: &GroupingSet) -> Result<Vec<Vec<Expr>>> {
    let count = match grouping {
        GroupingSet::Rollup(keys) => keys.len().checked_add(1).ok_or(Error::Overflow)?,
        GroupingSet::Cube(keys) => 1usize
            .checked_shl(u32::try_from(keys.len()).map_err(|_| Error::Overflow)?)
            .ok_or(Error::Overflow)?,
        GroupingSet::GroupingSets(sets) => sets.len(),
    };
    if count > MAX_GROUPING_SETS {
        return Err(Error::Invalid(format!(
            "grouping-set count {count} exceeds pinned planner limit {MAX_GROUPING_SETS}"
        )));
    }
    Ok(match grouping {
        GroupingSet::Rollup(keys) => (0..=keys.len()).map(|end| keys[..end].to_vec()).collect(),
        GroupingSet::Cube(keys) => powerset(keys)?
            .into_iter()
            .map(|set| set.into_iter().cloned().collect())
            .collect(),
        GroupingSet::GroupingSets(sets) => sets.clone(),
    })
}

/// Internal ID width is part of the pinned SQL schema, including ordinal capacity.
fn typed_identity(value: u64, data_type: &DataType) -> Result<ScalarValue> {
    Ok(match data_type {
        DataType::UInt8 => {
            ScalarValue::UInt8(Some(u8::try_from(value).map_err(|_| Error::Overflow)?))
        }
        DataType::UInt16 => {
            ScalarValue::UInt16(Some(u16::try_from(value).map_err(|_| Error::Overflow)?))
        }
        DataType::UInt32 => {
            ScalarValue::UInt32(Some(u32::try_from(value).map_err(|_| Error::Overflow)?))
        }
        DataType::UInt64 => ScalarValue::UInt64(Some(value)),
        _ => return Err(Error::Invalid("unexpected grouping identity type".into())),
    })
}

#[cfg(test)]
mod tests;
