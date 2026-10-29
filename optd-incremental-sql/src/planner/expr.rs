//! Signed-expression primitives and their local type/shape verifier.

use datafusion::arrow::datatypes::Schema;
use datafusion::logical_expr::{JoinType, LogicalPlan};
use serde::Serialize;

use super::{RuleContext, Strategy};
use crate::{Error, Result};

/// Maximum nesting of a custom signed-expression program, counting its root.
pub const MAX_EXPR_DEPTH: usize = 64;
/// Maximum primitive occurrences in one signed-expression program.
pub const MAX_EXPR_NODES: usize = 1024;

/// Which immutable relation snapshot a signed expression reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum SnapshotVersion {
    /// The committed relation before this candidate batch.
    Before,
    /// The candidate relation after this atomic batch.
    After,
}

/// Executable signed relations with references bound to this operator's ports.
///
/// `Linear` and `Join` retain the original logical operator's expressions. Their
/// legality is verified before execution; arbitrary operators cannot be lifted
/// as though they were linear or bilinear. `Empty` inherits its exact schema
/// from the typed position where it occurs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum DeltaExpr {
    /// Signed output changes from an original graph-input port.
    InputDelta(usize),
    /// Complete positive relation at an original graph-input port.
    InputSnapshot {
        /// Local logical input port, not a global node identity.
        input: usize,
        /// Whether to read its committed or candidate relation.
        version: SnapshotVersion,
    },
    /// Raw named-source changes; legal only at a non-fetch TableScan.
    SourceDelta,
    /// Jointly linear signed lifting, with one expression per logical input.
    /// A TableScan has one synthetic raw-source port.
    Linear(Vec<DeltaExpr>),
    /// Bilinear signed lifting of this node's inner join.
    Join {
        /// Expression having the original left input's complete schema.
        left: Box<DeltaExpr>,
        /// Expression having the original right input's complete schema.
        right: Box<DeltaExpr>,
    },
    /// Checked addition of nonempty same-schema signed expressions.
    Sum(Vec<DeltaExpr>),
    /// Checked reversal of all signed row multiplicities.
    Negate(Box<DeltaExpr>),
    /// A zero signed relation retaining the expected typed-port schema.
    Empty,
}

/// The compiled maintenance program that drives one logical operator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Maintenance {
    /// Execute exactly this verified signed-expression tree.
    Delta(DeltaExpr),
    /// Update the derived group index and recompute surviving touched groups.
    ChangedGroups,
    /// Re-evaluate this operator on candidate inputs, then subtract old output.
    Recompute,
    /// Retain a source-independent Values or EmptyRelation node.
    Constant,
}

impl Maintenance {
    /// Derive the public strategy from the executable program, never independently.
    pub fn strategy(&self) -> Strategy {
        match self {
            Self::Delta(_) | Self::Constant => Strategy::NativeDelta,
            Self::ChangedGroups => Strategy::ChangedGroupRecompute,
            Self::Recompute => Strategy::OperatorRecompute,
        }
    }
}

pub(super) fn supported_operator(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
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
            | LogicalPlan::Unnest(_)
    )
}

fn linear_operator(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(scan) => scan.fetch.is_none(),
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_) => true,
        _ => false,
    }
}

pub(super) fn verify(context: &RuleContext<'_>, maintenance: &Maintenance) -> Result<()> {
    if !supported_operator(context.logical_plan()) {
        return Err(Error::Invalid(format!(
            "unsupported maintenance operator {}",
            context.logical_plan().display()
        )));
    }
    if context.inputs().len() != context.logical_plan().inputs().len() {
        return Err(Error::Invalid(
            "graph-input count differs from original logical arity".into(),
        ));
    }
    if context
        .inputs()
        .iter()
        .any(|input| *input >= context.node_id())
    {
        return Err(Error::Invalid(
            "graph-input references must precede the current node".into(),
        ));
    }
    if context.ordered() && !matches!(maintenance, Maintenance::Recompute) {
        return Err(Error::Invalid(
            "ordered maintenance requires explicit local recomputation".into(),
        ));
    }
    match maintenance {
        Maintenance::Delta(expression) => {
            if !linear_operator(context.logical_plan())
                && !matches!(context.logical_plan(), LogicalPlan::Join(join) if join.join_type == JoinType::Inner)
            {
                return Err(Error::Invalid(
                    "this operator has no legal signed-expression primitives".into(),
                ));
            }
            verify_expr(
                context,
                expression,
                context.logical_plan().schema().as_arrow(),
                1,
                &mut 0,
            )
        }
        Maintenance::ChangedGroups => {
            if context.group_eligible()
                && matches!(context.logical_plan(), LogicalPlan::Aggregate(_))
            {
                Ok(())
            } else {
                Err(Error::Invalid(
                    "changed-group maintenance requires a qualified aggregate index".into(),
                ))
            }
        }
        Maintenance::Recompute => Ok(()),
        Maintenance::Constant => {
            if context.inputs().is_empty()
                && matches!(
                    context.logical_plan(),
                    LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_)
                )
            {
                Ok(())
            } else {
                Err(Error::Invalid(
                    "constant maintenance requires a source-independent constant operator".into(),
                ))
            }
        }
    }
}

fn verify_expr(
    context: &RuleContext<'_>,
    expression: &DeltaExpr,
    expected: &Schema,
    depth: usize,
    count: &mut usize,
) -> Result<()> {
    if depth > MAX_EXPR_DEPTH {
        return Err(Error::Invalid(format!(
            "delta expression exceeds maximum depth {MAX_EXPR_DEPTH}"
        )));
    }
    *count += 1;
    if *count > MAX_EXPR_NODES {
        return Err(Error::Invalid(format!(
            "delta expression exceeds maximum node count {MAX_EXPR_NODES}"
        )));
    }
    match expression {
        DeltaExpr::InputDelta(input) | DeltaExpr::InputSnapshot { input, .. } => {
            let plans = context.logical_plan().inputs();
            let plan = plans
                .get(*input)
                .ok_or_else(|| Error::Invalid(format!("invalid local input port {input}")))?;
            verify_schema(plan.schema().as_arrow(), expected, "input reference")
        }
        DeltaExpr::SourceDelta => match context.logical_plan() {
            LogicalPlan::TableScan(scan) if scan.fetch.is_none() => {
                verify_schema(&scan.source.schema(), expected, "raw source delta")
            }
            _ => Err(Error::Invalid(
                "SourceDelta is legal only at a non-fetch TableScan".into(),
            )),
        },
        DeltaExpr::Linear(expressions) => {
            if !linear_operator(context.logical_plan()) {
                return Err(Error::Invalid(
                    "Linear requires a jointly linear operator or a non-fetch scan".into(),
                ));
            }
            verify_schema(
                context.logical_plan().schema().as_arrow(),
                expected,
                "Linear output",
            )?;
            if let LogicalPlan::TableScan(scan) = context.logical_plan() {
                if expressions.len() != 1 {
                    return Err(Error::Invalid(
                        "scan Linear requires exactly one synthetic raw-source input".into(),
                    ));
                }
                verify_expr(
                    context,
                    &expressions[0],
                    &scan.source.schema(),
                    depth + 1,
                    count,
                )
            } else {
                let inputs = context.logical_plan().inputs();
                if expressions.len() != inputs.len() || inputs.is_empty() {
                    return Err(Error::Invalid(format!(
                        "Linear input arity {} differs from nonempty logical arity {}",
                        expressions.len(),
                        inputs.len()
                    )));
                }
                for (expression, input) in expressions.iter().zip(inputs) {
                    verify_expr(
                        context,
                        expression,
                        input.schema().as_arrow(),
                        depth + 1,
                        count,
                    )?;
                }
                Ok(())
            }
        }
        DeltaExpr::Join { left, right } => {
            if !matches!(context.logical_plan(), LogicalPlan::Join(join) if join.join_type == JoinType::Inner)
            {
                return Err(Error::Invalid(
                    "Join signed lifting requires an inner join".into(),
                ));
            }
            verify_schema(
                context.logical_plan().schema().as_arrow(),
                expected,
                "Join output",
            )?;
            let inputs = context.logical_plan().inputs();
            if inputs.len() != 2 {
                return Err(Error::Invalid(
                    "Join signed lifting requires two logical inputs".into(),
                ));
            }
            verify_expr(
                context,
                left,
                inputs[0].schema().as_arrow(),
                depth + 1,
                count,
            )?;
            verify_expr(
                context,
                right,
                inputs[1].schema().as_arrow(),
                depth + 1,
                count,
            )
        }
        DeltaExpr::Sum(terms) => {
            if terms.is_empty() {
                return Err(Error::Invalid(
                    "Sum requires a nonempty expression list; use Empty for zero".into(),
                ));
            }
            for term in terms {
                verify_expr(context, term, expected, depth + 1, count)?;
            }
            Ok(())
        }
        DeltaExpr::Negate(expression) => {
            verify_expr(context, expression, expected, depth + 1, count)
        }
        DeltaExpr::Empty => Ok(()),
    }
}

fn verify_schema(actual: &Schema, expected: &Schema, location: &str) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(Error::Invalid(format!(
            "{location} schema differs from its exact typed position: expected {expected:?}, got {actual:?}"
        )))
    }
}
