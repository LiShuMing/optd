//! Explicit built-in transformers; an unhandled operator has no fallback rule.

use std::sync::Arc;

use datafusion::logical_expr::{JoinType, LogicalPlan};

use super::{
    DeltaExpr, IncrementalRule, KeyScope, Maintenance, RuleContext, RuleRewrite, SnapshotVersion,
};
use crate::Result;

#[derive(Clone, Copy)]
enum Builtin {
    Constant,
    SourceDelta,
    SourceFetchRecompute,
    Projection,
    Filter,
    Alias,
    Union,
    Repartition,
    InnerJoin,
    Accumulator,
    GroupedAggregate,
    OrderedRecompute,
    AggregateRecompute,
    KeyedJoin,
    PartitionedWindow,
    NonInnerJoinRecompute,
    WindowRecompute,
    DistinctRecompute,
    LimitRecompute,
    UnnestRecompute,
}

pub(super) fn builtins() -> Vec<Arc<dyn IncrementalRule>> {
    [
        Builtin::Constant,
        Builtin::SourceDelta,
        Builtin::SourceFetchRecompute,
        Builtin::Projection,
        Builtin::Filter,
        Builtin::Alias,
        Builtin::Union,
        Builtin::Repartition,
        Builtin::InnerJoin,
        Builtin::Accumulator,
        Builtin::GroupedAggregate,
        Builtin::OrderedRecompute,
        Builtin::AggregateRecompute,
        Builtin::KeyedJoin,
        Builtin::PartitionedWindow,
        Builtin::NonInnerJoinRecompute,
        Builtin::WindowRecompute,
        Builtin::DistinctRecompute,
        Builtin::LimitRecompute,
        Builtin::UnnestRecompute,
    ]
    .into_iter()
    .map(|rule| Arc::new(rule) as Arc<dyn IncrementalRule>)
    .collect()
}

impl IncrementalRule for Builtin {
    fn name(&self) -> &str {
        match self {
            Self::Constant => "builtin.constant",
            Self::SourceDelta => "builtin.source.delta",
            Self::SourceFetchRecompute => "builtin.source.fetch_recompute",
            Self::Projection => "builtin.projection",
            Self::Filter => "builtin.filter",
            Self::Alias => "builtin.alias",
            Self::Union => "builtin.union",
            Self::Repartition => "builtin.repartition",
            Self::InnerJoin => "builtin.inner_join",
            Self::Accumulator => "builtin.aggregate.accumulator",
            Self::GroupedAggregate => "builtin.grouped_aggregate",
            Self::OrderedRecompute => "builtin.ordered_recompute",
            Self::AggregateRecompute => "builtin.aggregate_recompute",
            Self::KeyedJoin => "builtin.keyed_join",
            Self::PartitionedWindow => "builtin.partitioned_window",
            Self::NonInnerJoinRecompute => "builtin.non_inner_join_recompute",
            Self::WindowRecompute => "builtin.window_recompute",
            Self::DistinctRecompute => "builtin.distinct_recompute",
            Self::LimitRecompute => "builtin.limit_recompute",
            Self::UnnestRecompute => "builtin.unnest_recompute",
        }
    }

    fn priority(&self) -> i32 {
        if matches!(self, Self::OrderedRecompute) {
            100
        } else if matches!(self, Self::Accumulator) {
            10
        } else {
            0
        }
    }

    fn matches(&self, context: &RuleContext<'_>) -> Result<bool> {
        let plan = context.logical_plan();
        if context.ordered() {
            return Ok(matches!(self, Self::OrderedRecompute)
                && matches!(
                    plan,
                    LogicalPlan::Sort(_)
                        | LogicalPlan::Projection(_)
                        | LogicalPlan::Filter(_)
                        | LogicalPlan::SubqueryAlias(_)
                        | LogicalPlan::Limit(_)
                        | LogicalPlan::Repartition(_)
                ));
        }
        Ok(match self {
            Self::Constant => {
                matches!(plan, LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_))
            }
            Self::SourceDelta => {
                matches!(plan, LogicalPlan::TableScan(scan) if scan.fetch.is_none())
            }
            Self::SourceFetchRecompute => {
                matches!(plan, LogicalPlan::TableScan(scan) if scan.fetch.is_some())
            }
            Self::Projection => matches!(plan, LogicalPlan::Projection(_)),
            Self::Filter => matches!(plan, LogicalPlan::Filter(_)),
            Self::Alias => matches!(plan, LogicalPlan::SubqueryAlias(_)),
            Self::Union => matches!(plan, LogicalPlan::Union(_)),
            Self::Repartition => matches!(plan, LogicalPlan::Repartition(_)),
            Self::InnerJoin => {
                matches!(plan, LogicalPlan::Join(join) if join.join_type == JoinType::Inner)
            }
            Self::GroupedAggregate => {
                matches!(plan, LogicalPlan::Aggregate(_)) && context.group_eligible()
            }
            Self::Accumulator => context.accumulator_eligible(),
            Self::OrderedRecompute => false,
            Self::AggregateRecompute => {
                matches!(plan, LogicalPlan::Aggregate(_)) && !context.group_eligible()
            }
            Self::KeyedJoin => {
                matches!(plan, LogicalPlan::Join(join) if join.join_type != JoinType::Inner)
                    && context.supports_key_scope(KeyScope::JoinKeys)?
            }
            Self::PartitionedWindow => context.supports_key_scope(KeyScope::WindowPartitions)?,
            Self::NonInnerJoinRecompute => {
                matches!(plan, LogicalPlan::Join(join) if join.join_type != JoinType::Inner)
                    && !context.supports_key_scope(KeyScope::JoinKeys)?
            }
            Self::WindowRecompute => {
                matches!(plan, LogicalPlan::Window(_))
                    && !context.supports_key_scope(KeyScope::WindowPartitions)?
            }
            Self::DistinctRecompute => matches!(plan, LogicalPlan::Distinct(_)),
            Self::LimitRecompute => matches!(plan, LogicalPlan::Limit(_)),
            Self::UnnestRecompute => matches!(plan, LogicalPlan::Unnest(_)),
        })
    }

    fn rewrite(&self, context: &RuleContext<'_>) -> Result<RuleRewrite> {
        let (maintenance, reason) = match self {
            Self::Accumulator => (Maintenance::Accumulate, "update qualified builtin aggregate state from insertions/retractions without reading historical group inputs"),
            Self::Constant => (
                Maintenance::Constant,
                "source-independent constant relation",
            ),
            Self::SourceDelta => (
                Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::SourceDelta])),
                "apply the scan's pushed filters and projection to raw signed source changes",
            ),
            Self::Projection | Self::Filter | Self::Alias | Self::Union | Self::Repartition => (
                Maintenance::Delta(DeltaExpr::Linear(
                    (0..context.inputs().len())
                        .map(DeltaExpr::InputDelta)
                        .collect(),
                )),
                "jointly linear lifting over signed input changes",
            ),
            Self::InnerJoin => {
                let snapshot = |input, version| -> Result<DeltaExpr> {
                    if context.supports_key_scope(KeyScope::JoinKeys)? {
                        Ok(DeltaExpr::IndexedInputSnapshot { input, version })
                    } else {
                        Ok(DeltaExpr::InputSnapshot { input, version })
                    }
                };
                (
                Maintenance::Delta(DeltaExpr::Sum(vec![
                    DeltaExpr::Join {
                        left: Box::new(DeltaExpr::InputDelta(0)),
                        right: Box::new(snapshot(1, SnapshotVersion::Before)?),
                    },
                    DeltaExpr::Join {
                        left: Box::new(snapshot(0, SnapshotVersion::After)?),
                        right: Box::new(DeltaExpr::InputDelta(1)),
                    },
                ])),
                "delta-left join before-right plus after-left join delta-right counts the cross term once",
            )},
            Self::GroupedAggregate => (
                Maintenance::ChangedGroups,
                "qualified group keys and aggregate functions permit recomputing only changed groups",
            ),
            Self::KeyedJoin | Self::PartitionedWindow => {
                let scope = if matches!(self, Self::KeyedJoin) { KeyScope::JoinKeys } else { KeyScope::WindowPartitions };
                (Maintenance::Delta(DeltaExpr::Sum(vec![
                    DeltaExpr::ScopedSnapshot { scope, version: SnapshotVersion::After },
                    DeltaExpr::Negate(Box::new(DeltaExpr::ScopedSnapshot { scope, version: SnapshotVersion::Before })),
                ])), "after minus committed before over complete affected key buckets")
            },
            Self::SourceFetchRecompute => (
                Maintenance::Recompute,
                "scan fetch changes membership; evaluate on the complete candidate raw source",
            ),
            Self::OrderedRecompute => (
                Maintenance::Recompute,
                "retain ordered sequences and propagate sequence-only changes to consumers",
            ),
            Self::AggregateRecompute => (
                Maintenance::Recompute,
                "global, grouping-set or unqualified aggregates recompute this node over maintained input",
            ),
            Self::NonInnerJoinRecompute => (
                Maintenance::Recompute,
                "outer, semi and anti membership requires explicit local join recomputation",
            ),
            Self::WindowRecompute => (
                Maintenance::Recompute,
                "window partitions recompute from maintained child relations",
            ),
            Self::DistinctRecompute => (
                Maintenance::Recompute,
                "a remaining Distinct operator explicitly recomputes local set membership",
            ),
            Self::LimitRecompute => (
                Maintenance::Recompute,
                "limit membership recomputes from the maintained input sequence",
            ),
            Self::UnnestRecompute => (
                Maintenance::Recompute,
                "unnest uses explicit local recomputation",
            ),
        };
        Ok(RuleRewrite {
            maintenance,
            reason: reason.into(),
        })
    }
}
