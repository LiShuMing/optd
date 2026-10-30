//! Structural SQL-to-incremental-plan contracts, independent of refresh results.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use optd_incremental_sql::planner::{
    DeltaExpr, IncrementalOptimizer, IncrementalPlan, IncrementalRule, KeyScope, Maintenance,
    MaintenanceNode, RuleContext, RuleRewrite, SnapshotVersion,
};
use optd_incremental_sql::{Error, Result, SqlView, Strategy};
use serde_json::json;

fn tables() -> BTreeMap<String, RecordBatch> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30)])),
        ],
    )
    .unwrap();
    BTreeMap::from([("a".into(), batch.clone()), ("b".into(), batch)])
}

fn strict() -> IncrementalOptimizer {
    IncrementalOptimizer::default().require_incremental()
}

async fn compile(sql: &str, optimizer: IncrementalOptimizer) -> SqlView {
    SqlView::new_with_optimizer(sql, tables(), optimizer)
        .await
        .unwrap_or_else(|error| panic!("failed to compile {sql}: {error}"))
}

fn selected<'a>(plan: &'a IncrementalPlan, rule: &str) -> Vec<&'a MaintenanceNode> {
    let nodes = plan
        .nodes()
        .iter()
        .filter(|n| n.rule.name == rule)
        .collect::<Vec<_>>();
    assert!(!nodes.is_empty(), "missing {rule}: {}", plan.explain());
    nodes
}

fn assert_graph(plan: &IncrementalPlan) {
    assert_eq!(plan.root() + 1, plan.nodes().len());
    for (id, node) in plan.nodes().iter().enumerate() {
        assert_eq!(node.id, id);
        assert!(node.inputs.iter().all(|input| *input < id));
        assert!(!node.reason.is_empty());
    }
    let mut visited = BTreeSet::new();
    let mut pending = vec![plan.root()];
    while let Some(id) = pending.pop() {
        if visited.insert(id) {
            pending.extend(&plan.nodes()[id].inputs);
        }
    }
    assert_eq!(
        visited.len(),
        plan.nodes().len(),
        "detached maintenance nodes"
    );
}

fn scoped_difference(scope: KeyScope) -> Maintenance {
    Maintenance::Delta(DeltaExpr::Sum(vec![
        DeltaExpr::ScopedSnapshot {
            scope,
            version: SnapshotVersion::After,
        },
        DeltaExpr::Negate(Box::new(DeltaExpr::ScopedSnapshot {
            scope,
            version: SnapshotVersion::Before,
        })),
    ]))
}

fn assert_scoped(node: &MaintenanceNode, scope: KeyScope, arity: usize) {
    assert_eq!(node.maintenance, scoped_difference(scope));
    assert_eq!(node.maintenance.key_scope(), Some(scope));
    assert_eq!(node.maintenance.strategy(), Strategy::KeyedRecompute);
    assert_eq!(node.inputs.len(), arity);
    assert!(!node.ordered);
    assert_eq!(node.rule.version, 1);
    assert_eq!(node.rule.priority, 0);
}

#[tokio::test]
async fn inner_join_preserves_local_ports_and_before_after_cross_term_formula() {
    let view = compile(
        "SELECT a.k, a.v, b.v AS bv FROM a JOIN b ON a.k=b.k",
        strict(),
    )
    .await;
    let plan = view.incremental_plan();
    assert_graph(plan);
    for join in selected(plan, "builtin.inner_join") {
        assert_eq!(join.inputs.len(), 2);
        assert_ne!(join.inputs[0], join.inputs[1]);
        assert_eq!(
            join.maintenance,
            Maintenance::Delta(DeltaExpr::Sum(vec![
                DeltaExpr::Join {
                    left: Box::new(DeltaExpr::InputDelta(0)),
                    right: Box::new(DeltaExpr::InputSnapshot {
                        input: 1,
                        version: SnapshotVersion::Before
                    }),
                },
                DeltaExpr::Join {
                    left: Box::new(DeltaExpr::InputSnapshot {
                        input: 0,
                        version: SnapshotVersion::After
                    }),
                    right: Box::new(DeltaExpr::InputDelta(1)),
                },
            ]))
        );
        assert_eq!(join.maintenance.key_scope(), None);
        assert_eq!(join.maintenance.strategy(), Strategy::NativeDelta);
    }
}

#[tokio::test]
async fn every_outer_semi_anti_plan_declares_join_scope_and_snapshot_subtraction() {
    for kind in [
        "LEFT",
        "RIGHT",
        "FULL",
        "LEFT SEMI",
        "RIGHT SEMI",
        "LEFT ANTI",
        "RIGHT ANTI",
    ] {
        for condition in [
            "a.k=b.k",
            "a.k=b.k AND a.v=b.v",
            "a.k IS NOT DISTINCT FROM b.k",
            "a.k=b.k AND a.v < b.v",
        ] {
            let sql = format!("SELECT * FROM a {kind} JOIN b ON {condition}");
            let view = compile(&sql, strict()).await;
            let plan = view.incremental_plan();
            assert_graph(plan);
            let joins = selected(plan, "builtin.keyed_join");
            assert_eq!(joins.len(), 1, "{sql}");
            assert_scoped(joins[0], KeyScope::JoinKeys, 2);
            assert!(plan
                .nodes()
                .iter()
                .all(|n| n.maintenance != Maintenance::Recompute));
        }
    }
}

#[tokio::test]
async fn window_plans_declare_partition_scope_for_ranking_and_frames() {
    for expr in [
        "ROW_NUMBER() OVER (PARTITION BY k ORDER BY v)",
        "RANK() OVER (PARTITION BY k ORDER BY v)",
        "SUM(v) OVER (PARTITION BY k ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        "SUM(v) OVER (PARTITION BY k ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
    ] {
        let view = compile(&format!("SELECT k,v,{expr} AS w FROM a"), strict()).await;
        let plan = view.incremental_plan();
        assert_graph(plan);
        for window in selected(plan, "builtin.partitioned_window") {
            assert_scoped(window, KeyScope::WindowPartitions, 1);
        }
        assert!(plan
            .nodes()
            .iter()
            .all(|n| n.maintenance != Maintenance::Recompute));
    }
}

#[tokio::test]
async fn different_partition_keys_compile_as_separate_scoped_window_nodes() {
    let view = compile("SELECT k,v,ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rk, ROW_NUMBER() OVER (PARTITION BY v ORDER BY k) AS rv FROM a", strict()).await;
    let plan = view.incremental_plan();
    assert_graph(plan);
    let windows = selected(plan, "builtin.partitioned_window");
    assert_eq!(windows.len(), 2);
    for window in &windows {
        assert_scoped(window, KeyScope::WindowPartitions, 1);
    }
    let mut ancestors = BTreeSet::new();
    let mut pending = windows[1].inputs.clone();
    while let Some(id) = pending.pop() {
        if ancestors.insert(id) {
            pending.extend(&plan.nodes()[id].inputs);
        }
    }
    assert!(ancestors.contains(&windows[0].id));
    assert!(plan
        .nodes()
        .iter()
        .all(|n| n.maintenance != Maintenance::Recompute));
}

#[tokio::test]
async fn distinct_and_grouped_having_use_group_state_with_linear_consumers() {
    for sql in [
        "SELECT DISTINCT k,v FROM a",
        "SELECT DISTINCT k+1 AS shifted,v FROM a",
        "SELECT k,SUM(v) AS s FROM a GROUP BY k HAVING SUM(v)>0",
    ] {
        let view = compile(sql, strict()).await;
        let plan = view.incremental_plan();
        assert_graph(plan);
        for group in selected(plan, "builtin.grouped_aggregate") {
            assert_eq!(group.maintenance, Maintenance::ChangedGroups);
            assert_eq!(
                group.maintenance.strategy(),
                Strategy::ChangedGroupRecompute
            );
            assert_eq!(group.maintenance.key_scope(), None);
            assert_eq!(group.inputs.len(), 1);
        }
        for node in plan.nodes() {
            assert_ne!(node.rule.name, "builtin.distinct_recompute");
            assert_ne!(node.maintenance, Maintenance::Recompute);
            if matches!(
                node.rule.name.as_str(),
                "builtin.filter" | "builtin.projection"
            ) {
                assert_eq!(
                    node.maintenance,
                    Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::InputDelta(0)]))
                );
            }
        }
    }
}

#[tokio::test]
async fn repeated_union_inputs_share_graph_state_but_retain_both_delta_ports() {
    let view = compile("SELECT k,v FROM a UNION ALL SELECT k,v FROM a", strict()).await;
    let plan = view.incremental_plan();
    assert_graph(plan);
    let unions = selected(plan, "builtin.union");
    assert_eq!(unions.len(), 1);
    let union = unions[0];
    assert_eq!(union.inputs.len(), 2);
    assert_eq!(
        union.inputs[0], union.inputs[1],
        "identical inputs share maintained state"
    );
    assert_eq!(
        union.maintenance,
        Maintenance::Delta(DeltaExpr::Linear(vec![
            DeltaExpr::InputDelta(0),
            DeltaExpr::InputDelta(1)
        ]))
    );
    assert_eq!(selected(plan, "builtin.source.delta").len(), 1);
}

#[tokio::test]
async fn self_join_sharing_respects_scan_projections_and_qualified_ports() {
    for (sql, expected_scans) in [
        (
            "SELECT a.k,a.v,other.v FROM a FULL JOIN a AS other ON a.k=other.k",
            1,
        ),
        (
            "SELECT a.k,other.v FROM a FULL JOIN a AS other ON a.k=other.k",
            2,
        ),
    ] {
        let view = compile(sql, strict()).await;
        let plan = view.incremental_plan();
        assert_graph(plan);
        let sources = selected(plan, "builtin.source.delta");
        assert_eq!(sources.len(), expected_scans, "{sql}");
        let source_ids = sources.iter().map(|n| n.id).collect::<BTreeSet<_>>();
        let join = selected(plan, "builtin.keyed_join")[0];
        assert_scoped(join, KeyScope::JoinKeys, 2);
        assert_ne!(
            join.inputs[0], join.inputs[1],
            "qualified aliases retain separate ports"
        );
        let mut port_sources = vec![];
        for port in &join.inputs {
            let mut pending = vec![*port];
            let mut seen = BTreeSet::new();
            while let Some(id) = pending.pop() {
                if seen.insert(id) {
                    pending.extend(&plan.nodes()[id].inputs);
                }
            }
            let ancestors = seen
                .intersection(&source_ids)
                .copied()
                .collect::<BTreeSet<_>>();
            assert_eq!(ancestors.len(), 1);
            port_sources.push(ancestors);
        }
        // Equality of complete scan plans permits sharing. Different pushed
        // projections of the same named table must retain distinct scan nodes.
        assert_eq!(port_sources[0] == port_sources[1], expected_scans == 1);
        assert_eq!(
            port_sources[0]
                .union(&port_sources[1])
                .copied()
                .collect::<BTreeSet<_>>(),
            source_ids
        );
    }
}

#[tokio::test]
async fn diagnostics_are_deterministic_and_serialize_the_executed_snapshot_tree() {
    let sql = "SELECT a.k,b.v FROM a FULL JOIN b ON a.k=b.k";
    let first = compile(sql, strict()).await;
    let second = compile(sql, strict()).await;
    assert_eq!(first.incremental_plan(), second.incremental_plan());
    assert_eq!(
        first.incremental_plan().explain(),
        second.incremental_plan().explain()
    );
    let plan = first.incremental_plan();
    let join = selected(plan, "builtin.keyed_join")[0];
    let encoded = serde_json::to_value(plan).unwrap();
    let encoded_join = &encoded["nodes"][join.id];
    assert_eq!(encoded_join["inputs"], json!(join.inputs));
    assert_eq!(
        encoded_join["maintenance"],
        json!({"Delta":{"Sum":[
            {"ScopedSnapshot":{"scope":"JoinKeys","version":"After"}},
            {"Negate":{"ScopedSnapshot":{"scope":"JoinKeys","version":"Before"}}}
        ]}})
    );
    let explain = plan.explain();
    assert!(explain.contains("rule=builtin.keyed_join@1 priority=0 strategy=KeyedRecompute"));
    assert!(explain.contains("ScopedSnapshot { scope: JoinKeys, version: After }"));
    assert!(explain.contains("Negate(ScopedSnapshot { scope: JoinKeys, version: Before })"));
}

#[tokio::test]
async fn removing_scoped_rules_cannot_silently_select_complete_recompute() {
    for (sql, rule) in [
        (
            "SELECT * FROM a FULL JOIN b ON a.k=b.k",
            "builtin.keyed_join",
        ),
        (
            "SELECT ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) FROM a",
            "builtin.partitioned_window",
        ),
    ] {
        for optimizer in [IncrementalOptimizer::default(), strict()] {
            let err =
                SqlView::new_with_optimizer(sql, tables(), optimizer.without_rule(rule).unwrap())
                    .await
                    .err()
                    .unwrap();
            let Error::Rewrite {
                rule,
                phase,
                source,
                ..
            } = err
            else {
                panic!("missing scoped rule must fail planning");
            };
            assert_eq!(rule, "<unmatched>");
            assert_eq!(phase, "match");
            assert!(source.to_string().contains("no incremental rule matched"));
        }
    }
}

struct RecomputeJoin;
impl IncrementalRule for RecomputeJoin {
    fn name(&self) -> &str {
        "test.plan.recompute_join"
    }
    fn priority(&self) -> i32 {
        1000
    }
    fn matches(&self, context: &RuleContext<'_>) -> Result<bool> {
        context.supports_key_scope(KeyScope::JoinKeys)
    }
    fn rewrite(&self, _: &RuleContext<'_>) -> Result<RuleRewrite> {
        Ok(RuleRewrite {
            maintenance: Maintenance::Recompute,
            reason: "plan policy test".into(),
        })
    }
}

#[tokio::test]
async fn strict_policy_checks_selected_custom_rule_without_falling_back_to_builtin() {
    let sql = "SELECT * FROM a FULL JOIN b ON a.k=b.k";
    let optimizer = IncrementalOptimizer::default()
        .with_rule(Arc::new(RecomputeJoin))
        .unwrap();
    let compatible = compile(sql, optimizer.clone()).await;
    let custom = selected(compatible.incremental_plan(), "test.plan.recompute_join");
    assert_eq!(custom.len(), 1);
    assert_eq!(custom[0].maintenance, Maintenance::Recompute);
    let err = SqlView::new_with_optimizer(sql, tables(), optimizer.require_incremental())
        .await
        .err()
        .unwrap();
    assert!(
        matches!(err,Error::Rewrite {rule,phase:"policy",..} if rule=="test.plan.recompute_join")
    );
}

#[tokio::test]
async fn compatibility_recompute_is_explicit_and_strict_reports_the_same_first_node() {
    for (sql, expected_rule) in [
        ("SELECT SUM(v) FROM a", "builtin.aggregate_recompute"),
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY v) FROM a",
            "builtin.window_recompute",
        ),
        (
            "SELECT * FROM a FULL JOIN b ON a.v < b.v",
            "builtin.non_inner_join_recompute",
        ),
        (
            "SELECT * FROM a FULL JOIN b ON CAST(a.k AS DOUBLE)=CAST(b.k AS DOUBLE)",
            "builtin.non_inner_join_recompute",
        ),
        ("SELECT * FROM a ORDER BY k", "builtin.ordered_recompute"),
        ("SELECT * FROM a LIMIT 1", "builtin.source.fetch_recompute"),
    ] {
        let compatible = compile(sql, IncrementalOptimizer::default()).await;
        let plan = compatible.incremental_plan();
        assert_graph(plan);
        let fallback = plan
            .nodes()
            .iter()
            .find(|n| n.maintenance == Maintenance::Recompute)
            .unwrap();
        assert_eq!(fallback.rule.name, expected_rule, "{sql}");
        assert_eq!(fallback.maintenance.key_scope(), None);
        assert_eq!(fallback.maintenance.strategy(), Strategy::OperatorRecompute);
        let err = SqlView::new_with_optimizer(sql, tables(), strict())
            .await
            .err()
            .unwrap();
        let Error::Rewrite {
            node, rule, phase, ..
        } = err
        else {
            panic!("strict mode must identify its rejected plan node");
        };
        assert_eq!(node, fallback.id, "{sql}");
        assert_eq!(rule, expected_rule, "{sql}");
        assert_eq!(phase, "policy");
    }
}
