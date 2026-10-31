//! Planning qualification must be complete and must not execute SQL or source state.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::SqlView;

const BLOCKED: &str = "SELECT k,sum(CAST(v AS DOUBLE)) AS s FROM a GROUP BY k UNION ALL SELECT k,avg(v) AS s FROM b GROUP BY k ORDER BY k LIMIT 3";

fn tables() -> BTreeMap<String, RecordBatch> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
        ],
    )
    .unwrap();
    BTreeMap::from([("a".into(), batch.clone()), ("b".into(), batch)])
}

#[tokio::test]
async fn strict_creation_reports_both_numeric_branches_and_ordering() {
    let error = SqlView::new_with_optimizer(
        BLOCKED,
        tables(),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .err()
    .expect("the query needs several unqualified operators");
    let text = error.to_string();
    for marker in ["sum(", "avg(", "Float64", "Sort:"] {
        assert!(text.contains(marker), "missing {marker}: {text}");
    }
}

fn schemas() -> BTreeMap<String, datafusion::arrow::datatypes::SchemaRef> {
    tables()
        .into_iter()
        .map(|(name, batch)| (name, batch.schema()))
        .collect()
}

#[tokio::test]
async fn schema_only_reports_match_full_physical_compilation_for_empty_and_nonempty_inputs() {
    for sql in [
        BLOCKED,
        "SELECT k,sum(v),min(v),count(DISTINCT v) FROM a GROUP BY ROLLUP(k)",
        "SELECT * FROM a FULL JOIN b ON a.k=b.k AND a.v<b.v",
        "SELECT k,row_number() OVER (PARTITION BY k ORDER BY v) FROM a",
        "SELECT k,row_number() OVER (ORDER BY v) FROM a",
    ] {
        let report = SqlView::preflight(sql, schemas(), IncrementalOptimizer::default())
            .await
            .unwrap();
        let repeated = SqlView::preflight(
            sql,
            schemas(),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        assert_eq!(report, repeated, "strict policy must only defer refusal");
        for empty in [false, true] {
            let data = tables()
                .into_iter()
                .map(|(name, batch)| (name, if empty { batch.slice(0, 0) } else { batch }))
                .collect();
            let full = SqlView::new(sql, data).await.unwrap();
            let expected = full.strategies();
            assert_eq!(report.planned_nodes.len(), expected.len(), "{sql}");
            for (actual, expected) in report.planned_nodes.iter().zip(expected) {
                assert_eq!(actual.id, expected.id);
                assert_eq!(actual.operator, expected.operator);
                assert_eq!(actual.strategy, Some(expected.strategy));
                assert_eq!(actual.rule.as_ref().unwrap().name, expected.rule.name);
            }
            let blocked = full
                .strategies()
                .iter()
                .filter(|node| node.strategy == optd_incremental_sql::Strategy::OperatorRecompute)
                .map(|node| node.id)
                .collect::<Vec<_>>();
            assert_eq!(
                report
                    .blockers
                    .iter()
                    .map(|node| node.id)
                    .collect::<Vec<_>>(),
                blocked
            );
        }
        assert_eq!(report.strict_plan_eligible, report.blockers.is_empty());
        assert_eq!(
            serde_json::to_value(&report).unwrap()["strict_plan_eligible"],
            report.strict_plan_eligible
        );
    }
}

#[tokio::test]
async fn typed_reports_distinguish_numeric_and_scope_boundaries_without_running_rows() {
    for (sql, markers) in [
        (
            BLOCKED,
            vec![
                "function=sum",
                "function=avg",
                "Float64",
                "R03e",
                "ordered sequence",
            ],
        ),
        (
            "SELECT var_samp(v),stddev_samp(v) FROM a",
            vec!["var", "stddev", "no qualified", "Float64"],
        ),
        (
            "SELECT row_number() OVER (ORDER BY v) FROM a",
            vec!["common nonempty partition scope=false", "window"],
        ),
        (
            "SELECT * FROM a FULL JOIN b ON a.v<b.v",
            vec!["equality_keys=0", "scope=false"],
        ),
    ] {
        let report = SqlView::preflight(sql, schemas(), IncrementalOptimizer::default())
            .await
            .unwrap();
        assert!(!report.strict_plan_eligible, "{sql}");
        let text = report.to_string();
        for marker in markers {
            assert!(text.contains(marker), "missing {marker}: {text}");
        }
    }
    let sql = "SELECT 10/(k-k) AS fails_at_execution FROM a";
    let report = SqlView::preflight(sql, schemas(), IncrementalOptimizer::default())
        .await
        .unwrap();
    assert!(report.strict_plan_eligible);
    assert!(
        SqlView::new(sql, tables()).await.is_err(),
        "the witness must actually fail when rows execute"
    );
    for sql in [
        "SELECT random() FROM a",
        "SELECT missing FROM a",
        "INSERT INTO a VALUES (1,2)",
    ] {
        assert!(
            SqlView::preflight(sql, schemas(), IncrementalOptimizer::default())
                .await
                .is_err()
        );
    }
}

#[derive(Clone, Copy)]
enum Failure {
    Match,
    Rewrite,
    Verify,
}
struct FailedSourceRule(Failure);
impl optd_incremental_sql::planner::IncrementalRule for FailedSourceRule {
    fn name(&self) -> &str {
        "test.preflight.failed_source"
    }
    fn priority(&self) -> i32 {
        1000
    }
    fn matches(
        &self,
        context: &optd_incremental_sql::planner::RuleContext<'_>,
    ) -> optd_incremental_sql::Result<bool> {
        if !matches!(
            context.logical_plan(),
            datafusion::logical_expr::LogicalPlan::TableScan(_)
        ) {
            return Ok(false);
        }
        if matches!(self.0, Failure::Match) {
            return Err(optd_incremental_sql::Error::Invalid(
                "source match witness".into(),
            ));
        }
        Ok(true)
    }
    fn rewrite(
        &self,
        _: &optd_incremental_sql::planner::RuleContext<'_>,
    ) -> optd_incremental_sql::Result<optd_incremental_sql::planner::RuleRewrite> {
        if matches!(self.0, Failure::Rewrite) {
            return Err(optd_incremental_sql::Error::Invalid(
                "source rewrite witness".into(),
            ));
        }
        Ok(optd_incremental_sql::planner::RuleRewrite {
            maintenance: optd_incremental_sql::planner::Maintenance::Delta(
                optd_incremental_sql::planner::DeltaExpr::InputDelta(999),
            ),
            reason: "intentionally invalid port".into(),
        })
    }
}

#[tokio::test]
async fn all_unmatched_and_independent_rule_failures_are_retained_without_partial_execution() {
    let sql = "SELECT a.v,b.v FROM a JOIN b ON a.k=b.k";
    let report = SqlView::preflight(sql, schemas(), IncrementalOptimizer::empty())
        .await
        .unwrap();
    assert_eq!(report.planned_nodes.len(), report.blockers.len());
    assert!(report
        .planned_nodes
        .iter()
        .all(|node| node.strategy.is_none()));
    assert!(report.blockers.iter().all(|node| node.phase == "match"));
    for (failure, phase) in [
        (Failure::Match, "match"),
        (Failure::Rewrite, "rewrite"),
        (Failure::Verify, "verify"),
    ] {
        let registry = IncrementalOptimizer::default()
            .with_rule(Arc::new(FailedSourceRule(failure)))
            .unwrap();
        let report = SqlView::preflight(sql, schemas(), registry.clone())
            .await
            .unwrap();
        assert_eq!(report.blockers.len(), 2, "{report}");
        assert!(report.blockers.iter().all(
            |blocker| blocker.phase == phase && blocker.rule == "test.preflight.failed_source"
        ));
        assert!(report
            .planned_nodes
            .iter()
            .filter(|node| node.operator.starts_with("TableScan"))
            .all(|node| node.strategy.is_none()));
        let error = SqlView::new_with_optimizer(sql, tables(), registry.require_incremental())
            .await
            .err()
            .unwrap();
        assert_eq!(error.plan_preflight(), Some(&report));
    }
}

#[tokio::test]
async fn failed_create_and_retry_preserve_epoch_catalog_and_actual_storage_work() {
    use optd_incremental_sql::database::SqlSession;
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(k BIGINT,v BIGINT); CREATE TABLE b(k BIGINT,v BIGINT); INSERT INTO a VALUES (1,10),(NULL,20); INSERT INTO b VALUES (1,10)").await.unwrap();
    let epoch = db.epoch();
    let pointer = std::fs::read(root.path().join("CURRENT")).unwrap();
    let before = db.store_work().unwrap();
    let expected = db.preflight(BLOCKED).await.unwrap();
    assert!(!expected.strict_plan_eligible);
    assert_eq!(db.store_work().unwrap(), before);
    let statement = format!("CREATE MATERIALIZED VIEW blocked AS {BLOCKED}");
    let error = db.execute(&statement).await.err().unwrap();
    assert_eq!(error.plan_preflight(), Some(&expected));
    assert_eq!(db.store_work().unwrap(), before);
    for _ in 0..2 {
        let error = db
            .execute_request("refused-create", &statement)
            .await
            .err()
            .unwrap();
        assert_eq!(error.plan_preflight(), Some(&expected));
        assert_eq!(db.epoch(), epoch);
        assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), pointer);
        // Identified retries still perform existing request-history metadata
        // lookup (R07c). Refusal must perform no relation work or writes.
        let after = db.store_work().unwrap();
        assert_eq!(after.pointer_writes, before.pointer_writes);
        assert_eq!(after.manifest_writes, before.manifest_writes);
        assert_eq!(after.tables, before.tables);
        assert_eq!(after.view_outputs, before.view_outputs);
        assert_eq!(after.output_deltas, before.output_deltas);
        assert_eq!(after.table_materialization, before.table_materialization);
        assert_eq!(after.table_leaf_demand, before.table_leaf_demand);
        assert!(db.execute("SELECT * FROM blocked").await.is_err());
    }
    db.execute("BEGIN").await.unwrap();
    assert!(db.preflight("SELECT * FROM a").await.is_err());
    db.execute("ROLLBACK").await.unwrap();
    db.execute_request(
        "refused-create",
        "CREATE MATERIALIZED VIEW ok AS SELECT k,sum(v) AS s FROM a GROUP BY k",
    )
    .await
    .unwrap();
    db.execute("UPDATE a SET v=v+1 WHERE k=1").await.unwrap();
    let output = db.execute("SELECT * FROM ok").await.unwrap().rows.unwrap();
    let epoch = db.epoch();
    drop(db);
    let mut db = SqlSession::open(root.path()).await.unwrap();
    assert_eq!(db.epoch(), epoch);
    assert_eq!(
        db.execute("SELECT * FROM ok").await.unwrap().rows.unwrap(),
        output
    );
    assert!(
        db.preflight("SELECT * FROM a")
            .await
            .unwrap()
            .strict_plan_eligible
    );
}

struct ReplaceProjection {
    matcher: optd_core::rules::RuleMatcher<optd_incremental_sql::planner::plan::IncrementalNode>,
}
impl
    optd_core::rules::Rule<
        optd_incremental_sql::planner::plan::IncrementalNode,
        optd_core::heuristics::HeuristicsOptimizer<
            optd_incremental_sql::planner::plan::IncrementalNode,
        >,
    > for ReplaceProjection
{
    fn name(&self) -> &'static str {
        "test.preflight.physical_replace"
    }
    fn matcher(
        &self,
    ) -> &optd_core::rules::RuleMatcher<optd_incremental_sql::planner::plan::IncrementalNode> {
        &self.matcher
    }
    fn apply(
        &self,
        _: &optd_core::heuristics::HeuristicsOptimizer<
            optd_incremental_sql::planner::plan::IncrementalNode,
        >,
        binding: optd_incremental_sql::planner::plan::IncrementalRel,
    ) -> Vec<optd_core::nodes::PlanNodeOrGroup<optd_incremental_sql::planner::plan::IncrementalNode>>
    {
        use optd_incremental_sql::planner::plan::{logical, ApplyMode, IncrementalNode, Operation};
        let IncrementalNode::Logical(Operation::Apply {
            operator,
            mode: ApplyMode::SignedLinear,
        }) = &binding.typ
        else {
            return vec![];
        };
        if !matches!(
            operator.template.as_ref(),
            datafusion::logical_expr::LogicalPlan::Projection(_)
        ) {
            return vec![];
        }
        // The positive form no longer matches this transformation. It introduces
        // a real root Replace which final physical inspection must detect.
        let output = logical(
            Operation::Apply {
                operator: operator.clone(),
                mode: ApplyMode::Positive,
            },
            binding
                .children
                .iter()
                .map(|child| match child {
                    optd_core::nodes::PlanNodeOrGroup::PlanNode(child) => child.clone(),
                    _ => panic!("test input is fully materialized"),
                })
                .collect(),
        );
        vec![logical(
            Operation::Replace,
            vec![output, logical(Operation::Empty(operator.schema()), vec![])],
        )
        .into()]
    }
}

#[tokio::test]
async fn final_physical_replacement_is_blocked_even_with_native_frontend_maintenance() {
    use optd_core::rules::RuleMatcher;
    use optd_incremental_sql::planner::plan::{IncrementalNode, Operation};
    let optimizer = IncrementalOptimizer::default().with_plan_rule(Arc::new(ReplaceProjection {
        matcher: RuleMatcher::MatchDiscriminant {
            typ_discriminant: std::mem::discriminant(&IncrementalNode::Logical(Operation::Sum)),
            children: vec![RuleMatcher::AnyMany],
        },
    }));
    let sql = "SELECT v AS projected FROM a";
    let report = SqlView::preflight(sql, schemas(), optimizer.clone())
        .await
        .unwrap();
    // The original scan's pushed projection is a kernel too; the caller rule
    // transforms both that kernel and the visible alias projection.
    assert_eq!(report.blockers.len(), 2, "{report}");
    assert_eq!(
        report
            .blockers
            .iter()
            .map(|node| node.rule.as_str())
            .collect::<Vec<_>>(),
        vec!["builtin.source.delta", "builtin.projection"]
    );
    assert!(report
        .blockers
        .iter()
        .all(|node| node.reason.contains("final physical program")));
    assert_eq!(
        report.planned_nodes[report.blockers[0].id].strategy,
        Some(optd_incremental_sql::Strategy::OperatorRecompute)
    );
    let error = SqlView::new_with_optimizer(sql, tables(), optimizer.require_incremental())
        .await
        .err()
        .unwrap();
    assert_eq!(error.plan_preflight(), Some(&report));
}
