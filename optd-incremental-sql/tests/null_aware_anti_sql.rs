//! NULL-aware NOT IN has a global dependency outside an equal-key bucket closure.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use optd_incremental_sql::{SqlView, TableChange};

const SQL: &str = "SELECT a.k FROM a WHERE a.k NOT IN (SELECT b.k FROM b)";

fn batch(values: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

async fn full(tables: &BTreeMap<String, RecordBatch>) -> RecordBatch {
    let ctx = SessionContext::new();
    for (name, rows) in tables {
        ctx.register_batch(name, rows.clone()).unwrap();
    }
    let frame = ctx.sql(SQL).await.unwrap();
    let plan = frame.create_physical_plan().await.unwrap();
    concat_batches(
        &plan.schema(),
        &datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn inserting_an_unmatched_right_null_invalidates_all_not_in_output_keys() {
    let before = BTreeMap::from([
        ("a".into(), batch(&[Some(1), Some(2)])),
        ("b".into(), batch(&[Some(1)])),
    ]);
    let view = SqlView::new(SQL, before.clone()).await.unwrap();
    assert_eq!(view.contents().unwrap().num_rows(), 1);
    let candidate = view
        .prepare(&[TableChange {
            table: "b".into(),
            rows: batch(&[None]),
            weights: vec![1],
        }])
        .await
        .unwrap();
    let mut after = before;
    after.insert("b".into(), batch(&[Some(1), None]));
    let expected = full(&after).await;
    assert_eq!(expected.num_rows(), 0);
    assert_eq!(
        candidate.contents().num_rows(),
        expected.num_rows(),
        "{}",
        view.incremental_plan().explain()
    );
}

fn bag(batch: &RecordBatch) -> HashMap<Vec<datafusion::common::ScalarValue>, i64> {
    let mut result = HashMap::new();
    for row in 0..batch.num_rows() {
        let values = batch
            .columns()
            .iter()
            .map(|array| datafusion::common::ScalarValue::try_from_array(array, row).unwrap())
            .collect();
        *result.entry(values).or_default() += 1;
    }
    result
}

#[tokio::test]
async fn null_empty_and_duplicate_transitions_match_full_sql_without_a_false_strict_scope() {
    use optd_incremental_sql::planner::IncrementalOptimizer;
    let left = batch(&[Some(1), Some(2), Some(2), None]);
    let tables = BTreeMap::from([("a".into(), left.clone()), ("b".into(), batch(&[]))]);
    let mut view = SqlView::new(SQL, tables.clone()).await.unwrap();
    assert_eq!(bag(&view.contents().unwrap()), bag(&full(&tables).await));
    assert!(view
        .strategies()
        .iter()
        .any(|node| node.operator.contains("null_aware")
            && node.strategy == optd_incremental_sql::Strategy::OperatorRecompute));
    let schemas = tables
        .iter()
        .map(|(name, rows)| (name.clone(), rows.schema()))
        .collect();
    let report = SqlView::preflight(SQL, schemas, IncrementalOptimizer::default())
        .await
        .unwrap();
    assert!(!report.strict_plan_eligible);
    assert_eq!(report.blockers.len(), 1, "{report}");
    assert!(report
        .to_string()
        .contains("global right-side NULL/empty-input dependency"));
    let error = SqlView::new_with_optimizer(
        SQL,
        tables.clone(),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.plan_preflight(), Some(&report));
    let mut right = Vec::new();
    for (value, sign) in [
        (Some(1), 1),
        (None, 1),
        (None, 1),
        (None, -1),
        (None, -1),
        (Some(1), -1),
    ] {
        let delta = TableChange {
            table: "b".into(),
            rows: batch(&[value]),
            weights: vec![sign],
        };
        if sign == 1 {
            right.push(value);
        } else {
            right.remove(right.iter().position(|row| row == &value).unwrap());
        }
        let after = BTreeMap::from([("a".into(), left.clone()), ("b".into(), batch(&right))]);
        let expected = full(&after).await;
        let discarded = view.prepare(std::slice::from_ref(&delta)).await.unwrap();
        assert_eq!(bag(discarded.contents()), bag(&expected));
        drop(discarded);
        let candidate = view.prepare(&[delta]).await.unwrap();
        assert_eq!(candidate.contents().schema(), expected.schema());
        assert_eq!(bag(candidate.contents()), bag(&expected));
        view.acknowledge(candidate).unwrap();
    }
    assert_eq!(
        view.contents().unwrap().num_rows(),
        4,
        "NULL NOT IN empty is true too"
    );
    for sql in [
        "SELECT a.k FROM a LEFT ANTI JOIN b ON a.k=b.k",
        "SELECT a.k FROM a LEFT ANTI JOIN b ON a.k IS NOT DISTINCT FROM b.k",
        "SELECT a.k FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE a.k=b.k)",
    ] {
        let ordinary = SqlView::new_with_optimizer(
            sql,
            tables.clone(),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        assert!(
            ordinary
                .strategies()
                .iter()
                .all(|node| node.strategy != optd_incremental_sql::Strategy::OperatorRecompute),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn sql_create_refuses_null_aware_scope_before_materialization_and_publication() {
    use optd_incremental_sql::database::SqlSession;
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(k BIGINT); CREATE TABLE b(k BIGINT); INSERT INTO a VALUES (1),(2),(NULL); INSERT INTO b VALUES (1)").await.unwrap();
    let before = db.store_work().unwrap();
    let epoch = db.epoch();
    let pointer = std::fs::read(root.path().join("CURRENT")).unwrap();
    let error = db
        .execute(&format!("CREATE MATERIALIZED VIEW invalid AS {SQL}"))
        .await
        .err()
        .unwrap();
    assert!(error.plan_preflight().unwrap().to_string().contains("R06"));
    assert_eq!(db.epoch(), epoch);
    assert_eq!(db.store_work().unwrap(), before);
    assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), pointer);
    assert!(db.execute("SELECT * FROM invalid").await.is_err());
}
