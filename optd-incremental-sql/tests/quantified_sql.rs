//! Independent SQL UNKNOWN semantics must survive quantified-subquery admission.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use optd_incremental_sql::database::{display_rows, SqlSession};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::SqlView;

/// Duplicates and two right NULLs distinguish truth from match multiplicity.
async fn seeded(root: &Path) -> SqlSession {
    let mut db = SqlSession::open(root).await.unwrap();
    db.execute_script("CREATE TABLE a(k BIGINT); CREATE TABLE b(k BIGINT); CREATE TABLE target(k BIGINT); INSERT INTO a VALUES (1),(2),(2),(NULL); INSERT INTO b VALUES (1),(NULL),(NULL); CREATE MATERIALIZED VIEW keep AS SELECT k FROM a").await.unwrap();
    db
}

async fn rows(db: &mut SqlSession, sql: &str) -> Vec<Vec<String>> {
    let batch = db.execute(sql).await.unwrap().rows.unwrap();
    let mut result = display_rows(&batch).unwrap();
    result.sort();
    result
}

fn expected(values: &[&str]) -> Vec<Vec<String>> {
    let mut result = values
        .iter()
        .map(|v| vec![v.to_string()])
        .collect::<Vec<_>>();
    result.sort();
    result
}

fn assert_truth_refusal(error: &optd_incremental_sql::Error) {
    let text = error.to_string();
    assert!(
        text.contains("three-valued") && text.contains("quantified"),
        "{text}"
    );
}

#[tokio::test]
async fn unqualified_correlated_truth_refuses_instead_of_returning_wrong_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut db = seeded(root.path()).await;
    let epoch = db.epoch();
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    for sql in [
        "SELECT k FROM a WHERE k=9 OR k NOT IN (SELECT b.k FROM b WHERE b.k>=a.k)",
        "SELECT k FROM a WHERE NOT (k=9 OR k IN (SELECT b.k FROM b WHERE b.k>=a.k))",
        "SELECT k FROM a WHERE (k IN (SELECT b.k FROM b WHERE b.k>=a.k)) IS FALSE",
        "SELECT k FROM a WHERE (k IN (SELECT b.k FROM b WHERE b.k>=a.k)) = FALSE",
        "SELECT k,k IN (SELECT b.k FROM b WHERE b.k>=a.k) AS membership FROM a",
        "SELECT k FROM a WHERE CASE WHEN k IN (SELECT b.k FROM b WHERE b.k>=a.k) THEN FALSE ELSE TRUE END",
    ] {
        let error = db
            .execute(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("must preserve UNKNOWN before evaluating {sql}"));
        assert_truth_refusal(&error);
        assert_eq!(db.epoch(), epoch);
        assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
    }
    assert_eq!(
        rows(&mut db, "SELECT * FROM a").await,
        expected(&["1", "2", "2", "NULL"])
    );
}

/// These row bags follow the SQL truth table, independent of DataFusion plans.
#[tokio::test]
async fn positive_filter_membership_exists_and_conjunctive_anti_keep_sql_truth() {
    let root = tempfile::tempdir().unwrap();
    let mut db = seeded(root.path()).await;
    for (sql, reference) in [
        ("SELECT k FROM a WHERE k IN (SELECT k FROM b)", vec!["1"]),
        (
            "SELECT k FROM a WHERE k=2 OR k IN (SELECT k FROM b)",
            vec!["1", "2", "2"],
        ),
        (
            "SELECT k FROM a WHERE k IS NULL OR k IN (SELECT k FROM b)",
            vec!["1", "NULL"],
        ),
        (
            "SELECT k FROM a WHERE k>0 AND k IN (SELECT k FROM b)",
            vec!["1"],
        ),
        (
            "SELECT k FROM a WHERE k=9 OR EXISTS (SELECT 1 FROM b WHERE b.k=a.k)",
            vec!["1"],
        ),
        (
            "SELECT k FROM a WHERE k=9 OR NOT EXISTS (SELECT 1 FROM b WHERE b.k=a.k)",
            vec!["2", "2", "NULL"],
        ),
        ("SELECT k FROM a WHERE k NOT IN (SELECT k FROM b)", vec![]),
        (
            "SELECT k FROM a WHERE k>0 AND k NOT IN (SELECT k FROM b)",
            vec![],
        ),
        ("SELECT k FROM a WHERE NOT (k=9 OR k=1)", vec!["2", "2"]),
    ] {
        assert_eq!(rows(&mut db, sql).await, expected(&reference), "{sql}");
    }
    db.execute("DELETE FROM b WHERE k IS NULL").await.unwrap();
    assert_eq!(
        rows(&mut db, "SELECT k FROM a WHERE k NOT IN (SELECT k FROM b)").await,
        expected(&["2", "2"])
    );
    db.execute("DELETE FROM b").await.unwrap();
    assert_eq!(
        rows(&mut db, "SELECT k FROM a WHERE k IN (SELECT k FROM b)").await,
        expected(&[])
    );
    assert_eq!(
        rows(&mut db, "SELECT k FROM a WHERE k NOT IN (SELECT k FROM b)").await,
        expected(&["1", "2", "2", "NULL"])
    );
    db.verify_output_history().unwrap();
}

#[tokio::test]
async fn unsafe_dml_preserves_current_sources_and_does_not_claim_the_request_id() {
    let root = tempfile::tempdir().unwrap();
    let mut db = seeded(root.path()).await;
    for (round, sql) in [
        "INSERT INTO target SELECT k FROM a WHERE k=9 OR k NOT IN (SELECT b.k FROM b WHERE b.k>=a.k)",
        "DELETE FROM a WHERE k=9 OR k NOT IN (SELECT b.k FROM b WHERE b.k>=a.k)",
        "UPDATE a SET k=9 WHERE k=9 OR k NOT IN (SELECT b.k FROM b WHERE b.k>=a.k)",
    ]
    .into_iter()
    .enumerate()
    {
        let epoch = db.epoch();
        let current = std::fs::read(root.path().join("CURRENT")).unwrap();
        let before = rows(&mut db, "SELECT * FROM target").await;
        let id = format!("unsafe-{round}");
        let error = db
            .execute_request(&id, sql)
            .await
            .err()
            .expect("unsafe DML must be refused before staging");
        assert_truth_refusal(&error);
        assert_eq!(db.epoch(), epoch);
        assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
        assert_eq!(rows(&mut db, "SELECT * FROM target").await, before);
        assert_eq!(
            rows(&mut db, "SELECT * FROM a").await,
            expected(&["1", "2", "2", "NULL"])
        );
        assert_eq!(
            rows(&mut db, "SELECT * FROM keep").await,
            expected(&["1", "2", "2", "NULL"])
        );
        let valid = "INSERT INTO target VALUES (7)";
        let receipt = db.execute_request(&id, valid).await.unwrap();
        assert!(!receipt.replayed);
        let report = serde_json::to_value(receipt.commit.unwrap()).unwrap();
        drop(db);
        db = SqlSession::open(root.path()).await.unwrap();
        let retry = db.execute_request(&id, valid).await.unwrap();
        assert!(retry.replayed);
        assert_eq!(serde_json::to_value(retry.commit.unwrap()).unwrap(), report);
        db.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn preflight_and_create_refuse_observed_quantified_truth_before_publication() {
    let root = tempfile::tempdir().unwrap();
    let mut db = seeded(root.path()).await;
    let query = "SELECT k FROM a WHERE k=9 OR k NOT IN (SELECT b.k FROM b WHERE b.k>=a.k)";
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let error = SqlView::preflight(
        query,
        BTreeMap::from([("a".into(), schema.clone()), ("b".into(), schema)]),
        IncrementalOptimizer::default(),
    )
    .await
    .expect_err("preflight must retain quantified provenance");
    assert_truth_refusal(&error);
    let epoch = db.epoch();
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    let before = serde_json::to_value(db.store_work().unwrap()).unwrap();
    let error = db
        .execute(&format!("CREATE MATERIALIZED VIEW refused AS {query}"))
        .await
        .err()
        .expect("CREATE must not publish an unsafe mark plan");
    assert_truth_refusal(&error);
    assert_eq!(db.epoch(), epoch);
    assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
    assert_eq!(
        serde_json::to_value(db.store_work().unwrap()).unwrap(),
        before
    );
    assert!(db.explain_view("refused").is_err());
}
