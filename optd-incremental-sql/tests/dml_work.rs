//! Actual DML source consumption is distinct from logical relation cardinality.

use optd_incremental_sql::database::{display_rows, DmlWork, SqlSession};
use serde_json::Value;

async fn setup(root: &std::path::Path, n: usize) -> SqlSession {
    let mut db = SqlSession::open(root).await.unwrap();
    db.execute("CREATE TABLE t(k BIGINT NOT NULL)")
        .await
        .unwrap();
    let rows = (0..n).map(|k| format!("({k})")).collect::<Vec<_>>();
    db.execute(&format!("INSERT INTO t VALUES {}", rows.join(",")))
        .await
        .unwrap();
    db
}

async fn work(db: &mut SqlSession, sql: &str) -> Value {
    let report = db.execute(sql).await.unwrap().commit.unwrap();
    serde_json::to_value(&report.dml[0]).unwrap()
}

#[tokio::test]
async fn values_and_selective_updates_expose_changed_leaf_work_as_untouched_rows_grow() {
    for n in [32, 256] {
        let root = tempfile::tempdir().unwrap();
        let mut db = setup(root.path(), n).await;
        let w = work(&mut db, "INSERT INTO t VALUES (999)").await;
        assert_eq!(w["execution"]["selector"]["source_rows"], 0);
        assert_eq!(w["execution"]["selector"]["source_batches"], 0);
        assert_eq!(w["execution"]["new_image_rows"], 1);
        // Target writes copy bounded weighted leaves and validate only distinct rows.
        assert_eq!(w["execution"]["target"]["collected_rows"], 0);
        assert_eq!(w["execution"]["target"]["distinct_entries"], 0);
        assert_eq!(w["execution"]["target"]["delta_update"]["map_rebuilds"], 1);
        assert_eq!(
            w["execution"]["target"]["delta_update"]["old_row_entries_copied"],
            n
        );
        assert_eq!(
            w["execution"]["target"]["delta_update"]["materialized_rows"],
            0
        );
        assert_eq!(w["execution"]["target"]["leaf_validation"]["rows"], n + 1);

        let w = work(&mut db, "UPDATE t SET k=k+10 WHERE k=0").await;
        assert_eq!(w["affected_rows"], 1);
        assert_eq!(w["source_relation_rows"], n + 1);
        assert_eq!(w["execution"]["selector"]["source_rows"], n + 1);
        assert_eq!(w["execution"]["selector"]["output_rows"], 1);
        assert_eq!(w["execution"]["old_image_rows"], 1);
        assert_eq!(w["execution"]["new_image_rows"], 1);
        let mut actual =
            display_rows(&db.execute("SELECT k FROM t").await.unwrap().rows.unwrap()).unwrap();
        actual.sort();
        // Construct expected endpoints independently of the maintained table.
        let mut expected = (0..n)
            .map(|k| vec![if k == 0 { 10 } else { k }.to_string()])
            .collect::<Vec<_>>();
        expected.push(vec!["999".into()]);
        expected.sort();
        assert_eq!(actual, expected);
        db.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn optimized_away_reads_remain_distinct_from_available_relation_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut db = setup(root.path(), 8).await;
    let w = work(&mut db, "DELETE FROM t WHERE false").await;
    assert_eq!(w["source_relation_rows"], 8);
    assert_eq!(w["execution"]["selector"]["source_rows"], 0);
    assert_eq!(w["execution"]["selector"]["source_batches"], 0);
    assert_eq!(w["execution"]["target"]["collected_rows"], 0);
    assert_eq!(w["execution"]["target"]["delta_update"]["map_rebuilds"], 0);
    assert_eq!(
        w["execution"]["target"]["delta_update"]["materializations"],
        0
    );
    let w = work(&mut db, "INSERT INTO t SELECT COUNT(*) FROM t").await;
    assert_eq!(w["source_relation_rows"], 8);
    assert_eq!(w["execution"]["selector"]["source_rows"], 0);
    assert_eq!(w["execution"]["new_image_rows"], 1);
}

#[tokio::test]
async fn source_delivery_counts_limit_and_repeated_scan_executions() {
    let root = tempfile::tempdir().unwrap();
    let mut db = setup(root.path(), 8).await;
    db.execute("CREATE TABLE dst(k BIGINT NOT NULL)")
        .await
        .unwrap();
    let w = work(&mut db, "INSERT INTO dst SELECT k FROM t LIMIT 1").await;
    assert_eq!(w["source_relation_rows"], 8);
    assert_eq!(w["execution"]["selector"]["source_rows"], 1);
    assert_eq!(w["execution"]["selector"]["source_batches"], 1);
    let w = work(
        &mut db,
        "INSERT INTO dst SELECT k FROM t UNION ALL SELECT k FROM t",
    )
    .await;
    assert_eq!(w["source_relation_rows"], 8);
    assert_eq!(w["execution"]["selector"]["source_rows"], 16);
    assert!(
        w["execution"]["selector"]["source_batches"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(w["affected_rows"], 16);
    let w = work(
        &mut db,
        "INSERT INTO dst SELECT k FROM t UNION ALL SELECT 42",
    )
    .await;
    assert_eq!(w["execution"]["selector"]["source_rows"], 8);
    assert_eq!(w["affected_rows"], 9);
}

#[tokio::test]
async fn statement_work_preserves_transaction_snapshots_and_failed_statement_isolation() {
    let root = tempfile::tempdir().unwrap();
    let mut db = setup(root.path(), 8).await;
    let epoch = db.epoch();
    assert!(db.execute("UPDATE t SET k=NULL").await.is_err());
    assert_eq!(db.epoch(), epoch);
    db.execute("BEGIN").await.unwrap();
    db.execute("INSERT INTO t VALUES (99)").await.unwrap();
    db.execute("DELETE FROM t WHERE k=99").await.unwrap();
    let report = db.execute("COMMIT").await.unwrap().commit.unwrap();
    let w = serde_json::to_value(report.dml).unwrap();
    assert_eq!(w[0]["execution"]["selector"]["source_rows"], 0);
    assert_eq!(w[1]["execution"]["selector"]["source_rows"], 9);
    assert_eq!(w[1]["execution"]["old_image_rows"], 1);
    assert_eq!(w[1]["execution"]["target"]["collected_rows"], 0);
    assert_eq!(
        db.execute("SELECT * FROM t")
            .await
            .unwrap()
            .rows
            .unwrap()
            .num_rows(),
        8
    );
}

#[tokio::test]
async fn identified_retry_reopen_and_legacy_receipts_preserve_measurement_identity() {
    let legacy: DmlWork = serde_json::from_value(serde_json::json!({
        "affected_rows": 1, "source_relation_rows": 8, "net_changed_rows": 1
    }))
    .unwrap();
    assert!(serde_json::to_value(legacy)
        .unwrap()
        .get("execution")
        .is_none());
    let root = tempfile::tempdir().unwrap();
    let mut db = setup(root.path(), 8).await;
    let sql = "INSERT INTO t SELECT k+100 FROM t WHERE k=0";
    let report = db
        .execute_request("dml-work", sql)
        .await
        .unwrap()
        .commit
        .unwrap();
    let original = serde_json::to_value(&report).unwrap();
    assert_eq!(
        original["dml"][0]["execution"]["selector"]["source_rows"],
        8
    );
    drop(db);
    let mut db = SqlSession::open(root.path()).await.unwrap();
    let retry = db.execute_request("dml-work", sql).await.unwrap();
    assert!(retry.replayed);
    assert_eq!(
        serde_json::to_value(retry.commit.unwrap()).unwrap(),
        original
    );
    db.verify_output_history().unwrap();
}

#[tokio::test]
async fn concurrent_sessions_do_not_mix_selector_counts() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let mut a = setup(a.path(), 8).await;
    let mut b = setup(b.path(), 32).await;
    let (a, b) = tokio::join!(
        work(&mut a, "DELETE FROM t WHERE k=0"),
        work(&mut b, "DELETE FROM t WHERE k=0")
    );
    assert_eq!(a["execution"]["selector"]["source_rows"], 8);
    assert_eq!(b["execution"]["selector"]["source_rows"], 32);
}

#[tokio::test]
async fn scalar_subquery_sources_are_in_the_measured_selector_tree() {
    let root = tempfile::tempdir().unwrap();
    let mut db = setup(root.path(), 8).await;
    db.execute("CREATE TABLE dst(k BIGINT NOT NULL)")
        .await
        .unwrap();
    let w = work(
        &mut db,
        "INSERT INTO dst SELECT k+(SELECT MAX(k) FROM t) FROM t",
    )
    .await;
    assert_eq!(w["execution"]["selector"]["source_rows"], 16);
    let mut actual =
        display_rows(&db.execute("SELECT * FROM dst").await.unwrap().rows.unwrap()).unwrap();
    actual.sort();
    let mut expected = (7..15).map(|k| vec![k.to_string()]).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected);
}
