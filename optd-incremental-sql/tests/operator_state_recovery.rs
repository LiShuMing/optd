//! End-to-end tests for persistent operator states and bounded-replay recovery (R07b).

use std::fs;
use std::path::Path;

use datafusion::prelude::SessionContext;
use optd_incremental_sql::database::{
    display_rows, IncompatibleStatePolicy, OpenOptions, SqlSession,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

async fn sorted_query(db: &mut SqlSession, sql: &str) -> Vec<Vec<String>> {
    let batch = db.execute(sql).await.unwrap().rows.unwrap();
    let mut rows = display_rows(&batch).unwrap();
    rows.sort();
    rows
}

async fn oracle_from_db(db: &mut SqlSession, tables: &[&str], sql: &str) -> Vec<Vec<String>> {
    let ctx = SessionContext::new();
    for &t in tables {
        let batch = db
            .execute(&format!("SELECT * FROM {t}"))
            .await
            .unwrap()
            .rows
            .unwrap();
        ctx.register_batch(t, batch).unwrap();
    }
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let schema = ctx.sql(sql).await.unwrap().schema().as_arrow().clone();
    let combined =
        datafusion::arrow::compute::concat_batches(&std::sync::Arc::new(schema), &batches).unwrap();
    let mut rows = display_rows(&combined).unwrap();
    rows.sort();
    rows
}

#[tokio::test]
async fn all_operator_state_families_restore_incrementally_and_continue_dml() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script(
        "CREATE TABLE sales(id BIGINT, k BIGINT, amount BIGINT, score DOUBLE);
         CREATE TABLE dims(k BIGINT, weight BIGINT);
         INSERT INTO sales VALUES
           (1, 10, 100, 1.0),
           (2, 10, 200, 3.0),
           (3, 20, 50, 2.0),
           (4, NULL, 75, 4.0);
         INSERT INTO dims VALUES (10, 2), (20, 5), (30, 7);",
    )
    .await
    .unwrap();

    let mv_acc = "SELECT k, COUNT(*) AS n, SUM(amount) AS s, MIN(amount) AS mn, \
                  MAX(amount) AS mx, COUNT(DISTINCT amount) AS dn, \
                  AVG(CAST(amount AS DOUBLE)) AS iavg, STDDEV(score) AS sd, AVG(score) AS favg \
                  FROM sales GROUP BY k";
    let mv_match = "SELECT s.id, s.k, d.weight FROM sales s LEFT JOIN dims d ON s.k = d.k";
    let mv_res =
        "SELECT s.id, s.k, d.weight FROM sales s LEFT JOIN dims d ON s.k = d.k AND s.amount > d.weight * 40";
    let mv_win = "SELECT id, k, SUM(amount) OVER (PARTITION BY k) AS psum FROM sales";
    let mv_rank = "SELECT id, amount, RANK() OVER (ORDER BY amount DESC) AS rnk FROM sales";

    for (name, sql) in [
        ("v_acc", mv_acc),
        ("v_match", mv_match),
        ("v_res", mv_res),
        ("v_win", mv_win),
        ("v_rank", mv_rank),
    ] {
        db.execute(&format!("CREATE MATERIALIZED VIEW {name} AS {sql}"))
            .await
            .unwrap();
    }

    // Apply two DML commits within the default checkpoint interval (K = 16).
    db.execute("INSERT INTO sales VALUES (5, 10, 200, 5.0), (6, 30, 300, 6.0)")
        .await
        .unwrap();
    db.execute("UPDATE dims SET weight = 3 WHERE k = 10")
        .await
        .unwrap();
    db.execute("DELETE FROM sales WHERE id = 3").await.unwrap();
    drop(db);

    // Reopen must restore all 5 views incrementally with zero full rebuilds.
    let mut db = SqlSession::open(root.path()).await.unwrap();
    let rec = db.recovery_work().clone();
    assert_eq!(rec.rebuilt_views, 0);
    assert_eq!(rec.dependency_rows, 0);
    assert_eq!(rec.restored_views, Some(5));
    assert!(rec.replayed_epochs.unwrap() > 0);
    assert!(rec.replayed_delta_rows.unwrap() > 0);
    assert!(rec.state_payload_reads.as_ref().unwrap().files > 0);

    // Continue executing DML after incremental restore and verify every view against full SQL.
    db.execute("UPDATE sales SET k = 20, amount = 400 WHERE id = 1")
        .await
        .unwrap();
    db.execute("DELETE FROM dims WHERE k = 20").await.unwrap();
    db.verify_output_history().unwrap();

    for (name, tables, sql) in [
        ("v_acc", &["sales"][..], mv_acc),
        ("v_match", &["sales", "dims"][..], mv_match),
        ("v_res", &["sales", "dims"][..], mv_res),
        ("v_win", &["sales"][..], mv_win),
        ("v_rank", &["sales"][..], mv_rank),
    ] {
        let actual = sorted_query(&mut db, &format!("SELECT * FROM {name}")).await;
        let expected = oracle_from_db(&mut db, tables, sql).await;
        assert_eq!(actual, expected, "view {name} mismatch after restore + DML");
    }
}

#[tokio::test]
async fn checkpoint_interval_rolls_over_and_resets_replayed_epochs() {
    let root = tempfile::tempdir().unwrap();
    let opts = OpenOptions {
        checkpoint_interval: 4,
        incompatible_state: IncompatibleStatePolicy::Fail,
    };
    let mut db = SqlSession::open_with_options(root.path(), opts)
        .await
        .unwrap();
    db.execute_script(
        "CREATE TABLE t(k BIGINT, v BIGINT);
         INSERT INTO t VALUES (1, 10);
         CREATE MATERIALIZED VIEW mv AS SELECT k, SUM(v) AS total FROM t GROUP BY k;",
    )
    .await
    .unwrap();

    // Commits 1, 2, 3 stay within the replay window; commit 4 publishes a fresh state checkpoint.
    for step in 1..=4u64 {
        db.execute(&format!("INSERT INTO t VALUES (1, {step})"))
            .await
            .unwrap();
        drop(db);
        db = SqlSession::open_with_options(root.path(), opts)
            .await
            .unwrap();
        let rec = db.recovery_work();
        assert_eq!(rec.rebuilt_views, 0);
        assert_eq!(rec.restored_views, Some(1));
        assert_eq!(rec.dependency_rows, 0);
        let expected_replayed = if step == 4 { 0 } else { step };
        assert_eq!(rec.replayed_epochs, Some(expected_replayed), "step {step}");
    }
    assert_eq!(
        sorted_query(&mut db, "SELECT k, total FROM mv").await,
        vec![vec!["1".to_owned(), "20".to_owned()]]
    );
    db.verify_output_history().unwrap();
}

fn rewrite_manifest_and_current(root: &Path, mutate: impl FnOnce(&mut Value)) {
    let pointer: Value = serde_json::from_slice(&fs::read(root.join("CURRENT")).unwrap()).unwrap();
    let dir_name = pointer["directory"].as_str().unwrap().to_owned();
    let dir = root.join(&dir_name);
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(dir.join("manifest.json")).unwrap()).unwrap();
    mutate(&mut manifest);
    let bytes = serde_json::to_vec(&manifest).unwrap();
    fs::write(dir.join("manifest.json"), &bytes).unwrap();
    let sha = format!("{:x}", Sha256::digest(&bytes));
    let next_pointer = serde_json::json!({
        "directory": dir_name,
        "manifest": {
            "name": "manifest.json",
            "bytes": bytes.len() as u64,
            "sha256": sha,
        }
    });
    fs::write(
        root.join("CURRENT"),
        serde_json::to_vec(&next_pointer).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn fingerprint_mismatch_and_corrupt_state_respect_incompatible_policy() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script(
        "CREATE TABLE t(k BIGINT, v BIGINT);
         INSERT INTO t VALUES (1, 10), (2, 20);
         CREATE MATERIALIZED VIEW mv AS SELECT k, SUM(v) AS s FROM t GROUP BY k;",
    )
    .await
    .unwrap();
    drop(db);

    // Mutate plan_fingerprint in manifest (with valid manifest SHA-256) to simulate rule change.
    rewrite_manifest_and_current(root.path(), |m| {
        m["view_states"]["mv"]["plan_fingerprint"] = Value::String("0".repeat(64));
    });

    // Default Fail policy must reject opening without mutating disk.
    let err = SqlSession::open(root.path())
        .await
        .err()
        .expect("mismatched fingerprint must fail under Fail policy");
    assert!(
        err.to_string().contains("fingerprint"),
        "unexpected error: {err}"
    );

    // Explicit Rebuild policy reconstructs from base tables and records rebuilt_views == 1.
    let mut rebuilt = SqlSession::open_with_options(
        root.path(),
        OpenOptions {
            checkpoint_interval: 16,
            incompatible_state: IncompatibleStatePolicy::Rebuild,
        },
    )
    .await
    .unwrap();
    assert_eq!(rebuilt.recovery_work().rebuilt_views, 1);
    assert_eq!(rebuilt.recovery_work().restored_views, Some(0));
    assert_eq!(
        sorted_query(&mut rebuilt, "SELECT k, s FROM mv").await,
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "20".to_owned()],
        ]
    );
    rebuilt.verify_output_history().unwrap();
}
