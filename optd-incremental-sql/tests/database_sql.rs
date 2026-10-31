//! SQL-driven differential maintenance, transaction and recovery checks.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_incremental_sql::database::{display_rows, SqlSession};
use optd_incremental_sql::store::PublishPoint;
use optd_incremental_sql::Error;

async fn values(db: &mut SqlSession, sql: &str) -> Vec<Vec<String>> {
    let batch = db.execute(sql).await.unwrap().rows.unwrap();
    let mut rows = display_rows(&batch).unwrap();
    rows.sort();
    rows
}

fn expected(rows: &[&[&str]]) -> Vec<Vec<String>> {
    let mut result = rows
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect::<Vec<Vec<String>>>();
    result.sort();
    result
}

async fn oracle(sql: &str) -> RecordBatch {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let frame = ctx.sql(sql).await.unwrap();
    let schema = Arc::new(frame.schema().as_arrow().clone());
    concat_batches(&schema, &frame.collect().await.unwrap()).unwrap()
}

#[tokio::test]
async fn sql_only_group_key_update_delete_reopen_and_replay() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute("CREATE TABLE sales (id BIGINT, k BIGINT, amount BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW by_k AS SELECT k,COUNT(*) AS n,SUM(amount) AS total FROM sales GROUP BY k").await.unwrap();
    db.execute("INSERT INTO sales VALUES (1,10,100),(2,10,200)")
        .await
        .unwrap();
    assert_eq!(
        values(&mut db, "SELECT * FROM by_k").await,
        expected(&[&["10", "2", "300"]])
    );
    let result = db
        .execute("UPDATE sales SET k=20,amount=250 WHERE id=2")
        .await
        .unwrap();
    let stats = &result.commit.unwrap().maintenance["by_k"];
    assert!(stats["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|n| n["strategy"] == "AccumulatorDelta"
            && n["accumulator_work"]["historical_input_rows"] == 0));
    assert_eq!(
        values(&mut db, "SELECT * FROM by_k").await,
        expected(&[&["10", "1", "100"], &["20", "1", "250"]])
    );
    db.execute("DELETE FROM sales WHERE id=1").await.unwrap();
    let epoch = db.epoch();
    db.verify_output_history().unwrap();
    drop(db);
    let mut db = SqlSession::open(root.path()).await.unwrap();
    assert_eq!(db.epoch(), epoch);
    assert_eq!(
        values(&mut db, "SELECT * FROM by_k").await,
        expected(&[&["20", "1", "250"]])
    );
    let reference = oracle("SELECT k, COUNT(*) AS n, SUM(amount) AS total FROM (VALUES (CAST(2 AS BIGINT),CAST(20 AS BIGINT),CAST(250 AS BIGINT))) AS s(id,k,amount) GROUP BY k").await;
    assert_eq!(
        values(&mut db, "SELECT * FROM by_k").await,
        display_rows(&reference).unwrap()
    );
    db.verify_output_history().unwrap();
}

#[tokio::test]
async fn duplicate_null_predicates_assignment_swap_and_self_insert_have_sql_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script(
        "CREATE TABLE t(a BIGINT,b BIGINT); INSERT INTO t VALUES (1,2),(1,2),(NULL,7)",
    )
    .await
    .unwrap();
    let r = db.execute("UPDATE t SET a=b,b=a WHERE a=1").await.unwrap();
    assert_eq!(r.commit.unwrap().affected_rows, 2);
    assert_eq!(
        values(&mut db, "SELECT * FROM t WHERE a IS NOT NULL").await,
        expected(&[&["2", "1"], &["2", "1"]])
    );
    assert_eq!(
        db.execute("DELETE FROM t WHERE a=NULL")
            .await
            .unwrap()
            .commit
            .unwrap()
            .affected_rows,
        0
    );
    db.execute("INSERT INTO t SELECT * FROM t WHERE a=2")
        .await
        .unwrap();
    assert_eq!(values(&mut db, "SELECT * FROM t WHERE a=2").await.len(), 4);
    assert_eq!(
        db.execute("DELETE FROM t WHERE a=2")
            .await
            .unwrap()
            .commit
            .unwrap()
            .affected_rows,
        4
    );
    assert_eq!(
        values(&mut db, "SELECT b FROM t WHERE a IS NULL").await,
        expected(&[&["7"]])
    );
}

#[tokio::test]
async fn insert_column_mapping_casts_not_null_and_unsupported_clauses_are_atomic() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute("CREATE TABLE t(a BIGINT NOT NULL,b DECIMAL(18,2))")
        .await
        .unwrap();
    db.execute("INSERT INTO t(b,a) VALUES (1.25,7)")
        .await
        .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW m AS SELECT COUNT(*) AS n,SUM(b) AS s,AVG(b) AS av FROM t",
    )
    .await
    .unwrap();
    let epoch = db.epoch();
    let before = values(&mut db, "SELECT * FROM m").await;
    for sql in [
        "INSERT INTO t VALUES (NULL,2.5)",
        "UPDATE t SET a=NULL",
        "CREATE TABLE bad(a BIGINT PRIMARY KEY)",
        "CREATE TABLE bad(a BIGINT DEFAULT 1)",
        "CREATE MATERIALIZED VIEW bad AS SELECT * FROM t ORDER BY a",
        "INSERT INTO t VALUES (8,2) RETURNING *",
        "UPDATE t SET a=9 RETURNING *",
        "DELETE FROM t RETURNING *",
        "CREATE VIEW bad AS SELECT * FROM t",
        "UPDATE t SET a=1,a=2",
        "INSERT INTO t SELECT CAST(random()*10 AS BIGINT),1",
    ] {
        assert!(db.execute(sql).await.is_err(), "{sql}");
        assert_eq!(db.epoch(), epoch);
        assert_eq!(values(&mut db, "SELECT * FROM m").await, before);
    }
    db.verify_output_history().unwrap();
}

#[tokio::test]
async fn two_table_two_view_transaction_is_one_epoch_and_rollback_has_no_effect() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(k BIGINT,v BIGINT); CREATE TABLE b(k BIGINT,v BIGINT); CREATE MATERIALIZED VIEW j AS SELECT a.k,a.v,b.v AS bv FROM a JOIN b ON a.k=b.k; CREATE MATERIALIZED VIEW g AS SELECT k,SUM(v) AS s FROM a GROUP BY k").await.unwrap();
    let epoch = db.epoch();
    db.execute("BEGIN").await.unwrap();
    db.execute("INSERT INTO a VALUES (1,10)").await.unwrap();
    db.execute("INSERT INTO b SELECT k,v*2 FROM a")
        .await
        .unwrap();
    assert_eq!(db.epoch(), epoch);
    assert_eq!(
        values(&mut db, "SELECT * FROM b").await,
        expected(&[&["1", "20"]])
    );
    let report = db.execute("COMMIT").await.unwrap().commit.unwrap();
    assert_eq!(db.epoch(), epoch + 1);
    assert_eq!(report.maintenance.len(), 2);
    assert_eq!(
        values(&mut db, "SELECT * FROM j").await,
        expected(&[&["1", "10", "20"]])
    );
    db.execute("BEGIN").await.unwrap();
    db.execute("UPDATE a SET k=2").await.unwrap();
    assert!(db.execute("SELECT * FROM j").await.is_err());
    assert!(db.execute("COMMIT").await.is_err());
    db.execute("ROLLBACK").await.unwrap();
    assert_eq!(db.epoch(), epoch + 1);
    assert_eq!(
        values(&mut db, "SELECT * FROM j").await,
        expected(&[&["1", "10", "20"]])
    );
    db.verify_output_history().unwrap();
}

#[tokio::test]
async fn failure_in_second_view_leaves_sources_and_first_view_unpublished() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE t(k BIGINT,v BIGINT); INSERT INTO t VALUES (1,1); CREATE MATERIALIZED VIEW a AS SELECT k,SUM(v) AS s FROM t GROUP BY k; CREATE MATERIALIZED VIEW z AS SELECT k,10/(v-2) AS value FROM t").await.unwrap();
    let epoch = db.epoch();
    assert!(db.execute("INSERT INTO t VALUES (1,2)").await.is_err());
    assert_eq!(db.epoch(), epoch);
    assert_eq!(
        values(&mut db, "SELECT * FROM t").await,
        expected(&[&["1", "1"]])
    );
    assert_eq!(
        values(&mut db, "SELECT * FROM a").await,
        expected(&[&["1", "1"]])
    );
    drop(db);
    let mut db = SqlSession::open(root.path()).await.unwrap();
    assert_eq!(db.epoch(), epoch);
    assert_eq!(
        values(&mut db, "SELECT * FROM a").await,
        expected(&[&["1", "1"]])
    );
}

#[tokio::test]
async fn request_identity_survives_later_commits_and_rejects_conflicting_sql() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_request("create", "CREATE TABLE t(a BIGINT)")
        .await
        .unwrap();
    let first = db
        .execute_request("one", "INSERT INTO t VALUES (1)")
        .await
        .unwrap()
        .epoch;
    db.execute_request("two", "INSERT INTO t VALUES (2)")
        .await
        .unwrap();
    drop(db);
    let mut db = SqlSession::open(root.path()).await.unwrap();
    let retry = db
        .execute_request("one", "INSERT INTO t VALUES (1)")
        .await
        .unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.epoch, first);
    assert_eq!(
        values(&mut db, "SELECT * FROM t").await,
        expected(&[&["1"], &["2"]])
    );
    assert!(db
        .execute_request("one", "INSERT INTO t VALUES (9)")
        .await
        .is_err());
    let r = db
        .execute_transaction(
            "tx",
            "INSERT INTO t VALUES (3); UPDATE t SET a=a+10 WHERE a=3",
        )
        .await
        .unwrap();
    assert!(!r.replayed);
    assert!(
        db.execute_transaction(
            "tx",
            "INSERT INTO t VALUES (3); UPDATE t SET a=a+10 WHERE a=3"
        )
        .await
        .unwrap()
        .replayed
    );
    assert_eq!(
        values(&mut db, "SELECT a FROM t WHERE a>2").await,
        expected(&[&["13"]])
    );
}

#[tokio::test]
async fn publication_barriers_fence_session_and_reconcile_exact_retry() {
    for point in [PublishPoint::FilesDurable, PublishPoint::CurrentDurable] {
        let root = tempfile::tempdir().unwrap();
        let mut db = SqlSession::open(root.path()).await.unwrap();
        db.execute_script(
            "CREATE TABLE t(a BIGINT); CREATE MATERIALIZED VIEW m AS SELECT COUNT(*) AS n FROM t",
        )
        .await
        .unwrap();
        let epoch = db.epoch();
        assert!(db
            .execute_request_observed("write", "INSERT INTO t VALUES (1)", |p| if p == point {
                Err(Error::Invalid("lost response".into()))
            } else {
                Ok(())
            })
            .await
            .is_err());
        assert!(db.execute("SELECT * FROM t").await.is_err());
        drop(db);
        let mut db = SqlSession::open(root.path()).await.unwrap();
        assert_eq!(
            db.epoch(),
            epoch + u64::from(point == PublishPoint::CurrentDurable)
        );
        let retry = db
            .execute_request("write", "INSERT INTO t VALUES (1)")
            .await
            .unwrap();
        assert_eq!(retry.replayed, point == PublishPoint::CurrentDurable);
        assert_eq!(
            values(&mut db, "SELECT * FROM m").await,
            expected(&[&["1"]])
        );
        db.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn empty_global_group_drop_recreate_and_zero_net_transaction_replay() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE t(k BIGINT,v BIGINT); CREATE MATERIALIZED VIEW m AS SELECT COUNT(*) AS n,SUM(v) AS s FROM t").await.unwrap();
    assert_eq!(
        values(&mut db, "SELECT n FROM m").await,
        expected(&[&["0"]])
    );
    let response = db
        .execute_transaction(
            "noop",
            "INSERT INTO t VALUES (1,2); DELETE FROM t WHERE k=1",
        )
        .await
        .unwrap();
    assert!(response.commit.unwrap().maintenance.is_empty());
    db.execute("DROP MATERIALIZED VIEW m").await.unwrap();
    db.execute("CREATE MATERIALIZED VIEW m AS SELECT k FROM t")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (3,4)").await.unwrap();
    db.verify_output_history().unwrap();
    drop(db);
    let mut db = SqlSession::open(root.path()).await.unwrap();
    assert_eq!(
        values(&mut db, "SELECT * FROM m").await,
        expected(&[&["3"]])
    );
}

#[tokio::test]
async fn writer_lock_is_exclusive_and_malformed_sql_aborts_explicit_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    assert!(SqlSession::open(root.path()).await.is_err());
    db.execute("CREATE TABLE t(a BIGINT)").await.unwrap();
    db.execute("BEGIN").await.unwrap();
    db.execute("INSERT INTO t VALUES (1)").await.unwrap();
    assert!(db.execute("not valid sql").await.is_err());
    assert!(db.execute("SELECT * FROM t").await.is_err());
    db.execute("ROLLBACK").await.unwrap();
    assert!(values(&mut db, "SELECT * FROM t").await.is_empty());
}

#[tokio::test]
async fn qualified_outer_join_and_partitioned_window_are_maintained_from_sql_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(k BIGINT,v BIGINT); CREATE TABLE b(k BIGINT,v BIGINT); CREATE MATERIALIZED VIEW j AS SELECT a.k,a.v,b.v AS bv FROM a LEFT JOIN b ON a.k=b.k; CREATE MATERIALIZED VIEW w AS SELECT k,v,SUM(v) OVER(PARTITION BY k) AS s FROM a").await.unwrap();
    db.execute_transaction(
        "first",
        "INSERT INTO a VALUES (1,10),(1,20); INSERT INTO b VALUES (1,100)",
    )
    .await
    .unwrap();
    assert_eq!(
        values(&mut db, "SELECT * FROM j").await,
        expected(&[&["1", "10", "100"], &["1", "20", "100"]])
    );
    assert_eq!(
        values(&mut db, "SELECT * FROM w").await,
        expected(&[&["1", "10", "30"], &["1", "20", "30"]])
    );
    db.execute_transaction(
        "second",
        "UPDATE a SET k=2 WHERE v=20; DELETE FROM b WHERE k=1",
    )
    .await
    .unwrap();
    assert_eq!(
        values(&mut db, "SELECT k,v,s FROM w").await,
        expected(&[&["1", "10", "10"], &["2", "20", "20"]])
    );
    db.verify_output_history().unwrap();
}

#[tokio::test]
async fn source_and_output_checksums_are_validated_on_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE t(a BIGINT); INSERT INTO t VALUES (1)")
        .await
        .unwrap();
    drop(db);
    let pointer: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("CURRENT")).unwrap()).unwrap();
    let dir = root.path().join(pointer["directory"].as_str().unwrap());
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("manifest.json")).unwrap()).unwrap();
    let payload = &manifest["tables"]["t"]["data"];
    let path = root
        .path()
        .join(payload["directory"].as_str().unwrap())
        .join(payload["file"]["name"].as_str().unwrap());
    std::fs::write(path, b"corrupt").unwrap();
    assert!(SqlSession::open(root.path()).await.is_err());
}

#[tokio::test]
async fn failed_commit_and_script_parse_leave_transaction_aborted_until_rollback() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE t(k BIGINT,v BIGINT); INSERT INTO t VALUES (1,2); CREATE MATERIALIZED VIEW m AS SELECT k,10/v AS n FROM t").await.unwrap();
    let epoch = db.epoch();
    db.execute_script("BEGIN; UPDATE t SET v=0").await.unwrap();
    assert!(db.execute("COMMIT").await.is_err());
    assert_eq!(db.epoch(), epoch);
    assert!(db.execute("SELECT * FROM t").await.is_err());
    db.execute("ROLLBACK").await.unwrap();
    assert_eq!(
        values(&mut db, "SELECT * FROM t").await,
        expected(&[&["1", "2"]])
    );
    db.execute("BEGIN").await.unwrap();
    assert!(db
        .execute_script("INSERT INTO t VALUES (2,3); invalid sql")
        .await
        .is_err());
    assert!(db.execute("COMMIT").await.is_err());
    db.execute("ROLLBACK").await.unwrap();
    assert_eq!(
        values(&mut db, "SELECT * FROM t").await,
        expected(&[&["1", "2"]])
    );
}
