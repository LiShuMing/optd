//! Storage measurements describe invocation work, independently of durable receipts.

use std::path::Path;

use optd_incremental_sql::database::{display_rows, SqlSession};
use optd_incremental_sql::store::PublishPoint;
use serde_json::Value;

fn manifest(root: &Path) -> (Value, u64, u64) {
    let current = std::fs::read(root.join("CURRENT")).unwrap();
    let pointer: Value = serde_json::from_slice(&current).unwrap();
    let directory = root.join(pointer["directory"].as_str().unwrap());
    let bytes = std::fs::read(directory.join("manifest.json")).unwrap();
    (
        serde_json::from_slice(&bytes).unwrap(),
        current.len() as u64,
        bytes.len() as u64,
    )
}

fn payload_bytes(root: &Path, payload: &Value) -> u64 {
    let path = root
        .join(payload["directory"].as_str().unwrap())
        .join(payload["file"]["name"].as_str().unwrap());
    let actual = std::fs::metadata(path).unwrap().len();
    assert_eq!(actual, payload["file"]["bytes"].as_u64().unwrap());
    actual
}

async fn setup(root: &Path, count: usize) -> SqlSession {
    let mut db = SqlSession::open(root).await.unwrap();
    db.execute("CREATE TABLE t(k BIGINT)").await.unwrap();
    let values = (0..count).map(|k| format!("({k})")).collect::<Vec<_>>();
    db.execute(&format!(
        "INSERT INTO t VALUES {},(NULL),(0)",
        values.join(",")
    ))
    .await
    .unwrap();
    db.execute("CREATE TABLE untouched(k BIGINT)")
        .await
        .unwrap();
    db.execute("INSERT INTO untouched VALUES (100)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW v AS SELECT k FROM t")
        .await
        .unwrap();
    db
}

#[tokio::test]
async fn file_counts_match_serialized_files_and_expose_full_changed_payloads() {
    for count in [32, 256] {
        let root = tempfile::tempdir().unwrap();
        let mut db = setup(root.path(), count).await;
        let before = db.store_work().unwrap();
        db.execute("UPDATE t SET k=-1 WHERE k=1").await.unwrap();
        let after = db.store_work().unwrap();
        let (m, pointer_bytes, manifest_bytes) = manifest(root.path());
        assert_eq!(after.tables.written.files - before.tables.written.files, 1);
        assert_eq!(
            after.tables.written.rows - before.tables.written.rows,
            (count + 2) as u64
        );
        assert_eq!(
            after.tables.written.bytes - before.tables.written.bytes,
            payload_bytes(root.path(), &m["tables"]["t"]["data"])
        );
        assert_eq!(after.tables.reused.files - before.tables.reused.files, 1);
        assert_eq!(after.tables.reused.rows - before.tables.reused.rows, 1);
        assert_eq!(
            after.view_outputs.written.rows - before.view_outputs.written.rows,
            (count + 2) as u64
        );
        assert_eq!(
            after.view_outputs.written.bytes - before.view_outputs.written.bytes,
            payload_bytes(root.path(), &m["views"]["v"]["output"])
        );
        assert_eq!(
            after.output_deltas.written.rows - before.output_deltas.written.rows,
            2
        );
        assert_eq!(
            after.output_deltas.written.bytes - before.output_deltas.written.bytes,
            payload_bytes(root.path(), &m["deltas"]["v"])
        );
        assert_eq!(
            after.checksum_reads.bytes - before.checksum_reads.bytes,
            (after.tables.written.bytes - before.tables.written.bytes)
                + (after.view_outputs.written.bytes - before.view_outputs.written.bytes)
                + (after.output_deltas.written.bytes - before.output_deltas.written.bytes)
        );
        assert_eq!(
            after.manifest_writes.bytes - before.manifest_writes.bytes,
            manifest_bytes
        );
        assert_eq!(
            after.pointer_writes.bytes - before.pointer_writes.bytes,
            pointer_bytes
        );
        assert_eq!(after.tables.read.files, before.tables.read.files);
        // A net-zero statement still publishes metadata but reuses all relation payloads.
        let before = after;
        db.execute("UPDATE t SET k=k").await.unwrap();
        let after = db.store_work().unwrap();
        assert_eq!(after.tables.written, before.tables.written);
        assert_eq!(after.view_outputs.written, before.view_outputs.written);
        assert_eq!(after.output_deltas.written, before.output_deltas.written);
        assert_eq!(after.tables.reused.files - before.tables.reused.files, 2);
        assert_eq!(
            after.view_outputs.reused.files - before.view_outputs.reused.files,
            1
        );
    }
}

#[tokio::test]
async fn reopen_history_and_retry_have_separate_actual_work() {
    let root = tempfile::tempdir().unwrap();
    let mut db = setup(root.path(), 8).await;
    let sql = "INSERT INTO t VALUES (10)";
    let original = db
        .execute_request("once", sql)
        .await
        .unwrap()
        .commit
        .unwrap();
    let epoch = db.epoch();
    drop(db);
    let mut db = SqlSession::open(root.path()).await.unwrap();
    let open = db.store_work().unwrap();
    let recovery = db.recovery_work().clone();
    assert_eq!(recovery.store, open);
    assert_eq!(open.tables.read.files, 2);
    assert_eq!(open.tables.read.rows, 12);
    assert_eq!(open.view_outputs.read.rows, 11);
    assert_eq!(open.output_deltas.read.rows, 1);
    // The head is read for restoration and again during complete lineage validation.
    assert_eq!(open.manifest_reads.files, epoch + 1);
    assert_eq!(open.manifests_visited, epoch);
    assert_eq!(recovery.rebuilt_views, 1);
    assert_eq!(recovery.dependency_rows, 11);
    assert_eq!(recovery.output_rows, 11);
    assert_eq!(recovery.verified_output_rows, 11);
    assert!(recovery.output_array_bytes > 0);
    let retry = db.execute_request("once", sql).await.unwrap();
    assert!(retry.replayed);
    assert_eq!(
        serde_json::to_value(retry.commit.unwrap()).unwrap(),
        serde_json::to_value(original).unwrap()
    );
    let retried = db.store_work().unwrap();
    assert_eq!(
        retried.manifest_reads.files - open.manifest_reads.files,
        epoch
    );
    assert_eq!(retried.tables, open.tables);
    assert_eq!(retried.view_outputs, open.view_outputs);
    assert_eq!(retried.output_deltas, open.output_deltas);
    assert_eq!(retried.manifest_writes, open.manifest_writes);
    assert_eq!(db.recovery_work(), &recovery);
    db.verify_output_history().unwrap();
    let history = db.store_work().unwrap();
    assert_eq!(
        history.manifest_reads.files - retried.manifest_reads.files,
        epoch
    );
    assert!(history.tables.read.rows > retried.tables.read.rows);
    assert!(history.view_outputs.read.rows > retried.view_outputs.read.rows);
    assert_eq!(history.tables.written, retried.tables.written);
    // Independently construct duplicate/NULL-sensitive expected output after recovery.
    let mut actual =
        display_rows(&db.execute("SELECT * FROM v").await.unwrap().rows.unwrap()).unwrap();
    let mut expected = (0..8).map(|k| vec![k.to_string()]).collect::<Vec<_>>();
    expected.extend([vec!["NULL".into()], vec!["0".into()], vec!["10".into()]]);
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn failed_publication_records_completed_work_and_stays_fenced() {
    for barrier in [PublishPoint::FilesDurable, PublishPoint::CurrentDurable] {
        let root = tempfile::tempdir().unwrap();
        let mut db = setup(root.path(), 8).await;
        let epoch = db.epoch();
        let before = db.store_work().unwrap();
        let result = db
            .execute_request_observed("failed", "INSERT INTO t VALUES (99)", |point| {
                if point == barrier {
                    Err(optd_incremental_sql::Error::Invalid(
                        "injected store accounting failure".into(),
                    ))
                } else {
                    Ok(())
                }
            })
            .await;
        assert!(result.is_err());
        let after = db.store_work().unwrap();
        assert_eq!(after.tables.written.files - before.tables.written.files, 1);
        assert_eq!(
            after.manifest_writes.files - before.manifest_writes.files,
            1
        );
        assert_eq!(
            after.pointer_writes.files - before.pointer_writes.files,
            u64::from(barrier == PublishPoint::CurrentDurable)
        );
        assert!(db.execute("SELECT * FROM t").await.is_err());
        drop(db);
        let mut db = SqlSession::open(root.path()).await.unwrap();
        assert_eq!(
            db.epoch(),
            epoch + u64::from(barrier == PublishPoint::CurrentDurable)
        );
        let before = db.store_work().unwrap();
        let result = db
            .execute_request("failed", "INSERT INTO t VALUES (99)")
            .await
            .unwrap();
        assert_eq!(result.replayed, barrier == PublishPoint::CurrentDurable);
        let after = db.store_work().unwrap();
        assert_eq!(
            after.tables.written.files - before.tables.written.files,
            u64::from(barrier == PublishPoint::FilesDurable)
        );
        db.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn empty_and_concurrent_roots_have_independent_counters() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let (a, b) = tokio::join!(SqlSession::open(a.path()), SqlSession::open(b.path()));
    let (mut a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.store_work().unwrap(), Default::default());
    assert_eq!(a.recovery_work(), &Default::default());
    a.execute("CREATE TABLE t(k BIGINT)").await.unwrap();
    assert_eq!(a.store_work().unwrap().tables.written.files, 1);
    assert_eq!(b.store_work().unwrap(), Default::default());
}

#[tokio::test]
async fn recovery_counts_shared_dependencies_once_per_reconstructed_view() {
    let root = tempfile::tempdir().unwrap();
    let mut db = setup(root.path(), 8).await;
    db.execute("CREATE MATERIALIZED VIEW v2 AS SELECT k FROM t WHERE k IS NOT NULL")
        .await
        .unwrap();
    drop(db);
    let db = SqlSession::open(root.path()).await.unwrap();
    let recovery = db.recovery_work();
    assert_eq!(recovery.rebuilt_views, 2);
    assert_eq!(recovery.dependency_rows, 20);
    assert_eq!(recovery.output_rows, 19);
    assert_eq!(recovery.verified_output_rows, 19);
    // Payload decode reads t once, even though both engines rebuild using it.
    assert_eq!(recovery.store.tables.read.files, 2);
    assert_eq!(recovery.store.tables.read.rows, 11);
    assert_eq!(recovery.store.view_outputs.read.files, 2);
}
