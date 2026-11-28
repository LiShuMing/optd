//! A packed segment remains live as long as any retained reference needs it.
use optd_incremental_sql::database::{RetentionPolicy, SqlSession};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

fn head(root: &Path) -> Value {
    let pointer: Value = serde_json::from_slice(&fs::read(root.join("CURRENT")).unwrap()).unwrap();
    serde_json::from_slice(
        &fs::read(
            root.join(pointer["directory"].as_str().unwrap())
                .join("manifest.json"),
        )
        .unwrap(),
    )
    .unwrap()
}
fn path(root: &Path, payload: &Value) -> PathBuf {
    root.join(payload["directory"].as_str().unwrap())
        .join(payload["file"]["name"].as_str().unwrap())
}

#[tokio::test]
async fn compact_retains_shared_bundle_until_its_last_block_is_gone() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute("CREATE TABLE t(k BIGINT)").await.unwrap();
    let values = (0..1000)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    db.execute(&format!("INSERT INTO t VALUES {values}"))
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW v AS SELECT k FROM t")
        .await
        .unwrap();
    let old = head(root.path());
    let blocks = old["views"]["v"]["output"]["BlockRelation"]["blocks"]["Inline"]
        .as_array()
        .unwrap();
    assert!(blocks.len() > 1);
    let shared = path(root.path(), &blocks[0]["payload"]);
    assert!(blocks
        .iter()
        .all(|b| path(root.path(), &b["payload"]) == shared));
    db.execute("INSERT INTO t VALUES (1001)").await.unwrap();
    db.compact(RetentionPolicy { retain_epochs: 1 }).unwrap();
    assert!(shared.is_file());
    drop(db);
    let mut db = SqlSession::open(root.path()).await.unwrap();
    assert_eq!(
        db.execute("SELECT * FROM v")
            .await
            .unwrap()
            .rows
            .unwrap()
            .num_rows(),
        1001
    );
    db.verify_output_history().unwrap();
    db.execute("DROP MATERIALIZED VIEW v").await.unwrap();
    db.compact(RetentionPolicy { retain_epochs: 1 }).unwrap();
    assert!(!shared.exists());
    db.verify_output_history().unwrap();
}

#[tokio::test]
async fn damaged_log_segment_prevents_replay_and_damaged_head_prevents_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE t(k BIGINT); CREATE MATERIALIZED VIEW v AS SELECT COUNT(*) AS n FROM t; INSERT INTO t VALUES (1)").await.unwrap();
    let m = head(root.path());
    let output = &m["views"]["v"]["output"]["BlockRelation"]["blocks"]["Inline"][0]["payload"];
    let delta = &m["deltas"]["v"];
    let pack = path(root.path(), delta);
    assert_eq!(path(root.path(), output), pack);
    let offset = delta["offset"].as_u64().unwrap() as usize;
    assert!(offset > output["offset"].as_u64().unwrap() as usize);
    db.execute("INSERT INTO t VALUES (2)").await.unwrap();
    drop(db);
    let mut original = fs::read(&pack).unwrap();
    original[offset] ^= 1;
    fs::write(&pack, &original).unwrap();
    let db = SqlSession::open(root.path()).await.unwrap();
    assert!(db.verify_output_history().is_err());
    drop(db);
    let latest = path(root.path(), &head(root.path())["deltas"]["v"]);
    let file = fs::OpenOptions::new().write(true).open(latest).unwrap();
    file.set_len(file.metadata().unwrap().len() - 9).unwrap();
    assert!(SqlSession::open(root.path()).await.is_err());
}
