use std::{collections::BTreeMap, fs, path::Path};

use optd_incremental_sql::database::{RetentionPolicy, SqlSession};
use optd_incremental_sql::store::PublishPoint;
use optd_incremental_sql::Error;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const DML: &str = "INSERT INTO t VALUES (1)";
const MAX_LEGACY_BYTES: usize = 16 * 1024 * 1024;

fn json_file(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn write_hashed_json(path: &Path, value: &Value, descriptor: &mut Value) {
    let bytes = serde_json::to_vec(value).unwrap();
    fs::write(path, &bytes).unwrap();
    descriptor["bytes"] = json!(bytes.len());
    descriptor["sha256"] = json!(format!("{:x}", Sha256::digest(&bytes)));
}

fn read_payload(root: &Path, payload: &Value) -> Value {
    json_file(
        &root
            .join(payload["directory"].as_str().unwrap())
            .join(payload["file"]["name"].as_str().unwrap()),
    )
}

fn read_receipts(root: &Path, node: &Value, entries: &mut BTreeMap<String, Value>) {
    let node = read_payload(root, node);
    if let Some(leaf) = node["leaf"].as_object() {
        for (id, receipt) in leaf {
            assert!(entries.insert(id.clone(), receipt.clone()).is_none());
        }
    } else {
        for child in node["branch"].as_object().unwrap().values() {
            read_receipts(root, child, entries);
        }
    }
}

/// Reuse an actual receipt shape and SQL fingerprint to generate a bounded v2
/// legacy fixture. Populate it directly to avoid thousands of fsyncs in the test.
async fn legacy_warehouse(root: &Path, near_limit: bool) -> BTreeMap<String, Value> {
    let mut db = SqlSession::open(root).await.unwrap();
    db.execute("CREATE TABLE t (k BIGINT)").await.unwrap();
    db.execute_request("original-00000", DML).await.unwrap();
    drop(db);

    let pointer = root.join("CURRENT");
    let mut head = json_file(&pointer);
    let directory = root.join(head["directory"].as_str().unwrap());
    let manifest_path = directory.join("manifest.json");
    let mut manifest = json_file(&manifest_path);
    let original = if manifest["receipt_root"].is_null() {
        read_payload(root, &manifest["receipt_index"])["original-00000"].clone()
    } else {
        read_payload(root, &manifest["receipt_root"]["node"])["leaf"]["original-00000"].clone()
    };
    let mut entries = BTreeMap::from([("original-00000".to_owned(), original.clone())]);
    if near_limit {
        let entry_bytes = serde_json::to_vec(&entries).unwrap().len() - 1;
        let copies = (MAX_LEGACY_BYTES - 256) / entry_bytes - 1;
        for index in 0..copies {
            let id = format!("fixture--{index:05}");
            let mut receipt = original.clone();
            receipt["id"] = json!(id);
            entries.insert(id, receipt);
        }
        let padding_field =
            &mut entries.get_mut("fixture--00000").unwrap()["report"]["maintenance"];
        *padding_field = json!({"fixture": ""});
        let remaining = MAX_LEGACY_BYTES - 128 - serde_json::to_vec(&entries).unwrap().len();
        entries.get_mut("fixture--00000").unwrap()["report"]["maintenance"]["fixture"] =
            json!("x".repeat(remaining));
        assert_eq!(
            serde_json::to_vec(&entries).unwrap().len(),
            MAX_LEGACY_BYTES - 128
        );
    }
    let mut descriptor = json!({"name": "receipts.json"});
    write_hashed_json(
        &directory.join("receipts.json"),
        &serde_json::to_value(&entries).unwrap(),
        &mut descriptor,
    );
    manifest.as_object_mut().unwrap().remove("receipt_root");
    manifest["receipt_index"] = json!({"directory": head["directory"], "file": descriptor});
    write_hashed_json(&manifest_path, &manifest, &mut head["manifest"]);
    fs::write(pointer, serde_json::to_vec(&head).unwrap()).unwrap();
    entries
}

#[tokio::test]
async fn migrate_at_old_limit_without_losing_retries_and_reopen_after_compaction() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let expected = legacy_warehouse(root, true).await;
    let mut db = SqlSession::open(root).await.unwrap();
    assert!(
        db.execute_request("original-00000", DML)
            .await
            .unwrap()
            .replayed
    );
    let before = db.store_work().unwrap();
    assert!(!db.execute_request("new-00001", DML).await.unwrap().replayed);
    let work = serde_json::to_value(db.store_work().unwrap().since(&before).unwrap()).unwrap();
    assert_eq!(
        work["receipt_index"]["legacy_receipts_copied"],
        expected.len()
    );
    let before = db.store_work().unwrap();
    assert!(!db.execute_request("new-00002", DML).await.unwrap().replayed);
    let work = serde_json::to_value(db.store_work().unwrap().since(&before).unwrap()).unwrap();
    assert_eq!(work["receipt_index"]["legacy_receipts_copied"], 0);
    assert!(work["receipt_index"]["writes"]["bytes"].as_u64().unwrap() < 256 * 1024);
    assert!(work["receipt_index"]["writes"]["files"].as_u64().unwrap() < 12);

    db.compact(RetentionPolicy { retain_epochs: 1 }).unwrap();
    drop(db);
    let head = json_file(&root.join("CURRENT"));
    let manifest = json_file(
        &root
            .join(head["directory"].as_str().unwrap())
            .join("manifest.json"),
    );
    assert!(manifest["receipt_index"].is_null());
    let mut actual = BTreeMap::new();
    read_receipts(root, &manifest["receipt_root"]["node"], &mut actual);
    assert!(actual.remove("new-00001").is_some());
    assert!(actual.remove("new-00002").is_some());
    assert_eq!(actual, expected);

    let mut db = SqlSession::open(root).await.unwrap();
    assert!(
        db.execute_request("fixture--00001", DML)
            .await
            .unwrap()
            .replayed
    );
    assert!(db.execute_request("new-00002", DML).await.unwrap().replayed);
    assert!(db
        .execute_request("original-00000", "INSERT INTO t VALUES (2)")
        .await
        .is_err());
}

#[tokio::test]
async fn migrated_receipts_follow_both_publication_barriers() {
    for barrier in [PublishPoint::FilesDurable, PublishPoint::CurrentDurable] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        legacy_warehouse(root, false).await;
        let previous = fs::read(root.join("CURRENT")).unwrap();
        let mut db = SqlSession::open(root).await.unwrap();
        let failed = db
            .execute_request_observed("new-00001", DML, |point| {
                if point == barrier {
                    return Err(Error::Invalid("injected publication failure".into()));
                }
                Ok(())
            })
            .await;
        assert!(
            matches!(failed, Err(error) if error.to_string().contains("injected publication failure"))
        );
        drop(db);

        let committed = barrier == PublishPoint::CurrentDurable;
        assert_eq!(
            fs::read(root.join("CURRENT")).unwrap() != previous,
            committed
        );
        let mut db = SqlSession::open(root).await.unwrap();
        assert!(
            db.execute_request("original-00000", DML)
                .await
                .unwrap()
                .replayed
        );
        assert_eq!(
            db.execute_request("new-00001", DML).await.unwrap().replayed,
            committed
        );
        db.verify_output_history().unwrap();
    }
}
