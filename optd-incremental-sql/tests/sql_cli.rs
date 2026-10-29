//! Exercise the standalone command handler across real typed checkpoint files.

#[path = "../examples/sql_loop/mod.rs"]
mod command;

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::FileWriter;
use optd_incremental_sql::store::{CheckpointStore, Snapshot};
use serde_json::{json, Value};

fn batch(values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

fn ipc(path: &Path, values: &[i64]) {
    let batch = batch(values);
    let mut writer = FileWriter::try_new(File::create(path).unwrap(), &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
}

async fn call(args: &[&str]) -> optd_incremental_sql::Result<Value> {
    command::run(
        &args
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>(),
    )
    .await
}

fn input(path: &Path, value: Value) {
    fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
}

#[tokio::test]
async fn typed_cli_bootstrap_atomic_changes_restart_and_independent_log_consumption() {
    let root = tempfile::tempdir().unwrap();
    let warehouse = root.path().join("warehouse");
    let sql = root.path().join("view.sql");
    let tables = root.path().join("tables.json");
    let changes = root.path().join("changes.json");
    fs::write(
        &sql,
        "SELECT a.id, COUNT(*) AS n FROM a JOIN b ON a.id=b.id GROUP BY a.id ORDER BY a.id",
    )
    .unwrap();
    ipc(&root.path().join("a.arrow"), &[2, 1, 1]);
    ipc(&root.path().join("b.arrow"), &[1, 2]);
    input(&tables, json!({"a": "a.arrow", "b": "b.arrow"}));
    let warehouse = warehouse.to_str().unwrap();
    let sql = sql.to_str().unwrap();
    let tables = tables.to_str().unwrap();
    let changes = changes.to_str().unwrap();

    let initialized = call(&["init", warehouse, sql, tables]).await.unwrap();
    assert_eq!(initialized["epoch"], 0);
    assert_eq!(
        initialized["output"]["rows"],
        json!([["1", "2"], ["2", "1"]])
    );
    // Idempotent bootstrap is explicit; it must never claim a fresh creation.
    assert_eq!(
        call(&["init", warehouse, sql, tables]).await.unwrap()["status"],
        "initialized_or_identical_bootstrap"
    );

    ipc(&root.path().join("a-change.arrow"), &[1, 3]);
    ipc(&root.path().join("b-change.arrow"), &[3]);
    input(
        Path::new(changes),
        json!({"expected_epoch": 0, "changes": [
            {"table": "a", "ipc": "a-change.arrow", "weights": [-1, 1]},
            {"table": "b", "ipc": "b-change.arrow", "weights": [1]}
        ]}),
    );
    let inserted = call(&["apply", warehouse, changes]).await.unwrap();
    assert_eq!(inserted["epoch"], 1);
    assert_eq!(
        inserted["output"]["rows"],
        json!([["1", "1"], ["2", "1"], ["3", "1"]])
    );
    assert!(
        call(&["apply", warehouse, changes]).await.is_err(),
        "repeated predecessor must fail"
    );
    assert_eq!(call(&["show", warehouse]).await.unwrap()["epoch"], 1);
    assert!(
        call(&["init", warehouse, sql, tables]).await.is_err(),
        "init cannot reset an advanced store"
    );

    ipc(&root.path().join("a-change.arrow"), &[2]);
    ipc(&root.path().join("b-change.arrow"), &[1]);
    input(
        Path::new(changes),
        json!({"expected_epoch": 1, "changes": [
            {"table": "a", "ipc": "a-change.arrow", "weights": [-1]},
            {"table": "b", "ipc": "b-change.arrow", "weights": [-1]}
        ]}),
    );
    let deleted = call(&["apply", warehouse, changes]).await.unwrap();
    assert_eq!(deleted["epoch"], 2);
    assert_eq!(deleted["output"]["rows"], json!([["3", "1"]]));

    ipc(&root.path().join("a-change.arrow"), &[99]);
    input(
        Path::new(changes),
        json!({"expected_epoch": 2, "changes": [
            {"table": "a", "ipc": "a-change.arrow", "weights": [-1]}
        ]}),
    );
    assert!(call(&["apply", warehouse, changes]).await.is_err());
    let shown = call(&["show", warehouse]).await.unwrap();
    assert_eq!(shown["epoch"], 2);
    assert_eq!(shown["output"]["rows"], deleted["output"]["rows"]);
    assert_eq!(
        call(&["replay", warehouse]).await.unwrap()["epochs"],
        json!([
            {"epoch": 0, "rows": 2}, {"epoch": 1, "rows": 3}, {"epoch": 2, "rows": 1}
        ])
    );
}

#[tokio::test]
async fn replay_rejects_checksummed_but_semantically_inconsistent_delta() {
    let root = tempfile::tempdir().unwrap();
    let output = batch(&[1]);
    let delta = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("__optd_weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    )
    .unwrap();
    let store = CheckpointStore::open(root.path()).unwrap();
    store
        .commit(
            None,
            &Snapshot {
                sql: "SELECT id FROM a".into(),
                epoch: 0,
                tables: BTreeMap::from([("a".into(), output.clone())]),
                output,
                delta,
            },
        )
        .unwrap();
    drop(store);
    let error = call(&["replay", root.path().to_str().unwrap()])
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("replayed output differs at epoch 0"));
}

#[tokio::test]
async fn cli_rejects_unknown_options_and_future_epochs_without_advancing() {
    let root = tempfile::tempdir().unwrap();
    let warehouse = root.path().join("warehouse");
    let sql = root.path().join("q.sql");
    let tables = root.path().join("tables.json");
    let changes = root.path().join("changes.json");
    ipc(&root.path().join("a.arrow"), &[1]);
    fs::write(&sql, "SELECT id FROM a").unwrap();
    input(&tables, json!({"a": "a.arrow"}));
    call(&[
        "init",
        warehouse.to_str().unwrap(),
        sql.to_str().unwrap(),
        tables.to_str().unwrap(),
    ])
    .await
    .unwrap();
    input(&changes, json!({"expected_epoch": 99, "changes": []}));
    assert!(call(&[
        "apply",
        warehouse.to_str().unwrap(),
        changes.to_str().unwrap()
    ])
    .await
    .is_err());
    input(
        &changes,
        json!({"expected_epoch": 0, "changes": [], "ignore_errors": true}),
    );
    assert!(call(&[
        "apply",
        warehouse.to_str().unwrap(),
        changes.to_str().unwrap()
    ])
    .await
    .is_err());
    assert_eq!(
        call(&["show", warehouse.to_str().unwrap()]).await.unwrap()["epoch"],
        0
    );
    assert!(call(&["unknown", warehouse.to_str().unwrap()])
        .await
        .is_err());
}
