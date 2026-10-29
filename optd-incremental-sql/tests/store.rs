//! Storage-level acceptance, including real process death at both boundaries.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    ListArray, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use optd_incremental_sql::store::{CheckpointStore, PublishPoint, Snapshot};
use optd_incremental_sql::Error;

// A fork/vfork child briefly shares every open file description until exec
// closes CLOEXEC descriptors. In that window it can retain another test's flock
// after that test drops its File. Keep exact drop/reopen assertions outside the
// only process-spawn window in this test binary; production open stays immediate
// and nonblocking, and a live store must still reject a second writer at once.
static PROCESS_SPAWN_WINDOW: Mutex<()> = Mutex::new(());

fn integers(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap()
}

fn snapshot(epoch: u64) -> Snapshot {
    let value = i64::try_from(epoch).unwrap();
    Snapshot {
        sql: "SELECT id FROM items".into(),
        epoch,
        tables: BTreeMap::from([("items".into(), integers(vec![value, value + 10]))]),
        output: integers(vec![value, value + 10]),
        delta: RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("weight", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![value - 1, value])),
                Arc::new(Int64Array::from(vec![-1, 1])),
            ],
        )
        .unwrap(),
    }
}

fn assert_batch(actual: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.num_rows(), expected.num_rows());
    for (a, b) in actual.columns().iter().zip(expected.columns()) {
        assert_eq!(a.to_data(), b.to_data());
    }
}

fn assert_snapshot(actual: &Snapshot, expected: &Snapshot) {
    assert_eq!(actual.epoch, expected.epoch);
    assert_eq!(actual.sql, expected.sql);
    assert_eq!(actual.tables.len(), expected.tables.len());
    for (name, batch) in &expected.tables {
        assert_batch(&actual.tables[name], batch);
    }
    assert_batch(&actual.output, &expected.output);
    assert_batch(&actual.delta, &expected.delta);
}

fn attempt_count(root: &Path) -> usize {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| {
            entry.file_name().to_string_lossy().starts_with("epoch-")
                && entry.file_type().unwrap().is_dir()
        })
        .count()
}

#[test]
fn exclusive_ownership_empty_store_and_release() {
    let _spawn_window = PROCESS_SPAWN_WINDOW.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    assert!(store.load().unwrap().is_none());
    assert!(CheckpointStore::open(temp.path().join(".")).is_err());
    drop(store);
    assert!(CheckpointStore::open(temp.path()).is_ok());
}

#[test]
fn ipc_roundtrip_preserves_nullable_decimal_temporal_binary_nested_and_schema_metadata() {
    let _spawn_window = PROCESS_SPAWN_WINDOW.lock().unwrap();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(i64::MAX), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(-123456789012345_i128), None])
                .with_precision_and_scale(18, 4)
                .unwrap(),
        ),
        Arc::new(Date32Array::from(vec![Some(20000), None])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![Some(1700000000123456_i64), None])
                .with_timezone("UTC"),
        ),
        Arc::new(BinaryArray::from(vec![Some(&b"\0\xff"[..]), None])),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Float64Array::from(vec![Some(-0.125), None])),
        Arc::new(StringArray::from(vec![Some("typed 字符串"), None])),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(-2)]),
            None,
        ])),
    ];
    let fields: Vec<Field> = arrays
        .iter()
        .enumerate()
        .map(|(index, array)| Field::new(format!("c{index}"), array.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([("fixture".into(), "typed checkpoint".into())]),
    ));
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    let empty = batch.slice(0, 0);
    let expected = Snapshot {
        sql: "SELECT * FROM typed_table".into(),
        epoch: 0,
        tables: BTreeMap::from([
            ("table.with/a quoted name".into(), batch.clone()),
            ("empty".into(), empty),
        ]),
        output: batch.clone(),
        delta: batch,
    };
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    store.commit(None, &expected).unwrap();
    assert_snapshot(&store.load().unwrap().unwrap(), &expected);
    drop(store);
    let reopened = CheckpointStore::open(temp.path()).unwrap();
    assert_snapshot(&reopened.load().unwrap().unwrap(), &expected);
}

#[test]
fn predecessor_epoch_identity_and_schema_contracts_fail_without_publication() {
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    store.commit(None, &snapshot(0)).unwrap();
    let second = snapshot(1);
    store.commit(Some(0), &second).unwrap();
    let current = fs::read(temp.path().join("CURRENT")).unwrap();
    let attempts = attempt_count(temp.path());
    store.commit(Some(0), &second).unwrap();
    assert_eq!(attempt_count(temp.path()), attempts);
    assert!(store.commit(Some(1), &second).is_err());
    assert!(store.commit(None, &snapshot(2)).is_err());
    assert!(store.commit(Some(0), &snapshot(2)).is_err());
    assert!(store.commit(Some(1), &snapshot(3)).is_err());
    let mut conflict = second.clone();
    conflict.output = integers(vec![999]);
    assert!(store.commit(Some(0), &conflict).is_err());
    let mut changed_sql = snapshot(2);
    changed_sql.sql = "SELECT id + 1 FROM items".into();
    assert!(store.commit(Some(1), &changed_sql).is_err());
    let mut changed_schema = snapshot(2);
    changed_schema.tables.insert(
        "items".into(),
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["changed"]))],
        )
        .unwrap(),
    );
    assert!(store.commit(Some(1), &changed_schema).is_err());
    assert_eq!(fs::read(temp.path().join("CURRENT")).unwrap(), current);
    assert_eq!(attempt_count(temp.path()), attempts);
    assert_snapshot(&store.load().unwrap().unwrap(), &second);
}

#[test]
fn same_store_serializes_competing_thread_publications() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(CheckpointStore::open(temp.path()).unwrap());
    store.commit(None, &snapshot(0)).unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let handles: Vec<_> = [111, 222]
        .into_iter()
        .map(|value| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut candidate = snapshot(1);
                candidate.output = integers(vec![value]);
                barrier.wait();
                store.commit(Some(0), &candidate).is_ok()
            })
        })
        .collect();
    barrier.wait();
    let successes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .filter(|success| *success)
        .count();
    assert_eq!(successes, 1);
    assert_eq!(store.load().unwrap().unwrap().epoch, 1);
    assert_eq!(attempt_count(temp.path()), 2);
}

#[test]
fn failed_staging_preserves_orphans_and_lost_response_resolves_idempotently() {
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    store.commit(None, &snapshot(0)).unwrap();
    let result = store.commit_with_observer(Some(0), &snapshot(1), |point| {
        assert_eq!(point, PublishPoint::FilesDurable);
        Err(Error::Invalid("injected failure before publication".into()))
    });
    assert!(result.is_err());
    assert_snapshot(&store.load().unwrap().unwrap(), &snapshot(0));
    assert_eq!(attempt_count(temp.path()), 2);
    fs::create_dir(temp.path().join("unrecognized-orphan")).unwrap();
    let result = store.commit_with_observer(Some(0), &snapshot(1), |point| match point {
        PublishPoint::FilesDurable => Ok(()),
        PublishPoint::CurrentDurable => {
            Err(Error::Invalid("injected lost durable response".into()))
        }
    });
    assert!(result.is_err());
    assert_snapshot(&store.load().unwrap().unwrap(), &snapshot(1));
    assert_eq!(attempt_count(temp.path()), 3);
    store
        .commit_with_observer(Some(0), &snapshot(1), |_| {
            panic!("an already committed identical retry must not publish")
        })
        .unwrap();
    assert!(temp.path().join("unrecognized-orphan").is_dir());
    assert_eq!(attempt_count(temp.path()), 3);
}

fn committed_directory(root: &Path) -> std::path::PathBuf {
    let pointer: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("CURRENT")).unwrap()).unwrap();
    root.join(pointer["directory"].as_str().unwrap())
}

#[test]
fn checksums_reject_modified_manifest_and_ipc_before_any_successor_can_commit() {
    for file in [
        "manifest.json",
        "output.arrow",
        "table-0.arrow",
        "delta.arrow",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = CheckpointStore::open(temp.path()).unwrap();
        store.commit(None, &snapshot(0)).unwrap();
        let path = committed_directory(temp.path()).join(file);
        let mut bytes = fs::read(&path).unwrap();
        let index = bytes.len() / 2;
        bytes[index] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(store.load().is_err(), "corrupt {file}");
        assert!(store.commit(Some(0), &snapshot(1)).is_err());
        assert_eq!(attempt_count(temp.path()), 1);
    }
}

#[test]
fn malformed_or_escaping_current_fails_closed() {
    for bytes in [
        b"not JSON".as_slice(),
        br#"{"format":"optd-incremental-sql-checkpoint/1","directory":"../outside","manifest":{"name":"manifest.json","bytes":0,"sha256":""}}"#.as_slice(),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = CheckpointStore::open(temp.path()).unwrap();
        fs::write(temp.path().join("CURRENT"), bytes).unwrap();
        assert!(store.load().is_err());
        assert!(store.commit(None, &snapshot(0)).is_err());
    }
}

fn replayable_snapshot(epoch: u64) -> Snapshot {
    let mut snapshot = snapshot(epoch);
    let value = i64::try_from(epoch).unwrap();
    snapshot.output = integers((0..=value).collect());
    snapshot
        .tables
        .insert("items".into(), snapshot.output.clone());
    snapshot.delta = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![value])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ],
    )
    .unwrap();
    snapshot
}

#[test]
fn committed_history_ignores_same_epoch_orphans_and_replays_each_delta_once() {
    let _spawn_window = PROCESS_SPAWN_WINDOW.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    assert!(store.history().unwrap().is_empty());
    store.commit(None, &replayable_snapshot(0)).unwrap();
    for value in [111, 222] {
        let mut orphan = replayable_snapshot(1);
        orphan.output = integers(vec![value]);
        assert!(store
            .commit_with_observer(Some(0), &orphan, |point| {
                assert_eq!(point, PublishPoint::FilesDurable);
                Err(Error::Invalid("unpublished competing attempt".into()))
            })
            .is_err());
    }
    store.commit(Some(0), &replayable_snapshot(1)).unwrap();
    store.commit(Some(0), &replayable_snapshot(1)).unwrap();
    store.commit(Some(1), &replayable_snapshot(2)).unwrap();
    assert_eq!(attempt_count(temp.path()), 5);
    let history = store.history().unwrap();
    assert_eq!(
        history
            .iter()
            .map(|snapshot| snapshot.epoch)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    let mut folded = BTreeMap::<i64, i64>::new();
    for snapshot in &history {
        let values = snapshot
            .delta
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let weights = snapshot
            .delta
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..snapshot.delta.num_rows() {
            *folded.entry(values.value(index)).or_default() += weights.value(index);
        }
    }
    assert_eq!(folded, BTreeMap::from([(0, 1), (1, 1), (2, 1)]));
    assert_snapshot(history.last().unwrap(), &replayable_snapshot(2));
    drop(store);
    let reopened = CheckpointStore::open(temp.path()).unwrap();
    assert_eq!(reopened.history().unwrap().len(), 3);
}

#[test]
fn history_checks_predecessor_files_instead_of_only_the_current_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    store.commit(None, &replayable_snapshot(0)).unwrap();
    let predecessor = committed_directory(temp.path());
    store.commit(Some(0), &replayable_snapshot(1)).unwrap();
    fs::write(predecessor.join("delta.arrow"), b"corrupt prior delta").unwrap();
    assert!(store.load().is_ok());
    assert!(store.history().is_err());
}

#[test]
fn store_subprocess_worker() {
    let Ok(root) = std::env::var("OPTD_SQL_STORE_KILL_ROOT") else {
        return;
    };
    let epoch: u64 = std::env::var("OPTD_SQL_STORE_KILL_EPOCH")
        .unwrap()
        .parse()
        .unwrap();
    let expected = epoch.checked_sub(1);
    let stop_after = std::env::var("OPTD_SQL_STORE_KILL_POINT").unwrap();
    let store = CheckpointStore::open(&root).unwrap();
    store
        .commit_with_observer(expected, &snapshot(epoch), |point| {
            let matches = match point {
                PublishPoint::FilesDurable => stop_after == "files",
                PublishPoint::CurrentDurable => stop_after == "current",
            };
            if matches {
                fs::write(Path::new(&root).join("barrier"), b"ready").unwrap();
                loop {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            Ok(())
        })
        .unwrap();
    panic!("worker never reached the requested publication point");
}

#[test]
fn real_process_death_recovers_bootstrap_and_successor_at_both_publication_windows() {
    for epoch in [0_u64, 1] {
        for point in ["files", "current"] {
            let temp = tempfile::tempdir().unwrap();
            if epoch == 1 {
                let store = CheckpointStore::open(temp.path()).unwrap();
                store.commit(None, &snapshot(0)).unwrap();
            }
            let mut child = {
                let _spawn_window = PROCESS_SPAWN_WINDOW.lock().unwrap();
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "store_subprocess_worker",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env("OPTD_SQL_STORE_KILL_ROOT", temp.path())
                    .env("OPTD_SQL_STORE_KILL_EPOCH", epoch.to_string())
                    .env("OPTD_SQL_STORE_KILL_POINT", point)
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap()
            };
            let deadline = Instant::now() + Duration::from_secs(30);
            while !temp.path().join("barrier").exists() {
                if let Some(status) = child.try_wait().unwrap() {
                    let output = child.wait_with_output().unwrap();
                    panic!(
                        "store worker exited {status}: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                if Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("store worker did not reach {point} at epoch {epoch}");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            // A real OS kill runs no destructor, callback cleanup or unwind.
            child.kill().unwrap();
            assert!(!child.wait().unwrap().success());

            let recovered = CheckpointStore::open(temp.path()).unwrap();
            let committed = if point == "current" {
                Some(epoch)
            } else {
                epoch.checked_sub(1)
            };
            let loaded = recovered.load().unwrap();
            assert_eq!(loaded.as_ref().map(|value| value.epoch), committed);
            if let Some(value) = &loaded {
                assert_snapshot(value, &snapshot(value.epoch));
            }
            let next = committed.map_or(0, |value| value + 1);
            recovered.commit(committed, &snapshot(next)).unwrap();
            assert_snapshot(&recovered.load().unwrap().unwrap(), &snapshot(next));
        }
    }
}
