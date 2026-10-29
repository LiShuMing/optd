//! Independent metadata and format-2 durability acceptance without Iceberg IO.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use optd_incremental_sql::source_vector::{
    arrow_layout_digest, canonical_arrow_layout, iceberg_schema_digest, snapshot_descriptor_digest,
    CatalogBinding, PinnedSnapshot, PinnedSource, PinnedSourceVector,
};
use optd_incremental_sql::store::{CheckpointStore, PublishPoint, Snapshot, SourceSnapshot};
use optd_incremental_sql::{Error, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

// Isolate drop/reopen assertions from this binary's fork/exec windows. A child
// can briefly inherit another test's open file descriptions before exec.
static PROCESS_ISOLATION: Mutex<()> = Mutex::new(());

fn integers(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap()
}

fn source(name: &str, index: i64, schema: &Schema) -> PinnedSource {
    let iceberg_schema = json!({"type":"struct","schema-id":0,"identifier-field-ids":[],
        "fields":[{"id":1,"name":"id","required":true,"type":"long"}]});
    let arrow_layout = canonical_arrow_layout(schema).unwrap();
    PinnedSource {
        catalog: CatalogBinding { kind: "local/1".into(), canonical_root: "/fixture/catalog".into() },
        namespace: vec!["test".into()], name: name.into(),
        table_uuid: if name == "left_table" { "00000000-0000-0000-0000-000000000001" } else { "00000000-0000-0000-0000-000000000002" }.into(),
        reference: "main".into(),
        snapshot: PinnedSnapshot::Snapshot {
            id: index + 100,
            sequence_number: index + 1,
            descriptor_sha256: snapshot_descriptor_digest(&json!({"snapshot-id":index+100,"sequence-number":index+1,"nested":{"z":1,"a":[{"y":2,"b":3}]}})).unwrap(),
        },
        schema_id: 0, iceberg_schema_sha256: iceberg_schema_digest(&iceberg_schema).unwrap(), iceberg_schema,
        arrow_layout_sha256: arrow_layout_digest(&arrow_layout).unwrap(), arrow_layout, normalizer_version: 1,
    }
}

fn state(epoch: u64) -> SourceSnapshot {
    let value = i64::try_from(epoch).unwrap();
    let batch = integers(vec![value, value + 10]);
    let tables: BTreeMap<String, RecordBatch> = BTreeMap::from([
        ("left_table".into(), batch.clone()),
        ("right_table".into(), batch.clone()),
    ]);
    let sources = tables
        .iter()
        .map(|(name, batch)| (name.clone(), source(name, value, batch.schema().as_ref())))
        .collect();
    SourceSnapshot {
        snapshot: Snapshot {
            sql: "SELECT id FROM left_table".into(),
            epoch,
            tables,
            output: batch,
            delta: RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("weight", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![value])),
                    Arc::new(Int64Array::from(vec![1_i64])),
                ],
            )
            .unwrap(),
        },
        vector: PinnedSourceVector {
            version: 1,
            sources,
        },
    }
}

fn assert_state(actual: &SourceSnapshot, expected: &SourceSnapshot) {
    assert_eq!(actual.vector, expected.vector);
    assert_eq!(actual.position().unwrap(), expected.position().unwrap());
    assert_eq!(actual.snapshot.sql, expected.snapshot.sql);
    assert_eq!(actual.snapshot.epoch, expected.snapshot.epoch);
    for (name, batch) in &expected.snapshot.tables {
        assert_batch(&actual.snapshot.tables[name], batch);
    }
    assert_batch(&actual.snapshot.output, &expected.snapshot.output);
    assert_batch(&actual.snapshot.delta, &expected.snapshot.delta);
}

fn assert_batch(actual: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.num_rows(), expected.num_rows());
    for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
        assert_eq!(actual.to_data(), expected.to_data());
    }
}

fn directory(root: &Path) -> PathBuf {
    let current: Value = serde_json::from_slice(&fs::read(root.join("CURRENT")).unwrap()).unwrap();
    root.join(current["directory"].as_str().unwrap())
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

// Rebind all outer digests after a deliberate semantic metadata edit. This
// distinguishes lineage/schema validation from ordinary checksum detection.
fn replace_vector(root: &Path, bytes: &[u8]) {
    let dir = directory(root);
    fs::write(dir.join("source-vector.json"), bytes).unwrap();
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(dir.join("manifest.json")).unwrap()).unwrap();
    manifest["source_vector"]["bytes"] = json!(bytes.len());
    manifest["source_vector"]["sha256"] = json!(digest(bytes));
    replace_manifest(root, &manifest);
}

fn replace_manifest(root: &Path, manifest: &Value) {
    let dir = directory(root);
    let bytes = serde_json::to_vec(manifest).unwrap();
    fs::write(dir.join("manifest.json"), &bytes).unwrap();
    let mut current: Value =
        serde_json::from_slice(&fs::read(root.join("CURRENT")).unwrap()).unwrap();
    current["manifest"]["bytes"] = json!(bytes.len());
    current["manifest"]["sha256"] = json!(digest(&bytes));
    fs::write(root.join("CURRENT"), serde_json::to_vec(&current).unwrap()).unwrap();
}

fn canonical_json(value: &Value) -> Vec<u8> {
    fn encode(value: &Value) -> String {
        match value {
            Value::Object(map) => format!(
                "{{{}}}",
                map.iter()
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .map(|(key, value)| format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap(),
                        encode(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Value::Array(values) => format!(
                "[{}]",
                values.iter().map(encode).collect::<Vec<_>>().join(",")
            ),
            _ => serde_json::to_string(value).unwrap(),
        }
    }
    encode(value).into_bytes()
}

#[test]
fn canonical_vector_bytes_and_complete_metadata_are_stable_and_hash_bound() {
    let _isolation = PROCESS_ISOLATION.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    let mut expected = state(0);
    for source in expected.vector.sources.values_mut() {
        source.iceberg_schema["fields"][0]["write-default"] = json!({"z":[{"y":1,"a":2}],"a":0});
        source.iceberg_schema_sha256 = iceberg_schema_digest(&source.iceberg_schema).unwrap();
    }
    store.commit_source(None, &expected).unwrap();
    let bytes = fs::read(directory(temp.path()).join("source-vector.json")).unwrap();
    assert_eq!(
        bytes,
        canonical_json(&serde_json::to_value(&expected.vector).unwrap())
    );
    assert_eq!(digest(&bytes), expected.vector.digest().unwrap());
    assert_state(&store.load_source().unwrap().unwrap(), &expected);
    let layout_a = canonical_arrow_layout(&Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([
                ("z".into(), "a".into()),
                ("a".into(), "z".into()),
            ])),
        ],
        HashMap::from([("y".into(), "2".into()), ("b".into(), "1".into())]),
    ))
    .unwrap();
    let layout_b = canonical_arrow_layout(&Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([
                ("a".into(), "z".into()),
                ("z".into(), "a".into()),
            ])),
        ],
        HashMap::from([("b".into(), "1".into()), ("y".into(), "2".into())]),
    ))
    .unwrap();
    assert_eq!(
        arrow_layout_digest(&layout_a).unwrap(),
        arrow_layout_digest(&layout_b).unwrap()
    );
    assert!(store.load().is_err());
    assert!(store.history().is_err());
    assert!(store.commit(Some(0), &state(1).snapshot).is_err());
}

#[test]
fn arrow_layout_preserves_every_qualified_scalar_detail_and_rejects_other_types() {
    let types = vec![
        DataType::Boolean,
        DataType::Int32,
        DataType::Int64,
        DataType::Utf8,
        DataType::Decimal128(18, 4),
        DataType::Date32,
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    ];
    let schema = Schema::new(
        types
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| {
                Field::new(format!("c{index}"), data_type, index % 2 == 0).with_metadata(
                    HashMap::from([("PARQUET:field_id".into(), (index + 1).to_string())]),
                )
            })
            .collect::<Vec<_>>(),
    );
    let layout = canonical_arrow_layout(&schema).unwrap();
    assert_eq!(
        layout["fields"][4]["data_type"],
        json!({"name":"decimal128","precision":18,"scale":4})
    );
    assert_eq!(
        layout["fields"][6]["data_type"],
        json!({"name":"time64","unit":"microsecond"})
    );
    assert_eq!(layout["fields"][7]["data_type"]["timezone"], Value::Null);
    assert_eq!(layout["fields"][8]["data_type"]["timezone"], json!("UTC"));
    assert_eq!(
        layout["fields"][4]["metadata"]["PARQUET:field_id"],
        json!("5")
    );
    let original = arrow_layout_digest(&layout).unwrap();
    for (field, key, value) in [
        (4, "scale", json!(3)),
        (4, "precision", json!(19)),
        (8, "timezone", json!("+00:00")),
    ] {
        let mut changed = layout.clone();
        changed["fields"][field]["data_type"][key] = value;
        assert_ne!(original, arrow_layout_digest(&changed).unwrap());
    }
    for unsupported in [
        DataType::Float64,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::Timestamp(TimeUnit::Millisecond, None),
    ] {
        assert!(
            canonical_arrow_layout(&Schema::new(vec![Field::new("x", unsupported, true)])).is_err()
        );
    }
    assert!(canonical_arrow_layout(&Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("x", DataType::Int64, true)
    ]))
    .is_err());
    let mut nested = Value::Null;
    for _ in 0..130 {
        nested = Value::Array(vec![nested]);
    }
    assert!(iceberg_schema_digest(&nested).is_err());
}

#[test]
fn source_history_retry_and_zero_data_cursor_changes_preserve_exact_identity() {
    let _isolation = PROCESS_ISOLATION.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    assert!(store.load_source().unwrap().is_none());
    assert!(store.history_source().unwrap().is_empty());
    let first = state(0);
    store.commit_source(None, &first).unwrap();
    let expected = first.position().unwrap();
    let mut second = first.clone();
    second.snapshot.epoch = 1;
    second.snapshot.delta = second.snapshot.delta.slice(0, 0);
    second
        .vector
        .sources
        .get_mut("left_table")
        .unwrap()
        .snapshot = state(1).vector.sources["left_table"].snapshot.clone();
    assert!(store
        .commit_source_with_observer(Some(&expected), &second, |point| {
            if point == PublishPoint::CurrentDurable {
                Err(Error::Invalid("lost response".into()))
            } else {
                Ok(())
            }
        })
        .is_err());
    store
        .commit_source_with_observer(Some(&expected), &second, |_| {
            panic!("retry must not publish")
        })
        .unwrap();
    assert_state(&store.load_source().unwrap().unwrap(), &second);
    let mut conflict = second.clone();
    conflict
        .vector
        .sources
        .get_mut("left_table")
        .unwrap()
        .snapshot = state(2).vector.sources["left_table"].snapshot.clone();
    assert!(store.commit_source(Some(&expected), &conflict).is_err());
    let mut unchanged = second.clone();
    unchanged.snapshot.epoch = 2;
    assert!(store
        .commit_source(Some(&second.position().unwrap()), &unchanged)
        .is_err());
    assert_eq!(store.history_source().unwrap().len(), 2);
    drop(store);
    let reopened = CheckpointStore::open(temp.path()).unwrap();
    assert_state(&reopened.load_source().unwrap().unwrap(), &second);
}

#[test]
fn predecessor_bindings_schema_names_and_layout_fail_without_advancement() {
    let _isolation = PROCESS_ISOLATION.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    let first = state(0);
    store.commit_source(None, &first).unwrap();
    let expected = first.position().unwrap();
    let current = fs::read(temp.path().join("CURRENT")).unwrap();
    let mut stale = expected.clone();
    stale.vector_digest = "a".repeat(64);
    assert!(store.commit_source(Some(&stale), &state(1)).is_err());
    for case in 0..12 {
        let mut candidate = state(1);
        let source = candidate.vector.sources.get_mut("left_table").unwrap();
        match case {
            0 => source.table_uuid = "00000000-0000-0000-0000-000000000099".into(),
            1 => source.namespace = vec!["replacement".into()],
            2 => source.schema_id = 1,
            3 => {
                source.iceberg_schema["fields"][0]["id"] = json!(99);
                source.iceberg_schema_sha256 =
                    iceberg_schema_digest(&source.iceberg_schema).unwrap();
            }
            4 => source.arrow_layout["fields"][0]["nullable"] = json!(true),
            5 => source.snapshot = PinnedSnapshot::Empty,
            6 => {
                source.snapshot = PinnedSnapshot::Snapshot {
                    id: 100,
                    sequence_number: 2,
                    descriptor_sha256: "a".repeat(64),
                }
            }
            7 => {
                source.snapshot = PinnedSnapshot::Snapshot {
                    id: 102,
                    sequence_number: 1,
                    descriptor_sha256: "b".repeat(64),
                }
            }
            8 => {
                let binding = candidate.vector.sources.remove("left_table").unwrap();
                candidate
                    .vector
                    .sources
                    .insert("wrong_name".into(), binding);
            }
            9 => {
                source.iceberg_schema["fields"][0]["required"] = json!(false);
                source.iceberg_schema_sha256 =
                    iceberg_schema_digest(&source.iceberg_schema).unwrap();
            }
            10 => {
                source.iceberg_schema["fields"][0]["initial-default"] = json!(1);
                source.iceberg_schema_sha256 =
                    iceberg_schema_digest(&source.iceberg_schema).unwrap();
            }
            _ => {
                source.iceberg_schema["fields"][0]["type"] = json!("int");
                source.iceberg_schema_sha256 =
                    iceberg_schema_digest(&source.iceberg_schema).unwrap();
            }
        }
        assert!(
            store.commit_source(Some(&expected), &candidate).is_err(),
            "case {case}"
        );
        assert_eq!(fs::read(temp.path().join("CURRENT")).unwrap(), current);
        assert_state(&store.load_source().unwrap().unwrap(), &first);
    }
    let mut wrong_schema = state(1);
    wrong_schema.snapshot.tables.insert(
        "left_table".into(),
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["changed"]))],
        )
        .unwrap(),
    );
    assert!(store.commit_source(Some(&expected), &wrong_schema).is_err());
    let mut changed_metadata = state(1);
    let table = changed_metadata
        .snapshot
        .tables
        .get_mut("left_table")
        .unwrap();
    let fields = table
        .schema()
        .fields()
        .iter()
        .map(|field| {
            field
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([("changed".into(), "metadata".into())]))
        })
        .collect::<Vec<_>>();
    *table = RecordBatch::try_new(Arc::new(Schema::new(fields)), table.columns().to_vec()).unwrap();
    let source = changed_metadata
        .vector
        .sources
        .get_mut("left_table")
        .unwrap();
    source.arrow_layout = canonical_arrow_layout(table.schema().as_ref()).unwrap();
    source.arrow_layout_sha256 = arrow_layout_digest(&source.arrow_layout).unwrap();
    assert!(store
        .commit_source(Some(&expected), &changed_metadata)
        .is_err());
    assert_eq!(fs::read(temp.path().join("CURRENT")).unwrap(), current);
}

#[test]
fn initial_empty_cursor_is_typed_and_cannot_replace_a_real_snapshot() {
    let _isolation = PROCESS_ISOLATION.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    let mut first = state(0);
    first.vector.sources.get_mut("left_table").unwrap().snapshot = PinnedSnapshot::Empty;
    assert!(store.commit_source(None, &first).is_err());
    let left = first.snapshot.tables.get_mut("left_table").unwrap();
    *left = left.slice(0, 0);
    store.commit_source(None, &first).unwrap();
    let second = state(1);
    store
        .commit_source(Some(&first.position().unwrap()), &second)
        .unwrap();
    let mut reset = state(2);
    reset.vector.sources.get_mut("left_table").unwrap().snapshot = PinnedSnapshot::Empty;
    let left = reset.snapshot.tables.get_mut("left_table").unwrap();
    *left = left.slice(0, 0);
    assert!(store
        .commit_source(Some(&second.position().unwrap()), &reset)
        .is_err());
}

#[test]
fn format_modes_cannot_adopt_or_splice_one_another() {
    let _isolation = PROCESS_ISOLATION.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let store = CheckpointStore::open(temp.path()).unwrap();
    store.commit(None, &state(0).snapshot).unwrap();
    assert!(store.load_source().is_err());
    assert!(store.history_source().is_err());
    assert!(store.commit_source(None, &state(0)).is_err());
    assert!(store
        .commit_source(Some(&state(0).position().unwrap()), &state(1))
        .is_err());
    let temp2 = tempfile::tempdir().unwrap();
    let store2 = CheckpointStore::open(temp2.path()).unwrap();
    store2.commit_source(None, &state(0)).unwrap();
    store2
        .commit_source(Some(&state(0).position().unwrap()), &state(1))
        .unwrap();
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(directory(temp2.path()).join("manifest.json")).unwrap())
            .unwrap();
    manifest["previous"]["format"] = json!("optd-incremental-sql-checkpoint/1");
    replace_manifest(temp2.path(), &manifest);
    assert!(store2.load_source().is_err());
}

#[test]
fn vector_corruption_missing_bindings_and_coherently_rehashed_splices_fail_closed() {
    let _isolation = PROCESS_ISOLATION.lock().unwrap();
    for case in 0..6 {
        let temp = tempfile::tempdir().unwrap();
        let store = CheckpointStore::open(temp.path()).unwrap();
        store.commit_source(None, &state(0)).unwrap();
        let old = fs::read(directory(temp.path()).join("source-vector.json")).unwrap();
        store
            .commit_source(Some(&state(0).position().unwrap()), &state(1))
            .unwrap();
        let dir = directory(temp.path());
        match case {
            0 => fs::write(dir.join("source-vector.json"), b"corrupted").unwrap(),
            1 => {
                let mut manifest: Value =
                    serde_json::from_slice(&fs::read(dir.join("manifest.json")).unwrap()).unwrap();
                manifest.as_object_mut().unwrap().remove("source_vector");
                replace_manifest(temp.path(), &manifest);
            }
            2 => replace_vector(temp.path(), &old),
            3 => {
                let mut vector = serde_json::to_value(&state(1).vector).unwrap();
                vector["sources"]["left_table"]["table_uuid"] =
                    json!("00000000-0000-0000-0000-000000000099");
                replace_vector(temp.path(), &canonical_json(&vector));
            }
            4 => {
                let bytes = fs::read(dir.join("source-vector.json")).unwrap();
                let mut duplicate = b"{\"version\":1,".to_vec();
                duplicate.extend_from_slice(&bytes[1..]);
                replace_vector(temp.path(), &duplicate);
            }
            _ => {
                let mut manifest: Value =
                    serde_json::from_slice(&fs::read(dir.join("manifest.json")).unwrap()).unwrap();
                manifest["previous_vector_digest"] = json!("a".repeat(64));
                replace_manifest(temp.path(), &manifest);
            }
        }
        assert!(store.load_source().is_err(), "case {case}");
        assert!(store.history_source().is_err(), "case {case}");
        assert!(
            store
                .commit_source(Some(&state(1).position().unwrap()), &state(2))
                .is_err(),
            "case {case}"
        );
    }
}

struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn source_store_subprocess_worker() -> Result<()> {
    let Ok(root) = std::env::var("OPTD_SOURCE_STORE_ROOT") else {
        return Ok(());
    };
    let epoch: u64 = std::env::var("OPTD_SOURCE_STORE_EPOCH")
        .unwrap()
        .parse()
        .unwrap();
    let stop = std::env::var("OPTD_SOURCE_STORE_POINT").unwrap();
    let store = CheckpointStore::open(&root)?;
    let previous = epoch
        .checked_sub(1)
        .map(|epoch| state(epoch).position())
        .transpose()?;
    store.commit_source_with_observer(previous.as_ref(), &state(epoch), |point| {
        let name = if point == PublishPoint::FilesDurable {
            "files"
        } else {
            "current"
        };
        if stop == name {
            let marker = json!({"pid":std::process::id(),"epoch":epoch,"point":name});
            fs::write(
                Path::new(&root).join("barrier.tmp"),
                serde_json::to_vec(&marker)?,
            )?;
            fs::rename(
                Path::new(&root).join("barrier.tmp"),
                Path::new(&root).join("barrier"),
            )?;
            loop {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        Ok(())
    })
}

#[test]
fn process_death_recovers_exact_source_vector_and_continues_both_publication_windows() {
    let _isolation = PROCESS_ISOLATION.lock().unwrap();
    for epoch in [0_u64, 1] {
        for point in ["files", "current"] {
            let temp = tempfile::tempdir().unwrap();
            if epoch == 1 {
                let store = CheckpointStore::open(temp.path()).unwrap();
                store.commit_source(None, &state(0)).unwrap();
            }
            let mut child = OwnedChild(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "source_store_subprocess_worker",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env("OPTD_SOURCE_STORE_ROOT", temp.path())
                    .env("OPTD_SOURCE_STORE_EPOCH", epoch.to_string())
                    .env("OPTD_SOURCE_STORE_POINT", point)
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap(),
            );
            let deadline = Instant::now() + Duration::from_secs(30);
            while !temp.path().join("barrier").exists() {
                if let Some(status) = child.0.try_wait().unwrap() {
                    let mut stderr = String::new();
                    if let Some(pipe) = child.0.stderr.take() {
                        pipe.take(4096).read_to_string(&mut stderr).unwrap();
                    }
                    panic!("child exited {status} before barrier at {epoch}/{point}: {stderr}");
                }
                assert!(
                    Instant::now() < deadline,
                    "barrier timeout at {epoch}/{point}"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            let marker: Value =
                serde_json::from_slice(&fs::read(temp.path().join("barrier")).unwrap()).unwrap();
            assert_eq!(
                marker,
                json!({"pid":child.0.id(),"epoch":epoch,"point":point})
            );
            child.0.kill().unwrap();
            let status = child.0.wait().unwrap();
            assert!(!status.success());
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                assert_eq!(status.signal(), Some(9));
            }
            let store = CheckpointStore::open(temp.path()).unwrap();
            let committed = if point == "current" {
                Some(epoch)
            } else {
                epoch.checked_sub(1)
            };
            let loaded = store.load_source().unwrap();
            assert_eq!(loaded.as_ref().map(|state| state.snapshot.epoch), committed);
            if let Some(actual) = &loaded {
                assert_state(actual, &state(actual.snapshot.epoch));
            }
            let next = committed.map_or(0, |epoch| epoch + 1);
            let previous = loaded
                .as_ref()
                .map(SourceSnapshot::position)
                .transpose()
                .unwrap();
            store
                .commit_source(previous.as_ref(), &state(next))
                .unwrap();
            assert_state(&store.load_source().unwrap().unwrap(), &state(next));
            assert_eq!(
                store.history_source().unwrap().len(),
                usize::try_from(next + 1).unwrap()
            );
            drop(store);
            assert_state(
                &CheckpointStore::open(temp.path())
                    .unwrap()
                    .load_source()
                    .unwrap()
                    .unwrap(),
                &state(next),
            );
        }
    }
}
