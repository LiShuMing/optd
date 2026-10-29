#![cfg(feature = "iceberg")]
//! Real typed Parquet endpoints, exact tuples and process-death recovery.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use ::iceberg::arrow::schema_to_arrow_schema;
use ::iceberg::spec::{
    FormatVersion, ManifestListWriter, ManifestWriterBuilder, NestedField, Operation,
    PrimitiveType, Schema, Snapshot, Summary, Type,
};
use ::iceberg::transaction::{ApplyTransactionAction, Transaction};
use ::iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int32Array, Int64Array,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use optd_incremental::iceberg_sink::write_batch_files;
use optd_incremental::local_catalog::LocalCatalog;
use optd_incremental_sql::coordinator::{IcebergRefresh, IcebergSqlView};
use optd_incremental_sql::iceberg::{LocalIcebergSources, SnapshotSelection};
use optd_incremental_sql::source_vector::{PinnedSnapshot, PinnedSourceVector};
use optd_incremental_sql::store::{CheckpointStore, PublishPoint};
use optd_incremental_sql::Error;

const SQL: &str = "SELECT o.id, SUM(l.amount) AS total FROM orders o JOIN lineitems l ON o.id=l.id WHERE o.k>0 GROUP BY o.id ORDER BY o.id";
static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);
static PROCESS_ACTIVITY: Activity = Activity {
    active: Mutex::new(0),
    idle: Condvar::new(),
};

// A fork can inherit another test's flock until exec closes CLOEXEC handles.
// Share ordinary activities and exclude only the synchronous spawn handshake.
struct Activity {
    active: Mutex<usize>,
    idle: Condvar,
}
struct Lease<'a>(&'a Activity);
impl Activity {
    fn enter(&self) -> Lease<'_> {
        *self.active.lock().unwrap() += 1;
        Lease(self)
    }
    fn spawn(&self, command: &mut Command) -> Child {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut active = self.active.lock().unwrap();
        while *active != 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "typed-source test activities did not quiesce"
            );
            active = self.idle.wait_timeout(active, remaining).unwrap().0;
        }
        command.spawn().unwrap()
    }
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        *self.0.active.lock().unwrap() -= 1;
        self.0.idle.notify_all();
    }
}
fn active_block_on<F: Future>(runtime: &tokio::runtime::Runtime, work: F) -> F::Output {
    let _activity = PROCESS_ACTIVITY.enter();
    runtime.block_on(work)
}

#[derive(Clone)]
struct Row {
    id: i64,
    amount: Option<i128>,
    label: Option<&'static str>,
}
fn row(id: i64, amount: i128) -> Row {
    Row {
        id,
        amount: Some(amount),
        label: Some("typed 中文"),
    }
}
fn ident(name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("test".into()), name.into())
}
fn inputs(root: &Path) -> LocalIcebergSources {
    LocalIcebergSources::open(
        root,
        BTreeMap::from([
            ("orders".into(), ident("orders")),
            ("lineitems".into(), ident("lineitems")),
        ]),
    )
    .unwrap()
}
async fn fixture(root: &Path) -> LocalCatalog {
    let catalog = LocalCatalog::open(root).unwrap();
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, Default::default())
        .await
        .unwrap();
    let fields = [
        ("id", PrimitiveType::Long),
        ("k", PrimitiveType::Int),
        (
            "amount",
            PrimitiveType::Decimal {
                precision: 18,
                scale: 2,
            },
        ),
        ("day", PrimitiveType::Date),
        ("label", PrimitiveType::String),
        ("enabled", PrimitiveType::Boolean),
        ("clock", PrimitiveType::Time),
        ("at", PrimitiveType::Timestamp),
        ("zoned", PrimitiveType::Timestamptz),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (name, typ))| {
        Arc::new(if index == 0 {
            NestedField::required(1, name, Type::Primitive(typ))
        } else {
            NestedField::optional(
                i32::try_from(index + 1).unwrap(),
                name,
                Type::Primitive(typ),
            )
        })
    })
    .collect::<Vec<_>>();
    let schema = Schema::builder().with_fields(fields).build().unwrap();
    for name in ["orders", "lineitems"] {
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(name.into())
                    .schema(schema.clone())
                    .format_version(FormatVersion::V2)
                    .properties([("commit.retry.num-retries".into(), "0".into())])
                    .build(),
            )
            .await
            .unwrap();
    }
    catalog
}
fn batch(table: &::iceberg::table::Table, values: &[Row]) -> RecordBatch {
    let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap());
    let timezone = match schema.field(8).data_type() {
        DataType::Timestamp(_, Some(timezone)) => timezone.clone(),
        other => panic!("unexpected fixture timezone type {other}"),
    };
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            values.iter().map(|r| r.id).collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(vec![Some(1); values.len()])),
        Arc::new(
            Decimal128Array::from(values.iter().map(|r| r.amount).collect::<Vec<_>>())
                .with_precision_and_scale(18, 2)
                .unwrap(),
        ),
        Arc::new(Date32Array::from(
            values
                .iter()
                .map(|r| r.label.map(|_| 20_000))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            values.iter().map(|r| r.label).collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|r| r.label.map(|_| true))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Time64MicrosecondArray::from(
            values
                .iter()
                .map(|r| r.label.map(|_| 1_234_567))
                .collect::<Vec<_>>(),
        )),
        Arc::new(TimestampMicrosecondArray::from(
            values
                .iter()
                .map(|r| r.label.map(|_| 1_700_000_000_123_456))
                .collect::<Vec<_>>(),
        )),
        Arc::new(
            TimestampMicrosecondArray::from(
                values
                    .iter()
                    .map(|r| r.label.map(|_| 1_700_000_000_123_456))
                    .collect::<Vec<_>>(),
            )
            .with_timezone(timezone),
        ),
    ];
    RecordBatch::try_new(schema, arrays).unwrap()
}
async fn append(catalog: &LocalCatalog, name: &str, rows: &[Row]) {
    let table = catalog.load_table(&ident(name)).await.unwrap();
    let files = write_batch_files(&table, batch(&table, rows))
        .await
        .unwrap();
    let transaction = Transaction::new(&table);
    transaction
        .fast_append()
        .add_data_files(files)
        .apply(transaction)
        .unwrap()
        .commit(catalog)
        .await
        .unwrap();
}
async fn heads(root: &Path) -> PinnedSourceVector {
    let catalog = LocalCatalog::open(root).unwrap();
    let mut selections = BTreeMap::new();
    for name in ["orders", "lineitems"] {
        let table = catalog.load_table(&ident(name)).await.unwrap();
        selections.insert(
            name.into(),
            table
                .metadata()
                .current_snapshot_id()
                .map(SnapshotSelection::Snapshot)
                .unwrap_or(SnapshotSelection::Empty),
        );
    }
    inputs(root).pin(&selections).await.unwrap()
}

type Rows = HashMap<Vec<ScalarValue>, i64>;
fn rows(batch: &RecordBatch) -> Rows {
    let mut rows = HashMap::new();
    for index in 0..batch.num_rows() {
        let row = batch
            .columns()
            .iter()
            .map(|a| ScalarValue::try_from_array(a.as_ref(), index).unwrap())
            .collect();
        *rows.entry(row).or_default() += 1;
    }
    rows
}
fn equal(actual: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(rows(actual), rows(expected));
}
async fn oracle(root: &Path, vector: &PinnedSourceVector, sql: &str) -> RecordBatch {
    let catalog = LocalCatalog::open(root).unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for (name, source) in &vector.sources {
        let table = catalog.load_table(&ident(&source.name)).await.unwrap();
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap());
        let data = match source.snapshot {
            PinnedSnapshot::Empty => RecordBatch::new_empty(schema),
            PinnedSnapshot::Snapshot { id, .. } => {
                let batches = table
                    .scan()
                    .snapshot_id(id)
                    .select_all()
                    .build()
                    .unwrap()
                    .to_arrow()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                concat_batches(&schema, &batches).unwrap()
            }
        };
        ctx.register_batch(name, data).unwrap();
    }
    let plan = ctx.sql(sql).await.unwrap();
    let physical = plan.create_physical_plan().await.unwrap();
    let schema = physical.schema();
    let batches = datafusion::physical_plan::collect(physical, ctx.task_ctx())
        .await
        .unwrap();
    concat_batches(&schema, &batches).unwrap()
}
fn hand_totals(batch: &RecordBatch, expected: &[(i64, Option<i128>)]) {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (id, value)) in expected.iter().enumerate() {
        assert_eq!(ids.value(index), *id);
        assert_eq!(
            if totals.is_null(index) {
                None
            } else {
                Some(totals.value(index))
            },
            *value
        );
    }
}
fn folded_history(store: &Path) {
    let store = CheckpointStore::open(store).unwrap();
    let history = store.history_source().unwrap();
    let mut folded: Rows = HashMap::new();
    for committed in history {
        let delta = &committed.snapshot.delta;
        let weights = delta
            .column(delta.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..delta.num_rows() {
            let row = delta.columns()[..delta.num_columns() - 1]
                .iter()
                .map(|a| ScalarValue::try_from_array(a.as_ref(), index).unwrap())
                .collect::<Vec<_>>();
            let count = folded.get(&row).copied().unwrap_or(0) + weights.value(index);
            if count == 0 {
                folded.remove(&row);
            } else {
                folded.insert(row, count);
            }
        }
        assert!(folded.values().all(|count| *count > 0));
        assert_eq!(folded, rows(&committed.snapshot.output));
    }
}

async fn overwrite(catalog: &LocalCatalog, name: &str, values: &[Row]) {
    overwrite_profile(catalog, name, values, false).await;
}
async fn overwrite_profile(
    catalog: &LocalCatalog,
    name: &str,
    values: &[Row],
    delete_manifest: bool,
) {
    let table = catalog.load_table(&ident(name)).await.unwrap();
    let sequence = table.metadata().next_sequence_number();
    let mut id = 1000 + sequence;
    while table.metadata().snapshot_by_id(id).is_some() {
        id += 1;
    }
    let files = write_batch_files(&table, batch(&table, values))
        .await
        .unwrap();
    let mut manifests = Vec::new();
    if !files.is_empty() || delete_manifest {
        let output = table
            .file_io()
            .new_output(format!(
                "{}/metadata/typed-{id}.avro",
                table.metadata().location()
            ))
            .unwrap();
        let builder = ManifestWriterBuilder::new(
            output,
            Some(id),
            table.current_schema_ref(),
            table.metadata().default_partition_spec().as_ref().clone(),
        );
        let mut writer = if delete_manifest {
            builder.build_v2_deletes()
        } else {
            builder.build_v2_data()
        };
        for file in files {
            writer.add_file(file, sequence).unwrap();
        }
        manifests.push(writer.write_manifest_file().await.unwrap());
    }
    let list = format!(
        "{}/metadata/typed-list-{id}.avro",
        table.metadata().location()
    );
    let writer = table
        .file_io()
        .new_output(&list)
        .unwrap()
        .writer()
        .await
        .unwrap();
    let mut writer =
        ManifestListWriter::v2(writer, id, table.metadata().current_snapshot_id(), sequence);
    writer.add_manifests(manifests.into_iter()).unwrap();
    writer.close().await.unwrap();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(id)
        .with_parent_snapshot_id(table.metadata().current_snapshot_id())
        .with_sequence_number(sequence)
        .with_manifest_list(list)
        .with_timestamp_ms(
            i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
            .unwrap(),
        )
        .with_schema_id(table.metadata().current_schema_id())
        .with_summary(Summary {
            operation: Operation::Overwrite,
            additional_properties: Default::default(),
        })
        .build();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(table.metadata_location().map(str::to_owned))
        .set_branch_snapshot(snapshot, "main")
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    publish_metadata(
        Path::new(table.metadata().location()),
        serde_json::to_vec(&metadata).unwrap(),
    );
}
fn publish_metadata(table_root: &Path, bytes: Vec<u8>) {
    let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    // Later ordinary Iceberg transactions parse the metadata filename to choose
    // their successor. Keep the standard version/UUID shape for external writes.
    let current = fs::read_to_string(table_root.join("current")).unwrap();
    let location = current
        .parse::<::iceberg::MetadataLocation>()
        .unwrap()
        .with_next_version()
        .to_string();
    let path = PathBuf::from(location.trim_start_matches("file://"));
    fs::write(&path, bytes).unwrap();
    File::open(&path).unwrap().sync_all().unwrap();
    File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
    let pointer = table_root.join(format!("typed-pointer-{id}"));
    fs::write(&pointer, location).unwrap();
    File::open(&pointer).unwrap().sync_all().unwrap();
    fs::rename(pointer, table_root.join("current")).unwrap();
    File::open(table_root).unwrap().sync_all().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_typed_two_table_changes_overwrites_empty_rewrites_and_restart() {
    let _activity = PROCESS_ACTIVITY.enter();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let store = temp.path().join("store");
    let catalog = fixture(&root).await;
    let initial = heads(&root).await;
    let mut view = IcebergSqlView::bootstrap(&store, SQL, inputs(&root), initial.clone())
        .await
        .unwrap();
    assert_eq!(view.view().contents().unwrap().num_rows(), 0);
    append(&catalog, "orders", &[row(1, 100), row(2, 100)]).await;
    append(&catalog, "lineitems", &[row(1, 20_000)]).await;
    append(
        &catalog,
        "lineitems",
        &[
            row(1, 20_000),
            Row {
                id: 2,
                amount: None,
                label: None,
            },
        ],
    )
    .await;
    let target = heads(&root).await;
    let result = view.refresh(target.clone()).await.unwrap();
    let IcebergRefresh::Committed { source_work, .. } = result else {
        panic!("missing commit")
    };
    assert_eq!(source_work.changed_sources, 2);
    assert_eq!(source_work.endpoint_scans, 2); // Empty before endpoints do no IO.
    assert_eq!(source_work.decoded_rows, 5);
    let tables = view.view().tables().unwrap();
    for (name, values) in [
        ("orders", vec![row(1, 100), row(2, 100)]),
        (
            "lineitems",
            vec![
                row(1, 20_000),
                row(1, 20_000),
                Row {
                    id: 2,
                    amount: None,
                    label: None,
                },
            ],
        ),
    ] {
        let table = catalog.load_table(&ident(name)).await.unwrap();
        equal(&tables[name], &batch(&table, &values));
    }
    hand_totals(
        &view.view().contents().unwrap(),
        &[(1, Some(40_000)), (2, None)],
    );
    equal(
        &view.view().contents().unwrap(),
        &oracle(&root, &target, SQL).await,
    );
    for (name, source) in &target.sources {
        assert_eq!(
            source.arrow_layout,
            optd_incremental_sql::source_vector::canonical_arrow_layout(
                view.view().tables().unwrap()[name].schema().as_ref()
            )
            .unwrap()
        );
    }
    assert!(matches!(
        view.refresh(target).await.unwrap(),
        IcebergRefresh::Unchanged { epoch: 1 }
    ));
    overwrite(&catalog, "lineitems", &[row(1, 30_000)]).await;
    let target = heads(&root).await;
    view.refresh(target.clone()).await.unwrap();
    hand_totals(&view.view().contents().unwrap(), &[(1, Some(30_000))]);
    equal(
        &view.view().contents().unwrap(),
        &oracle(&root, &target, SQL).await,
    );
    overwrite(&catalog, "lineitems", &[]).await;
    view.refresh(heads(&root).await).await.unwrap();
    assert_eq!(view.view().contents().unwrap().num_rows(), 0);
    let epoch = view.view().epoch();
    overwrite(&catalog, "lineitems", &[]).await;
    let result = view.refresh(heads(&root).await).await.unwrap();
    let IcebergRefresh::Committed {
        source_work,
        sql_stats,
    } = result
    else {
        panic!("missing physical rewrite commit")
    };
    assert_eq!(sql_stats.epoch, epoch + 1);
    assert_eq!(source_work.signed_change_rows, 0);
    assert_eq!(source_work.endpoint_scans, 2);
    drop(view);
    let mut view = IcebergSqlView::reopen(&store, SQL, inputs(&root))
        .await
        .unwrap();
    append(&catalog, "orders", &[row(3, 100)]).await;
    append(&catalog, "lineitems", &[row(3, -12345)]).await;
    let target = heads(&root).await;
    view.refresh(target.clone()).await.unwrap();
    hand_totals(&view.view().contents().unwrap(), &[(3, Some(-12345))]);
    equal(
        &view.view().contents().unwrap(),
        &oracle(&root, &target, SQL).await,
    );
    drop(view);
    folded_history(&store);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frozen_and_mixed_time_tuples_do_not_follow_later_heads() {
    let _activity = PROCESS_ACTIVITY.enter();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let store = temp.path().join("store");
    let catalog = fixture(&root).await;
    let mut view = IcebergSqlView::bootstrap(&store, SQL, inputs(&root), heads(&root).await)
        .await
        .unwrap();
    append(&catalog, "orders", &[row(1, 100)]).await;
    append(&catalog, "lineitems", &[row(1, 100)]).await;
    let frozen = heads(&root).await;
    append(&catalog, "orders", &[row(2, 100)]).await;
    append(&catalog, "lineitems", &[row(2, 200)]).await;
    let latest = heads(&root).await;
    view.refresh(frozen.clone()).await.unwrap();
    hand_totals(&view.view().contents().unwrap(), &[(1, Some(100))]);
    let mut mixed = frozen;
    mixed
        .sources
        .insert("lineitems".into(), latest.sources["lineitems"].clone());
    view.refresh(mixed.clone()).await.unwrap();
    hand_totals(&view.view().contents().unwrap(), &[(1, Some(100))]);
    equal(
        &view.view().contents().unwrap(),
        &oracle(&root, &mixed, SQL).await,
    );
    view.refresh(latest.clone()).await.unwrap();
    hand_totals(
        &view.view().contents().unwrap(),
        &[(1, Some(100)), (2, Some(200))],
    );
    equal(
        &view.view().contents().unwrap(),
        &oracle(&root, &latest, SQL).await,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_self_join_uses_one_source_binding_and_exact_duplicate_counts() {
    let _activity = PROCESS_ACTIVITY.enter();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let catalog = fixture(&root).await;
    let sources = || {
        LocalIcebergSources::open(&root, BTreeMap::from([("orders".into(), ident("orders"))]))
            .unwrap()
    };
    let sql = "SELECT a.id, b.amount FROM orders a JOIN orders b ON a.id=b.id";
    let initial = sources()
        .pin(&BTreeMap::from([(
            "orders".into(),
            SnapshotSelection::Empty,
        )]))
        .await
        .unwrap();
    let mut view = IcebergSqlView::bootstrap(temp.path().join("store"), sql, sources(), initial)
        .await
        .unwrap();
    append(&catalog, "orders", &[row(1, 100), row(1, 100)]).await;
    let table = catalog.load_table(&ident("orders")).await.unwrap();
    let target = sources()
        .pin(&BTreeMap::from([(
            "orders".into(),
            SnapshotSelection::Snapshot(table.metadata().current_snapshot_id().unwrap()),
        )]))
        .await
        .unwrap();
    view.refresh(target.clone()).await.unwrap();
    assert_eq!(view.view().contents().unwrap().num_rows(), 4);
    equal(
        &view.view().contents().unwrap(),
        &oracle(&root, &target, sql).await,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_bindings_rollback_identity_and_schema_leave_current_unchanged() {
    let _activity = PROCESS_ACTIVITY.enter();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let store = temp.path().join("store");
    let catalog = fixture(&root).await;
    append(&catalog, "orders", &[row(1, 100)]).await;
    append(&catalog, "lineitems", &[row(1, 100)]).await;
    let old = heads(&root).await;
    append(&catalog, "orders", &[row(2, 100)]).await;
    let current = heads(&root).await;
    let mut view = IcebergSqlView::bootstrap(&store, SQL, inputs(&root), current.clone())
        .await
        .unwrap();
    let pointer = fs::read(store.join("CURRENT")).unwrap();
    let output = view.view().contents().unwrap();
    let mut missing = current.clone();
    missing.sources.remove("orders");
    assert!(view.refresh(missing).await.is_err());
    assert!(view.refresh(old).await.is_err());
    let table_root = root.join("test/orders");
    let original_pointer = fs::read(table_root.join("current")).unwrap();
    let metadata_path = PathBuf::from(
        std::str::from_utf8(&original_pointer)
            .unwrap()
            .trim_start_matches("file://"),
    );
    let original: serde_json::Value =
        serde_json::from_slice(&fs::read(metadata_path).unwrap()).unwrap();
    for mode in [
        "uuid",
        "field_id",
        "name",
        "order",
        "required",
        "default",
        "schema_id",
        "descriptor",
        "expired",
    ] {
        let mut metadata = original.clone();
        match mode {
            "uuid" => metadata["table-uuid"] = json_string("00000000-0000-0000-0000-000000000099"),
            "field_id" => {
                metadata["schemas"][0]["fields"][1]["id"] = 99.into();
                metadata["last-column-id"] = 99.into();
            }
            "name" => metadata["schemas"][0]["fields"][1]["name"] = json_string("changed_k"),
            "order" => metadata["schemas"][0]["fields"]
                .as_array_mut()
                .unwrap()
                .swap(1, 2),
            "required" => metadata["schemas"][0]["fields"][1]["required"] = true.into(),
            "default" => metadata["schemas"][0]["fields"][1]["initial-default"] = 7.into(),
            "schema_id" => {
                metadata["schemas"][0]["schema-id"] = 7.into();
                metadata["current-schema-id"] = 7.into();
            }
            "descriptor" => {
                let selected = metadata["current-snapshot-id"].as_i64().unwrap();
                let snapshot = metadata["snapshots"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|snapshot| snapshot["snapshot-id"].as_i64() == Some(selected))
                    .unwrap();
                snapshot["timestamp-ms"] = 42.into();
            }
            _ => metadata["snapshots"] = serde_json::json!([]),
        }
        publish_metadata(&table_root, serde_json::to_vec(&metadata).unwrap());
        assert!(view.refresh(current.clone()).await.is_err(), "mode={mode}");
        assert_eq!(
            fs::read(store.join("CURRENT")).unwrap(),
            pointer,
            "mode={mode}"
        );
        assert_eq!(view.view().epoch(), 0);
        equal(&view.view().contents().unwrap(), &output);
        fs::write(table_root.join("current"), &original_pointer).unwrap();
    }
    assert!(matches!(
        view.refresh(current).await.unwrap(),
        IcebergRefresh::Unchanged { epoch: 0 }
    ));
}
fn json_string(value: &str) -> serde_json::Value {
    serde_json::Value::String(value.into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_files_strict_restart_and_lost_acknowledgement_fail_closed() {
    let _activity = PROCESS_ACTIVITY.enter();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let store = temp.path().join("store");
    let catalog = fixture(&root).await;
    let mut view = IcebergSqlView::bootstrap(&store, SQL, inputs(&root), heads(&root).await)
        .await
        .unwrap();
    assert!(CheckpointStore::open(&store).is_err());
    append(&catalog, "orders", &[row(1, 100)]).await;
    append(&catalog, "lineitems", &[row(1, 100)]).await;
    let target = heads(&root).await;
    let result = view
        .refresh_with_observer(target.clone(), |point| {
            if point == PublishPoint::CurrentDurable {
                Err(Error::Invalid("lost source acknowledgement".into()))
            } else {
                Ok(())
            }
        })
        .await;
    assert!(result.is_err());
    assert_eq!(view.view().epoch(), 0);
    assert!(view.refresh(target.clone()).await.is_err());
    drop(view);
    let committed = {
        let healthy = CheckpointStore::open(&store).unwrap();
        healthy.load_source().unwrap().unwrap()
    };
    // A valid checksummed envelope is insufficient proof that its cached input
    // actually belongs to the bound retained snapshot. Strict IO restart checks it.
    let poisoned_store = temp.path().join("poisoned-store");
    let mut poisoned = committed;
    poisoned.snapshot.epoch = 0;
    let orders = catalog.load_table(&ident("orders")).await.unwrap();
    poisoned
        .snapshot
        .tables
        .insert("orders".into(), batch(&orders, &[row(99, 100)]));
    {
        let target = CheckpointStore::open(&poisoned_store).unwrap();
        target.commit_source(None, &poisoned).unwrap();
    }
    assert!(IcebergSqlView::reopen(&poisoned_store, SQL, inputs(&root))
        .await
        .is_err());
    let mut view = IcebergSqlView::reopen(&store, SQL, inputs(&root))
        .await
        .unwrap();
    assert_eq!(view.view().epoch(), 1);
    assert!(matches!(
        view.refresh(target.clone()).await.unwrap(),
        IcebergRefresh::Unchanged { epoch: 1 }
    ));
    hand_totals(&view.view().contents().unwrap(), &[(1, Some(100))]);
    drop(view);
    let table = catalog.load_table(&ident("lineitems")).await.unwrap();
    let task = table
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_next()
        .await
        .unwrap()
        .unwrap();
    let data_file = PathBuf::from(task.data_file_path().trim_start_matches("file://"));
    let hidden = data_file.with_extension("missing");
    fs::rename(&data_file, &hidden).unwrap();
    let pointer = fs::read(store.join("CURRENT")).unwrap();
    assert!(IcebergSqlView::reopen(&store, SQL, inputs(&root))
        .await
        .is_err());
    assert_eq!(fs::read(store.join("CURRENT")).unwrap(), pointer);
    fs::rename(hidden, data_file).unwrap();
    let view = IcebergSqlView::reopen(&store, SQL, inputs(&root))
        .await
        .unwrap();
    assert_eq!(view.source_vector(), &target);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_only_manifest_and_unknown_type_profiles_are_rejected_without_publication() {
    let _activity = PROCESS_ACTIVITY.enter();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let store = temp.path().join("store");
    let catalog = fixture(&root).await;
    let mut view = IcebergSqlView::bootstrap(&store, SQL, inputs(&root), heads(&root).await)
        .await
        .unwrap();
    overwrite_profile(&catalog, "lineitems", &[], true).await;
    let target = heads(&root).await;
    let pointer = fs::read(store.join("CURRENT")).unwrap();
    let error = view.refresh(target).await.err().unwrap().to_string();
    assert!(error.contains("delete manifests"), "{error}");
    assert_eq!(fs::read(store.join("CURRENT")).unwrap(), pointer);
    assert_eq!(view.view().epoch(), 0);
    let table_root = root.join("test/orders");
    let original = fs::read_to_string(table_root.join("current")).unwrap();
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(original.trim_start_matches("file://")).unwrap()).unwrap();
    metadata["schemas"][0]["fields"][1]["type"] = json_string("float");
    publish_metadata(&table_root, serde_json::to_vec(&metadata).unwrap());
    let lineitems_id = catalog
        .load_table(&ident("lineitems"))
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .unwrap();
    assert!(inputs(&root)
        .pin(&BTreeMap::from([
            ("orders".into(), SnapshotSelection::Empty),
            (
                "lineitems".into(),
                SnapshotSelection::Snapshot(lineitems_id)
            ),
        ]))
        .await
        .is_err());
    assert_eq!(fs::read(store.join("CURRENT")).unwrap(), pointer);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_batch_endpoint_preserves_duplicate_multiplicity_and_reports_actual_reads() {
    let _activity = PROCESS_ACTIVITY.enter();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let store = temp.path().join("store");
    let catalog = fixture(&root).await;
    let mut view = IcebergSqlView::bootstrap(&store, SQL, inputs(&root), heads(&root).await)
        .await
        .unwrap();
    append(&catalog, "orders", &[row(1, 100)]).await;
    append(&catalog, "lineitems", &vec![row(1, 1); 20_000]).await;
    let target = heads(&root).await;
    let IcebergRefresh::Committed { source_work, .. } = view.refresh(target.clone()).await.unwrap()
    else {
        panic!("missing source commit")
    };
    assert_eq!(source_work.decoded_rows, 20_001);
    assert!(source_work.decoded_batches >= 4);
    assert_eq!(source_work.signed_change_rows, 2);
    assert_eq!(
        view.view().tables().unwrap()["lineitems"].num_rows(),
        20_000
    );
    hand_totals(&view.view().contents().unwrap(), &[(1, Some(20_000))]);
    equal(
        &view.view().contents().unwrap(),
        &oracle(&root, &target, SQL).await,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_endpoint_budget_is_enforced_across_batches_before_publication() {
    let _activity = PROCESS_ACTIVITY.enter();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let store = temp.path().join("store");
    let catalog = fixture(&root).await;
    let mut view = IcebergSqlView::bootstrap(&store, SQL, inputs(&root), heads(&root).await)
        .await
        .unwrap();
    // All rows consolidate to one bag entry, so per-batch or distinct-row-only
    // limits would miss this complete-endpoint resource boundary.
    append(
        &catalog,
        "lineitems",
        &vec![row(1, 1); optd_incremental_sql::MAX_MATERIALIZED_ROWS + 1],
    )
    .await;
    let pointer = fs::read(store.join("CURRENT")).unwrap();
    let error = view
        .refresh(heads(&root).await)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("materialization limits"), "{error}");
    assert_eq!(fs::read(store.join("CURRENT")).unwrap(), pointer);
    assert_eq!(view.view().epoch(), 0);
    assert!(view
        .view()
        .tables()
        .unwrap()
        .values()
        .all(|batch| batch.num_rows() == 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn off_main_target_and_missing_retained_ancestry_or_cursor_are_rejected() {
    let _activity = PROCESS_ACTIVITY.enter();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let store = temp.path().join("store");
    let catalog = fixture(&root).await;
    append(&catalog, "orders", &[row(1, 100)]).await;
    append(&catalog, "lineitems", &[row(1, 100)]).await;
    let before = heads(&root).await;
    let snapshot_id = |vector: &PinnedSourceVector| match vector.sources["orders"].snapshot {
        PinnedSnapshot::Snapshot { id, .. } => id,
        _ => panic!("missing fixture snapshot"),
    };
    let first_id = snapshot_id(&before);
    let mut view = IcebergSqlView::bootstrap(&store, SQL, inputs(&root), before)
        .await
        .unwrap();
    append(&catalog, "orders", &[row(2, 100)]).await;
    let middle = heads(&root).await;
    let middle_id = snapshot_id(&middle);
    append(&catalog, "orders", &[row(3, 100)]).await;
    let target = heads(&root).await;
    let target_id = snapshot_id(&target);
    let table_root = root.join("test/orders");
    let original_pointer = fs::read_to_string(table_root.join("current")).unwrap();
    let original: serde_json::Value =
        serde_json::from_slice(&fs::read(original_pointer.trim_start_matches("file://")).unwrap())
            .unwrap();
    let pointer = fs::read(store.join("CURRENT")).unwrap();
    let mut fork = original.clone();
    fork["snapshots"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|snapshot| snapshot["snapshot-id"].as_i64() == Some(target_id))
        .unwrap()["parent-snapshot-id"] = first_id.into();
    publish_metadata(&table_root, serde_json::to_vec(&fork).unwrap());
    catalog.load_table(&ident("orders")).await.unwrap(); // Structurally valid fork.
    let error = view.refresh(middle).await.err().unwrap().to_string();
    assert!(error.contains("mainline"), "{error}");
    for (removed, message) in [(middle_id, "ancestor"), (first_id, "retained")] {
        let mut expired = original.clone();
        expired["snapshots"]
            .as_array_mut()
            .unwrap()
            .retain(|snapshot| snapshot["snapshot-id"].as_i64() != Some(removed));
        expired["snapshot-log"]
            .as_array_mut()
            .unwrap()
            .retain(|entry| entry["snapshot-id"].as_i64() != Some(removed));
        publish_metadata(&table_root, serde_json::to_vec(&expired).unwrap());
        catalog.load_table(&ident("orders")).await.unwrap(); // Valid expired metadata.
        let error = view
            .refresh(target.clone())
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains(message), "{error}");
        assert_eq!(fs::read(store.join("CURRENT")).unwrap(), pointer);
        assert_eq!(view.view().epoch(), 0);
    }
    drop(view);
    assert!(IcebergSqlView::reopen(&store, SQL, inputs(&root))
        .await
        .is_err());
    fs::write(table_root.join("current"), original_pointer).unwrap();
    let mut view = IcebergSqlView::reopen(&store, SQL, inputs(&root))
        .await
        .unwrap();
    view.refresh(target.clone()).await.unwrap();
    equal(
        &view.view().contents().unwrap(),
        &oracle(&root, &target, SQL).await,
    );
}

#[test]
fn process_death_recovers_the_exact_source_tuple_and_continues() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    for (point, expected_epoch) in [("files", 0), ("current", 1)] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("catalog");
        let store = temp.path().join("store");
        let target = active_block_on(&runtime, async {
            let catalog = fixture(&root).await;
            let view = IcebergSqlView::bootstrap(&store, SQL, inputs(&root), heads(&root).await)
                .await
                .unwrap();
            drop(view);
            append(&catalog, "orders", &[row(1, 100)]).await;
            append(&catalog, "lineitems", &[row(1, 100)]).await;
            heads(&root).await
        });
        let target_file = temp.path().join("target.json");
        fs::write(&target_file, serde_json::to_vec(&target).unwrap()).unwrap();
        let ready = temp.path().join("ready");
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "--exact", "typed_source_child", "--nocapture"])
            .env("OPTD_TYPED_SOURCE_ROOT", &root)
            .env("OPTD_TYPED_SOURCE_STORE", &store)
            .env("OPTD_TYPED_SOURCE_VECTOR", &target_file)
            .env("OPTD_TYPED_SOURCE_READY", &ready)
            .env("OPTD_TYPED_SOURCE_POINT", point)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = PROCESS_ACTIVITY.spawn(&mut command);
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ready.exists() {
            if child.try_wait().unwrap().is_some() {
                let output = child.wait_with_output().unwrap();
                panic!(
                    "source child exited before barrier: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                let output = child.wait_with_output().unwrap();
                panic!(
                    "source child barrier timed out: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        active_block_on(&runtime, async {
            let mut view = IcebergSqlView::reopen(&store, SQL, inputs(&root))
                .await
                .unwrap();
            assert_eq!(view.view().epoch(), expected_epoch);
            if expected_epoch == 0 {
                view.refresh(target.clone()).await.unwrap();
            }
            assert_eq!(view.source_vector(), &target);
            hand_totals(&view.view().contents().unwrap(), &[(1, Some(100))]);
            let catalog = LocalCatalog::open(&root).unwrap();
            append(&catalog, "orders", &[row(2, 100)]).await;
            append(&catalog, "lineitems", &[row(2, 200)]).await;
            let next = heads(&root).await;
            view.refresh(next.clone()).await.unwrap();
            equal(
                &view.view().contents().unwrap(),
                &oracle(&root, &next, SQL).await,
            );
            hand_totals(
                &view.view().contents().unwrap(),
                &[(1, Some(100)), (2, Some(200))],
            );
            drop(view);
            folded_history(&store);
        });
    }
}

#[test]
#[ignore = "subprocess-only durability barrier driver"]
fn typed_source_child() {
    let root = PathBuf::from(std::env::var_os("OPTD_TYPED_SOURCE_ROOT").unwrap());
    let store = PathBuf::from(std::env::var_os("OPTD_TYPED_SOURCE_STORE").unwrap());
    let target: PinnedSourceVector = serde_json::from_slice(
        &fs::read(std::env::var_os("OPTD_TYPED_SOURCE_VECTOR").unwrap()).unwrap(),
    )
    .unwrap();
    let ready = PathBuf::from(std::env::var_os("OPTD_TYPED_SOURCE_READY").unwrap());
    let point = std::env::var("OPTD_TYPED_SOURCE_POINT").unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut view = IcebergSqlView::reopen(&store, SQL, inputs(&root))
                .await
                .unwrap();
            view.refresh_with_observer(target, |actual| {
                if (point == "files" && actual == PublishPoint::FilesDurable)
                    || (point == "current" && actual == PublishPoint::CurrentDurable)
                {
                    fs::write(&ready, b"ready").unwrap();
                    loop {
                        std::thread::park();
                    }
                }
                Ok(())
            })
            .await
            .unwrap();
        });
}
