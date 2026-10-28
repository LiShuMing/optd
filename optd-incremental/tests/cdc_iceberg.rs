#![cfg(feature = "iceberg")]
//! Real Parquet/catalog and subprocess-kill acceptance for CDC streams and maintenance.
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use optd_incremental::cdc::*;
use optd_incremental::iceberg_sink::{write_batch_files, CdcLoop, CommitStage, IcebergCdcSink};
use optd_incremental::local_catalog::LocalCatalog;
use optd_incremental::{filter, scan, Expr, Multiset, Value};

fn ident(name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("test".into()), name.into())
}
fn view_plan() -> optd_incremental::IncPlanNode {
    filter(
        scan(0),
        Expr::Gt(Box::new(Expr::Col(0)), Box::new(Expr::Lit(Value::Int(0)))),
    )
}
async fn fixture(root: &Path) -> Arc<LocalCatalog> {
    let catalog = Arc::new(LocalCatalog::open(root).unwrap());
    let ns = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&ns, Default::default())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("source".into())
                .schema(schema)
                .format_version(FormatVersion::V2)
                .properties([("commit.retry.num-retries".into(), "0".into())])
                .build(),
        )
        .await
        .unwrap();
    catalog
}
async fn append(catalog: &LocalCatalog, values: &[i64]) {
    let table = catalog.load_table(&ident("source")).await.unwrap();
    let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap());
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap();
    let files = write_batch_files(&table, batch).await.unwrap();
    let tx = Transaction::new(&table);
    tx.fast_append()
        .add_data_files(files)
        .apply(tx)
        .unwrap()
        .commit(catalog)
        .await
        .unwrap();
}
async fn generation(catalog: &LocalCatalog) -> Generation {
    let table = catalog.load_table(&ident("source")).await.unwrap();
    Generation::new(
        "00000000-0000-0000-0000-000000000001".into(),
        "00000000-0000-0000-0000-000000000002".into(),
        OutputSchema::new(vec![OutputField {
            name: "id".into(),
            data_type: PayloadType::Long,
            required: true,
        }])
        .unwrap(),
        &view_plan(),
        BTreeMap::from([(
            0,
            SourceBinding {
                table_id: table.metadata().uuid().to_string(),
                reference: "main".into(),
            },
        )]),
    )
    .unwrap()
}
async fn recover(catalog: Arc<LocalCatalog>) -> CdcLoop {
    let gen = generation(catalog.as_ref()).await;
    CdcLoop::recover(
        catalog,
        ident("source"),
        ident("changes"),
        gen,
        view_plan(),
        format!("process-{}", std::process::id()),
    )
    .await
    .unwrap()
}
fn folded(batches: &[PreparedBatch]) -> Multiset {
    let mut out = Multiset::new();
    for b in batches {
        out = out.try_union(&b.delta().unwrap()).unwrap();
    }
    out
}
fn expected(values: &[i64]) -> Multiset {
    Multiset::from_rows(
        values
            .iter()
            .filter(|n| **n > 0)
            .map(|n| vec![Value::Int(*n)]),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_parquet_bootstrap_duplicates_empty_commits_and_changed_source() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    let mut loop_ = recover(catalog.clone()).await;
    let empty = loop_.step().await.unwrap().unwrap();
    assert_eq!(empty.record_count, 0);
    assert_eq!(empty.batch_seq, 1);
    assert!(loop_.step().await.unwrap().is_none());
    append(catalog.as_ref(), &[1, 1, 2]).await;
    let inserted = loop_.step().await.unwrap().unwrap();
    assert_eq!(inserted.record_count, 2);
    append(catalog.as_ref(), &[-9]).await;
    let cancelled = loop_.step().await.unwrap().unwrap();
    assert_eq!(cancelled.record_count, 0);
    let batches = loop_.sink().read_batches().await.unwrap();
    assert_eq!(batches.len(), 3);
    assert_eq!(folded(&batches), expected(&[1, 1, 2]));
    let table = catalog.load_table(&ident("changes")).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 3);
    assert_eq!(batches[1].rows()[0].weight, 2);
    let gen = generation(catalog.as_ref()).await;
    assert!(matches!(
        IcebergCdcSink::open(catalog.clone(), ident("changes"), gen).await,
        Err(CdcError::Ownership)
    ));
    drop(loop_);
    let reopened = Arc::new(LocalCatalog::open(temp.path()).unwrap());
    let mut loop_ = recover(reopened).await;
    assert_eq!(loop_.checkpoint().unwrap().batch_seq, 3);
    assert!(loop_.step().await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_file_stage_keeps_candidate_and_lost_ack_is_resolved_without_append_retry() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(catalog.as_ref(), &[1]).await;
    let mut loop_ = recover(catalog.clone()).await;
    loop_.step().await.unwrap();
    append(catalog.as_ref(), &[2]).await;
    assert!(loop_
        .step_with_observer(&|stage| if stage == CommitStage::FilesWritten {
            Err(CdcError::Storage("file-stage injection".into()))
        } else {
            Ok(())
        })
        .await
        .is_err());
    assert_eq!(loop_.view().contents(), &expected(&[1]));
    assert_eq!(loop_.checkpoint().unwrap().batch_seq, 1);
    // A newer source commit must not change the held candidate's pinned target.
    append(catalog.as_ref(), &[3]).await;
    let committed = loop_
        .step_with_observer(&|stage| {
            if stage == CommitStage::CatalogCommitted {
                Err(CdcError::Storage("lost acknowledgement".into()))
            } else {
                Ok(())
            }
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.batch_seq, 2);
    assert_eq!(loop_.view().contents(), &expected(&[1, 2]));
    loop_.step().await.unwrap();
    let batches = loop_.sink().read_batches().await.unwrap();
    assert_eq!(batches.len(), 3);
    assert_eq!(folded(&batches), expected(&[1, 2, 3]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_deltas_at_different_source_ranges_are_not_deduplicated() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(catalog.as_ref(), &[1]).await;
    let mut loop_ = recover(catalog.clone()).await;
    loop_.step().await.unwrap();
    append(catalog.as_ref(), &[2]).await;
    let a = loop_.step().await.unwrap().unwrap();
    append(catalog.as_ref(), &[2]).await;
    let b = loop_.step().await.unwrap().unwrap();
    assert_eq!(a.delta_hash, b.delta_hash);
    assert_ne!(a.batch_id, b.batch_id);
    assert_eq!(
        folded(&loop_.sink().read_batches().await.unwrap()),
        expected(&[1, 2, 2])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_settings_are_enforced_and_existing_batches_are_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(catalog.as_ref(), &[1]).await;
    let mut loop_ = recover(catalog.clone()).await;
    loop_.step().await.unwrap();
    let batch = loop_.sink().read_batches().await.unwrap().remove(0);
    drop(loop_);
    let gen = generation(catalog.as_ref()).await;
    let mut sink = IcebergCdcSink::open(catalog.clone(), ident("changes"), gen)
        .await
        .unwrap();
    assert_eq!(
        sink.emit(&batch).await.unwrap().batch_id,
        batch.envelope().batch_id
    );
    assert_eq!(sink.read_batches().await.unwrap().len(), 1);
    let table = catalog.load_table(&ident("changes")).await.unwrap();
    let tx = Transaction::new(&table);
    tx.update_table_properties()
        .set("commit.retry.num-retries".into(), "1".into())
        .apply(tx)
        .unwrap()
        .commit(catalog.as_ref())
        .await
        .unwrap();
    assert!(sink.last_checkpoint().await.is_err());
}

// The parent launches this exact test in a new process. Normal test runs have no worker env.
#[test]
fn subprocess_worker() {
    let Ok(root) = std::env::var("OPTD_CDC_TEST_WAREHOUSE") else {
        return;
    };
    let pause = std::env::var("OPTD_CDC_TEST_STAGE").unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let catalog = Arc::new(LocalCatalog::open(&root).unwrap());
        let mut loop_ = recover(catalog).await;
        loop_
            .step_with_observer(&|stage| {
                let requested = if pause == "files" {
                    CommitStage::FilesWritten
                } else {
                    CommitStage::CatalogCommitted
                };
                if stage == requested {
                    std::fs::write(Path::new(&root).join("barrier"), "ready").unwrap();
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

#[test]
fn real_process_kills_recover_bootstrap_and_incremental_windows() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    for bootstrap in [true, false] {
        for stage in ["files", "commit"] {
            let temp = tempfile::tempdir().unwrap();
            let catalog = runtime.block_on(fixture(temp.path()));
            runtime.block_on(append(catalog.as_ref(), &[1, 1, 2]));
            if !bootstrap {
                runtime.block_on(async {
                    let mut loop_ = recover(catalog.clone()).await;
                    loop_.step().await.unwrap();
                });
                runtime.block_on(append(catalog.as_ref(), &[3]));
            }
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "subprocess_worker",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("OPTD_CDC_TEST_WAREHOUSE", temp.path())
                .env("OPTD_CDC_TEST_STAGE", stage)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let start = Instant::now();
            while !temp.path().join("barrier").exists() {
                if let Some(status) = child.try_wait().unwrap() {
                    let output = child.wait_with_output().unwrap();
                    panic!(
                        "child exited {status}: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                if start.elapsed() > Duration::from_secs(30) {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("child did not reach {stage} barrier");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            // This is an actual OS kill: no destructor, unwind or simulated error recovery.
            child.kill().unwrap();
            let status = child.wait().unwrap();
            assert!(!status.success());
            // Advance the source before restart: loading at NOW with an old watermark doubles it.
            runtime.block_on(append(catalog.as_ref(), &[4]));
            runtime.block_on(async {
                let reopened = Arc::new(LocalCatalog::open(temp.path()).unwrap());
                let mut loop_ = recover(reopened).await;
                loop_.step().await.unwrap();
                let batches = loop_.sink().read_batches().await.unwrap();
                let values = if bootstrap {
                    vec![1, 1, 2, 4]
                } else {
                    vec![1, 1, 2, 3, 4]
                };
                assert_eq!(
                    folded(&batches),
                    expected(&values),
                    "bootstrap={bootstrap} stage={stage}"
                );
                assert_eq!(loop_.view().contents(), &expected(&values));
                assert!(loop_.step().await.unwrap().is_none());
                let expected_count = if bootstrap {
                    if stage == "commit" {
                        2
                    } else {
                        1
                    }
                } else if stage == "commit" {
                    3
                } else {
                    2
                };
                assert_eq!(batches.len(), expected_count);
            });
        }
    }
}

// Reproducible external-writer fixture: replace the source's live manifest set while
// retaining prior snapshots. No CDC output code is used to compute the expected delta.
async fn overwrite_source(catalog: &LocalCatalog, values: &[i64]) {
    use iceberg::spec::{ManifestListWriter, ManifestWriterBuilder, Operation, Snapshot, Summary};
    let table = catalog.load_table(&ident("source")).await.unwrap();
    let seq = table.metadata().next_sequence_number();
    let mut id = seq + 1000;
    while table.metadata().snapshot_by_id(id).is_some() {
        id += 1;
    }
    let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap());
    let files = write_batch_files(
        &table,
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap(),
    )
    .await
    .unwrap();
    let mut manifests = vec![];
    if !files.is_empty() {
        let output = table
            .file_io()
            .new_output(format!(
                "{}/metadata/fixture-{seq}.avro",
                table.metadata().location()
            ))
            .unwrap();
        let mut manifest = ManifestWriterBuilder::new(
            output,
            Some(id),
            table.current_schema_ref(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        for file in files {
            manifest.add_file(file, seq).unwrap();
        }
        manifests.push(manifest.write_manifest_file().await.unwrap());
    }
    let list = format!(
        "{}/metadata/fixture-list-{seq}.avro",
        table.metadata().location()
    );
    let writer = table
        .file_io()
        .new_output(&list)
        .unwrap()
        .writer()
        .await
        .unwrap();
    let mut list_writer =
        ManifestListWriter::v2(writer, id, table.metadata().current_snapshot_id(), seq);
    list_writer.add_manifests(manifests.into_iter()).unwrap();
    list_writer.close().await.unwrap();
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(id)
        .with_parent_snapshot_id(table.metadata().current_snapshot_id())
        .with_sequence_number(seq)
        .with_manifest_list(list)
        .with_timestamp_ms(now)
        .with_schema_id(table.metadata().current_schema_id())
        .with_summary(Summary {
            operation: Operation::Overwrite,
            additional_properties: Default::default(),
        })
        .build();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(table.metadata_location().map(str::to_string))
        .set_branch_snapshot(snapshot, "main")
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    publish_source_fixture(&table, metadata).await;
}
async fn publish_source_fixture(
    table: &iceberg::table::Table,
    metadata: iceberg::spec::TableMetadata,
) {
    use std::str::FromStr;
    let location = iceberg::MetadataLocation::from_str(table.metadata_location().unwrap())
        .unwrap()
        .with_next_version()
        .try_with_new_metadata(&metadata)
        .unwrap();
    metadata.write_to(table.file_io(), &location).await.unwrap();
    let dir = Path::new(table.metadata().location());
    std::fs::write(dir.join("fixture-pointer"), location.to_string()).unwrap();
    std::fs::rename(dir.join("fixture-pointer"), dir.join("current")).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_overwrite_retracts_duplicates_and_deletes_all_rows_with_full_images() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(catalog.as_ref(), &[1, 1, 2]).await;
    let mut loop_ = recover(catalog.clone()).await;
    loop_.step().await.unwrap();
    overwrite_source(catalog.as_ref(), &[1, 3]).await;
    loop_.step().await.unwrap();
    let batches = loop_.sink().read_batches().await.unwrap();
    let delta = batches[1].rows();
    assert_eq!(delta.len(), 3);
    assert_eq!(
        delta
            .iter()
            .map(|r| (&r.payload, r.op_type, r.weight))
            .collect::<Vec<_>>(),
        vec![
            (&vec![Value::Int(1)], 0, -1),
            (&vec![Value::Int(2)], 0, -1),
            (&vec![Value::Int(3)], 1, 1)
        ]
    );
    assert!(delta.iter().all(|r| !r.is_update && r.row_id.is_none()));
    assert_eq!(folded(&batches), expected(&[1, 3]));
    overwrite_source(catalog.as_ref(), &[]).await;
    loop_.step().await.unwrap();
    assert!(loop_.view().contents().is_empty());
    drop(loop_);
    let mut loop_ = recover(catalog).await;
    assert!(loop_.step().await.unwrap().is_none());
    assert!(folded(&loop_.sink().read_batches().await.unwrap()).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_checkpoint_and_changed_view_definition_are_rejected_on_restart() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(catalog.as_ref(), &[1]).await;
    let mut loop_ = recover(catalog.clone()).await;
    loop_.step().await.unwrap();
    let old = loop_.checkpoint().unwrap().source_to[0]
        .snapshot_id
        .unwrap();
    drop(loop_);
    let gen = generation(catalog.as_ref()).await;
    assert!(CdcLoop::recover(
        catalog.clone(),
        ident("source"),
        ident("changes"),
        gen,
        scan(0),
        "changed".into()
    )
    .await
    .is_err());
    append(catalog.as_ref(), &[2]).await;
    let table = catalog.load_table(&ident("source")).await.unwrap();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(table.metadata_location().map(str::to_string))
        .remove_snapshots(&[old])
        .build()
        .unwrap()
        .metadata;
    publish_source_fixture(&table, metadata).await;
    let gen = generation(catalog.as_ref()).await;
    assert!(CdcLoop::recover(
        catalog,
        ident("source"),
        ident("changes"),
        gen,
        view_plan(),
        "restarted".into()
    )
    .await
    .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_refuses_over_retraction_before_writing_a_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(catalog.as_ref(), &[1]).await;
    let mut driver = recover(catalog.clone()).await;
    let previous = driver.step().await.unwrap().unwrap();
    drop(driver);
    append(catalog.as_ref(), &[2]).await;
    let table = catalog.load_table(&ident("source")).await.unwrap();
    let source_to = optd_incremental::iceberg_source::checkpoint(
        &table,
        table.metadata().current_snapshot_id(),
    )
    .unwrap();
    let gen = generation(catalog.as_ref()).await;
    let mut invalid = Multiset::new();
    invalid.try_add_row(vec![Value::Int(1)], -2).unwrap();
    let batch = PreparedBatch::new(
        &gen,
        Some(&previous),
        vec![source_to],
        &invalid,
        3,
        "invalid-caller".into(),
    )
    .unwrap();
    let mut sink = IcebergCdcSink::open(catalog.clone(), ident("changes"), gen)
        .await
        .unwrap();
    assert!(matches!(sink.emit(&batch).await, Err(CdcError::InvalidRow)));
    assert_eq!(sink.read_batches().await.unwrap().len(), 1);
    assert_eq!(
        catalog
            .load_table(&ident("changes"))
            .await
            .unwrap()
            .metadata()
            .snapshots()
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_catalog_retains_required_input_snapshots() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(catalog.as_ref(), &[1]).await;
    let mut driver = recover(catalog.clone()).await;
    driver.step().await.unwrap();
    let old = driver.checkpoint().unwrap().source_to[0]
        .snapshot_id
        .unwrap();
    append(catalog.as_ref(), &[2]).await;
    let table = catalog.load_table(&ident("source")).await.unwrap();
    let tx = Transaction::new(&table);
    let expiration = tx
        .expire_snapshots()
        .expire_snapshot_ids([old])
        .retain_last(1)
        .apply(tx)
        .unwrap()
        .commit(catalog.as_ref())
        .await;
    assert!(expiration.is_err());
    assert!(catalog
        .load_table(&ident("source"))
        .await
        .unwrap()
        .metadata()
        .snapshot_by_id(old)
        .is_some());
    driver.step().await.unwrap();
    assert_eq!(driver.view().contents(), &expected(&[1, 2]));
}

async fn consumer(
    catalog: Arc<LocalCatalog>,
    name: &str,
) -> optd_incremental::consumer::LocalConsumer {
    let gen = generation(&catalog).await;
    let reader = optd_incremental::iceberg_sink::CdcLogReader::open(catalog, ident("changes"), gen)
        .await
        .unwrap();
    optd_incremental::consumer::LocalConsumer::open(reader, name.into())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_consumers_pin_ranges_and_commit_effects_with_cursor() {
    use optd_incremental::consumer::ConsumeStage;
    use optd_incremental::{DeltaSource, View};
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(&catalog, &[1, 1, 2]).await;
    let mut driver = recover(catalog.clone()).await;
    driver.step().await.unwrap();
    let mut first = consumer(catalog.clone(), "one").await;
    let mut second = consumer(catalog.clone(), "two").await;
    let pinned = first.peek().await.unwrap();
    let again = first.peek().await.unwrap();
    assert_eq!(pinned.upper(), again.upper());
    assert_eq!(first.cursor().batch_seq, 0);
    append(&catalog, &[3]).await;
    driver.step().await.unwrap();
    assert_eq!(pinned.upper().batch_seq, 1);
    let advanced = first.peek().await.unwrap();
    assert_eq!(advanced.upper().batch_seq, 2);
    let fail = first.consume_with_observer(&pinned, &|stage| {
        if stage == ConsumeStage::StateStaged {
            Err(CdcError::UnknownCommit)
        } else {
            Ok(())
        }
    });
    assert!(fail.is_err());
    assert_eq!(first.cursor().batch_seq, 0);
    assert!(first.contents().is_empty());
    let source = first.source(42).await.unwrap();
    let downstream = View::load(
        filter(
            scan(42),
            Expr::Gt(Box::new(Expr::Col(0)), Box::new(Expr::Lit(Value::Int(1)))),
        ),
        &source,
        vec![42],
    )
    .unwrap();
    assert_eq!(
        downstream.contents(),
        &Multiset::from_rows([vec![Value::Int(2)], vec![Value::Int(3)]])
    );
    first.consume(&pinned).unwrap();
    assert_eq!(first.contents(), &expected(&[1, 1, 2]));
    assert!(!first.consume(&pinned).unwrap());
    assert!(first.consume(&advanced).is_err()); // overlaps an already committed prefix
    assert_eq!(second.cursor().batch_seq, 0);
    second.consume(&second.peek().await.unwrap()).unwrap();
    assert_eq!(second.contents(), &expected(&[1, 1, 2, 3]));
    let suffix = first.peek().await.unwrap();
    assert!(first
        .consume_with_observer(&suffix, &|_| Err(CdcError::UnknownCommit))
        .is_err());
    first
        .consume_with_observer(&suffix, &|stage| {
            if stage == ConsumeStage::StateCommitted {
                Err(CdcError::UnknownCommit)
            } else {
                Ok(())
            }
        })
        .unwrap();
    assert_eq!(first.cursor(), second.cursor());
    assert_eq!(first.contents(), second.contents());
    let source = first.source(42).await.unwrap();
    assert!(source.snapshot_at(42, 1).is_err());
    drop(first);
    let mut first = consumer(catalog.clone(), "one").await;
    assert_eq!(first.contents(), second.contents());
    assert!(!first.consume(&suffix).unwrap());
    // A checkpoint plus suffix maintains a downstream view after a later commit.
    let mut downstream = View::load(scan(42), &first.source(42).await.unwrap(), vec![42]).unwrap();
    append(&catalog, &[4, 4, -9]).await;
    driver.step().await.unwrap();
    downstream
        .refresh(&first.source(42).await.unwrap())
        .unwrap();
    assert_eq!(downstream.contents(), &expected(&[1, 1, 2, 3, 4, 4]));
    let reader = optd_incremental::iceberg_sink::CdcLogReader::open(
        catalog,
        ident("changes"),
        driver.sink().generation().clone(),
    )
    .await
    .unwrap();
    assert!(
        optd_incremental::consumer::LocalConsumer::open(reader, "one".into())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_aggregate_pairs_survive_parquet_restart_and_key_changes() {
    use optd_incremental::{agg, AggKind};
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(&catalog, &[1, 2]).await;
    let plan = agg(scan(0), vec![0], vec![AggKind::Count]);
    let gen = Generation::new(
        "00000000-0000-0000-0000-000000000001".into(),
        "00000000-0000-0000-0000-000000000004".into(),
        OutputSchema::new(vec![
            OutputField {
                name: "id".into(),
                data_type: PayloadType::Long,
                required: true,
            },
            OutputField {
                name: "count".into(),
                data_type: PayloadType::Long,
                required: true,
            },
        ])
        .unwrap()
        .keyed(KeySpec(vec![1000]))
        .unwrap(),
        &plan,
        generation(&catalog).await.bindings().clone(),
    )
    .unwrap();
    let mut driver = CdcLoop::recover(
        catalog.clone(),
        ident("source"),
        ident("keyed"),
        gen.clone(),
        plan.clone(),
        "keyed".into(),
    )
    .await
    .unwrap();
    driver.step().await.unwrap();
    append(&catalog, &[1]).await;
    driver.step().await.unwrap();
    let batches = driver.sink().read_batches().await.unwrap();
    assert_eq!(batches[1].rows().len(), 2);
    assert!(batches[1].rows().iter().all(|r| r.is_update));
    let rowid = batches[1].rows()[0].row_id.clone();
    assert_eq!(rowid, batches[1].rows()[1].row_id);
    drop(driver);
    overwrite_source(&catalog, &[2, 3]).await;
    let mut driver = CdcLoop::recover(
        catalog.clone(),
        ident("source"),
        ident("keyed"),
        gen,
        plan,
        "restart".into(),
    )
    .await
    .unwrap();
    driver.step().await.unwrap();
    let batches = driver.sink().read_batches().await.unwrap();
    assert!(batches[2].rows().iter().all(|r| !r.is_update));
    assert_eq!(
        folded(&batches),
        Multiset::from_rows([
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(3), Value::Int(1)]
        ])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batched_empty_commits_compaction_expiration_preserve_replay_and_lagging_cursors() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(&catalog, &[1]).await;
    let gen = generation(&catalog).await;
    let source = catalog.load_table(&ident("source")).await.unwrap();
    let point = optd_incremental::iceberg_source::checkpoint(
        &source,
        source.metadata().current_snapshot_id(),
    )
    .unwrap();
    let first = PreparedBatch::new(
        &gen,
        None,
        vec![point.clone()],
        &expected(&[1]),
        1,
        "w".into(),
    )
    .unwrap();
    let mut sink = IcebergCdcSink::open(catalog.clone(), ident("changes"), gen.clone())
        .await
        .unwrap();
    sink.emit(&first).await.unwrap();
    let retried = PreparedBatch::new(
        &gen,
        None,
        vec![point],
        &expected(&[1]),
        99,
        "restart".into(),
    )
    .unwrap();
    assert_eq!(sink.emit(&retried).await.unwrap(), *first.envelope());

    let mut lagging = consumer(catalog.clone(), "lagging").await;
    let pin = lagging.peek().await.unwrap();
    append(&catalog, &[-1]).await;
    let source = catalog.load_table(&ident("source")).await.unwrap();
    let point2 = optd_incremental::iceberg_source::checkpoint(
        &source,
        source.metadata().current_snapshot_id(),
    )
    .unwrap();
    let empty = PreparedBatch::new(
        &gen,
        Some(first.envelope()),
        vec![point2],
        &Multiset::new(),
        2,
        "w".into(),
    )
    .unwrap();
    append(&catalog, &[2, 2]).await;
    let source = catalog.load_table(&ident("source")).await.unwrap();
    let point3 = optd_incremental::iceberg_source::checkpoint(
        &source,
        source.metadata().current_snapshot_id(),
    )
    .unwrap();
    let third = PreparedBatch::new(
        &gen,
        Some(empty.envelope()),
        vec![point3],
        &expected(&[2, 2]),
        3,
        "w".into(),
    )
    .unwrap();
    sink.emit_many_with_observer(&[empty.clone(), third.clone()], &|stage| {
        if stage == CommitStage::CatalogCommitted {
            Err(CdcError::UnknownCommit)
        } else {
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_eq!(
        catalog
            .load_table(&ident("changes"))
            .await
            .unwrap()
            .metadata()
            .snapshots()
            .count(),
        2
    );
    sink.emit_many(&[empty.clone(), third.clone()])
        .await
        .unwrap();
    let before = sink.read_batches().await.unwrap();
    assert_eq!(before.len(), 3);
    assert!(before[1].rows().is_empty());
    assert!(sink.expire_snapshots().await.is_err());
    sink.compact().await.unwrap();
    assert_eq!(sink.expire_snapshots().await.unwrap(), 2);
    let after = sink.read_batches().await.unwrap();
    for (a, b) in before.iter().zip(&after) {
        assert_eq!(a.envelope(), b.envelope());
        assert_eq!(a.rows(), b.rows());
    }
    lagging.consume(&pin).unwrap();
    let suffix = lagging.peek().await.unwrap();
    lagging.consume(&suffix).unwrap();
    assert_eq!(lagging.contents(), &expected(&[1, 2, 2]));
    assert_eq!(
        catalog
            .load_table(&ident("changes"))
            .await
            .unwrap()
            .metadata()
            .snapshots()
            .count(),
        1
    );
    assert_eq!(
        catalog
            .load_table(&ident("source"))
            .await
            .unwrap()
            .metadata()
            .snapshots()
            .count(),
        3
    );
    drop(sink);
    drop(lagging);
    let mut driver = recover(catalog.clone()).await;
    assert!(driver.step().await.unwrap().is_none());
    append(&catalog, &[4]).await;
    driver.step().await.unwrap();
    assert_eq!(
        folded(&driver.sink().read_batches().await.unwrap()),
        expected(&[1, 2, 2, 4])
    );
    let lagging = consumer(catalog, "lagging").await;
    assert_eq!(lagging.cursor().batch_seq, 3);
}

#[test]
fn maintenance_consumer_subprocess_worker() {
    use optd_incremental::consumer::ConsumeStage;
    let Ok(root) = std::env::var("OPTD_CDC_MAINT_WAREHOUSE") else {
        return;
    };
    let operation = std::env::var("OPTD_CDC_MAINT_OPERATION").unwrap();
    let after = std::env::var("OPTD_CDC_MAINT_AFTER").unwrap() == "yes";
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let barrier = || {
        std::fs::write(Path::new(&root).join("barrier"), "ready").unwrap();
        loop {
            std::thread::park();
        }
    };
    runtime.block_on(async {
        let catalog = Arc::new(LocalCatalog::open(&root).unwrap());
        if operation == "consume" {
            let mut target = consumer(catalog, "crash").await;
            let pin = target.peek().await.unwrap();
            target
                .consume_with_observer(&pin, &|stage| {
                    if stage
                        == if after {
                            ConsumeStage::StateCommitted
                        } else {
                            ConsumeStage::StateStaged
                        }
                    {
                        barrier();
                    }
                    Ok(())
                })
                .unwrap();
        } else {
            let gen = generation(&catalog).await;
            let mut sink = IcebergCdcSink::open(catalog.clone(), ident("changes"), gen)
                .await
                .unwrap();
            let observer = |stage| {
                if stage
                    == if after {
                        CommitStage::CatalogCommitted
                    } else {
                        CommitStage::FilesWritten
                    }
                {
                    barrier();
                }
                Ok(())
            };
            if operation == "batch" {
                let previous = sink.last_checkpoint().await.unwrap().unwrap();
                let batches = pending_two_batches(&catalog, sink.generation(), &previous).await;
                sink.emit_many_with_observer(&batches, &observer)
                    .await
                    .unwrap();
            } else {
                sink.compact_with_observer(&observer).await.unwrap();
            }
        }
    });
}

#[test]
fn real_process_kills_preserve_consumer_transactions_and_compaction_identity() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    for operation in ["consume", "compact"] {
        for after in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let catalog = runtime.block_on(fixture(temp.path()));
            let before = runtime.block_on(async {
                append(&catalog, &[1, 1]).await;
                let mut driver = recover(catalog.clone()).await;
                driver.step().await.unwrap();
                let mut destination = consumer(catalog.clone(), "crash").await;
                destination
                    .consume(&destination.peek().await.unwrap())
                    .unwrap();
                append(&catalog, &[2, 3]).await;
                driver.step().await.unwrap();
                driver.sink().read_batches().await.unwrap()
            });
            kill_maintenance_worker(temp.path(), operation, after);
            runtime.block_on(async {
                let reopened = Arc::new(LocalCatalog::open(temp.path()).unwrap());
                let mut destination = consumer(reopened.clone(), "crash").await;
                let committed = operation == "consume" && after;
                assert_eq!(
                    destination.cursor().batch_seq,
                    if committed { 2 } else { 1 }
                );
                assert_eq!(
                    destination.contents(),
                    &if committed {
                        expected(&[1, 1, 2, 3])
                    } else {
                        expected(&[1, 1])
                    }
                );
                destination
                    .consume(&destination.peek().await.unwrap())
                    .unwrap();
                assert_eq!(destination.contents(), &expected(&[1, 1, 2, 3]));
                let gen = generation(&reopened).await;
                let mut sink = IcebergCdcSink::open(reopened.clone(), ident("changes"), gen)
                    .await
                    .unwrap();
                let restored = sink.read_batches().await.unwrap();
                for (a, b) in before.iter().zip(&restored) {
                    assert_eq!(a.envelope(), b.envelope());
                    assert_eq!(a.rows(), b.rows());
                }
                if operation == "compact" {
                    assert_eq!(
                        reopened
                            .load_table(&ident("changes"))
                            .await
                            .unwrap()
                            .metadata()
                            .snapshots()
                            .count(),
                        if after { 3 } else { 2 }
                    );
                }
                sink.compact().await.unwrap();
                sink.expire_snapshots().await.unwrap();
                assert_eq!(
                    folded(&sink.read_batches().await.unwrap()),
                    expected(&[1, 1, 2, 3])
                );
                assert_eq!(destination.cursor().batch_seq, 2);
            });
        }
    }
}

fn kill_maintenance_worker(root: &Path, operation: &str, after: bool) {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "maintenance_consumer_subprocess_worker",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("OPTD_CDC_MAINT_WAREHOUSE", root)
        .env("OPTD_CDC_MAINT_OPERATION", operation)
        .env("OPTD_CDC_MAINT_AFTER", if after { "yes" } else { "no" })
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !root.join("barrier").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            let output = child.wait_with_output().unwrap();
            panic!(
                "{operation} after={after} child exited {status}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        if start.elapsed() > Duration::from_secs(30) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("barrier timeout");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_refuses_scan_contention_and_consumer_detects_corrupt_state() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(&catalog, &[1]).await;
    {
        let mut driver = recover(catalog.clone()).await;
        driver.step().await.unwrap();
    }
    let gen = generation(&catalog).await;
    let mut sink = IcebergCdcSink::open(catalog.clone(), ident("changes"), gen)
        .await
        .unwrap();
    let lock = catalog
        .writer_lock(&format!("scan-{}", sink.table_uuid()))
        .unwrap();
    assert!(matches!(sink.compact().await, Err(CdcError::Ownership)));
    drop(lock);
    let read_pin = catalog
        .reader_lock(&format!("scan-{}", sink.table_uuid()))
        .unwrap();
    // A producer audit and another consumer scan may share a read pin; maintenance may not.
    assert_eq!(sink.read_batches().await.unwrap().len(), 1);
    assert!(matches!(sink.compact().await, Err(CdcError::Ownership)));
    drop(read_pin);

    let mut target = consumer(catalog.clone(), "corrupt").await;
    target.consume(&target.peek().await.unwrap()).unwrap();
    drop(target);
    let state_dir = std::fs::read_dir(temp.path().join("consumers"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let path = state_dir.join("state.json");
    let text = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, text.replace("\"batch_seq\":1", "\"batch_seq\":2")).unwrap();
    let reader = optd_incremental::iceberg_sink::CdcLogReader::open(
        catalog,
        ident("changes"),
        sink.generation().clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        optd_incremental::consumer::LocalConsumer::open(reader, "corrupt".into()).await,
        Err(CdcError::DigestMismatch)
    ));
}

async fn assert_summary_checksum_rejected(
    catalog: &LocalCatalog,
    sink: &IcebergCdcSink,
    key: &str,
) {
    let table = catalog.load_table(&ident("changes")).await.unwrap();
    let id = table.metadata().current_snapshot_id().unwrap();
    let mut raw = serde_json::to_value(table.metadata()).unwrap();
    let snapshots = raw["snapshots"].as_array_mut().unwrap();
    let head = snapshots
        .iter_mut()
        .find(|s| s["snapshot-id"].as_i64() == Some(id))
        .unwrap();
    let mut index: serde_json::Value =
        serde_json::from_str(head["summary"][key].as_str().unwrap()).unwrap();
    index["checksum"] = serde_json::json!(format!("sha256:{}", "0".repeat(64)));
    head["summary"][key] = serde_json::json!(serde_json::to_string(&index).unwrap());
    let corrupt: iceberg::spec::TableMetadata = serde_json::from_value(raw).unwrap();
    publish_source_fixture(&table, corrupt).await;
    assert!(matches!(
        sink.read_batches().await,
        Err(CdcError::DigestMismatch)
    ));
    std::fs::write(
        Path::new(table.metadata().location()).join("current"),
        table.metadata_location().unwrap(),
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupted_multi_batch_index_or_compaction_archive_fails_closed() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    append(&catalog, &[1]).await;
    {
        let mut driver = recover(catalog.clone()).await;
        driver.step().await.unwrap();
    }
    let gen = generation(&catalog).await;
    let mut sink = IcebergCdcSink::open(catalog.clone(), ident("changes"), gen)
        .await
        .unwrap();
    assert_summary_checksum_rejected(&catalog, &sink, "optd.commits").await;
    sink.compact().await.unwrap();
    assert_summary_checksum_rejected(&catalog, &sink, "optd.envelopes").await;
    assert_eq!(folded(&sink.read_batches().await.unwrap()), expected(&[1]));
}

async fn pending_two_batches(
    catalog: &LocalCatalog,
    gen: &Generation,
    previous: &CommitEnvelope,
) -> Vec<PreparedBatch> {
    let source = catalog.load_table(&ident("source")).await.unwrap();
    let mut snapshots: Vec<_> = source
        .metadata()
        .snapshots()
        .filter(|s| s.sequence_number() > previous.source_to[0].sequence_number.unwrap())
        .collect();
    snapshots.sort_by_key(|s| s.sequence_number());
    assert_eq!(snapshots.len(), 2);
    let empty = PreparedBatch::new(
        gen,
        Some(previous),
        vec![optd_incremental::iceberg_source::checkpoint(
            &source,
            Some(snapshots[0].snapshot_id()),
        )
        .unwrap()],
        &Multiset::new(),
        2,
        "batch".into(),
    )
    .unwrap();
    let next = PreparedBatch::new(
        gen,
        Some(empty.envelope()),
        vec![optd_incremental::iceberg_source::checkpoint(
            &source,
            Some(snapshots[1].snapshot_id()),
        )
        .unwrap()],
        &expected(&[2, 3]),
        3,
        "batch".into(),
    )
    .unwrap();
    vec![empty, next]
}

#[test]
fn real_process_kills_publish_all_or_none_of_a_multi_batch_prefix() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    for after in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let catalog = runtime.block_on(fixture(temp.path()));
        let previous = runtime.block_on(async {
            append(&catalog, &[1, 1]).await;
            let mut driver = recover(catalog.clone()).await;
            let previous = driver.step().await.unwrap().unwrap();
            append(&catalog, &[-1]).await;
            append(&catalog, &[2, 3]).await;
            previous
        });
        kill_maintenance_worker(temp.path(), "batch", after);
        runtime.block_on(async {
            let gen = generation(&catalog).await;
            let mut sink = IcebergCdcSink::open(catalog.clone(), ident("changes"), gen.clone())
                .await
                .unwrap();
            let batches = sink.read_batches().await.unwrap();
            assert_eq!(batches.len(), if after { 3 } else { 1 });
            assert_eq!(
                catalog
                    .load_table(&ident("changes"))
                    .await
                    .unwrap()
                    .metadata()
                    .snapshots()
                    .count(),
                if after { 2 } else { 1 }
            );
            if after {
                assert!(batches[1].rows().is_empty());
            }
            let prepared = pending_two_batches(&catalog, &gen, &previous).await;
            sink.emit_many(&prepared).await.unwrap();
            assert_eq!(sink.read_batches().await.unwrap().len(), 3);
            assert_eq!(
                folded(&sink.read_batches().await.unwrap()),
                expected(&[1, 1, 2, 3])
            );
            drop(sink);
            let mut driver = recover(catalog.clone()).await;
            assert!(driver.step().await.unwrap().is_none());
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_empty_bootstrap_compacts_expires_and_resumes_from_no_source_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path()).await;
    let first = {
        let mut driver = recover(catalog.clone()).await;
        driver.step().await.unwrap().unwrap()
    };
    assert_eq!(first.record_count, 0);
    assert_eq!(first.source_to[0].snapshot_id, None);
    let gen = generation(&catalog).await;
    let mut sink = IcebergCdcSink::open(catalog.clone(), ident("changes"), gen)
        .await
        .unwrap();
    sink.compact().await.unwrap();
    assert_eq!(sink.expire_snapshots().await.unwrap(), 1);
    let batches = sink.read_batches().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].envelope(), &first);
    drop(sink);
    append(&catalog, &[5]).await;
    let mut driver = recover(catalog.clone()).await;
    driver.step().await.unwrap();
    let mut target = consumer(catalog, "empty").await;
    target.consume(&target.peek().await.unwrap()).unwrap();
    assert_eq!(target.cursor().batch_seq, 2);
    assert_eq!(target.contents(), &expected(&[5]));
}
