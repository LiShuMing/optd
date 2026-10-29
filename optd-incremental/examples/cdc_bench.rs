//! E10 measurement harness for the CDC output path.
//!
//! Every command prints one JSON object on stdout so a driver can append the
//! records to a JSONL file together with a label and the commit. Nothing here is
//! a test: the numbers only mean something against a recorded baseline, and the
//! cold-start numbers are warm-page-cache numbers unless the caller shows
//! otherwise (dev1 gives no unprivileged `drop_caches`).
//!
//! Run the release build. A debug build moves the Rust-side work by roughly an
//! order of magnitude and would make the observe-versus-recompute ratio
//! meaningless.
//!
//! Command shape, all against one warehouse directory:
//!
//! ```text
//! cdc_bench <warehouse> init [--rows N]
//! cdc_bench <warehouse> append [--rows N]
//! cdc_bench <warehouse> info
//! cdc_bench <warehouse> refresh [--steps K] [--batch N]
//! cdc_bench <warehouse> phases [--batch N]
//! cdc_bench <warehouse> recover
//! cdc_bench <warehouse> parts
//! cdc_bench <warehouse> read-since [--fraction F]
//! cdc_bench <warehouse> observe-vs-recompute [--batch N]
//! ```

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, RecordBatch};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use optd_incremental::cdc::{
    DeltaSink, Generation, OutputField, OutputSchema, PayloadType, SourceBinding,
};
use optd_incremental::consumer::Cursor;
use optd_incremental::iceberg_sink::{write_batch_files, CdcLogReader, CdcLoop, IcebergCdcSink};
use optd_incremental::local_catalog::LocalCatalog;
use optd_incremental::{filter, scan, DeltaSource, Expr, IcebergSource, Multiset, Value, View};

const SOURCE: &str = "source";
const OUTPUT: &str = "changes";
const NAMESPACE: &str = "demo";
const NEXT_ID: &str = "bench-next-id";

type AnyResult<T> = Result<T, Box<dyn Error>>;

fn ident(name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new(NAMESPACE.into()), name.into())
}

fn plan() -> optd_incremental::IncPlanNode {
    filter(
        scan(0),
        Expr::Gt(Box::new(Expr::Col(0)), Box::new(Expr::Lit(Value::Int(0)))),
    )
}

fn generation(source: &iceberg::table::Table) -> AnyResult<Generation> {
    Ok(Generation::new(
        "00000000-0000-0000-0000-000000000001".into(),
        "00000000-0000-0000-0000-000000000002".into(),
        OutputSchema::new(vec![OutputField {
            name: "id".into(),
            data_type: PayloadType::Long,
            required: true,
        }])?,
        &plan(),
        BTreeMap::from([(
            0,
            SourceBinding {
                table_id: source.metadata().uuid().to_string(),
                reference: "main".into(),
            },
        )]),
    )?)
}

async fn definition(catalog: &LocalCatalog) -> AnyResult<Generation> {
    let source = catalog.load_table(&ident(SOURCE)).await?;
    generation(&source)
}

fn seconds(started: Instant) -> f64 {
    started.elapsed().as_secs_f64()
}

fn flag(args: &[String], name: &str) -> Option<String> {
    let index = args.iter().position(|arg| arg == name)?;
    args.get(index + 1).cloned()
}

fn number<T: std::str::FromStr>(args: &[String], name: &str, fallback: T) -> AnyResult<T> {
    match flag(args, name) {
        Some(value) => Ok(value.parse::<T>().map_err(|_| {
            format!(
                "{name} expects a number, got {value:?} (see --rows, --batch, --steps, --fraction)"
            )
        })?),
        None => Ok(fallback),
    }
}

fn next_id_path(root: &Path) -> PathBuf {
    root.join(NEXT_ID)
}

fn read_next_id(root: &Path) -> u64 {
    fs::read_to_string(next_id_path(root))
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(1)
}

fn write_next_id(root: &Path, next: u64) -> AnyResult<()> {
    fs::write(next_id_path(root), next.to_string())?;
    Ok(())
}

async fn append_rows(catalog: &LocalCatalog, root: &Path, rows: u64) -> AnyResult<(u64, u64)> {
    let start = read_next_id(root);
    let values: Vec<i64> = (start..start + rows).map(|id| id as i64).collect();
    let table = catalog.load_table(&ident(SOURCE)).await?;
    let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])?;
    let files = write_batch_files(&table, batch).await?;
    let tx = Transaction::new(&table);
    tx.fast_append()
        .add_data_files(files)
        .apply(tx)?
        .commit(catalog)
        .await?;
    write_next_id(root, start + rows)?;
    Ok((start, start + rows - 1))
}

async fn read_counts(reader: &CdcLogReader) -> AnyResult<(usize, usize)> {
    let batches = reader.read_batches().await?;
    let rows = batches
        .iter()
        .map(|batch| batch.rows().len())
        .sum::<usize>();
    Ok((batches.len(), rows))
}

/// View state after `count` batches: the multiset a consumer would hold.
fn fold(batches: &[optd_incremental::cdc::PreparedBatch], count: usize) -> AnyResult<Multiset> {
    let mut state = Multiset::new();
    for batch in &batches[..count] {
        state = state.try_union(&batch.delta()?)?;
    }
    Ok(state)
}

fn cursor_after(batches: &[optd_incremental::cdc::PreparedBatch], count: usize) -> Cursor {
    if count == 0 {
        return Cursor::default();
    }
    let envelope = batches[count - 1].envelope();
    Cursor {
        batch_seq: envelope.batch_seq,
        batch_id: Some(envelope.batch_id.clone()),
    }
}

async fn init(root: &Path, rows: u64) -> AnyResult<()> {
    let catalog = Arc::new(LocalCatalog::open(root)?);
    catalog
        .create_namespace(&NamespaceIdent::new(NAMESPACE.into()), Default::default())
        .await?;
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()?;
    let started = Instant::now();
    catalog
        .create_table(
            ident(SOURCE).namespace(),
            TableCreation::builder()
                .name(SOURCE.into())
                .schema(schema)
                .format_version(FormatVersion::V2)
                .properties([("commit.retry.num-retries".into(), "0".into())])
                .build(),
        )
        .await?;
    let (_, last) = append_rows(&catalog, root, rows).await?;
    let load_s = seconds(started);

    let definition = definition(&catalog).await?;
    let started = Instant::now();
    let mut driver = CdcLoop::recover(
        catalog,
        ident(SOURCE),
        ident(OUTPUT),
        definition,
        plan(),
        "bench".into(),
    )
    .await?;
    let bootstrap = driver.step().await?;
    let bootstrap_s = seconds(started);
    let rows_written = bootstrap
        .as_ref()
        .and_then(|_| driver.checkpoint())
        .map(|_| {
            driver
                .view()
                .contents()
                .iter()
                .map(|(_, w)| *w)
                .sum::<i64>()
        })
        .unwrap_or(0);
    println!(
        "{}",
        serde_json::json!({
            "phase": "init",
            "source_rows": rows,
            "highest_id": last,
            "load_s": load_s,
            "bootstrap_s": bootstrap_s,
            "view_rows": rows_written,
        })
    );
    Ok(())
}

async fn append(root: &Path, rows: u64) -> AnyResult<()> {
    let catalog = LocalCatalog::open(root)?;
    let (first, last) = append_rows(&catalog, root, rows).await?;
    println!(
        "{}",
        serde_json::json!({"phase": "append", "rows": rows, "first_id": first, "last_id": last})
    );
    Ok(())
}

async fn info(root: &Path) -> AnyResult<()> {
    let catalog = Arc::new(LocalCatalog::open(root)?);
    let definition = definition(&catalog).await?;
    let started = Instant::now();
    let reader = CdcLogReader::open(catalog, ident(OUTPUT), definition).await?;
    let open_s = seconds(started);
    let started = Instant::now();
    let (batches, rows) = read_counts(&reader).await?;
    let scan_s = seconds(started);
    println!(
        "{}",
        serde_json::json!({
            "phase": "info",
            "batches": batches,
            "rows": rows,
            "index_open_s": open_s,
            "full_scan_s": scan_s,
        })
    );
    Ok(())
}

async fn refresh(root: &Path, steps: u64, batch: u64) -> AnyResult<()> {
    let catalog = Arc::new(LocalCatalog::open(root)?);
    let definition = definition(&catalog).await?;
    let mut driver = CdcLoop::recover(
        catalog.clone(),
        ident(SOURCE),
        ident(OUTPUT),
        definition,
        plan(),
        "bench".into(),
    )
    .await?;
    for step in 0..steps {
        append_rows(&catalog, root, batch).await?;
        let files = AtomicU64::new(0);
        let committed = AtomicU64::new(0);
        let started = Instant::now();
        let envelope = driver
            .step_with_observer(&|stage| {
                let nanos = started.elapsed().as_nanos() as u64;
                match stage {
                    optd_incremental::iceberg_sink::CommitStage::FilesWritten => {
                        files.store(nanos, Ordering::Relaxed)
                    }
                    optd_incremental::iceberg_sink::CommitStage::CatalogCommitted => {
                        committed.store(nanos, Ordering::Relaxed)
                    }
                }
                Ok(())
            })
            .await?;
        let total = started.elapsed().as_secs_f64();
        let files_s = files.load(Ordering::Relaxed) as f64 / 1e9;
        let committed_s = committed.load(Ordering::Relaxed) as f64 / 1e9;
        let record = match &envelope {
            Some(envelope) => serde_json::json!({
                "phase": "refresh",
                "step": step,
                "rows_added": batch,
                "delta_rows": envelope.record_count,
                "batch_seq": envelope.batch_seq,
                "pre_commit_s": files_s,
                "commit_s": committed_s - files_s,
                "total_s": total,
            }),
            None => serde_json::json!({
                "phase": "refresh",
                "step": step,
                "rows_added": batch,
                "delta_rows": 0,
                "note": "no source position advanced",
                "total_s": total,
            }),
        };
        println!("{record}");
    }
    Ok(())
}

/// Split one refresh into observe / Δ-evaluate / write / commit.
///
/// The observe and Δ timings are taken on the real loop state before `step`
/// runs, so `step` repeats that work; `write_est_s` is therefore derived by
/// subtraction and is reported as an estimate, not a measurement.
async fn phases(root: &Path, batch: u64) -> AnyResult<()> {
    let catalog = Arc::new(LocalCatalog::open(root)?);
    let definition = definition(&catalog).await?;
    let mut driver = CdcLoop::recover(
        catalog.clone(),
        ident(SOURCE),
        ident(OUTPUT),
        definition.clone(),
        plan(),
        "bench".into(),
    )
    .await?;
    append_rows(&catalog, root, batch).await?;

    let source_id = *definition
        .bindings()
        .keys()
        .next()
        .ok_or("generation has no binding")?;
    let table = catalog.load_table(&ident(SOURCE)).await?;
    let previous = driver.checkpoint().cloned();

    let started = Instant::now();
    let observation = IcebergSource::observe(
        source_id,
        &table,
        previous.as_ref().map(|envelope| &envelope.source_to[0]),
    )
    .await?;
    let observe_s = seconds(started);

    let started = Instant::now();
    let candidate = driver.view().prepare_refresh(&observation)?;
    let delta_s = seconds(started);
    let delta_rows = candidate
        .as_ref()
        .map(|candidate| candidate.delta().iter().map(|(_, w)| *w).sum::<i64>())
        .unwrap_or(0);

    let files = AtomicU64::new(0);
    let committed = AtomicU64::new(0);
    let started = Instant::now();
    let envelope = driver
        .step_with_observer(&|stage| {
            let nanos = started.elapsed().as_nanos() as u64;
            match stage {
                optd_incremental::iceberg_sink::CommitStage::FilesWritten => {
                    files.store(nanos, Ordering::Relaxed)
                }
                optd_incremental::iceberg_sink::CommitStage::CatalogCommitted => {
                    committed.store(nanos, Ordering::Relaxed)
                }
            }
            Ok(())
        })
        .await?;
    let total_s = started.elapsed().as_secs_f64();
    let files_s = files.load(Ordering::Relaxed) as f64 / 1e9;
    let commit_s = committed.load(Ordering::Relaxed) as f64 / 1e9 - files_s;
    println!(
        "{}",
        serde_json::json!({
            "phase": "phases",
            "rows_added": batch,
            "delta_rows": delta_rows,
            "observe_s": observe_s,
            "delta_s": delta_s,
            "step_pre_commit_s": files_s,
            "step_commit_s": commit_s,
            "step_total_s": total_s,
            "write_est_s": files_s - observe_s - delta_s,
            "committed_batches": envelope.map(|e| e.batch_seq),
        })
    );
    Ok(())
}

async fn recover(root: &Path) -> AnyResult<()> {
    let catalog = Arc::new(LocalCatalog::open(root)?);
    let definition = definition(&catalog).await?;
    let started = Instant::now();
    let driver = CdcLoop::recover(
        catalog,
        ident(SOURCE),
        ident(OUTPUT),
        definition,
        plan(),
        "bench".into(),
    )
    .await?;
    let recover_s = seconds(started);
    let resumed = driver.checkpoint().is_some();
    println!(
        "{}",
        serde_json::json!({
            "phase": "recover",
            "recover_s": recover_s,
            "resumed": resumed,
            "view_rows": driver.view().contents().iter().map(|(_, w)| *w).sum::<i64>(),
        })
    );
    Ok(())
}

/// The three segments a non-bootstrap `recover` is built from, measured
/// separately on the same warehouse. `IcebergCdcSink::open` and
/// `last_checkpoint` each run a full `read_batches`, and the loop's recovery
/// runs a third; `single_read_model_s` is index + one scan + fold, i.e. what a
/// recovery that read the log once would pay.
async fn parts(root: &Path) -> AnyResult<()> {
    let catalog = Arc::new(LocalCatalog::open(root)?);
    let definition = definition(&catalog).await?;
    let source_id = *definition
        .bindings()
        .keys()
        .next()
        .ok_or("generation has no binding")?;

    let started = Instant::now();
    let sink = IcebergCdcSink::open(catalog.clone(), ident(OUTPUT), definition.clone()).await?;
    let sink_open_s = seconds(started);

    let started = Instant::now();
    let previous = DeltaSink::last_checkpoint(&sink).await?;
    let last_checkpoint_s = seconds(started);
    drop(sink);

    let started = Instant::now();
    let reader = CdcLogReader::open(catalog.clone(), ident(OUTPUT), definition.clone()).await?;
    let index_s = seconds(started);

    let started = Instant::now();
    let batches = reader.read_batches().await?;
    let scan_s = seconds(started);
    let rows: usize = batches.iter().map(|batch| batch.rows().len()).sum();

    let started = Instant::now();
    let folded = fold(&batches, batches.len())?;
    let fold_s = seconds(started);

    // The writer replays the whole history through `apply_to` before it writes
    // (idempotency check); measure that replay on its own.
    let started = Instant::now();
    let mut replayed = Multiset::new();
    for batch in &batches {
        replayed = batch.apply_to(&definition, &replayed)?;
    }
    let apply_to_s = seconds(started);
    if replayed != folded {
        return Err("apply_to and delta folding disagree".into());
    }

    let table = catalog.load_table(&ident(SOURCE)).await?;
    let started = Instant::now();
    let observation = IcebergSource::observe(
        source_id,
        &table,
        previous.as_ref().map(|envelope| &envelope.source_to[0]),
    )
    .await?;
    let observe_s = seconds(started);

    let position = match &previous {
        Some(envelope) => u64::try_from(envelope.source_to[0].sequence_number.unwrap_or(0))?,
        None => observation.current_position(),
    };
    let started = Instant::now();
    let view = View::load_at(plan(), &observation, vec![source_id], position)?;
    let load_s = seconds(started);
    let matches = folded == *view.contents();
    let history_rows: i64 = view.contents().iter().map(|(_, w)| *w).sum();

    println!(
        "{}",
        serde_json::json!({
            "phase": "parts",
            "batches": batches.len(),
            "rows": rows,
            "view_rows": history_rows,
            "sink_open_s": sink_open_s,
            "last_checkpoint_s": last_checkpoint_s,
            "index_s": index_s,
            "scan_s": scan_s,
            "fold_s": fold_s,
            "apply_to_s": apply_to_s,
            "source_observe_s": observe_s,
            "view_load_s": load_s,
            "fold_matches_view": matches,
            "single_read_model_s": index_s + scan_s + fold_s,
            "as_is_three_scans_s": sink_open_s + last_checkpoint_s + scan_s,
        })
    );
    Ok(())
}

async fn read_since(root: &Path, fraction: f64) -> AnyResult<()> {
    if !(0.0..=1.0).contains(&fraction) {
        return Err("--fraction must be between 0 and 1".into());
    }
    let catalog = Arc::new(LocalCatalog::open(root)?);
    let definition = definition(&catalog).await?;
    let reader = CdcLogReader::open(catalog, ident(OUTPUT), definition).await?;

    let started = Instant::now();
    let all = reader.read_batches().await?;
    let full_s = seconds(started);
    let rows: usize = all.iter().map(|batch| batch.rows().len()).sum();

    let consumed = ((all.len() as f64) * fraction).floor() as usize;
    let state = fold(&all, consumed)?;
    let cursor = cursor_after(&all, consumed);
    let started = Instant::now();
    let suffix = reader.read_since(&cursor, &state).await?;
    let suffix_s = seconds(started);
    let suffix_rows: usize = suffix.iter().map(|batch| batch.rows().len()).sum();

    println!(
        "{}",
        serde_json::json!({
            "phase": "read_since",
            "fraction": fraction,
            "batches": all.len(),
            "rows": rows,
            "consumed_batches": consumed,
            "suffix_batches": suffix.len(),
            "suffix_rows": suffix_rows,
            "read_batches_s": full_s,
            "read_since_s": suffix_s,
        })
    );
    Ok(())
}

/// Incremental Δ evaluation against a full recompute over the same observation.
async fn observe_vs_recompute(root: &Path, batch: u64) -> AnyResult<()> {
    let catalog = Arc::new(LocalCatalog::open(root)?);
    let definition = definition(&catalog).await?;
    let driver = CdcLoop::recover(
        catalog.clone(),
        ident(SOURCE),
        ident(OUTPUT),
        definition.clone(),
        plan(),
        "bench".into(),
    )
    .await?;
    append_rows(&catalog, root, batch).await?;

    let source_id = *definition
        .bindings()
        .keys()
        .next()
        .ok_or("generation has no binding")?;
    let table = catalog.load_table(&ident(SOURCE)).await?;
    let previous = driver.checkpoint().cloned();

    let started = Instant::now();
    let observation = IcebergSource::observe(
        source_id,
        &table,
        previous.as_ref().map(|envelope| &envelope.source_to[0]),
    )
    .await?;
    let observe_s = seconds(started);

    let started = Instant::now();
    let candidate = driver.view().prepare_refresh(&observation)?;
    let delta_s = seconds(started);
    let delta_rows = candidate
        .as_ref()
        .map(|candidate| candidate.delta().iter().map(|(_, w)| *w).sum::<i64>())
        .unwrap_or(0);

    let started = Instant::now();
    let recomputed = driver.view().recompute(&observation)?;
    let recompute_s = seconds(started);

    println!(
        "{}",
        serde_json::json!({
            "phase": "observe_vs_recompute",
            "rows_added": batch,
            "delta_rows": delta_rows,
            "observe_s": observe_s,
            "delta_s": delta_s,
            "recompute_s": recompute_s,
            "view_rows": recomputed.iter().map(|(_, w)| *w).sum::<i64>(),
        })
    );
    Ok(())
}

const USAGE: &str = "usage: cdc_bench <warehouse> init|append|info|refresh|phases|recover|parts|read-since|observe-vs-recompute [options]";

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> AnyResult<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let root = PathBuf::from(args.first().ok_or(USAGE)?);
    let command = args.get(1).ok_or(USAGE)?.as_str();
    fs::create_dir_all(&root)?;
    match command {
        "init" => init(&root, number(&args, "--rows", 200_000)?).await?,
        "append" => append(&root, number(&args, "--rows", 2_000)?).await?,
        "info" => info(&root).await?,
        "refresh" => {
            refresh(
                &root,
                number(&args, "--steps", 10)?,
                number(&args, "--batch", 2_000)?,
            )
            .await?
        }
        "phases" => phases(&root, number(&args, "--batch", 2_000)?).await?,
        "recover" => recover(&root).await?,
        "parts" => parts(&root).await?,
        "read-since" => read_since(&root, number(&args, "--fraction", 0.9)?).await?,
        "observe-vs-recompute" => {
            observe_vs_recompute(&root, number(&args, "--batch", 2_000)?).await?
        }
        _ => return Err(USAGE.into()),
    }
    Ok(())
}
