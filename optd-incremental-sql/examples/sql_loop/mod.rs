//! Private command handler shared with the example's integration tests.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use optd_incremental_sql::store::{CheckpointStore, Snapshot};
use optd_incremental_sql::{DurableSqlView, Error, Result, TableChange, MAX_MATERIALIZED_ROWS};
use serde::Deserialize;
use serde_json::{json, Value};

const MAX_TEXT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_IPC_BYTES: u64 = 256 * 1024 * 1024;
const USAGE: &str = "usage: sql_loop init <warehouse> <sql-file> <tables.json> | apply <warehouse> <changes.json> | show <warehouse> | replay <warehouse>";
type Rows = HashMap<Vec<ScalarValue>, i64>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeRequest {
    expected_epoch: u64,
    changes: Vec<Change>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Change {
    table: String,
    ipc: PathBuf,
    weights: Vec<i64>,
}

/// Execute exactly one CLI operation, returning its single JSON response.
pub async fn run(args: &[String]) -> Result<Value> {
    match args {
        [command, warehouse, sql_file, tables_file] if command == "init" => {
            let sql = String::from_utf8(read_bounded(Path::new(sql_file), MAX_TEXT_BYTES)?)
                .map_err(|_| invalid("SQL file must be valid UTF-8"))?;
            let mappings: BTreeMap<String, PathBuf> =
                serde_json::from_slice(&read_bounded(Path::new(tables_file), MAX_TEXT_BYTES)?)?;
            let tables = mappings
                .into_iter()
                .map(|(name, path)| Ok((name, read_ipc(&resolve(tables_file, &path))?)))
                .collect::<Result<BTreeMap<_, _>>>()?;
            let view = DurableSqlView::open(warehouse, &sql, tables.clone()).await?;
            if view.view().epoch() != 0 || !same_tables(&tables, &view.view().tables()?)? {
                return Err(invalid(
                    "init requires an empty store or identical epoch-zero bootstrap",
                ));
            }
            Ok(json!({
                "operation": "init",
                "status": "initialized_or_identical_bootstrap",
                "epoch": view.view().epoch(),
                "output": display(&view.view().contents()?)?,
                "strategies": view.view().strategies(),
            }))
        }
        [command, warehouse, changes_file] if command == "apply" => {
            let request: ChangeRequest =
                serde_json::from_slice(&read_bounded(Path::new(changes_file), MAX_TEXT_BYTES)?)?;
            // Release this discovery handle before DurableSqlView takes the lock.
            // The expected epoch is checked only after the latter owns the store.
            let sql = {
                let store = CheckpointStore::open(warehouse)?;
                committed(&store)?.sql
            };
            let mut view = DurableSqlView::open(warehouse, &sql, BTreeMap::new()).await?;
            if view.view().epoch() != request.expected_epoch {
                return Err(invalid(format!(
                    "expected epoch {} differs from committed epoch {}",
                    request.expected_epoch,
                    view.view().epoch()
                )));
            }
            let changes = request
                .changes
                .into_iter()
                .map(|change| {
                    Ok(TableChange {
                        table: change.table,
                        rows: read_ipc(&resolve(changes_file, &change.ipc))?,
                        weights: change.weights,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let stats = view.refresh(&changes).await?;
            Ok(json!({
                "operation": "apply",
                "epoch": view.view().epoch(),
                "output": display(&view.view().contents()?)?,
                "refresh": stats,
            }))
        }
        [command, warehouse] if command == "show" => {
            let store = CheckpointStore::open(warehouse)?;
            let snapshot = committed(&store)?;
            Ok(json!({
                "operation": "show", "epoch": snapshot.epoch,
                "sql": snapshot.sql,
                "tables": snapshot.tables.iter().map(|(name, batch)| {
                    (name.clone(), json!({"rows": batch.num_rows()}))
                }).collect::<BTreeMap<_, _>>(),
                "output": display(&snapshot.output)?,
            }))
        }
        [command, warehouse] if command == "replay" => {
            let store = CheckpointStore::open(warehouse)?;
            let history = store.history()?;
            if history.is_empty() {
                return Err(invalid("warehouse has no committed epoch"));
            }
            let mut rows = Rows::new();
            let mut epochs = Vec::new();
            for snapshot in &history {
                apply_delta(&mut rows, snapshot)?;
                if rows != collect(&snapshot.output)? {
                    return Err(invalid(format!(
                        "replayed output differs at epoch {}",
                        snapshot.epoch
                    )));
                }
                epochs.push(json!({"epoch": snapshot.epoch, "rows": snapshot.output.num_rows()}));
            }
            Ok(json!({"operation": "replay", "epochs": epochs}))
        }
        _ => Err(invalid(USAGE)),
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

fn committed(store: &CheckpointStore) -> Result<Snapshot> {
    store
        .load()?
        .ok_or_else(|| invalid("warehouse has no committed epoch"))
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() > limit {
        return Err(invalid(format!(
            "input {} exceeds its {limit}-byte file limit",
            path.display()
        )));
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(invalid("input grew beyond its file limit while reading"));
    }
    Ok(bytes)
}

fn resolve(json_file: &str, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        Path::new(json_file)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(path)
    }
}

fn read_ipc(path: &Path) -> Result<RecordBatch> {
    let file = File::open(path)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() > MAX_IPC_BYTES {
        return Err(invalid("IPC input exceeds the 256 MiB file limit"));
    }
    let reader = FileReader::try_new(file, None)?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    let mut rows = 0_usize;
    let mut bytes = 0_usize;
    for batch in reader {
        let batch = batch?;
        rows = rows.checked_add(batch.num_rows()).ok_or(Error::Overflow)?;
        bytes = bytes
            .checked_add(batch.get_array_memory_size())
            .ok_or(Error::Overflow)?;
        if rows > MAX_MATERIALIZED_ROWS || bytes as u64 > MAX_IPC_BYTES {
            return Err(invalid("decoded IPC exceeds local row or byte limit"));
        }
        batches.push(batch);
    }
    Ok(concat_batches(&schema, &batches)?)
}

fn row(batch: &RecordBatch, index: usize, columns: usize) -> Result<Vec<ScalarValue>> {
    batch.columns()[..columns]
        .iter()
        .map(|column| Ok(ScalarValue::try_from_array(column.as_ref(), index)?))
        .collect()
}

fn add(rows: &mut Rows, key: Vec<ScalarValue>, weight: i64) -> Result<()> {
    let next = rows
        .get(&key)
        .copied()
        .unwrap_or_default()
        .checked_add(weight)
        .ok_or(Error::Overflow)?;
    if next == 0 {
        rows.remove(&key);
    } else {
        rows.insert(key, next);
    }
    if rows.len() > MAX_MATERIALIZED_ROWS {
        return Err(invalid("log consumer exceeds local row limit"));
    }
    Ok(())
}

fn collect(batch: &RecordBatch) -> Result<Rows> {
    if batch.num_rows() > MAX_MATERIALIZED_ROWS {
        return Err(invalid("output exceeds local row limit"));
    }
    let mut rows = Rows::new();
    for index in 0..batch.num_rows() {
        add(&mut rows, row(batch, index, batch.num_columns())?, 1)?;
    }
    Ok(rows)
}

fn apply_delta(rows: &mut Rows, snapshot: &Snapshot) -> Result<()> {
    let columns = snapshot.output.num_columns();
    let schema = snapshot.delta.schema();
    if snapshot.delta.num_rows() > MAX_MATERIALIZED_ROWS
        || schema.fields().len() != columns + 1
        || schema.fields()[..columns] != snapshot.output.schema().fields()[..]
        || schema.metadata() != snapshot.output.schema().metadata()
        || schema.field(columns).name() != "__optd_weight"
        || schema.field(columns).data_type() != &DataType::Int64
    {
        return Err(invalid("invalid signed output delta schema or row count"));
    }
    let weights = snapshot
        .delta
        .column(columns)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("delta weights are not Int64"))?;
    if weights.null_count() != 0 {
        return Err(invalid("delta weights contain NULL"));
    }
    for index in 0..snapshot.delta.num_rows() {
        add(
            rows,
            row(&snapshot.delta, index, columns)?,
            weights.value(index),
        )?;
    }
    let total = rows.values().try_fold(0_u64, |total, weight| {
        let weight = u64::try_from(*weight).map_err(|_| invalid("delta deletes an absent row"))?;
        total.checked_add(weight).ok_or(Error::Overflow)
    })?;
    if total > MAX_MATERIALIZED_ROWS as u64 {
        return Err(invalid("replayed output exceeds local row limit"));
    }
    Ok(())
}

fn same_tables(
    left: &BTreeMap<String, RecordBatch>,
    right: &BTreeMap<String, RecordBatch>,
) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (name, batch) in left {
        let Some(other) = right.get(name) else {
            return Ok(false);
        };
        if batch.schema() != other.schema() || collect(batch)? != collect(other)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn display(batch: &RecordBatch) -> Result<Value> {
    let fields: Vec<_> = batch.schema().fields().iter().map(|field| {
        json!({"name": field.name(), "type": field.data_type().to_string(), "nullable": field.is_nullable()})
    }).collect();
    let rows = (0..batch.num_rows())
        .map(|index| {
            Ok(row(batch, index, batch.num_columns())?
                .iter()
                .map(|value| {
                    if value.is_null() {
                        Value::Null
                    } else {
                        Value::String(value.to_string())
                    }
                })
                .collect::<Vec<_>>())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({"row_count": batch.num_rows(), "fields": fields, "rows": rows}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::ipc::writer::FileWriter;
    use datafusion::arrow::record_batch::RecordBatchOptions;
    use std::fs;
    use std::sync::Arc;

    #[test]
    fn ipc_preserves_zero_column_row_counts_across_batches() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("zero.arrow");
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        let mut writer = FileWriter::try_new(File::create(&path).unwrap(), &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        assert_eq!(read_ipc(&path).unwrap().num_rows(), 6);
    }

    #[test]
    fn text_and_ipc_inputs_have_explicit_byte_limits() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("large");
        File::create(&path)
            .unwrap()
            .set_len(MAX_TEXT_BYTES + 1)
            .unwrap();
        assert!(read_bounded(&path, MAX_TEXT_BYTES).is_err());
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(MAX_IPC_BYTES + 1)
            .unwrap();
        assert!(read_ipc(&path).is_err());
    }
}
