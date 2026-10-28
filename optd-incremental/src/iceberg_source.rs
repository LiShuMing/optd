// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! An Iceberg-backed [`DeltaSource`] — the *snapshot-diff* shape.
//!
//! The feed takes its changes from the table's snapshot history: the relation at
//! position `p` is the table as of the `p`-th snapshot, and `changes(from, to)` is
//! `contents(to) ⊖ contents(from)`. That is shape **B** in
//! `docs/src/incremental/change_capture.md`: correct, needing no cooperation from
//! writers, but it reads whole tables rather than only what changed.
//!
//! Two consequences worth stating plainly:
//!
//! * The Iceberg crate has no changelog/incremental scan yet (only the v3
//!   `_change_type` column *definitions*), so shape A is not available here. The
//!   only time-travel primitive is `TableScanBuilder::snapshot_id`.
//! * This is the naive implementation: it scans both endpoints and diffs the
//!   resulting multisets. A later version can do a manifest-level diff (read only
//!   the data files added between two snapshots) without changing this contract.
//!
//! ## Positions
//!
//! A [`Position`] here is a **logical epoch**, not an Iceberg snapshot id: epoch
//! `i` is "the `i`-th snapshot on each table's mainline", with the last snapshot
//! carried forward once a table stops advancing. This keeps positions comparable
//! across tables, which a raw per-table snapshot id would not be: snapshot ids are
//! arbitrary, not monotone, and not comparable between tables.
//!
//! The epoch → snapshot-id map is built by walking **backwards from the current
//! snapshot along `parent_snapshot_id()` and reversing**. It must not come from
//! `TableMetadata::snapshots()`, which is a `HashMap` — its iteration order is
//! random per process, so `current_position()` would point at an arbitrary
//! historical snapshot and `changes` could compute "old minus new".
//!
//! ## Lifetime
//!
//! The timeline is read **once, in [`IcebergSource::new`]**. The feed does not
//! observe commits made after construction: reusing one source after the table
//! advances silently returns zero increments. Treat a source as belonging to a
//! single build — rebuild it (from a freshly loaded [`Table`]) after loading a new
//! snapshot.
//!
//! Because positions are indexes, they are only meaningful for the timeline they
//! were produced from: expiring old snapshots shifts every index. Persist
//! watermarks as **snapshot ids**, not epoch numbers — on restart, rebuild the
//! chain and resolve the id; if it is no longer on the chain (expired or
//! rolled back), reload the view in full rather than shifting the index.
//!
//! ## Schema changes
//!
//! Rows are compared **positionally**, so a view is only maintainable while the
//! top-level field-id layout is stable. If the field ids (in order) differ between
//! the two endpoints, this feed panics rather than emit a plausible-looking but
//! misaligned delta; rebuild the view instead.
//!
//! ## Supported column types
//!
//! `Value` has only `Null`, `Int(i64)` and `Str`, so the mapping is:
//!
//! | Arrow type | `Value` |
//! |---|---|
//! | Boolean | `Int(0/1)` |
//! | Int8/16/32/64, UInt8/16/32, Date32/64, Time32/64, Timestamp | `Int` (raw value; the time unit is the Arrow type's) |
//! | Float32/64, Decimal128, Utf8/LargeUtf8/Utf8View, Binary/LargeBinary/FixedSizeBinary | `Str` (decimals exact, floats shortest-round-trip, binary hex) |
//!
//! UInt64, Decimal256 and nested types (List/Struct/Map) are rejected **before
//! scanning**, naming the column. They are not silently coerced.
//!
//! ## Blocking
//!
//! [`DeltaSource`] is synchronous and Iceberg's scan API is asynchronous, so this
//! adapter owns a Tokio runtime and drives each scan to completion inside the
//! call. Calling a method from inside another Tokio runtime panics —
//! `block_on` cannot nest. Treat that as known debt: the honest fix is an async
//! `DeltaSource`, which is a trait-wide change rather than a local one.
//!
//! ### The table's runtime, not ours, runs the reads
//!
//! A [`Table`] captures the Tokio runtime that was current when it was
//! **constructed** — `StaticTable::from_metadata_file` calls
//! `iceberg::Runtime::try_current()` and keeps the handle — and the Arrow
//! reader spawns its file-read tasks on that captured runtime. The blocking
//! runtime this adapter creates cannot drive them. So if the table was built on
//! a `current_thread` runtime whose driver thread is the one now blocked here,
//! the scan never finishes: no error, no panic, just a hang (a debugger shows
//! the adapter parked in `epoll_wait` waiting for tasks that nothing polls).
//!
//! Construct the table on a multi-threaded runtime — `#[tokio::test(flavor =
//! "multi_thread")]`, or
//! `tokio::runtime::Builder::new_multi_thread().enable_all().build()` — and keep
//! that runtime alive for as long as any source built from the table. This
//! cannot be validated in [`IcebergSource::new`]: it receives an already-built
//! table and has no way to inspect the runtime captured inside it.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use futures::TryStreamExt;
use iceberg::arrow::type_to_arrow_type;
use iceberg::scan::ArrowRecordBatchStream;
use iceberg::spec::{Schema, SchemaRef, TableMetadata};
use iceberg::table::Table;

use crate::change::{Capabilities, Multiset, Position, Row, TableId, Value};
use crate::source::DeltaSource;

/// What went wrong while reading Iceberg.
#[derive(Debug)]
pub enum IcebergSourceError {
    /// An Iceberg call failed.
    Iceberg(iceberg::Error),
    /// A column has a type this adapter does not map to [`Value`].
    UnsupportedType { column: String, data_type: String },
    /// A snapshot the timeline refers to is gone (expired from metadata).
    MissingSnapshot { snapshot_id: i64 },
}

impl std::fmt::Display for IcebergSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IcebergSourceError::Iceberg(e) => write!(f, "iceberg: {}", e),
            IcebergSourceError::UnsupportedType { column, data_type } => {
                write!(f, "column `{}` has unsupported type {}", column, data_type)
            }
            IcebergSourceError::MissingSnapshot { snapshot_id } => {
                write!(f, "snapshot {} is no longer in the table metadata", snapshot_id)
            }
        }
    }
}

impl std::error::Error for IcebergSourceError {}

impl From<iceberg::Error> for IcebergSourceError {
    fn from(e: iceberg::Error) -> Self {
        IcebergSourceError::Iceberg(e)
    }
}

/// A change feed over one or more Iceberg tables, driven by snapshot history.
pub struct IcebergSource {
    runtime: tokio::runtime::Runtime,
    tables: BTreeMap<TableId, Arc<Table>>,
    /// `epochs[i]` is the snapshot id of each table at logical position `i + 1`.
    /// A table absent from an epoch has no snapshot yet (an empty relation).
    epochs: Vec<BTreeMap<TableId, i64>>,
    cross_table_position: bool,
}

impl IcebergSource {
    /// Build a feed from tables that are already resolved (via a catalog, or
    /// [`iceberg::table::StaticTable`] when the metadata file is known).
    ///
    /// Epochs line up the tables' mainlines: epoch `i` is the `i`-th snapshot of
    /// each table. A table with fewer snapshots keeps its last one, so positions
    /// stay monotone per table and comparable across them.
    ///
    /// The timeline is captured here and does not follow later commits — see the
    /// module docs.
    pub fn new(tables: Vec<(TableId, Table)>) -> Result<Self, IcebergSourceError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;

        // Snapshot ids on each table's mainline, in commit order.
        let mut timelines: BTreeMap<TableId, Vec<i64>> = BTreeMap::new();
        let mut by_id: BTreeMap<TableId, Arc<Table>> = BTreeMap::new();
        for (id, table) in tables {
            timelines.insert(id, mainline_snapshot_ids(table.metadata()));
            by_id.insert(id, Arc::new(table));
        }

        let depth = timelines.values().map(|v| v.len()).max().unwrap_or(0);
        let mut epochs = Vec::with_capacity(depth);
        for i in 0..depth {
            let mut at = BTreeMap::new();
            for (id, ids) in &timelines {
                if ids.is_empty() {
                    continue;
                }
                // Carry the last snapshot forward.
                let snapshot_id = ids[i.min(ids.len() - 1)];
                at.insert(*id, snapshot_id);
            }
            epochs.push(at);
        }

        Ok(Self {
            runtime,
            tables: by_id,
            epochs,
            cross_table_position: false,
        })
    }

    /// Declare whether the epoch schedule is assumed to be consistent across
    /// tables, which unlocks multi-table views. The default is `false`: lining up
    /// snapshots by index does not by itself prove they were committed in one
    /// transaction, and a torn read would be a silent error.
    pub fn with_cross_table_position(mut self, assume_consistent: bool) -> Self {
        self.cross_table_position = assume_consistent;
        self
    }

    /// The relation of `table` at logical position, plus the field-id layout used.
    ///
    /// `None` for the layout means "no snapshot at this position" (position 0, or
    /// a table that has never been written); the relation is then empty.
    fn scan_at(&self, table: TableId, position: Position) -> (Option<Vec<i32>>, Multiset) {
        if position == 0 {
            return (None, Multiset::new());
        }
        let idx = (position - 1) as usize;
        assert!(
            idx < self.epochs.len(),
            "position {} is beyond the feed's current position {}",
            position,
            self.epochs.len()
        );
        let Some(&snapshot_id) = self.epochs[idx].get(&table) else {
            return (None, Multiset::new());
        };
        let Some(table_ref) = self.tables.get(&table) else {
            panic!("table {} is not part of this feed", table);
        };
        match self.scan_snapshot(table_ref, snapshot_id) {
            Ok(v) => v,
            // A read failure must not look like "no rows" — that would silently
            // retract the whole table from the view. Panicking is the honest
            // option until the trait can report errors.
            Err(e) => panic!(
                "iceberg read failed for table {} at snapshot {}: {}",
                table, snapshot_id, e
            ),
        }
    }

    fn scan_snapshot(
        &self,
        table: &Table,
        snapshot_id: i64,
    ) -> Result<(Option<Vec<i32>>, Multiset), IcebergSourceError> {
        let schema = snapshot_schema(table, snapshot_id)?;
        // Reject unsupported columns before touching data, with real names.
        check_supported(&schema)?;
        let layout = field_layout(&schema);

        let scan = table.scan().snapshot_id(snapshot_id).build()?;
        let stream: ArrowRecordBatchStream = self.runtime.block_on(scan.to_arrow())?;
        let batches = self.runtime.block_on(stream.try_collect::<Vec<_>>())?;
        let mut out = Multiset::new();
        for batch in &batches {
            batch_into(&mut out, batch)?;
        }
        Ok((Some(layout), out))
    }
}

impl DeltaSource for IcebergSource {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            cross_table_position: self.cross_table_position,
            ..Capabilities::snapshot_diff()
        }
    }

    fn current_position(&self) -> Position {
        self.epochs.len() as Position
    }

    fn changes(&self, table: TableId, from: Position, to: Position) -> Multiset {
        debug_assert!(
            to <= self.current_position(),
            "change request past the feed's current position"
        );
        if from >= to {
            return Multiset::new();
        }
        let (after_layout, after) = self.scan_at(table, to);
        let (before_layout, before) = self.scan_at(table, from);

        // Rows are compared positionally. A changed field-id layout (add/drop/
        // reorder) makes that comparison meaningless, and a silent misalignment is
        // worse than refusing. Rebuild the view instead.
        if let (Some(after), Some(before)) = (&after_layout, &before_layout) {
            if after != before {
                panic!(
                    "iceberg schema layout changed for table {} between position {} and {} \
                     ({:?} -> {:?}); this snapshot-diff feed compares rows positionally, so \
                     rebuild the view instead of maintaining it",
                    table, from, to, before, after
                );
            }
        }

        after.difference(&before)
    }

    fn snapshot(&self, table: TableId) -> Multiset {
        let (_, contents) = self.scan_at(table, self.current_position());
        contents
    }
}

/// Snapshot ids on the table's mainline, oldest first.
///
/// `TableMetadata::snapshots()` is a `HashMap`, so its order is random; using it
/// would make the feed nondeterministic. Walking parents from the current
/// snapshot gives commit order and excludes branch snapshots (a branch tip is not
/// an ancestor of `current_snapshot_id()`). If an ancestor has been expired, the
/// walk stops there — the surviving suffix is still correctly ordered.
fn mainline_snapshot_ids(metadata: &TableMetadata) -> Vec<i64> {
    let mut ids = Vec::new();
    let mut current = metadata.current_snapshot_id();
    while let Some(id) = current {
        let Some(snapshot) = metadata.snapshot_by_id(id) else {
            break;
        };
        ids.push(id);
        current = snapshot.parent_snapshot_id();
    }
    ids.reverse();
    ids
}

fn snapshot_schema(table: &Table, snapshot_id: i64) -> Result<SchemaRef, IcebergSourceError> {
    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or(IcebergSourceError::MissingSnapshot { snapshot_id })?;
    Ok(snapshot.schema(metadata)?)
}

/// The top-level field ids, in order. Two endpoints with equal layouts can be
/// compared row-by-row positionally.
fn field_layout(schema: &Schema) -> Vec<i32> {
    schema.as_struct().fields().iter().map(|f| f.id).collect()
}

/// Reject columns whose Arrow type this adapter cannot map, before reading data.
fn check_supported(schema: &Schema) -> Result<(), IcebergSourceError> {
    for field in schema.as_struct().fields() {
        let arrow_type = type_to_arrow_type(&field.field_type)?;
        if !is_supported(&arrow_type) {
            return Err(IcebergSourceError::UnsupportedType {
                column: field.name.clone(),
                data_type: format!("{:?}", arrow_type),
            });
        }
    }
    Ok(())
}

fn is_supported(ty: &ArrowDataType) -> bool {
    use ArrowDataType::*;
    matches!(
        ty,
        Null
            | Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | Float32
            | Float64
            | Utf8
            | Utf8View
            | LargeUtf8
            | Binary
            | LargeBinary
            | FixedSizeBinary(_)
            | Date32
            | Date64
            | Time32(_)
            | Time64(_)
            | Timestamp(_, _)
            | Decimal128(_, _)
    )
}

/// Append one Arrow batch to `multiset`, mapping each row to a [`Value`] row.
fn batch_into(
    multiset: &mut Multiset,
    batch: &arrow::array::RecordBatch,
) -> Result<(), IcebergSourceError> {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{
        DataType, Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int8Type,
        Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
        Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
        UInt16Type, UInt32Type,
    };

    let columns = batch.columns();
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let mut rows: Vec<Row> = Vec::with_capacity(batch.num_rows());
    for r in 0..batch.num_rows() {
        let mut row = Vec::with_capacity(columns.len());
        for (i, c) in columns.iter().enumerate() {
            let value = if c.is_null(r) {
                Value::Null
            } else {
                match c.data_type() {
                    DataType::Null => Value::Null,
                    DataType::Boolean => Value::Int(c.as_boolean().value(r) as i64),
                    DataType::Int8 => Value::Int(c.as_primitive::<Int8Type>().value(r) as i64),
                    DataType::Int16 => Value::Int(c.as_primitive::<Int16Type>().value(r) as i64),
                    DataType::Int32 => Value::Int(c.as_primitive::<Int32Type>().value(r) as i64),
                    DataType::Int64 => Value::Int(c.as_primitive::<Int64Type>().value(r)),
                    DataType::UInt8 => Value::Int(c.as_primitive::<UInt8Type>().value(r) as i64),
                    DataType::UInt16 => Value::Int(c.as_primitive::<UInt16Type>().value(r) as i64),
                    DataType::UInt32 => Value::Int(c.as_primitive::<UInt32Type>().value(r) as i64),
                    DataType::Float32 => {
                        Value::Str(format!("{:?}", c.as_primitive::<Float32Type>().value(r)))
                    }
                    DataType::Float64 => {
                        Value::Str(format!("{:?}", c.as_primitive::<Float64Type>().value(r)))
                    }
                    DataType::Utf8 => Value::Str(c.as_string::<i32>().value(r).to_string()),
                    DataType::Utf8View => Value::Str(c.as_string_view().value(r).to_string()),
                    DataType::LargeUtf8 => Value::Str(c.as_string::<i64>().value(r).to_string()),
                    DataType::Binary => Value::Str(hex(c.as_binary::<i32>().value(r))),
                    DataType::LargeBinary => Value::Str(hex(c.as_binary::<i64>().value(r))),
                    DataType::FixedSizeBinary(_) => {
                        Value::Str(hex(c.as_fixed_size_binary().value(r)))
                    }
                    DataType::Date32 => Value::Int(c.as_primitive::<Date32Type>().value(r) as i64),
                    DataType::Date64 => Value::Int(c.as_primitive::<Date64Type>().value(r)),
                    DataType::Time32(TimeUnit::Second) => {
                        Value::Int(c.as_primitive::<Time32SecondType>().value(r) as i64)
                    }
                    DataType::Time32(TimeUnit::Millisecond) => {
                        Value::Int(c.as_primitive::<Time32MillisecondType>().value(r) as i64)
                    }
                    DataType::Time64(TimeUnit::Microsecond) => {
                        Value::Int(c.as_primitive::<Time64MicrosecondType>().value(r))
                    }
                    DataType::Time64(TimeUnit::Nanosecond) => {
                        Value::Int(c.as_primitive::<Time64NanosecondType>().value(r))
                    }
                    DataType::Timestamp(TimeUnit::Second, _) => {
                        Value::Int(c.as_primitive::<TimestampSecondType>().value(r))
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, _) => {
                        Value::Int(c.as_primitive::<TimestampMillisecondType>().value(r))
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        Value::Int(c.as_primitive::<TimestampMicrosecondType>().value(r))
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        Value::Int(c.as_primitive::<TimestampNanosecondType>().value(r))
                    }
                    DataType::Decimal128(_, scale) => {
                        let v = c.as_primitive::<Decimal128Type>().value(r);
                        Value::Str(format_decimal(v, *scale))
                    }
                    other => {
                        return Err(IcebergSourceError::UnsupportedType {
                            column: names
                                .get(i)
                                .cloned()
                                .unwrap_or_else(|| format!("#{}", i)),
                            data_type: format!("{:?}", other),
                        })
                    }
                }
            };
            row.push(value);
        }
        rows.push(row);
    }
    for row in rows {
        multiset.add_row(row, 1);
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// Exact decimal string, e.g. `1234` at scale 2 -> `"12.34"`.
fn format_decimal(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }
    let scale = scale as usize;
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let rendered = if digits.len() <= scale {
        format!("0.{:0>width$}", digits, width = scale)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        format!("-{}", rendered)
    } else {
        rendered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_is_exact() {
        assert_eq!(format_decimal(1234, 2), "12.34");
        assert_eq!(format_decimal(-1234, 2), "-12.34");
        assert_eq!(format_decimal(5, 2), "0.05");
        assert_eq!(format_decimal(1234, 0), "1234");
        assert_eq!(format_decimal(-5, 3), "-0.005");
    }

    #[test]
    fn hex_is_lowercase() {
        assert_eq!(hex(&[0x00, 0xab, 0xff]), "00abff");
    }

    /// The timeline must follow the parent chain, not `snapshots()` (a HashMap,
    /// whose order is random per process). The fixture is Iceberg's own
    /// deep-history table: linear chain, and a snapshot-log in commit order to
    /// check against.
    #[test]
    fn timeline_is_commit_order_not_hash_order() {
        let metadata: TableMetadata = serde_json::from_str(include_str!(
            "../testdata/example_table_metadata_v2_deep_history.json"
        ))
        .expect("fixture parses");

        let expected: Vec<i64> = metadata
            .history()
            .iter()
            .map(|entry| entry.snapshot_id)
            .collect();
        assert_eq!(expected.len(), 5, "fixture is the 5-snapshot deep history");
        assert_eq!(mainline_snapshot_ids(&metadata), expected);

        // The last epoch is the current snapshot — this is what `snapshot()` and
        // `current_position()` depend on.
        assert_eq!(
            mainline_snapshot_ids(&metadata).last().copied(),
            metadata.current_snapshot_id()
        );
    }
}