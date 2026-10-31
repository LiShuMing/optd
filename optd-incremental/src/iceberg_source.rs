// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Async Iceberg observation followed by synchronous evaluation over pinned snapshots.
//! No adapter-owned runtime or nested block_on is used. Construct tables and await reads
//! on a live application runtime. Durable recovery uses UUID/snapshot identities;
//! logical positions are snapshot sequence numbers, never rebuilt history indices.

use std::collections::BTreeMap;

use arrow::datatypes::DataType as ArrowDataType;
use futures::TryStreamExt;
use iceberg::arrow::type_to_arrow_type;
use iceberg::scan::ArrowRecordBatchStream;
use iceberg::spec::{Schema, SchemaRef, TableMetadata};
use iceberg::table::Table;

use crate::cdc::{fingerprint, SourceCheckpoint};
use crate::change::{Capabilities, Multiset, Position, Row, TableId, Value};
use crate::source::{DeltaSource, SourceError};

/// What went wrong while reading Iceberg.
#[derive(Debug)]
pub enum IcebergSourceError {
    /// An Iceberg call failed.
    Iceberg(iceberg::Error),
    /// A column has a type this adapter does not map to [`Value`].
    UnsupportedType {
        /// Column whose Iceberg type cannot be represented by this adapter.
        column: String,
        /// Diagnostic rendering of the unsupported source type.
        data_type: String,
    },
    /// A snapshot the timeline refers to is gone (expired from metadata).
    MissingSnapshot {
        /// Required Iceberg snapshot that is absent from retained metadata.
        snapshot_id: i64,
    },
    /// Checkpoint is incompatible with this observation.
    InvalidCheckpoint,
    /// Checked row arithmetic or metadata encoding failed.
    Source(SourceError),
}

impl std::fmt::Display for IcebergSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IcebergSourceError::Iceberg(e) => write!(f, "iceberg: {}", e),
            IcebergSourceError::UnsupportedType { column, data_type } => {
                write!(f, "column `{}` has unsupported type {}", column, data_type)
            }
            IcebergSourceError::MissingSnapshot { snapshot_id } => {
                write!(
                    f,
                    "snapshot {} is no longer in the table metadata",
                    snapshot_id
                )
            }
            IcebergSourceError::InvalidCheckpoint => {
                write!(f, "checkpoint is not on this source's mainline")
            }
            IcebergSourceError::Source(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for IcebergSourceError {}

impl From<iceberg::Error> for IcebergSourceError {
    fn from(e: iceberg::Error) -> Self {
        IcebergSourceError::Iceberg(e)
    }
}

/// A fully materialized, pinned observation over a single Iceberg source.
/// Async IO finishes before this object is passed to the reference evaluator.
pub struct IcebergSource {
    table_id: TableId,
    current: Position,
    snapshots: BTreeMap<Position, (FieldLayout, Multiset)>,
    checkpoint: SourceCheckpoint,
}

impl IcebergSource {
    /// Materialize retained snapshots for compatibility with arbitrary historical LOAD.
    /// Multi-table observation is rejected until a consistency protocol is available.
    pub async fn new(tables: Vec<(TableId, Table)>) -> Result<Self, IcebergSourceError> {
        if tables.len() != 1 {
            return Err(IcebergSourceError::InvalidCheckpoint);
        }
        let (table_id, table) = tables
            .into_iter()
            .next()
            .ok_or(IcebergSourceError::InvalidCheckpoint)?;
        let mut source = Self::observe(table_id, &table, None).await?;
        for id in mainline_snapshot_ids(table.metadata()) {
            let seq = table
                .metadata()
                .snapshot_by_id(id)
                .ok_or(IcebergSourceError::MissingSnapshot { snapshot_id: id })?
                .sequence_number();
            let at = u64::try_from(seq).map_err(|_| IcebergSourceError::InvalidCheckpoint)?;
            source
                .snapshots
                .insert(at, read_snapshot(&table, id).await?);
        }
        Ok(source)
    }

    /// Observe latest metadata already loaded by the caller, pinning the checkpoint and head.
    /// A refresh must call this again with a freshly loaded table to see later commits.
    pub async fn observe(
        table_id: TableId,
        table: &Table,
        previous: Option<&SourceCheckpoint>,
    ) -> Result<Self, IcebergSourceError> {
        check_supported(table.metadata().current_schema())?;
        let head = checkpoint(table, table.metadata().current_snapshot_id())?;
        let current = u64::try_from(head.sequence_number.unwrap_or(0))
            .map_err(|_| IcebergSourceError::InvalidCheckpoint)?;
        if head.snapshot_id.is_some() && current == 0 {
            return Err(IcebergSourceError::InvalidCheckpoint);
        }
        let mut snapshots = BTreeMap::new();
        if let Some(old) = previous {
            if old.table_id != head.table_id
                || old.reference != "main"
                || old.schema_fingerprint != head.schema_fingerprint
            {
                return Err(IcebergSourceError::InvalidCheckpoint);
            }
            let expected = checkpoint(table, old.snapshot_id)?;
            if &expected != old {
                return Err(IcebergSourceError::InvalidCheckpoint);
            }
            if let Some(id) = old.snapshot_id {
                if !mainline_snapshot_ids(table.metadata()).contains(&id) {
                    return Err(IcebergSourceError::InvalidCheckpoint);
                }
                let seq = u64::try_from(
                    old.sequence_number
                        .ok_or(IcebergSourceError::InvalidCheckpoint)?,
                )
                .map_err(|_| IcebergSourceError::InvalidCheckpoint)?;
                snapshots.insert(seq, read_snapshot(table, id).await?);
            }
        }
        if let Some(id) = head.snapshot_id {
            snapshots.insert(current, read_snapshot(table, id).await?);
        }
        let layout = field_layout(table.metadata().current_schema());
        snapshots.insert(0, (layout, Multiset::new()));
        Ok(Self {
            table_id,
            current,
            snapshots,
            checkpoint: head,
        })
    }

    /// Durable input identity for the observed head.
    pub fn checkpoint(&self) -> &SourceCheckpoint {
        &self.checkpoint
    }
}

impl DeltaSource for IcebergSource {
    fn capabilities(&self) -> Capabilities {
        Capabilities::snapshot_diff()
    }
    fn current_position(&self) -> Position {
        self.current
    }
    fn changes(
        &self,
        table: TableId,
        from: Position,
        to: Position,
    ) -> Result<Multiset, SourceError> {
        if from > to || to > self.current {
            return Err(SourceError::InvalidPosition);
        }
        let before = self.snapshot_at(table, from)?;
        let after = self.snapshot_at(table, to)?;
        let a = &self
            .snapshots
            .get(&from)
            .ok_or(SourceError::MissingSnapshot)?
            .0;
        let b = &self
            .snapshots
            .get(&to)
            .ok_or(SourceError::MissingSnapshot)?
            .0;
        if a != b {
            return Err(SourceError::IncompatibleSchema);
        }
        after
            .try_difference(&before)
            .map_err(|_| SourceError::Overflow)
    }
    fn snapshot_at(&self, table: TableId, position: Position) -> Result<Multiset, SourceError> {
        if table != self.table_id || position > self.current {
            return Err(SourceError::InvalidPosition);
        }
        self.snapshots
            .get(&position)
            .map(|(_, m)| m.clone())
            .ok_or(SourceError::MissingSnapshot)
    }
}

/// Fingerprint and stable identity of an exact source state, without scanning rows.
pub fn checkpoint(
    table: &Table,
    snapshot_id: Option<i64>,
) -> Result<SourceCheckpoint, IcebergSourceError> {
    let schema = match snapshot_id {
        Some(id) => snapshot_schema(table, id)?,
        None => table.current_schema_ref(),
    };
    let schema_json = serde_json::to_value(schema.as_ref())
        .map_err(|e| IcebergSourceError::Source(SourceError::Storage(e.to_string())))?;
    let schema_fingerprint = fingerprint(&serde_json::json!(["optd-source-schema/1", schema_json]))
        .map_err(|e| IcebergSourceError::Source(SourceError::Storage(e.to_string())))?;
    let sequence_number = match snapshot_id {
        Some(id) => Some(
            table
                .metadata()
                .snapshot_by_id(id)
                .ok_or(IcebergSourceError::MissingSnapshot { snapshot_id: id })?
                .sequence_number(),
        ),
        None => None,
    };
    Ok(SourceCheckpoint {
        table_id: table.metadata().uuid().to_string(),
        reference: "main".into(),
        snapshot_id,
        sequence_number,
        schema_fingerprint,
    })
}

async fn read_snapshot(
    table: &Table,
    id: i64,
) -> Result<(FieldLayout, Multiset), IcebergSourceError> {
    let schema = snapshot_schema(table, id)?;
    check_supported(&schema)?;
    let scan = table.scan().snapshot_id(id).build()?;
    let stream: ArrowRecordBatchStream = scan.to_arrow().await?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let mut out = Multiset::new();
    for batch in &batches {
        batch_into(&mut out, batch)?;
    }
    Ok((field_layout(&schema), out))
}

/// Snapshot ids on the table's mainline, oldest first.
///
/// `TableMetadata::snapshots()` is a `HashMap`, so its order is random; using it
/// would make the feed nondeterministic. Walking parents from the current
/// snapshot gives commit order and excludes branch snapshots (a branch tip is not
/// an ancestor of `current_snapshot_id()`). If an ancestor has been expired, the
/// walk stops there — the surviving suffix is still correctly ordered.
pub(crate) fn mainline_snapshot_ids(metadata: &TableMetadata) -> Vec<i64> {
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

/// Complete positional layout: stable ids alone do not catch a changed type or nullability.
type FieldLayout = Vec<(i32, String, Box<iceberg::spec::Type>, bool)>;

fn field_layout(schema: &Schema) -> FieldLayout {
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| (f.id, f.name.clone(), f.field_type.clone(), f.required))
        .collect()
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
        Null | Boolean
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
        DataType, Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int16Type,
        Int32Type, Int64Type, Int8Type, Time32MillisecondType, Time32SecondType,
        Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type,
        UInt32Type, UInt8Type,
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
                            column: names.get(i).cloned().unwrap_or_else(|| format!("#{}", i)),
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
        multiset
            .try_add_row(row, 1)
            .map_err(|_| IcebergSourceError::Source(SourceError::Overflow))?;
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

#[cfg(test)]
mod layout_regressions {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use std::sync::Arc;
    #[test]
    fn equal_field_ids_do_not_hide_changed_type_or_nullability() {
        let schema = |typ, required| {
            Schema::builder()
                .with_fields(vec![Arc::new(if required {
                    NestedField::required(1, "x", Type::Primitive(typ))
                } else {
                    NestedField::optional(1, "x", Type::Primitive(typ))
                })])
                .build()
                .unwrap()
        };
        let before = schema(PrimitiveType::Long, true);
        assert_ne!(
            field_layout(&before),
            field_layout(&schema(PrimitiveType::String, true))
        );
        assert_ne!(
            field_layout(&before),
            field_layout(&schema(PrimitiveType::Long, false))
        );
    }
}
