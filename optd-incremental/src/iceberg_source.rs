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
//! `i` is "the `i`-th snapshot of every table", with the last snapshot carried
//! forward once a table stops advancing. This keeps positions comparable across
//! tables, which a raw per-table snapshot id would not be.
//!
//! ## Blocking
//!
//! [`DeltaSource`] is synchronous and Iceberg's scan API is asynchronous, so this
//! adapter owns a Tokio runtime and drives each scan to completion inside the
//! call. Calling a method from inside another Tokio runtime panics —
//! `block_on` cannot nest. Treat that as known debt: the honest fix is an async
//! `DeltaSource`, which is a trait-wide change rather than a local one.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use iceberg::scan::ArrowRecordBatchStream;
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
}

impl std::fmt::Display for IcebergSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IcebergSourceError::Iceberg(e) => write!(f, "iceberg: {}", e),
            IcebergSourceError::UnsupportedType { column, data_type } => {
                write!(f, "column `{}` has unsupported type {}", column, data_type)
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
    /// Epochs line up the tables' snapshots in commit order: epoch `i` is the
    /// `i`-th snapshot of each table. A table that has fewer snapshots keeps its
    /// last one, so positions stay monotone per table and comparable across them.
    pub fn new(tables: Vec<(TableId, Table)>) -> Result<Self, IcebergSourceError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;

        // Snapshot ids in commit order, per table.
        let mut timelines: BTreeMap<TableId, Vec<i64>> = BTreeMap::new();
        let mut by_id: BTreeMap<TableId, Arc<Table>> = BTreeMap::new();
        for (id, table) in tables {
            let ids: Vec<i64> = table
                .metadata()
                .snapshots()
                .map(|s| s.snapshot_id())
                .collect();
            timelines.insert(id, ids);
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
                let sn = ids[i.min(ids.len() - 1)];
                at.insert(*id, sn);
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

    /// The relation of `table` at logical position `position`.
    fn scan_at(&self, table: TableId, position: Position) -> Multiset {
        if position == 0 {
            return Multiset::new();
        }
        let snapshot_id = match self
            .epochs
            .get((position - 1) as usize)
            .and_then(|at| at.get(&table))
        {
            Some(id) => *id,
            None => return Multiset::new(),
        };
        let table = match self.tables.get(&table) {
            Some(t) => t,
            None => return Multiset::new(),
        };
        match self.scan_snapshot(table, snapshot_id) {
            Ok(m) => m,
            // A read failure must not look like "no rows" — that would silently
            // retract the whole table from the view. Panicking is the honest
            // option until the trait can report errors.
            Err(e) => panic!("iceberg read failed at snapshot {}: {}", snapshot_id, e),
        }
    }

    fn scan_snapshot(&self, table: &Table, snapshot_id: i64) -> Result<Multiset, IcebergSourceError> {
        let scan = table.scan().snapshot_id(snapshot_id).build()?;
        let stream: ArrowRecordBatchStream = self.runtime.block_on(scan.to_arrow())?;
        let batches = self.runtime.block_on(stream.try_collect::<Vec<_>>())?;
        let mut out = Multiset::new();
        for batch in &batches {
            batch_into(&mut out, batch)?;
        }
        Ok(out)
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
        if from >= to {
            return Multiset::new();
        }
        let after = self.scan_at(table, to);
        let before = self.scan_at(table, from);
        after.difference(&before)
    }

    fn snapshot(&self, table: TableId) -> Multiset {
        self.scan_at(table, self.current_position())
    }
}

/// Append one Arrow batch to `multiset`, mapping each row to a [`Value`] row.
fn batch_into(
    multiset: &mut Multiset,
    batch: &arrow::array::RecordBatch,
) -> Result<(), IcebergSourceError> {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::DataType;

    let columns = batch.columns();
    let mut rows: Vec<Row> = Vec::with_capacity(batch.num_rows());
    for r in 0..batch.num_rows() {
        let mut row = Vec::with_capacity(columns.len());
        for c in columns {
            let value = match c.data_type() {
                DataType::Null => Value::Null,
                DataType::Int8 => match c.is_null(r) {
                    true => Value::Null,
                    false => Value::Int(c.as_primitive::<arrow::datatypes::Int8Type>().value(r) as i64),
                },
                DataType::Int16 => match c.is_null(r) {
                    true => Value::Null,
                    false => Value::Int(c.as_primitive::<arrow::datatypes::Int16Type>().value(r) as i64),
                },
                DataType::Int32 => match c.is_null(r) {
                    true => Value::Null,
                    false => Value::Int(c.as_primitive::<arrow::datatypes::Int32Type>().value(r) as i64),
                },
                DataType::Int64 => match c.is_null(r) {
                    true => Value::Null,
                    false => Value::Int(c.as_primitive::<arrow::datatypes::Int64Type>().value(r)),
                },
                DataType::Utf8 | DataType::Utf8View => match c.is_null(r) {
                    true => Value::Null,
                    false => Value::Str(c.as_string::<i32>().value(r).to_string()),
                },
                DataType::LargeUtf8 => match c.is_null(r) {
                    true => Value::Null,
                    false => Value::Str(c.as_string::<i64>().value(r).to_string()),
                },
                other => {
                    return Err(IcebergSourceError::UnsupportedType {
                        column: format!("#{}", row.len()),
                        data_type: format!("{:?}", other),
                    })
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