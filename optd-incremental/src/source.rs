// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! The change-feed contract.
//!
//! Everything in this crate assumes a `Δ` can be obtained for a base table. That
//! assumption lives here and nowhere else: the rest of the framework depends on
//! [`DeltaSource`], not on whether the feed is a log, a snapshot diff, or a query
//! over a version column. The capability flags are what turn "can this view be
//! maintained?" into a mechanical check rather than per-source special casing —
//! see `docs/src/incremental/change_capture.md` §1 and §7.

use std::collections::BTreeMap;

use crate::change::{Capabilities, Multiset, Position, Row, TableId};

/// A rejected source range or a failed snapshot read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceError {
    /// Requested range is outside this pinned observation.
    InvalidPosition,
    /// The source no longer retains the requested historical state.
    MissingSnapshot,
    /// The source or schema does not match the checkpoint.
    IncompatibleSchema,
    /// Checked multiset arithmetic failed.
    Overflow,
    /// A storage operation failed; the message carries storage context.
    Storage(String),
}
impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "source: {self:?}")
    }
}
impl std::error::Error for SourceError {}

/// A change feed for one or more base tables.
pub trait DeltaSource {
    /// What this feed can express. Consult before choosing incremental maintenance.
    fn capabilities(&self) -> Capabilities;

    /// The current end of the feed.
    fn current_position(&self) -> Position;

    /// The changes to `table` in the half-open range `(from, to]`, as a weighted
    /// multiset (`+1` insert, `-1` delete).
    fn changes(
        &self,
        table: TableId,
        from: Position,
        to: Position,
    ) -> Result<Multiset, SourceError>;

    /// The full contents of `table` at the current position.
    fn snapshot(&self, table: TableId) -> Result<Multiset, SourceError> {
        self.snapshot_at(table, self.current_position())
    }

    /// Read the exact historical state within this pinned observation.
    fn snapshot_at(&self, table: TableId, position: Position) -> Result<Multiset, SourceError>;
}

/// An in-memory source used for tests and for the differential harness.
///
/// It keeps, per table, the current contents plus an append-only log of the
/// changes, so `changes(from, to)` can be answered for any range and
/// `snapshot` is always at the latest position. That is a *change log* source
/// ([`Capabilities::change_log`]); the other feed shapes in the design differ
/// only in this implementation.
#[derive(Clone, Debug)]
pub struct InMemorySource {
    capabilities: Capabilities,
    tables: BTreeMap<TableId, Multiset>,
    /// position -> list of (table, row, weight)
    log: BTreeMap<Position, Vec<(TableId, Row, i64)>>,
    position: Position,
}

impl InMemorySource {
    pub fn new(capabilities: Capabilities) -> Self {
        Self {
            capabilities,
            tables: BTreeMap::new(),
            log: BTreeMap::new(),
            position: 0,
        }
    }

    /// Apply a batch of changes, logging it at the next position.
    ///
    /// Returns the new position, which is the `to` for a subsequent refresh.
    pub fn apply_batch(&mut self, changes: Vec<(TableId, Row, i64)>) -> Position {
        if changes.is_empty() {
            return self.position;
        }
        self.position += 1;
        let at = self.position;
        for (table, row, weight) in changes.iter() {
            self.tables
                .entry(*table)
                .or_default()
                .add_row(row.clone(), *weight);
        }
        self.log.insert(at, changes);
        at
    }

    /// Convenience for tests: insert one row into one table.
    pub fn insert(&mut self, table: TableId, row: Row) -> Position {
        self.apply_batch(vec![(table, row, 1)])
    }

    /// Convenience for tests: delete one row from one table.
    pub fn delete(&mut self, table: TableId, row: Row) -> Position {
        self.apply_batch(vec![(table, row, -1)])
    }
}

impl DeltaSource for InMemorySource {
    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    fn current_position(&self) -> Position {
        self.position
    }

    fn changes(
        &self,
        table: TableId,
        from: Position,
        to: Position,
    ) -> Result<Multiset, SourceError> {
        if from > to || to > self.position {
            return Err(SourceError::InvalidPosition);
        }
        let mut out = Multiset::new();
        use std::ops::Bound::{Excluded, Included};
        for (_, batch) in self.log.range((Excluded(from), Included(to))) {
            for (t, row, weight) in batch {
                if *t == table {
                    out.try_add_row(row.clone(), *weight)
                        .map_err(|_| SourceError::Overflow)?;
                }
            }
        }
        Ok(out)
    }

    fn snapshot_at(&self, table: TableId, position: Position) -> Result<Multiset, SourceError> {
        if position > self.position {
            return Err(SourceError::InvalidPosition);
        }
        self.changes(table, 0, position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::Value;

    fn row(i: i64) -> Row {
        vec![Value::Int(i)]
    }

    #[test]
    fn changes_range_is_half_open() {
        let mut src = InMemorySource::new(Capabilities::change_log());
        src.insert(0, row(1)); // position 1
        src.insert(0, row(2)); // position 2
        src.insert(0, row(3)); // position 3

        // (1, 3] must contain rows 2 and 3, not row 1.
        let d = src.changes(0, 1, 3).unwrap();
        assert_eq!(d.len(), 2);
        assert!(d.iter().all(|(r, _)| r != &row(1)));
    }

    #[test]
    fn snapshot_reflects_all_applied_batches() {
        let mut src = InMemorySource::new(Capabilities::change_log());
        src.insert(0, row(1));
        src.insert(0, row(2));
        src.delete(0, row(1));
        let snap = src.snapshot(0).unwrap();
        assert_eq!(snap.total(), 1);
    }

    #[test]
    fn an_empty_batch_does_not_advance_the_position() {
        let mut src = InMemorySource::new(Capabilities::change_log());
        src.insert(0, row(1));
        let p = src.current_position();
        assert_eq!(src.apply_batch(vec![]), p);
        assert_eq!(src.current_position(), p);
    }
}
