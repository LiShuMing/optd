// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Local transactional destinations and pinned CDC observations. State and cursor
//! share one atomic file; this does not transact effects in external services.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::cdc::{fingerprint, parse_unique_json, CdcError, PreparedBatch};
use crate::change::{Capabilities, Multiset, Position, Row, TableId};
use crate::iceberg_sink::CdcLogReader;
use crate::local_catalog::WriterLock;
use crate::source::{DeltaSource, SourceError};

/// A complete-batch bookmark; sequence zero denotes the empty predecessor.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    /// Logical sequence, independent of physical Iceberg snapshots.
    pub batch_seq: i64,
    /// Identity at that sequence, absent only at zero.
    pub batch_id: Option<String>,
}
impl Cursor {
    fn at(batch: &PreparedBatch) -> Self {
        Self {
            batch_seq: batch.envelope().batch_seq,
            batch_id: Some(batch.envelope().batch_id.clone()),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    protocol: String,
    consumer_id: String,
    output_table_uuid: String,
    generation_id: String,
    cursor: Cursor,
    rows: Vec<(Row, i64)>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateFile {
    state: State,
    checksum: String,
}

/// An immutable `(from, upper]` observation. Peeking does not advance the cursor.
#[derive(Clone, Debug)]
pub struct PinnedChanges {
    table_uuid: String,
    generation_id: String,
    from: Cursor,
    upper: Cursor,
    batches: Vec<PreparedBatch>,
}
impl PinnedChanges {
    /// Starting bookmark excluded from the observation.
    pub fn from(&self) -> &Cursor {
        &self.from
    }
    /// Included upper bound, unchanged when later producer commits appear.
    pub fn upper(&self) -> &Cursor {
        &self.upper
    }
    /// Validated complete batches in replay order.
    pub fn batches(&self) -> &[PreparedBatch] {
        &self.batches
    }
}

/// Durability boundaries for local destination fault injection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumeStage {
    /// Checksummed replacement has been fsynced; old state is still authoritative.
    StateStaged,
    /// Replacement and its containing directory have been fsynced.
    StateCommitted,
}

/// One independently named, OS-owned materialized destination and its bookmark.
pub struct LocalConsumer {
    reader: CdcLogReader,
    consumer_id: String,
    dir: PathBuf,
    _owner: WriterLock,
    cursor: Cursor,
    relation: Multiset,
}
impl LocalConsumer {
    /// Open a destination; a second process cannot consume under the same name.
    pub async fn open(reader: CdcLogReader, consumer_id: String) -> Result<Self, CdcError> {
        if consumer_id.is_empty() {
            return Err(CdcError::InvalidCheckpoint);
        }
        let identity = fingerprint(&json!([
            "optd-consumer/1",
            reader.table_name(),
            consumer_id
        ]))?;
        let dir = reader.catalog().consumer_dir(&identity[7..])?;
        let owner = reader
            .catalog()
            .writer_lock(&format!("consumer-{}", &identity[7..]))?;
        let mut out = Self {
            reader,
            consumer_id,
            dir,
            _owner: owner,
            cursor: Cursor::default(),
            relation: Multiset::new(),
        };
        if let Some((cursor, relation)) = out.read_state()? {
            out.reader.read_since(&cursor, &relation).await?;
            out.cursor = cursor;
            out.relation = relation;
        }
        Ok(out)
    }
    /// Durable local bookmark.
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }
    /// Destination contents committed atomically with that bookmark.
    pub fn contents(&self) -> &Multiset {
        &self.relation
    }
    fn read_state(&self) -> Result<Option<(Cursor, Multiset)>, CdcError> {
        let input = match fs::read_to_string(self.dir.join("state.json")) {
            Ok(input) => input,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let saved: StateFile = serde_json::from_value(parse_unique_json(&input)?)?;
        if fingerprint(&json!(["optd-consumer-state/1", &saved.state]))? != saved.checksum {
            return Err(CdcError::DigestMismatch);
        }
        let state = saved.state;
        if state.protocol != "optd-consumer/1"
            || state.consumer_id != self.consumer_id
            || state.output_table_uuid != self.reader.table_uuid()
            || state.generation_id != self.reader.generation().generation_id()
        {
            return Err(CdcError::InvalidCheckpoint);
        }
        let mut relation = Multiset::new();
        let mut last: Option<&Row> = None;
        for (row, weight) in &state.rows {
            if *weight <= 0 || last.is_some_and(|prev| prev >= row) {
                return Err(CdcError::InvalidRow);
            }
            relation
                .try_add_row(row.clone(), *weight)
                .map_err(|_| CdcError::Overflow)?;
            last = Some(row);
        }
        self.reader
            .generation()
            .schema()
            .validate_relation(&relation)?;
        if state.cursor == Cursor::default() && !relation.is_empty() {
            return Err(CdcError::InvalidCheckpoint);
        }
        Ok(Some((state.cursor, relation)))
    }
    /// Pin the currently available suffix without changing effects or cursor.
    pub async fn peek(&self) -> Result<PinnedChanges, CdcError> {
        let batches = self.reader.read_since(&self.cursor, &self.relation).await?;
        let upper = batches
            .last()
            .map(Cursor::at)
            .unwrap_or_else(|| self.cursor.clone());
        let suffix = batches
            .into_iter()
            .filter(|b| b.envelope().batch_seq > self.cursor.batch_seq)
            .collect();
        Ok(PinnedChanges {
            table_uuid: self.reader.table_uuid().into(),
            generation_id: self.reader.generation().generation_id().into(),
            from: self.cursor.clone(),
            upper,
            batches: suffix,
        })
    }
    /// Atomically apply this pinned range and its cursor. Repeating it is a no-op.
    pub fn consume(&mut self, changes: &PinnedChanges) -> Result<bool, CdcError> {
        self.consume_with_observer(changes, &|_| Ok(()))
    }
    /// Instrument the actual atomic replacement, without executing external effects.
    pub fn consume_with_observer(
        &mut self,
        changes: &PinnedChanges,
        observer: &dyn Fn(ConsumeStage) -> Result<(), CdcError>,
    ) -> Result<bool, CdcError> {
        if changes.table_uuid != self.reader.table_uuid()
            || changes.generation_id != self.reader.generation().generation_id()
        {
            return Err(CdcError::InvalidCheckpoint);
        }
        if changes.upper == self.cursor {
            return Ok(false);
        }
        if changes.from != self.cursor {
            return Err(CdcError::Conflict);
        }
        let mut next = self.relation.clone();
        for batch in &changes.batches {
            next = batch.apply_to(self.reader.generation(), &next)?;
        }
        let state = State {
            protocol: "optd-consumer/1".into(),
            consumer_id: self.consumer_id.clone(),
            output_table_uuid: changes.table_uuid.clone(),
            generation_id: changes.generation_id.clone(),
            cursor: changes.upper.clone(),
            rows: next.iter().map(|(r, w)| (r.clone(), *w)).collect(),
        };
        let checksum = fingerprint(&json!(["optd-consumer-state/1", &state]))?;
        let mut file = File::create(self.dir.join("state.pending"))?;
        file.write_all(&serde_json::to_vec(&StateFile { state, checksum })?)?;
        file.sync_all()?;
        observer(ConsumeStage::StateStaged)?;
        fs::rename(self.dir.join("state.pending"), self.dir.join("state.json"))?;
        File::open(&self.dir)?.sync_all()?;
        // Resolve a lost acknowledgement from the durable destination, never reapply it.
        let outcome = observer(ConsumeStage::StateCommitted);
        let (cursor, relation) = self.read_state()?.ok_or(CdcError::UnknownCommit)?;
        if cursor != changes.upper || relation != next {
            return Err(CdcError::DigestMismatch);
        }
        self.cursor = cursor;
        self.relation = relation;
        // Publication is authoritative even if the observer lost its response.
        let _ = outcome;
        Ok(true)
    }
    /// Pin source states starting at this consumer's validated state checkpoint.
    pub async fn source(&self, table_id: TableId) -> Result<CdcLogSource, CdcError> {
        let changes = self.peek().await?;
        let start =
            u64::try_from(self.cursor.batch_seq).map_err(|_| CdcError::InvalidCheckpoint)?;
        let mut states = BTreeMap::from([(start, self.relation.clone())]);
        let mut relation = self.relation.clone();
        for batch in changes.batches() {
            relation = batch.apply_to(self.reader.generation(), &relation)?;
            states.insert(
                u64::try_from(batch.envelope().batch_seq)
                    .map_err(|_| CdcError::InvalidCheckpoint)?,
                relation.clone(),
            );
        }
        Ok(CdcLogSource {
            table_id,
            states,
            upper: changes.upper.batch_seq as u64,
        })
    }
}

/// Synchronous, pinned DeltaSource reconstructed from one checkpoint plus its suffix.
pub struct CdcLogSource {
    table_id: TableId,
    states: BTreeMap<Position, Multiset>,
    upper: Position,
}
impl DeltaSource for CdcLogSource {
    fn capabilities(&self) -> Capabilities {
        let mut caps = Capabilities::change_log();
        caps.cross_table_position = false;
        caps
    }
    fn current_position(&self) -> Position {
        self.upper
    }
    fn snapshot_at(&self, table: TableId, position: Position) -> Result<Multiset, SourceError> {
        if table != self.table_id {
            return Err(SourceError::IncompatibleSchema);
        }
        self.states
            .get(&position)
            .cloned()
            .ok_or(SourceError::MissingSnapshot)
    }
    fn changes(
        &self,
        table: TableId,
        from: Position,
        to: Position,
    ) -> Result<Multiset, SourceError> {
        if from > to || to > self.upper {
            return Err(SourceError::InvalidPosition);
        }
        self.snapshot_at(table, to)?
            .try_difference(&self.snapshot_at(table, from)?)
            .map_err(|_| SourceError::Overflow)
    }
}
