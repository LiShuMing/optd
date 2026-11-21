//! A database root is the sole authority for all tables and materialized views.
//!
//! Format `optd-sql-database/2` persists base tables and unordered materialized views
//! as immutable weighted blocks (`RowBlocks`), reusing untouched blocks across epochs
//! by `BlockUid`. Legacy `optd-sql-database/1` directories are rejected explicitly.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};

use super::response::CommitReport;
use super::store_work::{PayloadAction, PayloadKind, StoreWork, StoreWorkCounter};
use super::table_rows::TableRows;
use crate::bag::{Bag, MAX_MATERIALIZED_BYTES};
use crate::error::invalid;
use crate::row_blocks::{BlockOrderMode, BlockUid, PersistedBlock};
use crate::store::{
    batches_equal, component, json_bytes, read_batch, read_json_file, sync_dir, verified_file,
    write_batch, write_bytes, CheckpointDirectories, FileEntry, PublishPoint,
};
use crate::{Error, Result};

const FORMAT: &str = "optd-sql-database/2";
const MAX_HISTORY: usize = 100_000;
const INLINE_BLOCK_LIMIT: usize = 128;
static ATTEMPT: AtomicU64 = AtomicU64::new(0);

/// Base-table identity and contents at one database epoch.
#[derive(Clone)]
pub(super) struct TableData {
    pub id: u64,
    pub rows: TableRows,
}

/// Maintained view output: unordered views use block storage; ordered views keep sequence.
#[derive(Clone)]
pub(super) enum ViewOutput {
    Blocks(TableRows),
    Ordered(RecordBatch),
}

impl ViewOutput {
    pub(super) fn schema(&self) -> SchemaRef {
        match self {
            Self::Blocks(rows) => rows.schema(),
            Self::Ordered(batch) => batch.schema(),
        }
    }

    pub(super) fn num_rows(&self) -> usize {
        match self {
            Self::Blocks(rows) => rows.num_rows(),
            Self::Ordered(batch) => batch.num_rows(),
        }
    }

    pub(super) fn to_bag(&self) -> Result<Bag> {
        match self {
            Self::Blocks(rows) => rows.to_bag(),
            Self::Ordered(batch) => Bag::from_batch(batch),
        }
    }

    pub(super) fn same_contents(&self, other: &Self) -> Result<bool> {
        match (self, other) {
            (Self::Blocks(left), Self::Blocks(right)) => left.same_contents(right),
            (Self::Ordered(left), Self::Ordered(right)) => Ok(batches_equal(left, right)),
            _ => Ok(false),
        }
    }
}

/// Persisted view definition and output; execution indexes are restored or rebuilt on open.
#[derive(Clone)]
pub(super) struct ViewData {
    pub id: u64,
    pub sql: String,
    pub dependencies: BTreeSet<String>,
    pub output: ViewOutput,
}

/// Tables, view outputs and signed deltas published together under one root.
#[derive(Clone, Default)]
pub(super) struct DatabaseSnapshot {
    pub epoch: u64,
    pub next_id: u64,
    pub tables: BTreeMap<String, TableData>,
    pub views: BTreeMap<String, ViewData>,
    pub table_deltas: BTreeMap<String, RecordBatch>,
    pub deltas: BTreeMap<String, RecordBatch>,
}

/// Persisted request identity and original outcome for exact identified-write retries.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Receipt {
    pub id: String,
    pub fingerprint: String,
    pub report: CommitReport,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Root {
    directory: String,
    manifest: FileEntry,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    directory: String,
    file: FileEntry,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum PersistedOrderMode {
    Encoded,
    SingleLeaf,
}

impl From<BlockOrderMode> for PersistedOrderMode {
    fn from(mode: BlockOrderMode) -> Self {
        match mode {
            BlockOrderMode::Encoded => Self::Encoded,
            BlockOrderMode::SingleLeaf => Self::SingleLeaf,
        }
    }
}

impl From<PersistedOrderMode> for BlockOrderMode {
    fn from(mode: PersistedOrderMode) -> Self {
        match mode {
            PersistedOrderMode::Encoded => Self::Encoded,
            PersistedOrderMode::SingleLeaf => Self::SingleLeaf,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockEntry {
    min_key_sha256: String,
    row_count: usize,
    distinct_entries: usize,
    payload: Payload,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum BlockListRef {
    Inline(Vec<BlockEntry>),
    External {
        block_count: usize,
        payload: Payload,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockRelationEntry {
    schema: Payload,
    order_mode: PersistedOrderMode,
    blocks: BlockListRef,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableEntry {
    id: u64,
    data: BlockRelationEntry,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum ViewOutputEntry {
    BlockRelation(BlockRelationEntry),
    OrderedPayload(Payload),
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ViewEntry {
    id: u64,
    sql: String,
    dependencies: BTreeSet<String>,
    output: ViewOutputEntry,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    epoch: u64,
    previous: Option<Root>,
    next_id: u64,
    tables: BTreeMap<String, TableEntry>,
    views: BTreeMap<String, ViewEntry>,
    table_deltas: BTreeMap<String, Payload>,
    deltas: BTreeMap<String, Payload>,
    receipt_index: Option<Payload>,
    receipt: Option<Receipt>,
}

struct LoadedSnapshot {
    snapshot: DatabaseSnapshot,
    committed_blocks: BTreeMap<BlockUid, BlockEntry>,
    committed_schemas: BTreeMap<u64, Payload>,
}

struct CommitStage<'a> {
    dir: &'a Path,
    dir_name: &'a str,
    next_blocks: BTreeMap<BlockUid, BlockEntry>,
    next_schemas: BTreeMap<u64, Payload>,
}

/// Exclusive host-local writer; `CURRENT` alone determines committed visibility.
/// Unreferenced attempt directories are never treated as committed epochs.
pub(super) struct DatabaseStore {
    root: PathBuf,
    _lock: File,
    head: Option<Root>,
    manifest: Option<Manifest>,
    snapshot: DatabaseSnapshot,
    committed_blocks: BTreeMap<BlockUid, BlockEntry>,
    committed_schemas: BTreeMap<u64, Payload>,
    receipts: BTreeMap<String, Receipt>,
    work: StoreWorkCounter,
}

impl DatabaseStore {
    /// Acquire the writer lock and validate the current snapshot and metadata lineage.
    pub fn open(path: &Path) -> Result<Self> {
        let dirs = CheckpointDirectories::create(path)?;
        let root = dirs.root.clone();
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("WRITER.lock"))?;
        lock.try_lock()
            .map_err(|_| invalid("database already has a writer"))?;
        dirs.synchronize(sync_dir)?;
        let mut out = Self {
            root,
            _lock: lock,
            work: StoreWorkCounter::default(),
            head: None,
            manifest: None,
            snapshot: DatabaseSnapshot {
                next_id: 1,
                ..Default::default()
            },
            committed_blocks: BTreeMap::new(),
            committed_schemas: BTreeMap::new(),
            receipts: BTreeMap::new(),
        };
        match fs::symlink_metadata(out.root.join("CURRENT")) {
            Ok(_) => {
                let bytes = read_json_file(&out.root.join("CURRENT"))?;
                out.work.pointer_read(bytes.len())?;
                let head: Root = serde_json::from_slice(&bytes)?;
                let m = out.read_manifest(&head)?;
                let loaded = out.read_snapshot(&m)?;
                out.receipts = match &m.receipt_index {
                    Some(payload) => out.read_receipt_index(payload)?,
                    None => BTreeMap::new(),
                };
                out.snapshot = loaded.snapshot;
                out.committed_blocks = loaded.committed_blocks;
                out.committed_schemas = loaded.committed_schemas;
                out.manifest = Some(m);
                out.head = Some(head);
                // Validate the complete metadata lineage, without materializing old tables.
                out.manifests()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(out)
    }

    /// Clone catalog state while retaining shared immutable blocks and Arrow buffers.
    pub fn snapshot(&self) -> DatabaseSnapshot {
        self.snapshot.clone()
    }

    /// Snapshot completed file work; available even when the owning session is fenced.
    pub fn work(&self) -> Result<StoreWork> {
        self.work.snapshot()
    }

    fn directory(&self, name: &str) -> Result<PathBuf> {
        if !component(name) || !name.starts_with("db-") {
            return Err(invalid("invalid database payload directory"));
        }
        let path = self.root.join(name);
        if !fs::symlink_metadata(&path)?.file_type().is_dir() {
            return Err(invalid(
                "database payload directory is not a real directory",
            ));
        }
        Ok(path)
    }

    fn read_manifest(&self, root: &Root) -> Result<Manifest> {
        if root.manifest.name != "manifest.json" || root.manifest.bytes > 16 * 1024 * 1024 {
            return Err(invalid("invalid database manifest descriptor"));
        }
        let dir = self.directory(&root.directory)?;
        let file = verified_file(&dir, &root.manifest)?;
        let value: serde_json::Value = serde_json::from_reader(file)?;
        self.work.manifest_read(root.manifest.bytes)?;
        let format = value
            .get("format")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| invalid("missing database format"))?;
        if format != FORMAT {
            return Err(invalid(format!(
                "unsupported database format {format}; expected {FORMAT}"
            )));
        }
        let m: Manifest = serde_json::from_value(value)?;
        if m.epoch == 0 || m.next_id == 0 {
            return Err(invalid("invalid database epoch or relation id counter"));
        }
        let mut ids = BTreeSet::new();
        for (name, id) in m
            .tables
            .iter()
            .map(|(n, t)| (n, t.id))
            .chain(m.views.iter().map(|(n, v)| (n, v.id)))
        {
            super::sql::validate_name(name)?;
            if id == 0 || id >= m.next_id || !ids.insert(id) {
                return Err(invalid("duplicate or invalid database relation identity"));
            }
        }
        if m.views.keys().any(|name| m.tables.contains_key(name)) {
            return Err(invalid("table/view name collision"));
        }
        for view in m.views.values() {
            if view.dependencies.iter().any(|n| !m.tables.contains_key(n)) {
                return Err(invalid("view has an unbound base-table dependency"));
            }
        }
        if m.deltas.keys().any(|n| !m.views.contains_key(n)) {
            return Err(invalid("delta has no live materialized view"));
        }
        if m.table_deltas.keys().any(|n| !m.tables.contains_key(n)) {
            return Err(invalid("table delta has no live base table"));
        }
        Ok(m)
    }

    fn read_receipt_index(&self, payload: &Payload) -> Result<BTreeMap<String, Receipt>> {
        if payload.file.bytes > 16 * 1024 * 1024 {
            return Err(invalid("database receipt index exceeds byte budget"));
        }
        let dir = self.directory(&payload.directory)?;
        let file = verified_file(&dir, &payload.file)?;
        let map: BTreeMap<String, Receipt> = serde_json::from_reader(file)?;
        for (id, receipt) in &map {
            if id != &receipt.id {
                return Err(invalid("receipt index key differs from receipt ID"));
            }
        }
        Ok(map)
    }

    fn read_block_list(&self, list: &BlockListRef) -> Result<Vec<BlockEntry>> {
        match list {
            BlockListRef::Inline(entries) => Ok(entries.clone()),
            BlockListRef::External {
                block_count,
                payload,
            } => {
                if payload.file.bytes > 16 * 1024 * 1024 {
                    return Err(invalid("block list payload exceeds byte budget"));
                }
                let dir = self.directory(&payload.directory)?;
                let file = verified_file(&dir, &payload.file)?;
                let entries: Vec<BlockEntry> = serde_json::from_reader(file)?;
                if entries.len() != *block_count {
                    return Err(invalid("external block list count differs"));
                }
                Ok(entries)
            }
        }
    }

    fn read_ipc(
        &self,
        p: &Payload,
        kind: PayloadKind,
        logical_rows: Option<usize>,
    ) -> Result<RecordBatch> {
        if p.file.bytes > MAX_MATERIALIZED_BYTES as u64 {
            return Err(invalid("database IPC payload exceeds byte budget"));
        }
        let batch = read_batch(&self.directory(&p.directory)?, &p.file)?;
        let rows = logical_rows.unwrap_or_else(|| batch.num_rows());
        self.work
            .payload(kind, PayloadAction::Read, p.file.bytes, rows)?;
        Ok(batch)
    }

    fn batch(&self, p: &Payload, kind: PayloadKind) -> Result<RecordBatch> {
        let batch = self.read_ipc(p, kind, None)?;
        if !matches!(kind, PayloadKind::Table | PayloadKind::TableDelta) {
            Bag::from_batch(&batch)?;
        }
        Ok(batch)
    }

    fn read_block_relation(
        &self,
        entry: &BlockRelationEntry,
        kind: PayloadKind,
        committed_blocks: &mut BTreeMap<BlockUid, BlockEntry>,
    ) -> Result<TableRows> {
        let schema_batch = read_batch(
            &self.directory(&entry.schema.directory)?,
            &entry.schema.file,
        )?;
        if schema_batch.num_rows() != 0 {
            return Err(invalid("persisted schema batch must contain zero rows"));
        }
        let schema = schema_batch.schema();
        let entries = self.read_block_list(&entry.blocks)?;
        if entries.is_empty() {
            // Count empty relation schema decode so empty-table open accounting stays exact.
            self.work
                .payload(kind, PayloadAction::Read, entry.schema.file.bytes, 0)?;
        }
        let mut persisted = Vec::with_capacity(entries.len());
        for block in &entries {
            let weighted_batch = self.read_ipc(&block.payload, kind, Some(block.row_count))?;
            persisted.push(PersistedBlock {
                expected_min_key_sha256: block.min_key_sha256.clone(),
                expected_row_count: block.row_count,
                expected_distinct_entries: block.distinct_entries,
                weighted_batch,
            });
        }
        let (rows, uids, validation) =
            TableRows::from_persisted_blocks(schema, entry.order_mode.into(), persisted)?;
        if matches!(kind, PayloadKind::Table) {
            let bootstrap_work = crate::database::TableBootstrapWork {
                collected_rows: rows.num_rows() as u64,
                distinct_entries: entries.iter().try_fold(0u64, |sum, b| {
                    sum.checked_add(b.distinct_entries as u64)
                        .ok_or(Error::Overflow)
                })?,
                ..Default::default()
            };
            self.work.table_bootstrap(&bootstrap_work, &validation)?;
        }
        for (uid, block) in uids.into_iter().zip(entries) {
            committed_blocks.insert(uid, block);
        }
        Ok(rows)
    }

    fn read_snapshot(&self, m: &Manifest) -> Result<LoadedSnapshot> {
        let mut committed_blocks = BTreeMap::new();
        let mut committed_schemas = BTreeMap::new();
        let mut tables = BTreeMap::new();
        for (n, t) in &m.tables {
            let rows =
                self.read_block_relation(&t.data, PayloadKind::Table, &mut committed_blocks)?;
            committed_schemas.insert(t.id, t.data.schema.clone());
            tables.insert(n.clone(), TableData { id: t.id, rows });
        }
        let mut views = BTreeMap::new();
        for (n, v) in &m.views {
            let output = match &v.output {
                ViewOutputEntry::BlockRelation(entry) => {
                    committed_schemas.insert(v.id, entry.schema.clone());
                    ViewOutput::Blocks(self.read_block_relation(
                        entry,
                        PayloadKind::ViewOutput,
                        &mut committed_blocks,
                    )?)
                }
                ViewOutputEntry::OrderedPayload(p) => {
                    ViewOutput::Ordered(self.batch(p, PayloadKind::ViewOutput)?)
                }
            };
            views.insert(
                n.clone(),
                ViewData {
                    id: v.id,
                    sql: v.sql.clone(),
                    dependencies: v.dependencies.clone(),
                    output,
                },
            );
        }
        let table_deltas = m
            .table_deltas
            .iter()
            .map(|(n, p)| Ok((n.clone(), self.batch(p, PayloadKind::TableDelta)?)))
            .collect::<Result<_>>()?;
        let deltas = m
            .deltas
            .iter()
            .map(|(n, p)| Ok((n.clone(), self.batch(p, PayloadKind::OutputDelta)?)))
            .collect::<Result<_>>()?;
        Ok(LoadedSnapshot {
            snapshot: DatabaseSnapshot {
                epoch: m.epoch,
                next_id: m.next_id,
                tables,
                views,
                table_deltas,
                deltas,
            },
            committed_blocks,
            committed_schemas,
        })
    }

    fn manifests(&self) -> Result<Vec<Manifest>> {
        self.work.start_history()?;
        let mut current = self.head.clone();
        let mut result = Vec::new();
        let mut visited = BTreeSet::new();
        let mut expected = self.snapshot.epoch;
        while let Some(root) = current {
            if result.len() >= MAX_HISTORY || !visited.insert(root.directory.clone()) {
                return Err(invalid("database history is cyclic or exceeds its budget"));
            }
            let m = self.read_manifest(&root)?;
            if m.epoch != expected {
                return Err(invalid("noncontiguous database lineage"));
            }
            self.work.visit_manifest()?;
            expected = expected.checked_sub(1).ok_or(Error::Overflow)?;
            current = m.previous.clone();
            result.push(m);
        }
        if expected != 0 {
            return Err(invalid("truncated database lineage"));
        }
        result.reverse();
        Ok(result)
    }

    /// O(log H) lookup in the persisted receipt index; zero manifest-history traversal.
    pub fn receipt(&self, id: &str, fingerprint: &str) -> Result<Option<CommitReport>> {
        let Some(receipt) = self.receipts.get(id) else {
            return Ok(None);
        };
        if receipt.fingerprint != fingerprint {
            return Err(invalid("request identifier reused with different SQL"));
        }
        Ok(Some(receipt.report.clone()))
    }

    /// Read consolidated base-table deltas strictly after `after_epoch` through `CURRENT`.
    #[allow(dead_code)]
    pub fn table_deltas_since(
        &self,
        after_epoch: u64,
    ) -> Result<Vec<(u64, BTreeMap<String, RecordBatch>)>> {
        let mut current = self.head.clone();
        let mut collected = Vec::new();
        let mut expected = self.snapshot.epoch;
        while let Some(root) = current {
            if expected <= after_epoch {
                break;
            }
            let m = self.read_manifest(&root)?;
            if m.epoch != expected {
                return Err(invalid("noncontiguous database lineage"));
            }
            let deltas = m
                .table_deltas
                .iter()
                .map(|(n, p)| Ok((n.clone(), self.batch(p, PayloadKind::TableDelta)?)))
                .collect::<Result<BTreeMap<_, _>>>()?;
            collected.push((m.epoch, deltas));
            expected = expected.checked_sub(1).ok_or(Error::Overflow)?;
            current = m.previous.clone();
        }
        collected.reverse();
        Ok(collected)
    }

    /// Materialize every committed snapshot in epoch order, within the lineage budget.
    pub fn history(&self) -> Result<Vec<DatabaseSnapshot>> {
        self.manifests()?
            .iter()
            .map(|m| Ok(self.read_snapshot(m)?.snapshot))
            .collect()
    }

    fn write_block_relation(
        &self,
        stage: &mut CommitStage<'_>,
        prefix: &str,
        id: u64,
        rows: &TableRows,
        kind: PayloadKind,
    ) -> Result<BlockRelationEntry> {
        let schema = match self.committed_schemas.get(&id) {
            Some(existing) => existing.clone(),
            None => {
                let empty = RecordBatch::new_empty(rows.schema());
                let file = write_batch(stage.dir, &format!("schema-{prefix}-{id}.arrow"), &empty)?;
                if rows.block_count() == 0 {
                    // Empty relation creation writes only its schema file.
                    self.work
                        .payload(kind, PayloadAction::Write, file.bytes, 0)?;
                }
                Payload {
                    directory: stage.dir_name.to_owned(),
                    file,
                }
            }
        };
        stage.next_schemas.insert(id, schema.clone());

        let mut entries = Vec::with_capacity(rows.block_count());
        for (seq, block) in rows.iter_blocks().enumerate() {
            let uid = block.uid();
            let entry = if let Some(existing) = self.committed_blocks.get(&uid) {
                self.work.payload(
                    kind,
                    PayloadAction::Reuse,
                    existing.payload.file.bytes,
                    existing.row_count,
                )?;
                existing.clone()
            } else {
                let weighted = block.weighted_batch()?;
                let filename = format!("{prefix}-{id}-b-{seq}.arrow");
                let file = write_batch(stage.dir, &filename, &weighted)?;
                self.work
                    .payload(kind, PayloadAction::Write, file.bytes, block.row_count())?;
                BlockEntry {
                    min_key_sha256: block.min_key_sha256(),
                    row_count: block.row_count(),
                    distinct_entries: block.distinct_entries(),
                    payload: Payload {
                        directory: stage.dir_name.to_owned(),
                        file,
                    },
                }
            };
            stage.next_blocks.insert(uid, entry.clone());
            entries.push(entry);
        }

        let blocks = if entries.len() <= INLINE_BLOCK_LIMIT {
            BlockListRef::Inline(entries)
        } else {
            let block_count = entries.len();
            let file = write_bytes(
                stage.dir,
                &format!("blocks-{prefix}-{id}.json"),
                &json_bytes(&entries)?,
            )?;
            BlockListRef::External {
                block_count,
                payload: Payload {
                    directory: stage.dir_name.to_owned(),
                    file,
                },
            }
        };

        Ok(BlockRelationEntry {
            schema,
            order_mode: rows.order_mode().into(),
            blocks,
        })
    }

    /// Publish exactly the successor epoch, checking that `CURRENT` still matches.
    ///
    /// Unchanged blocks are reused by immutable `BlockUid` without flattening or
    /// rewriting. In-memory block/schema/receipt indexes update only after `CURRENT`
    /// and both publication barriers succeed.
    pub fn commit(
        &mut self,
        next: DatabaseSnapshot,
        receipt: Option<Receipt>,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<()> {
        if next.epoch != self.snapshot.epoch.checked_add(1).ok_or(Error::Overflow)? {
            return Err(invalid("database predecessor differs"));
        }
        if let Some(r) = &receipt {
            if self.receipts.contains_key(&r.id) {
                return Err(invalid("request identifier already committed"));
            }
        }
        let disk = match fs::read(self.root.join("CURRENT")) {
            Ok(bytes) => {
                self.work.pointer_read(bytes.len())?;
                Some(serde_json::from_slice::<Root>(&bytes)?)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        if json_bytes(&disk)? != json_bytes(&self.head)? {
            return Err(invalid("database CURRENT changed; reopen required"));
        }
        let (name, dir) = loop {
            let name = format!(
                "db-{}-{}-{}",
                next.epoch,
                std::process::id(),
                ATTEMPT.fetch_add(1, Ordering::Relaxed)
            );
            let dir = self.root.join(&name);
            match fs::create_dir(&dir) {
                Ok(()) => break (name, dir),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.into()),
            }
        };

        let write_payload =
            |kind: PayloadKind, filename: String, batch: &RecordBatch| -> Result<Payload> {
                let file = write_batch(&dir, &filename, batch)?;
                self.work
                    .payload(kind, PayloadAction::Write, file.bytes, batch.num_rows())?;
                Ok(Payload {
                    directory: name.clone(),
                    file,
                })
            };

        let mut stage = CommitStage {
            dir: &dir,
            dir_name: &name,
            next_blocks: BTreeMap::new(),
            next_schemas: BTreeMap::new(),
        };
        let mut tables = BTreeMap::new();
        for (n, t) in &next.tables {
            let reusable = match self.snapshot.tables.get(n) {
                Some(old) if old.id == t.id && old.rows.same_contents(&t.rows)? => self
                    .manifest
                    .as_ref()
                    .and_then(|manifest| manifest.tables.get(n)),
                _ => None,
            };
            let data = match reusable {
                Some(old) => {
                    let existing_entries = self.read_block_list(&old.data.blocks)?;
                    if existing_entries.is_empty() {
                        self.work.payload(
                            PayloadKind::Table,
                            PayloadAction::Reuse,
                            old.data.schema.file.bytes,
                            0,
                        )?;
                    } else {
                        for entry in &existing_entries {
                            self.work.payload(
                                PayloadKind::Table,
                                PayloadAction::Reuse,
                                entry.payload.file.bytes,
                                entry.row_count,
                            )?;
                        }
                    }
                    // Retain live block UIDs and schema mapping for future commits.
                    stage.next_schemas.insert(t.id, old.data.schema.clone());
                    for block in t.rows.iter_blocks() {
                        if let Some(committed) = self.committed_blocks.get(&block.uid()) {
                            stage.next_blocks.insert(block.uid(), committed.clone());
                        }
                    }
                    old.data.clone()
                }
                None => {
                    self.write_block_relation(&mut stage, "t", t.id, &t.rows, PayloadKind::Table)?
                }
            };
            tables.insert(n.clone(), TableEntry { id: t.id, data });
        }

        let mut views = BTreeMap::new();
        for (n, v) in &next.views {
            let reusable = match self.snapshot.views.get(n) {
                Some(old) if old.id == v.id && old.output.same_contents(&v.output)? => self
                    .manifest
                    .as_ref()
                    .and_then(|manifest| manifest.views.get(n)),
                _ => None,
            };
            let output = match reusable {
                Some(old) => {
                    match &old.output {
                        ViewOutputEntry::BlockRelation(entry) => {
                            let existing_entries = self.read_block_list(&entry.blocks)?;
                            if existing_entries.is_empty() {
                                self.work.payload(
                                    PayloadKind::ViewOutput,
                                    PayloadAction::Reuse,
                                    entry.schema.file.bytes,
                                    0,
                                )?;
                            } else {
                                for block in &existing_entries {
                                    self.work.payload(
                                        PayloadKind::ViewOutput,
                                        PayloadAction::Reuse,
                                        block.payload.file.bytes,
                                        block.row_count,
                                    )?;
                                }
                            }
                            stage.next_schemas.insert(v.id, entry.schema.clone());
                            if let ViewOutput::Blocks(rows) = &v.output {
                                for block in rows.iter_blocks() {
                                    if let Some(committed) = self.committed_blocks.get(&block.uid())
                                    {
                                        stage.next_blocks.insert(block.uid(), committed.clone());
                                    }
                                }
                            }
                        }
                        ViewOutputEntry::OrderedPayload(p) => {
                            self.work.payload(
                                PayloadKind::ViewOutput,
                                PayloadAction::Reuse,
                                p.file.bytes,
                                v.output.num_rows(),
                            )?;
                        }
                    }
                    old.output.clone()
                }
                None => match &v.output {
                    ViewOutput::Blocks(rows) => {
                        ViewOutputEntry::BlockRelation(self.write_block_relation(
                            &mut stage,
                            "v",
                            v.id,
                            rows,
                            PayloadKind::ViewOutput,
                        )?)
                    }
                    ViewOutput::Ordered(batch) => ViewOutputEntry::OrderedPayload(write_payload(
                        PayloadKind::ViewOutput,
                        format!("view-{}.arrow", v.id),
                        batch,
                    )?),
                },
            };
            views.insert(
                n.clone(),
                ViewEntry {
                    id: v.id,
                    sql: v.sql.clone(),
                    dependencies: v.dependencies.clone(),
                    output,
                },
            );
        }

        let table_deltas = next
            .table_deltas
            .iter()
            .map(|(n, b)| {
                let id = next
                    .tables
                    .get(n)
                    .ok_or_else(|| invalid("unbound base-table delta"))?
                    .id;
                Ok((
                    n.clone(),
                    write_payload(PayloadKind::TableDelta, format!("tdelta-{id}.arrow"), b)?,
                ))
            })
            .collect::<Result<_>>()?;

        let deltas = next
            .deltas
            .iter()
            .map(|(n, b)| {
                let id = next
                    .views
                    .get(n)
                    .ok_or_else(|| invalid("unbound output delta"))?
                    .id;
                Ok((
                    n.clone(),
                    write_payload(PayloadKind::OutputDelta, format!("delta-{id}.arrow"), b)?,
                ))
            })
            .collect::<Result<_>>()?;

        let mut next_receipts = self.receipts.clone();
        let receipt_index = if let Some(r) = &receipt {
            next_receipts.insert(r.id.clone(), r.clone());
            let file = write_bytes(&dir, "receipts.json", &json_bytes(&next_receipts)?)?;
            Some(Payload {
                directory: name.clone(),
                file,
            })
        } else {
            self.manifest
                .as_ref()
                .and_then(|manifest| manifest.receipt_index.clone())
        };

        let m = Manifest {
            format: FORMAT.into(),
            epoch: next.epoch,
            previous: self.head.clone(),
            next_id: next.next_id,
            tables,
            views,
            table_deltas,
            deltas,
            receipt_index,
            receipt,
        };
        let manifest = write_bytes(&dir, "manifest.json", &json_bytes(&m)?)?;
        self.work.manifest_write(manifest.bytes)?;
        sync_dir(&dir)?;
        sync_dir(&self.root)?;
        observer(PublishPoint::FilesDurable)?;
        let head = Root {
            directory: name.clone(),
            manifest,
        };
        let pointer = format!("CURRENT-{name}.tmp");
        let entry = write_bytes(&self.root, &pointer, &json_bytes(&head)?)?;
        self.work.pointer_write(entry.bytes)?;
        fs::rename(self.root.join(pointer), self.root.join("CURRENT"))?;
        sync_dir(&self.root)?;
        observer(PublishPoint::CurrentDurable)?;
        self.head = Some(head);
        self.manifest = Some(m);
        self.snapshot = next;
        self.committed_blocks = stage.next_blocks;
        self.committed_schemas = stage.next_schemas;
        self.receipts = next_receipts;
        Ok(())
    }
}
