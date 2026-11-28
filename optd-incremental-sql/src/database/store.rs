//! A database root is the sole authority for all tables and materialized views.
//!
//! Format `optd-sql-database/3` persists base tables and unordered materialized views
//! as immutable weighted blocks (`RowBlocks`), reusing untouched blocks across epochs
//! by `BlockUid`. New view blocks and output deltas share a synced per-view file.

mod receipt_index;
mod view_bundle;
use view_bundle::ViewBundle;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};

use self::receipt_index::ReceiptRoot;

use super::response::CommitReport;
use super::store_work::{PayloadAction, PayloadKind, StoreWork, StoreWorkCounter};
use super::table_rows::TableRows;
use crate::bag::{Bag, MAX_MATERIALIZED_BYTES};
use crate::error::invalid;
use crate::row_blocks::{BlockOrderMode, BlockUid, PersistedBlock};
use crate::state_codec::{self, StateReadStats, ViewStateSnapshot};
use crate::store::{
    batches_equal, component, json_bytes, read_batch, read_json_file, sync_dir, verified_file,
    write_batch, write_bytes, CheckpointDirectories, FileEntry, PublishPoint, MAX_JSON_BYTES,
};
use crate::{Error, Result};

const FORMAT: &str = "optd-sql-database/3";
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

/// History retention policy governing database compaction (`R07c`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Number of trailing committed epochs to retain in lineage metadata (must be >= 1).
    /// Compaction advances older live checkpoints before dropping their replay history.
    pub retain_epochs: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self { retain_epochs: 16 }
    }
}

/// Observed file and directory reclamation summary from one compaction pass (`R07c`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct CompactionReport {
    /// Committed database epoch at compaction time (unchanged by compaction).
    pub head_epoch: u64,
    /// Earliest retained manifest epoch after compaction (`1` if uncompacted or `0` if empty).
    pub retained_from_epoch: u64,
    /// Number of retained manifests in `retained_from_epoch..=head_epoch`.
    pub retained_manifests: u64,
    /// Unreachable individual files removed from partially live epoch directories.
    pub removed_files: u64,
    /// Unreachable epoch/orphan directories and stale state subdirectories removed.
    pub removed_dirs: u64,
    /// Total bytes reclaimed across removed files and directories.
    pub reclaimed_bytes: u64,
    /// Older checkpoints advanced by reusing unchanged operator state.
    pub promoted_checkpoints: u64,
    /// Older or missing checkpoints replaced with current committed state.
    pub written_checkpoints: u64,
    /// New state files written for those checkpoints, before writing the database manifest.
    pub checkpoint_bytes_written: u64,
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
    /// Byte position in a view bundle; the length and digest describe that segment.
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<u64>,
}

impl Payload {
    fn batch(&self, directory: &Path) -> Result<RecordBatch> {
        match self.offset {
            Some(offset) => view_bundle::read(directory, &self.file, offset),
            None => read_batch(directory, &self.file),
        }
    }
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
pub(super) struct ViewStateRef {
    pub checkpoint_epoch: u64,
    pub affected_commits: u64,
    pub plan_fingerprint: String,
    directory: String,
    state_subdir: String,
    operator_manifest: FileEntry,
    node_relations: Vec<Option<BlockRelationEntry>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    epoch: u64,
    previous: Option<Root>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retained_from_epoch: Option<u64>,
    next_id: u64,
    tables: BTreeMap<String, TableEntry>,
    views: BTreeMap<String, ViewEntry>,
    table_deltas: BTreeMap<String, Payload>,
    deltas: BTreeMap<String, Payload>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    view_states: BTreeMap<String, ViewStateRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt_index: Option<Payload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt_root: Option<ReceiptRoot>,
    // Legacy manifests retained an inline copy of their latest receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt: Option<Receipt>,
}

/// State to publish for a live view that would otherwise need pruned history.
pub(super) enum CompactionCheckpoint {
    Promote,
    Write(ViewStateSnapshot),
}

#[derive(Default)]
struct ReachablePayloads {
    files: BTreeMap<String, BTreeSet<String>>,
    state_dirs: BTreeMap<String, BTreeSet<String>>,
}

impl ReachablePayloads {
    fn mark(&mut self, payload: &Payload) {
        self.files
            .entry(payload.directory.clone())
            .or_default()
            .insert(payload.file.name.clone());
    }

    fn contains(&self, payload: &Payload) -> bool {
        self.files
            .get(&payload.directory)
            .is_some_and(|names| names.contains(&payload.file.name))
    }
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
    legacy_receipts: BTreeMap<String, Receipt>,
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
            legacy_receipts: BTreeMap::new(),
        };
        match fs::symlink_metadata(out.root.join("CURRENT")) {
            Ok(_) => {
                let bytes = read_json_file(&out.root.join("CURRENT"))?;
                out.work.pointer_read(bytes.len())?;
                let head: Root = serde_json::from_slice(&bytes)?;
                let m = out.read_manifest(&head)?;
                let loaded = out.read_snapshot(&m)?;
                if let Some(root) = &m.receipt_root {
                    root.validate_root(&out)?;
                }
                if let Some(payload) = &m.receipt_index {
                    out.legacy_receipts = out.read_legacy_receipts(payload)?;
                }
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
        if let Some(root) = &m.receipt_root {
            if m.receipt_index.is_some() || m.receipt.is_some() {
                return Err(invalid("database manifest mixes receipt index formats"));
            }
            root.validate_version()?;
        }
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
        if m.view_states.keys().any(|n| !m.views.contains_key(n)) {
            return Err(invalid(
                "persisted view state has no live materialized view",
            ));
        }
        Ok(m)
    }

    fn read_legacy_receipts(&self, payload: &Payload) -> Result<BTreeMap<String, Receipt>> {
        if payload.file.bytes > MAX_JSON_BYTES {
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
        self.work.receipt_read(payload.file.bytes)?;
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
        let batch = p.batch(&self.directory(&p.directory)?)?;
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

    /// Earliest retained manifest epoch (`0` for an empty database, `1` if uncompacted).
    pub fn retained_from_epoch(&self) -> u64 {
        match &self.manifest {
            None => 0,
            Some(m) => m.retained_from_epoch.unwrap_or(1),
        }
    }

    fn manifests_with_roots(&self) -> Result<Vec<(Root, Manifest)>> {
        self.read_lineage(
            self.head.clone(),
            self.snapshot.epoch,
            self.retained_from_epoch(),
        )
    }

    fn read_lineage(
        &self,
        mut current: Option<Root>,
        mut expected: u64,
        floor: u64,
    ) -> Result<Vec<(Root, Manifest)>> {
        self.work.start_history()?;
        if current.is_none() {
            return Ok(Vec::new());
        }
        if floor == 0 || floor > expected {
            return Err(invalid("invalid retained_from_epoch in database manifest"));
        }
        let mut result = Vec::new();
        let mut visited = BTreeSet::new();
        while let Some(root) = current {
            if result.len() >= MAX_HISTORY || !visited.insert(root.directory.clone()) {
                return Err(invalid("database history is cyclic or exceeds its budget"));
            }
            let m = self.read_manifest(&root)?;
            if m.epoch != expected {
                return Err(invalid("noncontiguous database lineage"));
            }
            self.work.visit_manifest()?;
            let prev = m.previous.clone();
            let hit_floor = m.epoch == floor;
            result.push((root, m));
            if hit_floor {
                if floor == 1 && prev.is_some() {
                    return Err(invalid("epoch 1 manifest must not reference a predecessor"));
                }
                expected = floor.checked_sub(1).ok_or(Error::Overflow)?;
                break;
            }
            expected = expected.checked_sub(1).ok_or(Error::Overflow)?;
            current = prev;
        }
        if expected != floor - 1 {
            return Err(invalid("truncated database lineage"));
        }
        result.reverse();
        Ok(result)
    }

    fn manifests(&self) -> Result<Vec<Manifest>> {
        Ok(self
            .manifests_with_roots()?
            .into_iter()
            .map(|(_, m)| m)
            .collect())
    }

    /// Resolve an exact receipt without traversing historical manifests.
    pub fn receipt(&self, id: &str, fingerprint: &str) -> Result<Option<CommitReport>> {
        let receipt = match self.manifest.as_ref().and_then(|m| m.receipt_root.as_ref()) {
            Some(root) => root.lookup(self, id)?,
            None => self.legacy_receipts.get(id).cloned(),
        };
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        if receipt.fingerprint != fingerprint {
            return Err(invalid("request identifier reused with different SQL"));
        }
        Ok(Some(receipt.report))
    }

    /// Return the latest persisted operator-state descriptor for a view, if any.
    pub fn view_state_ref(&self, name: &str) -> Option<&ViewStateRef> {
        self.manifest.as_ref()?.view_states.get(name)
    }

    /// Load and SHA-256-verify persisted operator state and intermediate relation blocks for a view.
    pub fn load_view_state(
        &mut self,
        name: &str,
        expected_fingerprint: &str,
    ) -> Result<(ViewStateSnapshot, StateReadStats)> {
        let state_ref = self
            .manifest
            .as_ref()
            .and_then(|m| m.view_states.get(name))
            .cloned()
            .ok_or_else(|| invalid("missing persisted operator state for view"))?;
        if state_ref.plan_fingerprint != expected_fingerprint {
            return Err(invalid(
                "persisted view state fingerprint differs from compiled plan",
            ));
        }
        if !component(&state_ref.state_subdir) || !state_ref.state_subdir.starts_with("states-v-") {
            return Err(invalid("invalid persisted view state subdirectory"));
        }
        let state_dir = self
            .directory(&state_ref.directory)?
            .join(&state_ref.state_subdir);
        let mut loaded = state_codec::read_operator_states(
            &state_dir,
            &state_ref.operator_manifest,
            expected_fingerprint,
        )?;
        let mut node_relations = Vec::with_capacity(state_ref.node_relations.len());
        for rel_entry in &state_ref.node_relations {
            let Some(rel_entry) = rel_entry else {
                node_relations.push(None);
                continue;
            };
            let schema_batch = read_batch(
                &self.directory(&rel_entry.schema.directory)?,
                &rel_entry.schema.file,
            )?;
            if schema_batch.num_rows() != 0 {
                return Err(invalid(
                    "persisted state relation schema must have zero rows",
                ));
            }
            let schema = schema_batch.schema();
            let entries = self.read_block_list(&rel_entry.blocks)?;
            let mut persisted = Vec::with_capacity(entries.len());
            for block in &entries {
                let weighted_batch = block
                    .payload
                    .batch(&self.directory(&block.payload.directory)?)?;
                loaded.stats.files = loaded.stats.files.checked_add(1).ok_or(Error::Overflow)?;
                loaded.stats.bytes = loaded
                    .stats
                    .bytes
                    .checked_add(block.payload.file.bytes)
                    .ok_or(Error::Overflow)?;
                loaded.stats.rows = loaded
                    .stats
                    .rows
                    .checked_add(block.row_count as u64)
                    .ok_or(Error::Overflow)?;
                persisted.push(PersistedBlock {
                    expected_min_key_sha256: block.min_key_sha256.clone(),
                    expected_row_count: block.row_count,
                    expected_distinct_entries: block.distinct_entries,
                    weighted_batch,
                });
            }
            let (rows, uids, _) =
                TableRows::from_persisted_blocks(schema, rel_entry.order_mode.into(), persisted)?;
            for (uid, block) in uids.into_iter().zip(entries) {
                self.committed_blocks.insert(uid, block);
            }
            node_relations.push(Some(rows));
        }
        Ok((
            ViewStateSnapshot {
                view_epoch: loaded.view_epoch,
                plan_fingerprint: loaded.plan_fingerprint,
                node_relations,
                operators: loaded.operators,
            },
            loaded.stats,
        ))
    }

    /// Read consolidated base-table deltas strictly after `after_epoch` through `CURRENT`.
    pub fn table_deltas_since(
        &self,
        after_epoch: u64,
        dependencies: &BTreeSet<String>,
    ) -> Result<Vec<(u64, BTreeMap<String, RecordBatch>)>> {
        if after_epoch < self.retained_from_epoch() || after_epoch > self.snapshot.epoch {
            return Err(invalid(
                "view checkpoint lies outside retained database history",
            ));
        }
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
                .filter(|(name, _)| dependencies.contains(*name))
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
        id: u64,
        rows: &TableRows,
        mut bundle: Option<&mut ViewBundle<'_>>,
    ) -> Result<BlockRelationEntry> {
        let (prefix, kind) = if bundle.is_some() {
            ("v", PayloadKind::ViewOutput)
        } else {
            ("t", PayloadKind::Table)
        };
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
                    offset: None,
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
                let payload = match bundle.as_deref_mut() {
                    Some(writer) => writer.append(&weighted, kind, block.row_count())?,
                    None => {
                        let file = write_batch(
                            stage.dir,
                            &format!("{prefix}-{id}-b-{seq}.arrow"),
                            &weighted,
                        )?;
                        self.work.payload(
                            kind,
                            PayloadAction::Write,
                            file.bytes,
                            block.row_count(),
                        )?;
                        Payload {
                            directory: stage.dir_name.to_owned(),
                            file,
                            offset: None,
                        }
                    }
                };
                BlockEntry {
                    min_key_sha256: block.min_key_sha256(),
                    row_count: block.row_count(),
                    distinct_entries: block.distinct_entries(),
                    payload,
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
                    offset: None,
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

    fn write_state_block_relation(
        &self,
        stage: &mut CommitStage<'_>,
        prefix: &str,
        rows: &TableRows,
    ) -> Result<BlockRelationEntry> {
        let empty = RecordBatch::new_empty(rows.schema());
        let schema_file = write_batch(stage.dir, &format!("schema-{prefix}.arrow"), &empty)?;
        let schema = Payload {
            offset: None,
            directory: stage.dir_name.to_owned(),
            file: schema_file,
        };
        let mut entries = Vec::with_capacity(rows.block_count());
        for (seq, block) in rows.iter_blocks().enumerate() {
            let uid = block.uid();
            let entry = if let Some(existing) = stage
                .next_blocks
                .get(&uid)
                .or_else(|| self.committed_blocks.get(&uid))
            {
                existing.clone()
            } else {
                let weighted = block.weighted_batch()?;
                let filename = format!("{prefix}-b-{seq}.arrow");
                let file = write_batch(stage.dir, &filename, &weighted)?;
                BlockEntry {
                    min_key_sha256: block.min_key_sha256(),
                    row_count: block.row_count(),
                    distinct_entries: block.distinct_entries(),
                    payload: Payload {
                        offset: None,
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
                &format!("blocks-{prefix}.json"),
                &json_bytes(&entries)?,
            )?;
            BlockListRef::External {
                block_count,
                payload: Payload {
                    offset: None,
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
        mut view_state_checkpoints: BTreeMap<String, ViewStateSnapshot>,
        receipt: Option<Receipt>,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<()> {
        if next.epoch != self.snapshot.epoch.checked_add(1).ok_or(Error::Overflow)? {
            return Err(invalid("database predecessor differs"));
        }
        self.verify_current()?;
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
                    offset: None,
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
                None => self.write_block_relation(&mut stage, t.id, &t.rows, None)?,
            };
            tables.insert(n.clone(), TableEntry { id: t.id, data });
        }

        let mut views = BTreeMap::new();
        let mut deltas = BTreeMap::new();
        for (n, v) in &next.views {
            let mut bundle = ViewBundle::new(&dir, &name, v.id);
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
                None => {
                    match &v.output {
                        ViewOutput::Blocks(rows) => ViewOutputEntry::BlockRelation(
                            self.write_block_relation(&mut stage, v.id, rows, Some(&mut bundle))?,
                        ),
                        ViewOutput::Ordered(batch) => ViewOutputEntry::OrderedPayload(
                            bundle.append(batch, PayloadKind::ViewOutput, batch.num_rows())?,
                        ),
                    }
                }
            };
            if let Some(delta) = next.deltas.get(n) {
                deltas.insert(
                    n.clone(),
                    bundle.append(delta, PayloadKind::OutputDelta, delta.num_rows())?,
                );
            }
            bundle.finish(&self.work)?;
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

        if deltas.len() != next.deltas.len() {
            return Err(invalid("unbound output delta"));
        }

        let mut view_states = BTreeMap::new();
        for (n, v) in &next.views {
            if let Some(snap) = view_state_checkpoints.remove(n) {
                view_states.insert(
                    n.clone(),
                    self.write_view_checkpoint(&mut stage, next.epoch, v.id, snap)?,
                );
            } else if let Some(prev_ref) = self
                .manifest
                .as_ref()
                .and_then(|manifest| manifest.view_states.get(n))
            {
                let mut carried = prev_ref.clone();
                if next.deltas.contains_key(n) {
                    carried.affected_commits = carried
                        .affected_commits
                        .checked_add(1)
                        .ok_or(Error::Overflow)?;
                }
                view_states.insert(n.clone(), carried);
            }
        }

        let prior_receipt_root = self.manifest.as_ref().and_then(|m| m.receipt_root.as_ref());
        let (receipt_index, receipt_root) = match receipt {
            Some(receipt) => (
                None,
                Some(receipt_index::Writer::new(self, &dir, &name).stage(
                    prior_receipt_root,
                    &self.legacy_receipts,
                    receipt,
                )?),
            ),
            None => (
                self.manifest.as_ref().and_then(|m| m.receipt_index.clone()),
                prior_receipt_root.cloned(),
            ),
        };

        let retained_from_epoch = self.manifest.as_ref().and_then(|m| m.retained_from_epoch);
        let m = Manifest {
            format: FORMAT.into(),
            epoch: next.epoch,
            previous: self.head.clone(),
            retained_from_epoch,
            next_id: next.next_id,
            tables,
            views,
            table_deltas,
            deltas,
            view_states,
            receipt_index,
            receipt_root,
            receipt: None,
        };
        let head = self.write_manifest(&dir, &name, &m)?;
        self.publish_head(&dir, &head, observer)?;
        self.head = Some(head);
        if m.receipt_root.is_some() {
            self.legacy_receipts.clear();
        }
        self.manifest = Some(m);
        self.snapshot = next;
        self.committed_blocks = stage.next_blocks;
        self.committed_schemas = stage.next_schemas;
        Ok(())
    }

    fn write_view_checkpoint(
        &self,
        stage: &mut CommitStage<'_>,
        epoch: u64,
        id: u64,
        snapshot: ViewStateSnapshot,
    ) -> Result<ViewStateRef> {
        let state_subdir = format!("states-v-{id}");
        let operator_manifest = state_codec::write_operator_states(
            &stage.dir.join(&state_subdir),
            snapshot.view_epoch,
            &snapshot.plan_fingerprint,
            &snapshot.operators,
        )?;
        let node_relations = snapshot
            .node_relations
            .iter()
            .enumerate()
            .map(|(node, relation)| {
                relation
                    .as_ref()
                    .map(|rows| {
                        self.write_state_block_relation(stage, &format!("s{id}-n{node}"), rows)
                    })
                    .transpose()
            })
            .collect::<Result<_>>()?;
        Ok(ViewStateRef {
            checkpoint_epoch: epoch,
            affected_commits: 0,
            plan_fingerprint: snapshot.plan_fingerprint,
            directory: stage.dir_name.to_owned(),
            state_subdir,
            operator_manifest,
            node_relations,
        })
    }

    fn verify_current(&self) -> Result<()> {
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
        Ok(())
    }

    fn write_manifest(&self, directory: &Path, name: &str, manifest: &Manifest) -> Result<Root> {
        let manifest = write_bytes(directory, "manifest.json", &json_bytes(manifest)?)?;
        self.work.manifest_write(manifest.bytes)?;
        Ok(Root {
            directory: name.to_owned(),
            manifest,
        })
    }

    fn publish_head(
        &self,
        directory: &Path,
        head: &Root,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<()> {
        sync_dir(directory)?;
        sync_dir(&self.root)?;
        observer(PublishPoint::FilesDurable)?;
        let pointer = format!("CURRENT-{}.tmp", head.directory);
        let entry = write_bytes(&self.root, &pointer, &json_bytes(head)?)?;
        self.work.pointer_write(entry.bytes)?;
        fs::rename(self.root.join(pointer), self.root.join("CURRENT"))?;
        sync_dir(&self.root)?;
        observer(PublishPoint::CurrentDurable)
    }

    /// Compute the requested floor without changing the committed root.
    pub(super) fn compaction_floor(&self, policy: RetentionPolicy) -> Result<u64> {
        if policy.retain_epochs == 0 {
            return Err(invalid("retain_epochs must be at least 1"));
        }
        Ok(self
            .snapshot
            .epoch
            .saturating_sub(policy.retain_epochs - 1)
            .max(self.retained_from_epoch()))
    }

    /// Publish new recovery points and retention together before sweeping unreachable files.
    pub(super) fn compact(
        &mut self,
        floor: u64,
        checkpoints: BTreeMap<String, CompactionCheckpoint>,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<CompactionReport> {
        self.verify_current()?;
        let mut report = CompactionReport {
            head_epoch: self.snapshot.epoch,
            retained_from_epoch: floor,
            ..CompactionReport::default()
        };
        let reachable = if floor > self.retained_from_epoch() || !checkpoints.is_empty() {
            self.compact_with_checkpoints(floor, checkpoints, &mut report, observer)?
        } else {
            self.compaction_reachability(self.manifests_with_roots()?, &mut report)?
        };

        (
            report.removed_files,
            report.removed_dirs,
            report.reclaimed_bytes,
        ) = sweep_unreachable(&self.root, &reachable.files, &reachable.state_dirs)?;
        self.committed_blocks
            .retain(|_, block| reachable.contains(&block.payload));
        self.committed_schemas
            .retain(|_, schema| reachable.contains(schema));
        Ok(report)
    }

    fn compact_with_checkpoints(
        &mut self,
        floor: u64,
        checkpoints: BTreeMap<String, CompactionCheckpoint>,
        report: &mut CompactionReport,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<ReachablePayloads> {
        let mut updated = self
            .manifest
            .clone()
            .ok_or_else(|| invalid("missing database for compaction"))?;
        let epoch = updated.epoch;
        let (name, dir) = loop {
            let name = format!(
                "db-{epoch}-c-{}-{}",
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
        let mut stage = CommitStage {
            dir: &dir,
            dir_name: &name,
            next_blocks: BTreeMap::new(),
            next_schemas: BTreeMap::new(),
        };
        for (view, checkpoint) in checkpoints {
            let definition = updated
                .views
                .get(&view)
                .ok_or_else(|| invalid("compaction has an unbound view"))?;
            match checkpoint {
                CompactionCheckpoint::Promote => {
                    let state = updated
                        .view_states
                        .get_mut(&view)
                        .filter(|state| state.affected_commits == 0)
                        .ok_or_else(|| {
                            invalid("compaction cannot promote a changed view checkpoint")
                        })?;
                    state.checkpoint_epoch = epoch;
                    report.promoted_checkpoints += 1;
                }
                CompactionCheckpoint::Write(snapshot) => {
                    let state =
                        self.write_view_checkpoint(&mut stage, epoch, definition.id, snapshot)?;
                    updated.view_states.insert(view, state);
                    report.written_checkpoints += 1;
                }
            }
        }
        if updated.views.keys().ne(updated.view_states.keys())
            || updated
                .view_states
                .values()
                .any(|state| state.checkpoint_epoch < floor)
        {
            return Err(invalid("compaction would prune a required view checkpoint"));
        }
        report.checkpoint_bytes_written = dir_size(&dir)?;
        updated.retained_from_epoch = Some(floor);
        if floor == epoch {
            updated.previous = None;
        }
        let head = self.write_manifest(&dir, &name, &updated)?;
        let reachable = self.compaction_reachability(
            self.read_lineage(Some(head.clone()), epoch, floor)?,
            report,
        )?;

        // Do not drop the predecessor's payloads until CURRENT points to a durable,
        // validated replacement. An observer error fences the owning session.
        self.publish_head(&dir, &head, observer)?;
        self.head = Some(head);
        self.manifest = Some(updated);
        self.committed_blocks.extend(stage.next_blocks);
        Ok(reachable)
    }

    fn compaction_reachability(
        &self,
        retained: Vec<(Root, Manifest)>,
        report: &mut CompactionReport,
    ) -> Result<ReachablePayloads> {
        report.retained_manifests = retained.len() as u64;
        let mut reachable = ReachablePayloads::default();
        let mut receipt_reachability = receipt_index::Reachability::default();
        for (root, manifest) in retained {
            reachable.mark(&Payload {
                offset: None,
                directory: root.directory,
                file: root.manifest,
            });
            for table in manifest.tables.values() {
                self.mark_block_relation(&table.data, &mut reachable.files)?;
            }
            for view in manifest.views.values() {
                match &view.output {
                    ViewOutputEntry::BlockRelation(relation) => {
                        self.mark_block_relation(relation, &mut reachable.files)?;
                    }
                    ViewOutputEntry::OrderedPayload(payload) => reachable.mark(payload),
                }
            }
            for delta in manifest
                .table_deltas
                .values()
                .chain(manifest.deltas.values())
            {
                reachable.mark(delta);
            }
            if let Some(payload) = &manifest.receipt_index {
                reachable.mark(payload);
            }
            if let Some(root) = &manifest.receipt_root {
                receipt_reachability.mark(self, root, &mut reachable.files)?;
            }
            for state in manifest.view_states.values() {
                reachable
                    .state_dirs
                    .entry(state.directory.clone())
                    .or_default()
                    .insert(state.state_subdir.clone());
                for relation in state.node_relations.iter().flatten() {
                    self.mark_block_relation(relation, &mut reachable.files)?;
                }
            }
        }
        Ok(reachable)
    }

    fn mark_block_relation(
        &self,
        rel: &BlockRelationEntry,
        reachable_files: &mut BTreeMap<String, BTreeSet<String>>,
    ) -> Result<()> {
        reachable_files
            .entry(rel.schema.directory.clone())
            .or_default()
            .insert(rel.schema.file.name.clone());
        if let BlockListRef::External { payload, .. } = &rel.blocks {
            reachable_files
                .entry(payload.directory.clone())
                .or_default()
                .insert(payload.file.name.clone());
        }
        for block in self.read_block_list(&rel.blocks)? {
            reachable_files
                .entry(block.payload.directory)
                .or_default()
                .insert(block.payload.file.name);
        }
        Ok(())
    }
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        let bytes = if meta.is_dir() {
            dir_size(&entry.path())?
        } else {
            meta.len()
        };
        total = total.checked_add(bytes).ok_or(Error::Overflow)?;
    }
    Ok(total)
}

fn sweep_unreachable(
    root: &Path,
    reachable_files: &BTreeMap<String, BTreeSet<String>>,
    reachable_subdirs: &BTreeMap<String, BTreeSet<String>>,
) -> Result<(u64, u64, u64)> {
    let mut removed_files = 0u64;
    let mut removed_dirs = 0u64;
    let mut reclaimed_bytes = 0u64;
    let mut touched_root = false;

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_dir() {
            continue;
        }
        let Ok(dir_name) = entry.file_name().into_string() else {
            continue;
        };
        if !dir_name.starts_with("db-") {
            continue;
        }
        let live_files = reachable_files.get(&dir_name);
        let live_subdirs = reachable_subdirs.get(&dir_name);
        let dir_path = entry.path();
        if live_files.is_none() && live_subdirs.is_none() {
            let bytes = dir_size(&dir_path)?;
            fs::remove_dir_all(&dir_path)?;
            removed_dirs = removed_dirs.checked_add(1).ok_or(Error::Overflow)?;
            reclaimed_bytes = reclaimed_bytes.checked_add(bytes).ok_or(Error::Overflow)?;
            touched_root = true;
            continue;
        }
        let mut touched_dir = false;
        for child in fs::read_dir(&dir_path)? {
            let child = child?;
            let Ok(child_name) = child.file_name().into_string() else {
                continue;
            };
            let child_path = child.path();
            if child.file_type()?.is_dir() {
                let keep = live_subdirs.is_some_and(|set| set.contains(&child_name));
                if !keep {
                    let bytes = dir_size(&child_path)?;
                    fs::remove_dir_all(&child_path)?;
                    removed_dirs = removed_dirs.checked_add(1).ok_or(Error::Overflow)?;
                    reclaimed_bytes = reclaimed_bytes.checked_add(bytes).ok_or(Error::Overflow)?;
                    touched_dir = true;
                }
            } else {
                let keep = live_files.is_some_and(|set| set.contains(&child_name));
                if !keep {
                    let bytes = child.metadata()?.len();
                    fs::remove_file(&child_path)?;
                    removed_files = removed_files.checked_add(1).ok_or(Error::Overflow)?;
                    reclaimed_bytes = reclaimed_bytes.checked_add(bytes).ok_or(Error::Overflow)?;
                    touched_dir = true;
                }
            }
        }
        if touched_dir {
            sync_dir(&dir_path)?;
        }
    }
    if touched_root {
        sync_dir(root)?;
    }
    Ok((removed_files, removed_dirs, reclaimed_bytes))
}
