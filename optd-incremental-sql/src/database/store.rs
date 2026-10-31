//! A database root is the sole authority for all tables and materialized views.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};

use super::response::CommitReport;
use crate::bag::{Bag, MAX_MATERIALIZED_BYTES};
use crate::error::invalid;
use crate::store::{
    batches_equal, component, json_bytes, read_batch, read_json_file, sync_dir, verified_file,
    write_batch, write_bytes, CheckpointDirectories, FileEntry, PublishPoint,
};
use crate::{Error, Result};

const FORMAT: &str = "optd-sql-database/1";
const MAX_HISTORY: usize = 100_000;
static ATTEMPT: AtomicU64 = AtomicU64::new(0);

/// Base-table identity and contents at one database epoch.
#[derive(Clone)]
pub(super) struct TableData {
    pub id: u64,
    pub batch: RecordBatch,
}

/// Persisted view definition and output; execution indexes are rebuilt on open.
#[derive(Clone)]
pub(super) struct ViewData {
    pub id: u64,
    pub sql: String,
    pub dependencies: BTreeSet<String>,
    pub output: RecordBatch,
}

/// Tables, view outputs and output deltas published together under one root.
///
/// Deltas belong only to this epoch. Cloning shares Arrow buffers while isolating
/// catalog mutations; it does not eliminate later table scans or materialization.
#[derive(Clone, Default)]
pub(super) struct DatabaseSnapshot {
    pub epoch: u64,
    pub next_id: u64,
    pub tables: BTreeMap<String, TableData>,
    pub views: BTreeMap<String, ViewData>,
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableEntry {
    id: u64,
    data: Payload,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ViewEntry {
    id: u64,
    sql: String,
    dependencies: BTreeSet<String>,
    output: Payload,
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
    deltas: BTreeMap<String, Payload>,
    receipt: Option<Receipt>,
}

/// Exclusive host-local writer; `CURRENT` alone determines committed visibility.
/// Unreferenced attempt directories are never treated as committed epochs.
pub(super) struct DatabaseStore {
    root: PathBuf,
    _lock: File,
    head: Option<Root>,
    manifest: Option<Manifest>,
    snapshot: DatabaseSnapshot,
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
            head: None,
            manifest: None,
            snapshot: DatabaseSnapshot {
                next_id: 1,
                ..Default::default()
            },
        };
        match fs::symlink_metadata(out.root.join("CURRENT")) {
            Ok(_) => {
                let head: Root =
                    serde_json::from_slice(&read_json_file(&out.root.join("CURRENT"))?)?;
                let m = out.read_manifest(&head)?;
                out.snapshot = out.read_snapshot(&m)?;
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

    /// Clone catalog state while retaining shared immutable Arrow buffers.
    pub fn snapshot(&self) -> DatabaseSnapshot {
        self.snapshot.clone()
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
        let m: Manifest = serde_json::from_reader(file)?;
        if m.format != FORMAT || m.epoch == 0 || m.next_id == 0 {
            return Err(invalid("invalid database format or epoch"));
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
        Ok(m)
    }

    fn batch(&self, p: &Payload) -> Result<RecordBatch> {
        if p.file.bytes > MAX_MATERIALIZED_BYTES as u64 {
            return Err(invalid("database IPC payload exceeds byte budget"));
        }
        let batch = read_batch(&self.directory(&p.directory)?, &p.file)?;
        Bag::from_batch(&batch)?;
        Ok(batch)
    }

    fn read_snapshot(&self, m: &Manifest) -> Result<DatabaseSnapshot> {
        Ok(DatabaseSnapshot {
            epoch: m.epoch,
            next_id: m.next_id,
            tables: m
                .tables
                .iter()
                .map(|(n, t)| {
                    Ok((
                        n.clone(),
                        TableData {
                            id: t.id,
                            batch: self.batch(&t.data)?,
                        },
                    ))
                })
                .collect::<Result<_>>()?,
            views: m
                .views
                .iter()
                .map(|(n, v)| {
                    Ok((
                        n.clone(),
                        ViewData {
                            id: v.id,
                            sql: v.sql.clone(),
                            dependencies: v.dependencies.clone(),
                            output: self.batch(&v.output)?,
                        },
                    ))
                })
                .collect::<Result<_>>()?,
            deltas: m
                .deltas
                .iter()
                .map(|(n, p)| Ok((n.clone(), self.batch(p)?)))
                .collect::<Result<_>>()?,
        })
    }

    fn manifests(&self) -> Result<Vec<Manifest>> {
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

    /// Search committed metadata for an exact retry; reject ID reuse with other SQL.
    pub fn receipt(&self, id: &str, fingerprint: &str) -> Result<Option<CommitReport>> {
        for m in self.manifests()?.into_iter().rev() {
            if let Some(r) = m.receipt {
                if r.id == id {
                    if r.fingerprint != fingerprint {
                        return Err(invalid("request identifier reused with different SQL"));
                    }
                    return Ok(Some(r.report));
                }
            }
        }
        Ok(None)
    }

    /// Materialize every committed snapshot in epoch order, within the lineage budget.
    pub fn history(&self) -> Result<Vec<DatabaseSnapshot>> {
        self.manifests()?
            .iter()
            .map(|m| self.read_snapshot(m))
            .collect()
    }

    /// Publish exactly the successor epoch, checking that `CURRENT` still matches.
    ///
    /// Payloads and manifest become durable before atomic pointer replacement;
    /// observers run after the files barrier and after the pointer durability barrier.
    /// The in-memory head advances only after both callbacks succeed.
    ///
    /// An IO or observer error can occur after `CURRENT` was replaced. Failure
    /// therefore does not imply rollback: the caller must fence the session and
    /// reopen before further use, even if this store's snapshot still looks old.
    pub fn commit(
        &mut self,
        next: DatabaseSnapshot,
        receipt: Option<Receipt>,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<()> {
        if next.epoch != self.snapshot.epoch.checked_add(1).ok_or(Error::Overflow)? {
            return Err(invalid("database predecessor differs"));
        }
        let disk = match fs::read(self.root.join("CURRENT")) {
            Ok(bytes) => Some(serde_json::from_slice::<Root>(&bytes)?),
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
        let payload = |filename: String, batch: &RecordBatch| -> Result<Payload> {
            Ok(Payload {
                directory: name.clone(),
                file: write_batch(&dir, &filename, batch)?,
            })
        };
        let mut tables = BTreeMap::new();
        for (n, t) in &next.tables {
            let reusable = self
                .snapshot
                .tables
                .get(n)
                .filter(|old| old.id == t.id && batches_equal(&old.batch, &t.batch))
                .and_then(|_| self.manifest.as_ref()?.tables.get(n));
            let data = match reusable {
                Some(old) => old.data.clone(),
                None => payload(format!("table-{}.arrow", t.id), &t.batch)?,
            };
            tables.insert(n.clone(), TableEntry { id: t.id, data });
        }
        let mut views = BTreeMap::new();
        for (n, v) in &next.views {
            let reusable = self
                .snapshot
                .views
                .get(n)
                .filter(|old| old.id == v.id && batches_equal(&old.output, &v.output))
                .and_then(|_| self.manifest.as_ref()?.views.get(n));
            let output = match reusable {
                Some(old) => old.output.clone(),
                None => payload(format!("view-{}.arrow", v.id), &v.output)?,
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
        let deltas = next
            .deltas
            .iter()
            .map(|(n, b)| {
                let id = next
                    .views
                    .get(n)
                    .ok_or_else(|| invalid("unbound output delta"))?
                    .id;
                Ok((n.clone(), payload(format!("delta-{id}.arrow"), b)?))
            })
            .collect::<Result<_>>()?;
        let m = Manifest {
            format: FORMAT.into(),
            epoch: next.epoch,
            previous: self.head.clone(),
            next_id: next.next_id,
            tables,
            views,
            deltas,
            receipt,
        };
        let manifest = write_bytes(&dir, "manifest.json", &json_bytes(&m)?)?;
        sync_dir(&dir)?;
        sync_dir(&self.root)?;
        observer(PublishPoint::FilesDurable)?;
        let head = Root {
            directory: name.clone(),
            manifest,
        };
        let pointer = format!("CURRENT-{name}.tmp");
        write_bytes(&self.root, &pointer, &json_bytes(&head)?)?;
        fs::rename(self.root.join(pointer), self.root.join("CURRENT"))?;
        sync_dir(&self.root)?;
        observer(PublishPoint::CurrentDurable)?;
        self.head = Some(head);
        self.manifest = Some(m);
        self.snapshot = next;
        Ok(())
    }
}
