//! Durable, single-host checkpoints for typed multi-table SQL epochs.
//!
//! `CURRENT` is the sole commit authority. Every publication uses a newly
//! reserved directory, so an interrupted attempt is retained without being
//! mistaken for committed state or overwritten by a retry.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::source_vector::{valid_digest, PinnedSourceVector};
use crate::{Error, Result};

const FORMAT: &str = "optd-incremental-sql-checkpoint/1";
const SOURCE_FORMAT: &str = "optd-incremental-sql-checkpoint/2";
const MAX_JSON_BYTES: u64 = 16 * 1024 * 1024;
static NEXT_ATTEMPT: AtomicU64 = AtomicU64::new(0);

/// All source and output state committed at one logical epoch.
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// The original view definition, unchanged for the lifetime of this store.
    pub sql: String,
    /// A logical position; every successor advances by exactly one.
    pub epoch: u64,
    /// Complete typed source relations, keyed by their registered SQL names.
    pub tables: BTreeMap<String, RecordBatch>,
    /// The complete maintained output, including its presentation order.
    pub output: RecordBatch,
    /// The typed, signed output changes for this epoch.
    pub delta: RecordBatch,
}

/// A format-2 epoch whose complete input tuple shares its commit authority.
#[derive(Clone, Debug)]
pub struct SourceSnapshot {
    /// Complete typed source/output state at this epoch.
    pub snapshot: Snapshot,
    /// Exact external endpoints represented by the source IPC files.
    pub vector: PinnedSourceVector,
}

/// Exact predecessor required for source-aware publication or retry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourcePosition {
    /// Committed logical epoch.
    pub epoch: u64,
    /// Digest of the committed canonical source-vector file.
    pub vector_digest: String,
}

impl SourceSnapshot {
    /// Identify this epoch and its complete source tuple.
    pub fn position(&self) -> Result<SourcePosition> {
        Ok(SourcePosition {
            epoch: self.snapshot.epoch,
            vector_digest: self.vector.digest()?,
        })
    }
}

/// Instrumentation at the two actual durability boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishPoint {
    /// Immutable IPC files and their manifest are durable; CURRENT is unchanged.
    FilesDurable,
    /// CURRENT and its containing directory are durable; the epoch is committed.
    CurrentDurable,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileEntry {
    pub(crate) name: String,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    sql: String,
    epoch: u64,
    previous_epoch: Option<u64>,
    previous: Option<Current>,
    tables: BTreeMap<String, FileEntry>,
    output: FileEntry,
    delta: FileEntry,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_vector: Option<FileEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous_vector_digest: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Current {
    format: String,
    directory: String,
    manifest: FileEntry,
}

struct Publication<'a> {
    expected: Option<u64>,
    snapshot: &'a Snapshot,
    previous: Option<Current>,
    vector: Option<&'a PinnedSourceVector>,
    previous_vector_digest: Option<String>,
}

/// An exclusively owned checkpoint store on a durable local filesystem.
///
/// The OS lock is held until this object is dropped, including during callbacks.
/// The mutex additionally serializes calls through the same object; a file lock
/// alone would not prevent two threads from publishing against one predecessor.
pub struct CheckpointStore {
    root: PathBuf,
    _writer_lock: File,
    serial: Mutex<()>,
}

impl CheckpointStore {
    /// Open/create a store and acquire its nonblocking host-local writer lock.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_sync(path.as_ref(), sync_dir)
    }

    fn open_with_sync(path: &Path, synchronize: impl FnMut(&Path) -> Result<()>) -> Result<Self> {
        let directories = CheckpointDirectories::create(path)?;
        let root = directories.root.clone();
        let writer_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("WRITER.lock"))?;
        writer_lock
            .try_lock()
            .map_err(|_| invalid("checkpoint store already has a writer"))?;
        // A directory left by an interrupted open may exist without a durable
        // parent entry. Persist the complete canonical chain on every open.
        directories.synchronize(synchronize)?;
        Ok(Self {
            root,
            _writer_lock: writer_lock,
            serial: Mutex::new(()),
        })
    }

    /// Load and verify the committed epoch, ignoring every unreferenced attempt.
    pub fn load(&self) -> Result<Option<Snapshot>> {
        let _serial = self.serial()?;
        Ok(self.read_committed()?.map(|(_, _, snapshot)| snapshot))
    }

    /// Read the exact committed lineage from bootstrap through CURRENT.
    ///
    /// Each predecessor is bound by its manifest digest, not by directory names
    /// or epoch-number scans. Unpublished attempts never enter the change log.
    pub fn history(&self) -> Result<Vec<Snapshot>> {
        let _serial = self.serial()?;
        let mut pointer = self.read_current()?;
        let mut visited = HashSet::new();
        let mut history = Vec::new();
        while let Some(current) = pointer {
            if !visited.insert(current.directory.clone()) {
                return Err(invalid("checkpoint history contains a cycle"));
            }
            let (manifest, snapshot) = self.read_epoch(&current)?;
            pointer = manifest.previous;
            history.push(snapshot);
        }
        history.reverse();
        if history.first().is_some_and(|snapshot| snapshot.epoch != 0) {
            return Err(invalid("checkpoint history does not start at epoch zero"));
        }
        for adjacent in history.windows(2) {
            if adjacent[0].epoch.checked_add(1) != Some(adjacent[1].epoch) {
                return Err(invalid("checkpoint history skips or repeats an epoch"));
            }
            validate_definition(&adjacent[0], &adjacent[1])?;
        }
        Ok(history)
    }

    /// Load a source-aware epoch and validate its complete immutable lineage.
    ///
    /// A nonempty format-1 store is rejected rather than adopted implicitly.
    /// Source IO is deliberately absent; strict online writer restart must also
    /// revalidate retained external endpoints through the source adapter.
    pub fn load_source(&self) -> Result<Option<SourceSnapshot>> {
        let _serial = self.serial()?;
        Ok(self
            .read_source_lineage()?
            .pop()
            .map(|(_, _, snapshot)| snapshot))
    }

    /// Read all source-aware epochs, verifying bindings, schemas and cursors.
    pub fn history_source(&self) -> Result<Vec<SourceSnapshot>> {
        let _serial = self.serial()?;
        Ok(self
            .read_source_lineage()?
            .into_iter()
            .map(|(_, _, snapshot)| snapshot)
            .collect())
    }

    /// Atomically publish a typed snapshot together with its exact source tuple.
    pub fn commit_source(
        &self,
        expected: Option<&SourcePosition>,
        candidate: &SourceSnapshot,
    ) -> Result<()> {
        self.commit_source_with_observer(expected, candidate, |_| Ok(()))
    }

    /// Publish a source-aware epoch at the same durability barriers as format 1.
    ///
    /// Lost-response retries must match the complete vector, payload and exact
    /// predecessor. A different vector with identical output is a conflict.
    /// Callbacks must not recursively call this store while it holds its lock.
    pub fn commit_source_with_observer(
        &self,
        expected: Option<&SourcePosition>,
        candidate: &SourceSnapshot,
        mut callback: impl FnMut(PublishPoint) -> Result<()>,
    ) -> Result<()> {
        self.commit_source_with_sync(expected, candidate, &mut callback, &mut sync_dir)
    }

    fn commit_source_with_sync(
        &self,
        expected: Option<&SourcePosition>,
        candidate: &SourceSnapshot,
        callback: &mut impl FnMut(PublishPoint) -> Result<()>,
        synchronize: &mut impl FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        let _serial = self.serial()?;
        candidate.vector.validate_for(&candidate.snapshot.tables)?;
        if expected.is_some_and(|position| !valid_digest(&position.vector_digest)) {
            return Err(invalid("invalid expected source-vector digest"));
        }
        let existing = self.read_source_lineage()?.pop();
        if let Some((_, manifest, current)) = &existing {
            if current.snapshot.epoch == candidate.snapshot.epoch {
                if manifest.previous_epoch == expected.map(|position| position.epoch)
                    && manifest.previous_vector_digest.as_deref()
                        == expected.map(|position| position.vector_digest.as_str())
                    && snapshots_equal(&current.snapshot, &candidate.snapshot)
                    && current.vector == candidate.vector
                {
                    // A prior rename may be visible after its parent fsync
                    // failed. Identical bytes alone do not prove durability.
                    synchronize(&self.root)?;
                    return Ok(());
                }
                return Err(invalid(
                    "conflicting retry of an already committed source epoch",
                ));
            }
        }
        let actual = existing
            .as_ref()
            .map(|(_, _, current)| current.position())
            .transpose()?;
        if actual.as_ref() != expected {
            return Err(invalid(
                "source checkpoint predecessor does not match CURRENT",
            ));
        }
        let previous_epoch = expected.map(|position| position.epoch);
        validate_epoch(previous_epoch, &candidate.snapshot)?;
        if let Some((_, _, current)) = &existing {
            validate_definition(&current.snapshot, &candidate.snapshot)?;
            candidate.vector.validate_successor(&current.vector)?;
        }
        self.publish_epoch(
            Publication {
                expected: previous_epoch,
                snapshot: &candidate.snapshot,
                previous: existing.map(|(current, _, _)| current),
                vector: Some(&candidate.vector),
                previous_vector_digest: expected.map(|position| position.vector_digest.clone()),
            },
            callback,
            synchronize,
        )
    }

    /// Publish against an exact predecessor, or resolve an identical retry.
    pub fn commit(&self, expected: Option<u64>, snapshot: &Snapshot) -> Result<()> {
        self.commit_with_observer(expected, snapshot, |_| Ok(()))
    }

    /// Publish with instrumentation suitable for actual process-death tests.
    ///
    /// An observer failure at `CurrentDurable` leaves a committed epoch. Resolve
    /// that lost response by loading or retrying the exact same candidate.
    /// Callbacks must not recursively call this store: publication owns its lock.
    pub fn commit_with_observer(
        &self,
        expected: Option<u64>,
        snapshot: &Snapshot,
        mut callback: impl FnMut(PublishPoint) -> Result<()>,
    ) -> Result<()> {
        self.commit_with_sync(expected, snapshot, &mut callback, &mut sync_dir)
    }

    fn commit_with_sync(
        &self,
        expected: Option<u64>,
        snapshot: &Snapshot,
        callback: &mut impl FnMut(PublishPoint) -> Result<()>,
        synchronize: &mut impl FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        let _serial = self.serial()?;
        let existing = self.read_committed()?;
        if let Some((_, manifest, current)) = &existing {
            if current.epoch == snapshot.epoch {
                if manifest.previous_epoch == expected && snapshots_equal(current, snapshot) {
                    synchronize(&self.root)?;
                    return Ok(());
                }
                return Err(invalid("conflicting retry of an already committed epoch"));
            }
        }
        let actual = existing.as_ref().map(|(_, _, current)| current.epoch);
        if actual != expected {
            return Err(invalid("checkpoint predecessor does not match CURRENT"));
        }
        validate_epoch(expected, snapshot)?;
        if let Some((_, _, current)) = &existing {
            validate_definition(current, snapshot)?;
        }

        self.publish_epoch(
            Publication {
                expected,
                snapshot,
                previous: existing.map(|(current, _, _)| current),
                vector: None,
                previous_vector_digest: None,
            },
            callback,
            synchronize,
        )
    }

    fn publish_epoch(
        &self,
        publication: Publication<'_>,
        callback: &mut impl FnMut(PublishPoint) -> Result<()>,
        synchronize: &mut impl FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        let Publication {
            expected,
            snapshot,
            previous,
            vector,
            previous_vector_digest,
        } = publication;
        let (name, directory) = self.reserve_attempt(snapshot.epoch)?;
        let mut tables = BTreeMap::new();
        for (index, (table, batch)) in snapshot.tables.iter().enumerate() {
            let entry = write_batch(&directory, &format!("table-{index}.arrow"), batch)?;
            tables.insert(table.clone(), entry);
        }
        let output = write_batch(&directory, "output.arrow", &snapshot.output)?;
        let delta = write_batch(&directory, "delta.arrow", &snapshot.delta)?;
        let source_vector = vector
            .map(|vector| {
                let bytes = vector.canonical_bytes()?;
                if bytes.len() as u64 > MAX_JSON_BYTES {
                    return Err(invalid("oversized source-vector JSON file"));
                }
                write_bytes(&directory, "source-vector.json", &bytes)
            })
            .transpose()?;
        let format = if vector.is_some() {
            SOURCE_FORMAT
        } else {
            FORMAT
        };
        let manifest = Manifest {
            format: format.into(),
            sql: snapshot.sql.clone(),
            epoch: snapshot.epoch,
            previous_epoch: expected,
            previous,
            tables,
            output,
            delta,
            source_vector,
            previous_vector_digest,
        };
        let manifest_bytes = json_bytes(&manifest)?;
        let manifest_entry = write_bytes(&directory, "manifest.json", &manifest_bytes)?;
        synchronize(&directory)?;
        // Persist the attempt's directory entry before a pointer can name it.
        synchronize(&self.root)?;
        callback(PublishPoint::FilesDurable)?;

        let current = Current {
            format: format.into(),
            directory: name.clone(),
            manifest: manifest_entry,
        };
        let pointer_name = format!("CURRENT-{name}.tmp");
        write_bytes(&self.root, &pointer_name, &json_bytes(&current)?)?;
        fs::rename(self.root.join(&pointer_name), self.root.join("CURRENT"))?;
        // Atomic visibility is not durability: sync CURRENT's direct parent
        // after rename, and only then allow the caller to acknowledge memory.
        synchronize(&self.root)?;
        callback(PublishPoint::CurrentDurable)
    }

    fn serial(&self) -> Result<MutexGuard<'_, ()>> {
        self.serial
            .lock()
            .map_err(|_| invalid("checkpoint store publication lock was poisoned"))
    }

    fn reserve_attempt(&self, epoch: u64) -> Result<(String, PathBuf)> {
        loop {
            let sequence = NEXT_ATTEMPT.fetch_add(1, Ordering::Relaxed);
            let name = format!("epoch-{epoch}-{}-{sequence}", std::process::id());
            let path = self.root.join(&name);
            match fs::create_dir(&path) {
                Ok(()) => return Ok((name, path)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn read_committed(&self) -> Result<Option<(Current, Manifest, Snapshot)>> {
        let Some(current) = self.read_current()? else {
            return Ok(None);
        };
        let (manifest, snapshot) = self.read_epoch(&current)?;
        Ok(Some((current, manifest, snapshot)))
    }

    fn read_current(&self) -> Result<Option<Current>> {
        let current_path = self.root.join("CURRENT");
        let bytes = match read_json_file(&current_path) {
            Ok(bytes) => bytes,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let current: Current = parse_json(&bytes)?;
        Ok(Some(current))
    }

    fn read_epoch(&self, current: &Current) -> Result<(Manifest, Snapshot)> {
        let (manifest, snapshot, _) = self.read_epoch_mode(current, false)?;
        Ok((manifest, snapshot))
    }

    fn read_source_lineage(&self) -> Result<Vec<(Current, Manifest, SourceSnapshot)>> {
        let mut pointer = self.read_current()?;
        let mut visited = HashSet::new();
        let mut lineage = Vec::new();
        while let Some(current) = pointer {
            if !visited.insert(current.directory.clone()) {
                return Err(invalid("source checkpoint history contains a cycle"));
            }
            let (manifest, snapshot, vector) = self.read_epoch_mode(&current, true)?;
            let vector = vector.ok_or_else(|| invalid("source checkpoint lacks its vector"))?;
            pointer = manifest.previous.clone();
            lineage.push((current, manifest, SourceSnapshot { snapshot, vector }));
        }
        lineage.reverse();
        if lineage
            .first()
            .is_some_and(|(_, _, state)| state.snapshot.epoch != 0)
        {
            return Err(invalid(
                "source checkpoint history does not start at epoch zero",
            ));
        }
        for adjacent in lineage.windows(2) {
            let (_, _, before) = &adjacent[0];
            let (_, manifest, after) = &adjacent[1];
            if before.snapshot.epoch.checked_add(1) != Some(after.snapshot.epoch)
                || manifest.previous_vector_digest.as_deref()
                    != Some(before.vector.digest()?.as_str())
            {
                return Err(invalid(
                    "source checkpoint history has an invalid predecessor",
                ));
            }
            validate_definition(&before.snapshot, &after.snapshot)?;
            after.vector.validate_successor(&before.vector)?;
        }
        Ok(lineage)
    }

    fn read_epoch_mode(
        &self,
        current: &Current,
        source: bool,
    ) -> Result<(Manifest, Snapshot, Option<PinnedSourceVector>)> {
        let format = if source { SOURCE_FORMAT } else { FORMAT };
        if current.format != format || !component(&current.directory) {
            return Err(invalid("invalid CURRENT version or directory"));
        }
        let directory = self.root.join(&current.directory);
        if !fs::symlink_metadata(&directory)?.file_type().is_dir() {
            return Err(invalid("CURRENT does not identify an immutable directory"));
        }
        if current.manifest.name != "manifest.json" || current.manifest.bytes > MAX_JSON_BYTES {
            return Err(invalid("invalid manifest entry"));
        }
        let mut file = verified_file(&directory, &current.manifest)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let manifest: Manifest = parse_json(&bytes)?;
        if manifest.format != format
            || manifest.sql.trim().is_empty()
            || manifest.tables.keys().any(|name| name.is_empty())
            || manifest
                .previous_epoch
                .is_some_and(|previous| previous.checked_add(1) != Some(manifest.epoch))
            || manifest.previous_epoch.is_some() != manifest.previous.is_some()
            || (manifest.previous_epoch.is_none() && manifest.epoch != 0)
            || manifest.output.name != "output.arrow"
            || manifest.delta.name != "delta.arrow"
            || manifest
                .previous
                .as_ref()
                .is_some_and(|pointer| pointer.format != format)
        {
            return Err(invalid("invalid checkpoint manifest"));
        }
        if source {
            if manifest.source_vector.as_ref().is_none_or(|entry| {
                entry.name != "source-vector.json" || entry.bytes > MAX_JSON_BYTES
            }) || manifest.previous_vector_digest.is_some() != manifest.previous_epoch.is_some()
                || manifest
                    .previous_vector_digest
                    .as_ref()
                    .is_some_and(|digest| !valid_digest(digest))
            {
                return Err(invalid("invalid source-vector manifest binding"));
            }
        } else if manifest.source_vector.is_some() || manifest.previous_vector_digest.is_some() {
            return Err(invalid(
                "format-1 checkpoints cannot contain source-vector metadata",
            ));
        }
        let mut tables = BTreeMap::new();
        for (index, (name, entry)) in manifest.tables.iter().enumerate() {
            if entry.name != format!("table-{index}.arrow") {
                return Err(invalid("invalid source file binding"));
            }
            tables.insert(name.clone(), read_batch(&directory, entry)?);
        }
        let snapshot = Snapshot {
            sql: manifest.sql.clone(),
            epoch: manifest.epoch,
            tables,
            output: read_batch(&directory, &manifest.output)?,
            delta: read_batch(&directory, &manifest.delta)?,
        };
        let vector = if let Some(entry) = &manifest.source_vector {
            let mut file = verified_file(&directory, entry)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            let vector: PinnedSourceVector = parse_json(&bytes)?;
            if vector.canonical_bytes()? != bytes || vector.digest()? != entry.sha256 {
                return Err(invalid(
                    "source-vector JSON is not its canonical committed representation",
                ));
            }
            vector.validate_for(&snapshot.tables)?;
            Some(vector)
        } else {
            None
        };
        Ok((manifest, snapshot, vector))
    }
}

fn validate_epoch(expected: Option<u64>, snapshot: &Snapshot) -> Result<()> {
    if let Some(previous) = expected {
        if previous.checked_add(1) != Some(snapshot.epoch) {
            return Err(invalid("checkpoint epoch must advance by exactly one"));
        }
    } else if snapshot.epoch != 0 {
        return Err(invalid("checkpoint bootstrap must be epoch zero"));
    }
    if snapshot.sql.trim().is_empty() || snapshot.tables.keys().any(|name| name.is_empty()) {
        return Err(invalid("SQL and registered table names must be nonempty"));
    }
    Ok(())
}

fn validate_definition(before: &Snapshot, after: &Snapshot) -> Result<()> {
    if before.sql != after.sql
        || before.tables.len() != after.tables.len()
        || before.tables.iter().any(|(name, batch)| {
            after
                .tables
                .get(name)
                .is_none_or(|other| batch.schema() != other.schema())
        })
        || before.output.schema() != after.output.schema()
        || before.delta.schema() != after.delta.schema()
    {
        return Err(invalid(
            "a checkpoint cannot change its SQL or typed source/output schemas",
        ));
    }
    Ok(())
}

fn snapshots_equal(left: &Snapshot, right: &Snapshot) -> bool {
    left.sql == right.sql
        && left.epoch == right.epoch
        && left.tables.len() == right.tables.len()
        && left.tables.iter().all(|(name, batch)| {
            right
                .tables
                .get(name)
                .is_some_and(|other| batches_equal(batch, other))
        })
        && batches_equal(&left.output, &right.output)
        && batches_equal(&left.delta, &right.delta)
}

pub(crate) fn batches_equal(left: &RecordBatch, right: &RecordBatch) -> bool {
    left.schema() == right.schema()
        && left.num_rows() == right.num_rows()
        && left
            .columns()
            .iter()
            .zip(right.columns())
            .all(|(a, b)| a.to_data() == b.to_data())
}

pub(crate) fn write_batch(directory: &Path, name: &str, batch: &RecordBatch) -> Result<FileEntry> {
    let mut file = new_file(&directory.join(name))?;
    {
        let mut writer = FileWriter::try_new(&mut file, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    file.sync_all()?;
    describe_file(name, file)
}

pub(crate) fn read_batch(directory: &Path, entry: &FileEntry) -> Result<RecordBatch> {
    let file = verified_file(directory, entry)?;
    let mut reader = FileReader::try_new(file, None)?;
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| invalid("checkpoint IPC file has no record batch"))?;
    if reader.next().transpose()?.is_some() {
        return Err(invalid(
            "checkpoint IPC file has more than one record batch",
        ));
    }
    Ok(batch)
}

pub(crate) fn write_bytes(directory: &Path, name: &str, bytes: &[u8]) -> Result<FileEntry> {
    let mut file = new_file(&directory.join(name))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(FileEntry {
        name: name.into(),
        bytes: u64::try_from(bytes.len()).map_err(|_| invalid("file size overflow"))?,
        sha256: digest(bytes),
    })
}

fn new_file(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?)
}

fn describe_file(name: &str, mut file: File) -> Result<FileEntry> {
    file.seek(SeekFrom::Start(0))?;
    let (bytes, sha256) = hash_file(&mut file)?;
    Ok(FileEntry {
        name: name.into(),
        bytes,
        sha256,
    })
}

pub(crate) fn verified_file(directory: &Path, entry: &FileEntry) -> Result<File> {
    if !component(&entry.name)
        || entry.sha256.len() != 64
        || !entry.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid("invalid checkpoint file descriptor"));
    }
    let path = directory.join(&entry.name);
    if !fs::symlink_metadata(&path)?.file_type().is_file() {
        return Err(invalid("checkpoint entry is not a regular file"));
    }
    let mut file = File::open(path)?;
    if file.metadata()?.len() != entry.bytes {
        return Err(invalid("checkpoint file size mismatch"));
    }
    let (bytes, sha256) = hash_file(&mut file)?;
    if bytes != entry.bytes || sha256 != entry.sha256 {
        return Err(invalid("checkpoint file checksum mismatch"));
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

fn hash_file(file: &mut File) -> Result<(u64, String)> {
    let mut bytes = 0_u64;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes = bytes
            .checked_add(count as u64)
            .ok_or_else(|| invalid("file size overflow"))?;
        hasher.update(&buffer[..count]);
    }
    Ok((bytes, format!("{:x}", hasher.finalize())))
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn read_json_file(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_JSON_BYTES {
        return Err(invalid("invalid or oversized checkpoint JSON file"));
    }
    let bytes = fs::read(path)?;
    if bytes.len() as u64 > MAX_JSON_BYTES {
        return Err(invalid("oversized checkpoint JSON file"));
    }
    Ok(bytes)
}

pub(crate) fn json_bytes(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| invalid(format!("checkpoint JSON: {error}")))?;
    if bytes.len() as u64 > MAX_JSON_BYTES {
        return Err(invalid("oversized checkpoint JSON file"));
    }
    Ok(bytes)
}

fn parse_json<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|error| invalid(format!("checkpoint JSON: {error}")))
}

pub(crate) fn component(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// Actual directory chain needed to persist the canonical warehouse path.
///
/// Existing directories are not assumed durable: a previous failed open may
/// have created them. Canonicalization resolves relative paths and symlinks;
/// synchronizing every ancestor also handles retries after an interrupted mkdir.
pub(crate) struct CheckpointDirectories {
    pub(crate) root: PathBuf,
}

impl CheckpointDirectories {
    pub(crate) fn create(path: &Path) -> Result<Self> {
        if path.as_os_str().is_empty() {
            return Err(invalid("checkpoint directory path must be nonempty"));
        }
        let requested = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        fs::create_dir_all(&requested)?;
        let root = fs::canonicalize(requested)?;
        if !fs::metadata(&root)?.is_dir() {
            return Err(invalid("checkpoint path is not a directory"));
        }
        Ok(Self { root })
    }

    pub(crate) fn synchronize(
        &self,
        mut synchronize: impl FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        for directory in self.root.ancestors() {
            synchronize(directory)?;
        }
        Ok(())
    }
}

pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

#[cfg(test)]
mod directory_tests {
    use super::*;

    fn observed_open(path: &Path) -> Result<Vec<PathBuf>> {
        let mut observed = Vec::new();
        let store = CheckpointStore::open_with_sync(path, |directory| {
            observed.push(directory.to_path_buf());
            sync_dir(directory)
        })?;
        drop(store);
        Ok(observed)
    }

    #[test]
    fn new_nested_warehouse_synchronizes_every_created_parent_entry() -> Result<()> {
        let base = tempfile::tempdir()?;
        let base = fs::canonicalize(base.path())?;
        let root = base.join("first/second/warehouse");
        let mut expected = vec![root.clone(), base.join("first/second"), base.join("first")];
        expected.extend(base.ancestors().map(Path::to_path_buf));
        assert_eq!(observed_open(&root)?, expected);
        Ok(())
    }

    #[test]
    fn existing_warehouse_still_synchronizes_all_ancestors() -> Result<()> {
        let base = tempfile::tempdir()?;
        let root = fs::canonicalize(base.path())?;
        assert_eq!(
            observed_open(&root)?,
            root.ancestors().map(Path::to_path_buf).collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn relative_path_uses_actual_current_directory_without_changing_process_cwd() -> Result<()> {
        let cwd = std::env::current_dir()?;
        let base = tempfile::Builder::new()
            .prefix("optd-directory-test-")
            .tempdir_in(&cwd)?;
        let relative = base
            .path()
            .strip_prefix(&cwd)
            .map_err(|_| invalid("fixture must be beneath the existing current directory"))?
            .join("new/warehouse");
        let canonical_base = fs::canonicalize(base.path())?;
        let mut expected = vec![
            canonical_base.join("new/warehouse"),
            canonical_base.join("new"),
        ];
        expected.extend(canonical_base.ancestors().map(Path::to_path_buf));
        assert_eq!(observed_open(&relative)?, expected);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn symlink_parent_alias_synchronizes_the_canonical_destination_chain() -> Result<()> {
        let base = tempfile::tempdir()?;
        let target = base.path().join("target");
        fs::create_dir(&target)?;
        let alias = base.path().join("alias");
        std::os::unix::fs::symlink(&target, &alias)?;
        let target = fs::canonicalize(target)?;
        let mut expected = vec![target.join("new/warehouse"), target.join("new")];
        expected.extend(target.ancestors().map(Path::to_path_buf));
        assert_eq!(observed_open(&alias.join("new/warehouse"))?, expected);
        Ok(())
    }

    #[test]
    fn parent_components_synchronize_the_resulting_path_not_incidental_directories() -> Result<()> {
        let base = tempfile::tempdir()?;
        let canonical = fs::canonicalize(base.path())?;
        let mut expected = vec![canonical.join("new/warehouse"), canonical.join("new")];
        expected.extend(canonical.ancestors().map(Path::to_path_buf));
        assert_eq!(
            observed_open(&base.path().join("incidental/../new/warehouse"))?,
            expected
        );
        Ok(())
    }

    #[test]
    fn escaped_existing_directory_still_synchronizes_the_complete_destination_chain() -> Result<()>
    {
        let base = tempfile::tempdir()?;
        let existing = base.path().join("existing");
        fs::create_dir(&existing)?;
        let canonical = fs::canonicalize(base.path())?;
        let mut expected = vec![canonical.join("other/warehouse"), canonical.join("other")];
        expected.extend(canonical.ancestors().map(Path::to_path_buf));
        assert_eq!(
            observed_open(&existing.join("new/../../other/warehouse"))?,
            expected
        );
        Ok(())
    }

    #[test]
    fn files_non_directory_parents_and_empty_paths_fail_before_synchronization() -> Result<()> {
        let base = tempfile::tempdir()?;
        let file = base.path().join("file");
        fs::write(&file, b"not a directory")?;
        let child = file.join("warehouse");
        for path in [file.as_path(), child.as_path(), Path::new("")] {
            let mut calls = 0;
            assert!(CheckpointStore::open_with_sync(path, |_| {
                calls += 1;
                Ok(())
            })
            .is_err());
            assert_eq!(calls, 0);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn dangling_symlink_parent_fails_without_reported_success() -> Result<()> {
        let base = tempfile::tempdir()?;
        let alias = base.path().join("dangling");
        std::os::unix::fs::symlink(base.path().join("absent"), &alias)?;
        assert!(observed_open(&alias.join("warehouse")).is_err());
        Ok(())
    }

    #[test]
    fn ancestor_sync_error_stops_open_and_releases_its_writer_lock() -> Result<()> {
        let base = tempfile::tempdir()?;
        let base = fs::canonicalize(base.path())?;
        let root = base.join("first/warehouse");
        let failed_at = base.join("first");
        let mut observed = Vec::new();
        let failed = CheckpointStore::open_with_sync(&root, |directory| {
            observed.push(directory.to_path_buf());
            if directory == failed_at {
                Err(invalid("injected ancestor fsync failure"))
            } else {
                sync_dir(directory)
            }
        });
        assert!(failed.is_err());
        assert_eq!(observed, vec![root.clone(), failed_at]);
        // The first attempt left existing but insufficiently synced directories.
        // Retry must still visit ancestors beyond the previous failed point.
        assert_eq!(
            observed_open(&root)?,
            root.ancestors().map(Path::to_path_buf).collect::<Vec<_>>()
        );
        // A failed open also drops its lock rather than leaving a live owner.
        let reopened = CheckpointStore::open(root)?;
        assert!(reopened.load()?.is_none());
        Ok(())
    }
}

#[cfg(test)]
mod publication_tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;

    use super::*;
    use crate::source_vector::{
        arrow_layout_digest, canonical_arrow_layout, iceberg_schema_digest,
        snapshot_descriptor_digest, CatalogBinding, PinnedSnapshot, PinnedSource,
    };

    fn state(epoch: u64) -> Result<SourceSnapshot> {
        let value = i64::try_from(epoch).map_err(|_| invalid("fixture epoch overflow"))?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )?;
        let iceberg_schema = json!({"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"}]});
        let layout = canonical_arrow_layout(batch.schema().as_ref())?;
        let source = PinnedSource {
            catalog: CatalogBinding {
                kind: "local/1".into(),
                canonical_root: "/fixture/catalog".into(),
            },
            namespace: vec!["test".into()],
            name: "items".into(),
            table_uuid: "00000000-0000-0000-0000-000000000001".into(),
            reference: "main".into(),
            snapshot: PinnedSnapshot::Snapshot {
                id: value + 100,
                sequence_number: value + 1,
                descriptor_sha256: snapshot_descriptor_digest(
                    &json!({"snapshot-id":value+100,"sequence-number":value+1}),
                )?,
            },
            schema_id: 0,
            iceberg_schema_sha256: iceberg_schema_digest(&iceberg_schema)?,
            iceberg_schema,
            arrow_layout_sha256: arrow_layout_digest(&layout)?,
            arrow_layout: layout,
            normalizer_version: 1,
        };
        Ok(SourceSnapshot {
            snapshot: Snapshot {
                sql: "SELECT id FROM items".into(),
                epoch,
                tables: BTreeMap::from([("items".into(), batch.clone())]),
                output: batch,
                delta: RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int64, false),
                        Field::new("weight", DataType::Int64, false),
                    ])),
                    vec![
                        Arc::new(Int64Array::from(vec![value])),
                        Arc::new(Int64Array::from(vec![1_i64])),
                    ],
                )?,
            },
            vector: PinnedSourceVector {
                version: 1,
                sources: BTreeMap::from([("items".into(), source)]),
            },
        })
    }

    fn commit(
        store: &CheckpointStore,
        source: bool,
        expected: Option<&SourcePosition>,
        candidate: &SourceSnapshot,
        callback: &mut impl FnMut(PublishPoint) -> Result<()>,
        synchronize: &mut impl FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        if source {
            store.commit_source_with_sync(expected, candidate, callback, synchronize)
        } else {
            store.commit_with_sync(
                expected.map(|position| position.epoch),
                &candidate.snapshot,
                callback,
                synchronize,
            )
        }
    }

    fn failed_rename_sync_retry(source: bool) -> Result<()> {
        for epoch in [0_u64, 1] {
            let temp = tempfile::tempdir()?;
            let store = CheckpointStore::open(temp.path())?;
            if epoch == 1 {
                commit(
                    &store,
                    source,
                    None,
                    &state(0)?,
                    &mut |_| Ok(()),
                    &mut sync_dir,
                )?;
            }
            let candidate = state(epoch)?;
            let expected = epoch
                .checked_sub(1)
                .map(|epoch| state(epoch)?.position())
                .transpose()?;
            let mut root_calls = 0;
            let mut points = Vec::new();
            let result = commit(
                &store,
                source,
                expected.as_ref(),
                &candidate,
                &mut |point| {
                    points.push(point);
                    Ok(())
                },
                &mut |path| {
                    if path == store.root {
                        root_calls += 1;
                        if root_calls == 2 {
                            // This is the real parent-sync call after the production
                            // rename, not an observer standing in for that window.
                            let pointer = store
                                .read_current()?
                                .ok_or_else(|| invalid("rename did not publish CURRENT"))?;
                            let (_, snapshot, _) = store.read_epoch_mode(&pointer, source)?;
                            assert_eq!(snapshot.epoch, epoch);
                            return Err(invalid("injected post-rename directory fsync failure"));
                        }
                    }
                    sync_dir(path)
                },
            );
            assert!(result.is_err());
            assert_eq!(root_calls, 2);
            assert_eq!(points, vec![PublishPoint::FilesDurable]);
            let pointer = fs::read(store.root.join("CURRENT"))?;
            let mut retries = Vec::new();
            let result = commit(
                &store,
                source,
                expected.as_ref(),
                &candidate,
                &mut |_| panic!("an identical retry must not publish"),
                &mut |path| {
                    retries.push(path.to_path_buf());
                    Err(invalid("injected retry directory fsync failure"))
                },
            );
            assert!(result.is_err());
            assert_eq!(retries, vec![store.root.clone()]);
            retries.clear();
            commit(
                &store,
                source,
                expected.as_ref(),
                &candidate,
                &mut |_| panic!("an identical retry must not publish"),
                &mut |path| {
                    retries.push(path.to_path_buf());
                    sync_dir(path)
                },
            )?;
            assert_eq!(retries, vec![store.root.clone()]);
            assert_eq!(fs::read(store.root.join("CURRENT"))?, pointer);
            drop(store);
            let reopened = CheckpointStore::open(temp.path())?;
            if source {
                let history = reopened.history_source()?;
                assert_eq!(
                    history.len(),
                    usize::try_from(epoch + 1).map_err(|_| invalid("fixture epoch overflow"))?
                );
                let current = history
                    .last()
                    .ok_or_else(|| invalid("missing source history"))?;
                assert_eq!(current.vector, candidate.vector);
                assert!(snapshots_equal(&current.snapshot, &candidate.snapshot));
            } else {
                let history = reopened.history()?;
                assert_eq!(
                    history.len(),
                    usize::try_from(epoch + 1).map_err(|_| invalid("fixture epoch overflow"))?
                );
                assert!(snapshots_equal(
                    history.last().ok_or_else(|| invalid("missing history"))?,
                    &candidate.snapshot
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn memory_retry_requires_successful_directory_sync_after_visible_rename() -> Result<()> {
        failed_rename_sync_retry(false)
    }

    #[test]
    fn source_retry_requires_successful_directory_sync_after_visible_rename() -> Result<()> {
        failed_rename_sync_retry(true)
    }
}
