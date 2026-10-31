//! Completed database file operations and successful open-time reconstruction.

use std::sync::Mutex;

use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::error::invalid;
use crate::{Error, Result};

/// Serialized file lengths summed once per completed operation, including repeats.
/// These lengths do not measure device traffic, allocation or partial failed IO.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileWork {
    /// Number of successfully completed file operations.
    pub files: u64,
    /// Sum of their actual serialized file lengths.
    pub bytes: u64,
}

impl FileWork {
    fn since(&self, before: &Self) -> Result<Self> {
        Ok(Self {
            files: subtract(self.files, before.files)?,
            bytes: subtract(self.bytes, before.bytes)?,
        })
    }

    fn record(&mut self, bytes: u64) -> Result<()> {
        add(&mut self.files, 1)?;
        add(&mut self.bytes, bytes)
    }
}

/// Serialized payload volume and expanded batch rows, including duplicates.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadWork {
    /// Number of completed payload operations or reused descriptors.
    pub files: u64,
    /// Sum of serialized file lengths; reused bytes are references, not IO.
    pub bytes: u64,
    /// Expanded rows decoded, written or referenced by those operations.
    pub rows: u64,
}

impl PayloadWork {
    fn since(&self, before: &Self) -> Result<Self> {
        Ok(Self {
            files: subtract(self.files, before.files)?,
            bytes: subtract(self.bytes, before.bytes)?,
            rows: subtract(self.rows, before.rows)?,
        })
    }

    fn record(&mut self, bytes: u64, rows: usize) -> Result<()> {
        add(&mut self.files, 1)?;
        add(&mut self.bytes, bytes)?;
        add(
            &mut self.rows,
            u64::try_from(rows).map_err(|_| Error::Overflow)?,
        )
    }
}

/// Complete payload reads, writes and unchanged-file reuse, measured separately.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadIoWork {
    /// Successfully decoded files; checksum passes are counted separately.
    pub read: PayloadWork,
    /// Successfully serialized, synced and described new files.
    pub written: PayloadWork,
    /// Existing payload descriptors retained without reading or writing the file.
    pub reused: PayloadWork,
}

impl PayloadIoWork {
    fn since(&self, before: &Self) -> Result<Self> {
        Ok(Self {
            read: self.read.since(&before.read)?,
            written: self.written.since(&before.written)?,
            reused: self.reused.since(&before.reused)?,
        })
    }
}

/// Cumulative, host-local SQL database work since this session began opening.
///
/// Read byte volumes describe successfully decoded file lengths, not reader/syscall
/// traffic. Checksum passes are explicit and must not be mistaken for unique bytes.
/// Complete operations count even if a later publication barrier fails. Partial
/// failed helpers, directory operations, fsyncs and comparison CPU work are excluded.
/// This diagnostic is never stored in receipts and does not certify publication.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreWork {
    /// Successful CURRENT reads during open and predecessor checks.
    pub pointer_reads: FileWork,
    /// Successful temporary CURRENT writes, before atomic rename.
    pub pointer_writes: FileWork,
    /// Successfully parsed manifest files, including repeated history reads.
    pub manifest_reads: FileWork,
    /// Successfully written manifest files.
    pub manifest_writes: FileWork,
    /// Complete file hash passes (verified reads and IPC write descriptions).
    pub checksum_reads: FileWork,
    /// Base-table payload operations.
    pub tables: PayloadIoWork,
    /// Materialized-view output payload operations.
    pub view_outputs: PayloadIoWork,
    /// Per-epoch signed output-delta payload operations.
    pub output_deltas: PayloadIoWork,
    /// Started complete metadata lineage traversals, including empty ones.
    pub history_traversals: u64,
    /// Validated manifest visits inside those traversals.
    pub manifests_visited: u64,
}

impl StoreWork {
    /// Completed operations between two snapshots from the same session.
    ///
    /// The caller must supply an earlier snapshot from this session; counters
    /// contain no session identity. Reject decreasing counters instead of wrapping.
    pub fn since(&self, before: &Self) -> Result<Self> {
        Ok(Self {
            pointer_reads: self.pointer_reads.since(&before.pointer_reads)?,
            pointer_writes: self.pointer_writes.since(&before.pointer_writes)?,
            manifest_reads: self.manifest_reads.since(&before.manifest_reads)?,
            manifest_writes: self.manifest_writes.since(&before.manifest_writes)?,
            checksum_reads: self.checksum_reads.since(&before.checksum_reads)?,
            tables: self.tables.since(&before.tables)?,
            view_outputs: self.view_outputs.since(&before.view_outputs)?,
            output_deltas: self.output_deltas.since(&before.output_deltas)?,
            history_traversals: subtract(self.history_traversals, before.history_traversals)?,
            manifests_visited: subtract(self.manifests_visited, before.manifests_visited)?,
        })
    }
}

/// Successful SQL open work, immutable for the lifetime of the session.
///
/// Reconstruction rows count the inputs/results of each rebuilt view separately.
/// They do not count internal operator visits, planning work or allocator traffic.
/// CREATE MV bootstrap is outside this open-time record.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryWork {
    /// Actual completed store operations while opening the database.
    pub store: StoreWork,
    /// Views whose strict plans and derived execution state were rebuilt.
    pub rebuilt_views: u64,
    /// Expanded dependency rows supplied to those view reconstructions.
    pub dependency_rows: u64,
    /// Expanded rows materialized for reconstructed view-output verification.
    pub output_rows: u64,
    /// Sum of Arrow array buffer sizes for those reconstructed output batches.
    pub output_array_bytes: u64,
    /// Durable view-output rows compared with reconstructed output Bags.
    pub verified_output_rows: u64,
}

impl RecoveryWork {
    /// Called only after the reconstruction output and dependency checks succeeded.
    pub(super) fn record_view(
        &mut self,
        dependency_rows: u64,
        output: &RecordBatch,
        verified_rows: usize,
    ) -> Result<()> {
        let mut next = self.clone();
        add(&mut next.rebuilt_views, 1)?;
        add(&mut next.dependency_rows, dependency_rows)?;
        add(&mut next.output_rows, output.num_rows() as u64)?;
        add(
            &mut next.output_array_bytes,
            output.get_array_memory_size() as u64,
        )?;
        add(&mut next.verified_output_rows, verified_rows as u64)?;
        *self = next;
        Ok(())
    }
}

/// Which durable relation a payload operation belongs to.
#[derive(Clone, Copy)]
pub(super) enum PayloadKind {
    Table,
    ViewOutput,
    OutputDelta,
}

/// A descriptor reuse is deliberately distinct from a completed file operation.
pub(super) enum PayloadAction {
    Read,
    Write,
    Reuse,
}

/// Per-store counter preserving &self history APIs and atomic event updates.
#[derive(Default)]
pub(super) struct StoreWorkCounter(Mutex<StoreWork>);

impl StoreWorkCounter {
    pub(super) fn snapshot(&self) -> Result<StoreWork> {
        Ok(self
            .0
            .lock()
            .map_err(|_| invalid("store work counter poisoned"))?
            .clone())
    }

    /// Apply a complete event atomically: overflow leaves every counter unchanged.
    pub(super) fn record(&self, event: impl FnOnce(&mut StoreWork) -> Result<()>) -> Result<()> {
        let mut work = self
            .0
            .lock()
            .map_err(|_| invalid("store work counter poisoned"))?;
        let mut next = work.clone();
        event(&mut next)?;
        *work = next;
        Ok(())
    }

    pub(super) fn pointer_read(&self, bytes: usize) -> Result<()> {
        self.record(|w| w.pointer_reads.record(bytes as u64))
    }

    pub(super) fn pointer_write(&self, bytes: u64) -> Result<()> {
        self.record(|w| w.pointer_writes.record(bytes))
    }

    pub(super) fn manifest_read(&self, bytes: u64) -> Result<()> {
        self.record(|w| {
            w.manifest_reads.record(bytes)?;
            w.checksum_reads.record(bytes)
        })
    }

    pub(super) fn manifest_write(&self, bytes: u64) -> Result<()> {
        self.record(|w| w.manifest_writes.record(bytes))
    }

    pub(super) fn start_history(&self) -> Result<()> {
        self.record(|w| add(&mut w.history_traversals, 1))
    }

    pub(super) fn visit_manifest(&self) -> Result<()> {
        self.record(|w| add(&mut w.manifests_visited, 1))
    }

    pub(super) fn payload(
        &self,
        kind: PayloadKind,
        action: PayloadAction,
        bytes: u64,
        rows: usize,
    ) -> Result<()> {
        self.record(|w| {
            let payload = match kind {
                PayloadKind::Table => &mut w.tables,
                PayloadKind::ViewOutput => &mut w.view_outputs,
                PayloadKind::OutputDelta => &mut w.output_deltas,
            };
            let counts = match action {
                PayloadAction::Read => &mut payload.read,
                PayloadAction::Write => &mut payload.written,
                PayloadAction::Reuse => &mut payload.reused,
            };
            counts.record(bytes, rows)?;
            if !matches!(action, PayloadAction::Reuse) {
                // Existing helpers hash the full file before a read or after IPC serialization.
                w.checksum_reads.record(bytes)?;
            }
            Ok(())
        })
    }
}

fn subtract(after: u64, before: u64) -> Result<u64> {
    after.checked_sub(before).ok_or_else(|| {
        invalid("store work counters decreased; snapshots must come from the same session in order")
    })
}

fn add(counter: &mut u64, amount: u64) -> Result<()> {
    *counter = counter.checked_add(amount).ok_or(Error::Overflow)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_keeps_the_complete_event_unchanged() {
        let counter = StoreWorkCounter::default();
        counter
            .record(|w| {
                w.tables.written.bytes = u64::MAX;
                Ok(())
            })
            .unwrap();
        let before = counter.snapshot().unwrap();
        assert!(matches!(
            counter.payload(PayloadKind::Table, PayloadAction::Write, 1, 1),
            Err(Error::Overflow)
        ));
        assert_eq!(counter.snapshot().unwrap(), before);
    }
}

#[cfg(test)]
mod interval_tests {
    use super::*;
    use serde_json::Value;

    fn uniform(value: &mut Value, count: u64) {
        match value {
            Value::Object(fields) => {
                for value in fields.values_mut() {
                    uniform(value, count);
                }
            }
            Value::Number(_) => *value = Value::from(count),
            _ => panic!("work fields must be numeric objects"),
        }
    }

    #[test]
    fn intervals_cover_every_serialized_counter_and_reject_reverse_order() {
        let mut value = serde_json::to_value(StoreWork::default()).unwrap();
        uniform(&mut value, 3);
        let after: StoreWork = serde_json::from_value(value.clone()).unwrap();
        uniform(&mut value, 1);
        let before: StoreWork = serde_json::from_value(value.clone()).unwrap();
        uniform(&mut value, 2);
        assert_eq!(
            serde_json::to_value(after.since(&before).unwrap()).unwrap(),
            value
        );
        assert!(before.since(&after).is_err());
        assert_eq!(after.since(&after).unwrap(), StoreWork::default());
    }
}
