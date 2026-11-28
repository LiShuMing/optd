//! Immutable IPC segments share one file synchronization per view and epoch.
use super::{Payload, PayloadKind, StoreWorkCounter};
use crate::bag::MAX_MATERIALIZED_BYTES;
use crate::error::invalid;
use crate::store::{component, decode_batch, FileEntry};
use crate::{Error, Result};
use datafusion::arrow::{array::RecordBatch, ipc::writer::FileWriter};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{self, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

struct SegmentWriter<'a> {
    output: &'a mut BufWriter<File>,
    hash: Sha256,
    bytes: usize,
}

impl Write for SegmentWriter<'_> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() > MAX_MATERIALIZED_BYTES - self.bytes {
            return Err(io::Error::other("view IPC segment exceeds byte budget"));
        }
        let count = self.output.write(data)?;
        self.hash.update(&data[..count]);
        self.bytes += count;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

pub(super) struct ViewBundle<'a> {
    directory: &'a Path,
    epoch_dir: &'a str,
    name: String,
    output: Option<BufWriter<File>>,
    position: u64,
    segments: Vec<(PayloadKind, u64, usize)>,
}

impl<'a> ViewBundle<'a> {
    pub(super) fn new(directory: &'a Path, epoch_dir: &'a str, view_id: u64) -> Self {
        Self {
            directory,
            epoch_dir,
            name: format!("view-{view_id}.pack"),
            output: None,
            position: 0,
            segments: Vec::new(),
        }
    }

    pub(super) fn append(
        &mut self,
        batch: &RecordBatch,
        kind: PayloadKind,
        rows: usize,
    ) -> Result<Payload> {
        if self.output.is_none() {
            self.output = Some(BufWriter::new(File::create_new(
                self.directory.join(&self.name),
            )?));
        }
        let mut segment = SegmentWriter {
            output: self
                .output
                .as_mut()
                .ok_or_else(|| invalid("missing view bundle writer"))?,
            hash: Sha256::new(),
            bytes: 0,
        };
        let mut ipc = FileWriter::try_new(&mut segment, batch.schema().as_ref())?;
        ipc.write(batch)?;
        ipc.finish()?;
        drop(ipc);
        let offset = self.position;
        let bytes = segment.bytes as u64;
        self.position = offset.checked_add(bytes).ok_or(Error::Overflow)?;
        self.segments.push((kind, bytes, rows));
        Ok(Payload {
            directory: self.epoch_dir.into(),
            offset: Some(offset),
            file: FileEntry {
                name: self.name.clone(),
                bytes,
                sha256: format!("{:x}", segment.hash.finalize()),
            },
        })
    }

    pub(super) fn finish(self, work: &StoreWorkCounter) -> Result<()> {
        if let Some(mut file) = self.output {
            let length = self.position.checked_add(8).ok_or(Error::Overflow)?;
            file.write_all(&length.to_le_bytes())?;
            file.flush()?;
            file.get_ref().sync_all()?;
            work.view_bundle(length, &self.segments)?;
        }
        Ok(())
    }
}

pub(super) fn read(directory: &Path, entry: &FileEntry, offset: u64) -> Result<RecordBatch> {
    if !component(&entry.name)
        || !entry.name.ends_with(".pack")
        || entry.bytes > MAX_MATERIALIZED_BYTES as u64
    {
        return Err(invalid("invalid view bundle segment descriptor"));
    }
    let path = directory.join(&entry.name);
    if !fs::symlink_metadata(&path)?.file_type().is_file() {
        return Err(invalid("view bundle must be a regular file"));
    }
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let footer = size
        .checked_sub(8)
        .ok_or_else(|| invalid("truncated view bundle"))?;
    if offset.checked_add(entry.bytes).ok_or(Error::Overflow)? > footer {
        return Err(invalid("view bundle segment outside file"));
    }
    file.seek(SeekFrom::Start(footer))?;
    let mut encoded_size = [0u8; 8];
    file.read_exact(&mut encoded_size)?;
    if u64::from_le_bytes(encoded_size) != size {
        return Err(invalid("view bundle size mismatch"));
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut data = vec![0; usize::try_from(entry.bytes).map_err(|_| Error::Overflow)?];
    file.read_exact(&mut data)?;
    if format!("{:x}", Sha256::digest(&data)) != entry.sha256 {
        return Err(invalid("view bundle segment checksum mismatch"));
    }
    decode_batch(Cursor::new(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use std::sync::Arc;

    #[test]
    fn ordered_output_preserves_row_sequence_across_reopen() -> Result<()> {
        use super::super::{DatabaseSnapshot, DatabaseStore, ViewData, ViewOutput};
        use crate::bag::Bag;
        use datafusion::arrow::array::{ArrayRef, Int64Array};
        use std::collections::{BTreeMap, BTreeSet};

        let dir = tempfile::tempdir()?;
        let output = RecordBatch::try_from_iter([(
            "k",
            Arc::new(Int64Array::from(vec![3, 1, 2, 1])) as ArrayRef,
        )])?;
        let log = Bag::from_batch(&output)?.weighted_batch()?;
        let next = DatabaseSnapshot {
            epoch: 1,
            next_id: 2,
            views: BTreeMap::from([(
                "v".into(),
                ViewData {
                    id: 1,
                    sql: "SELECT k FROM t ORDER BY k".into(),
                    dependencies: BTreeSet::new(),
                    output: ViewOutput::Ordered(output.clone()),
                },
            )]),
            deltas: BTreeMap::from([("v".into(), log.clone())]),
            ..Default::default()
        };
        let mut store = DatabaseStore::open(dir.path())?;
        store.commit(next, BTreeMap::new(), None, &mut |_| Ok(()))?;
        drop(store);
        let store = DatabaseStore::open(dir.path())?;
        let snapshot = store.snapshot();
        let ViewOutput::Ordered(restored) = &snapshot.views["v"].output else {
            panic!("expected ordered output")
        };
        assert_eq!(restored, &output);
        assert_eq!(store.history()?[0].deltas["v"], log);
        Ok(())
    }

    #[test]
    fn reject_out_of_range_and_overflowing_segment_before_decode() {
        let dir = tempfile::tempdir().unwrap();
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let mut bundle = ViewBundle::new(dir.path(), "epoch", 1);
        let payload = bundle.append(&batch, PayloadKind::ViewOutput, 0).unwrap();
        bundle.finish(&StoreWorkCounter::default()).unwrap();
        for offset in [1, u64::MAX] {
            assert!(read(dir.path(), &payload.file, offset).is_err());
        }
        assert!(read(dir.path(), &payload.file, 0).is_ok());
    }
}
