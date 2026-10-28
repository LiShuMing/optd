// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Single-host Iceberg CDC output with durable envelopes and read-back validation.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, NestedField, Operation, PrimitiveType, Schema, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation, TableIdent};

use crate::cdc::{
    system_fields, CdcError, ChangeRecord, CommitEnvelope, DeltaSink, Generation, PayloadType,
    PreparedBatch, PROTOCOL,
};
use crate::change::{Multiset, Value};
use crate::local_catalog::{LocalCatalog, WriterLock};

fn storage(e: impl std::fmt::Display) -> CdcError {
    CdcError::Storage(e.to_string())
}

/// Build the precise v1 Iceberg schema, including stable field ids.
pub fn output_schema(generation: &Generation) -> Result<Schema, CdcError> {
    let mut fields = Vec::new();
    for (index, field) in generation.schema().fields().iter().enumerate() {
        let id = i32::try_from(index)
            .ok()
            .and_then(|n| n.checked_add(1000))
            .ok_or(CdcError::Overflow)?;
        let typ = Type::Primitive(match field.data_type {
            PayloadType::Long => PrimitiveType::Long,
            PayloadType::String => PrimitiveType::String,
        });
        fields.push(Arc::new(if field.required {
            NestedField::required(id, field.name.clone(), typ)
        } else {
            NestedField::optional(id, field.name.clone(), typ)
        }));
    }
    for (id, name, typ, required) in system_fields() {
        let typ = Type::Primitive(match typ {
            "int" => PrimitiveType::Int,
            "long" => PrimitiveType::Long,
            "boolean" => PrimitiveType::Boolean,
            "string" => PrimitiveType::String,
            "timestamptz" => PrimitiveType::Timestamptz,
            _ => return Err(CdcError::InvalidSchema),
        });
        fields.push(Arc::new(if required {
            NestedField::required(id, name, typ)
        } else {
            NestedField::optional(id, name, typ)
        }));
    }
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(storage)
}

/// Translate validated weighted rows into Arrow, retaining metadata field ids.
pub fn record_batch(
    generation: &Generation,
    batch: &PreparedBatch,
) -> Result<RecordBatch, CdcError> {
    let schema = Arc::new(schema_to_arrow_schema(&output_schema(generation)?).map_err(storage)?);
    let rows = batch.rows();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (index, field) in generation.schema().fields().iter().enumerate() {
        let column: ArrayRef = match field.data_type {
            PayloadType::Long => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| match &r.payload[index] {
                        Value::Int(n) => Some(*n),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            PayloadType::String => Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| match &r.payload[index] {
                        Value::Str(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
        };
        columns.push(column);
    }
    let e = batch.envelope();
    columns.extend([
        Arc::new(Int32Array::from(
            rows.iter().map(|r| r.op_type).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(BooleanArray::from(
            rows.iter().map(|r| r.is_update).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.weight).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.row_id.as_deref()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec![e.batch_id.as_str(); rows.len()])),
        Arc::new(Int64Array::from(vec![e.batch_seq; rows.len()])),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.ordinal).collect::<Vec<_>>(),
        )),
        Arc::new(
            TimestampMicrosecondArray::from(vec![e.prepared_at_us; rows.len()])
                .with_timezone("+00:00"),
        ),
    ]);
    RecordBatch::try_new(schema, columns).map_err(storage)
}

/// Write one batch's Parquet files. Empty CDC batches intentionally create no data files.
pub async fn write_batch_files(
    table: &Table,
    batch: RecordBatch,
) -> Result<Vec<DataFile>, CdcError> {
    if batch.num_rows() == 0 {
        return Ok(vec![]);
    }
    let writer = ParquetWriterBuilder::from_table_properties(
        &table.metadata().table_properties(),
        table.current_schema_ref(),
    )
    .map_err(storage)?;
    // Atomically reserve an attempt directory. PID/time alone can collide after clock
    // adjustment or PID reuse; create_dir prevents any old/committed file overwrite.
    let data = std::path::Path::new(table.metadata().location()).join("data");
    if !data.is_absolute() {
        return Err(CdcError::InvalidSchema);
    }
    std::fs::create_dir_all(&data)?;
    let mut nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(storage)?
        .as_nanos();
    let attempt = loop {
        let attempt = data.join(format!("attempt-{}-{nonce}", std::process::id()));
        match std::fs::create_dir(&attempt) {
            Ok(()) => break attempt,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                nonce = nonce.checked_add(1).ok_or(CdcError::Overflow)?;
            }
            Err(e) => return Err(e.into()),
        }
    };
    let filenames = DefaultFileNameGenerator::new("optd".into(), None, DataFileFormat::Parquet);
    let location = DefaultLocationGenerator::with_data_location(
        attempt.to_str().ok_or(CdcError::InvalidSchema)?.to_string(),
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        writer,
        table.file_io().clone(),
        location,
        filenames,
    );
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .map_err(storage)?;
    writer.write(batch).await.map_err(storage)?;
    writer.close().await.map_err(storage)
}

/// Durable storage boundaries exposed to instrumentation and subprocess fault tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitStage {
    /// Data files closed; metadata commit has not started.
    FilesWritten,
    /// Catalog has accepted the snapshot; memory has not been acknowledged.
    CatalogCommitted,
}

/// Iceberg sink restricted to the qualified local catalog and one OS-held writer.
pub struct IcebergCdcSink {
    catalog: Arc<LocalCatalog>,
    ident: TableIdent,
    generation: Generation,
    table_uuid: String,
    _owner: Option<WriterLock>,
    pending: Option<Vec<(String, String)>>,
}
impl IcebergCdcSink {
    /// Open or initialize a generation, then acquire lifetime exclusive writer ownership.
    pub async fn open(
        catalog: Arc<LocalCatalog>,
        ident: TableIdent,
        generation: Generation,
    ) -> Result<Self, CdcError> {
        let table = if catalog.table_exists(&ident).await.map_err(storage)? {
            catalog.load_table(&ident).await.map_err(storage)?
        } else {
            // Serialize initialization as well; a competing caller may finish creation first.
            let init_name =
                crate::cdc::fingerprint(&serde_json::json!(["optd-init/1", ident.to_string()]))?;
            let _init = catalog.writer_lock(&init_name[7..])?;
            if catalog.table_exists(&ident).await.map_err(storage)? {
                catalog.load_table(&ident).await.map_err(storage)?
            } else {
                let creation = TableCreation::builder()
                    .name(ident.name().to_string())
                    .schema(output_schema(&generation)?)
                    .format_version(FormatVersion::V2)
                    .properties(generation.properties()?)
                    .build();
                catalog
                    .create_table(ident.namespace(), creation)
                    .await
                    .map_err(storage)?
            }
        };
        let table_uuid = table.metadata().uuid().to_string();
        let owner = catalog.writer_lock(&table_uuid)?;
        let sink = Self {
            catalog,
            ident,
            generation,
            table_uuid,
            _owner: Some(owner),
            pending: None,
        };
        sink.validate_table(&sink.load().await?)?;
        sink.read_batches().await?;
        Ok(sink)
    }
    /// Immutable output definition.
    pub fn generation(&self) -> &Generation {
        &self.generation
    }
    /// Stable Iceberg output table identity.
    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }
    async fn load(&self) -> Result<Table, CdcError> {
        let table = self
            .catalog
            .load_table(&self.ident)
            .await
            .map_err(storage)?;
        self.validate_table(&table)?;
        Ok(table)
    }
    fn validate_table(&self, table: &Table) -> Result<(), CdcError> {
        if table.metadata().uuid().to_string() != self.table_uuid
            || table.metadata().format_version() != FormatVersion::V2
            || !table.metadata().default_partition_spec().is_unpartitioned()
            || table.metadata().current_schema().as_struct()
                != output_schema(&self.generation)?.as_struct()
        {
            return Err(CdcError::InvalidSchema);
        }
        for (k, v) in self.generation.properties()? {
            if table.metadata().properties().get(&k) != Some(&v) {
                return Err(CdcError::InvalidCheckpoint);
            }
        }
        Ok(())
    }
    fn envelopes(&self, table: &Table) -> Result<Vec<CommitEnvelope>, CdcError> {
        let mut segments = Vec::new();
        let mut visited = std::collections::BTreeSet::new();
        let mut id = table.metadata().current_snapshot_id();
        while let Some(snapshot_id) = id {
            if !visited.insert(snapshot_id) {
                return Err(CdcError::InvalidEnvelope);
            }
            let s = table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or(CdcError::InvalidCheckpoint)?;
            let props = &s.summary().additional_properties;
            if props.get("optd.protocol").map(String::as_str) != Some(PROTOCOL) {
                return Err(CdcError::InvalidEnvelope);
            }
            if let Some(archive) = props.get("optd.envelopes") {
                if s.summary().operation != Operation::Replace {
                    return Err(CdcError::InvalidEnvelope);
                }
                segments.push(decode_index("optd-envelope-archive/1", archive)?);
                break;
            }
            if s.summary().operation != Operation::Append {
                return Err(CdcError::Conflict);
            }
            let segment = if let Some(index) = props.get("optd.commits") {
                let entries = decode_index("optd-commit-index/1", index)?;
                let last = CommitEnvelope::from_json(
                    props.get("optd.commit").ok_or(CdcError::InvalidEnvelope)?,
                )?;
                if entries.last() != Some(&last) {
                    return Err(CdcError::InvalidEnvelope);
                }
                entries
            } else {
                vec![CommitEnvelope::from_json(
                    props.get("optd.commit").ok_or(CdcError::InvalidEnvelope)?,
                )?]
            };
            segments.push(segment);
            id = s.parent_snapshot_id();
        }
        let envelopes: Vec<_> = segments.into_iter().rev().flatten().collect();
        for i in 0..envelopes.len() {
            envelopes[i].validate_generation(&self.generation)?;
            envelopes[i].follows(i.checked_sub(1).map(|j| &envelopes[j]))?;
        }
        Ok(envelopes)
    }
    /// Independently scan actual Parquet data and validate every committed batch/envelope.
    /// Full audit, including endpoint invariants. Consumer reads can use a trusted checkpoint.
    pub async fn read_batches(&self) -> Result<Vec<PreparedBatch>, CdcError> {
        let _scan = self
            .catalog
            .reader_lock(&format!("scan-{}", self.table_uuid))?;
        self.read_batches_unlocked().await
    }
    async fn read_batches_unlocked(&self) -> Result<Vec<PreparedBatch>, CdcError> {
        self.read_since_unlocked(&crate::consumer::Cursor::default(), &Multiset::new())
            .await
    }
    async fn read_since_unlocked(
        &self,
        cursor: &crate::consumer::Cursor,
        seed: &Multiset,
    ) -> Result<Vec<PreparedBatch>, CdcError> {
        let table = self.load().await?;
        let envelopes = self.envelopes(&table)?;
        self.generation.schema().validate_relation(seed)?;
        if cursor.batch_seq == 0 {
            if cursor.batch_id.is_some() || !seed.is_empty() {
                return Err(CdcError::InvalidCheckpoint);
            }
        } else {
            let previous = usize::try_from(cursor.batch_seq)
                .ok()
                .and_then(|i| i.checked_sub(1))
                .and_then(|i| envelopes.get(i))
                .ok_or(CdcError::InvalidCheckpoint)?;
            if cursor.batch_id.as_ref() != Some(&previous.batch_id) {
                return Err(CdcError::InvalidCheckpoint);
            }
        }
        if envelopes.is_empty() {
            return Ok(vec![]);
        }
        let mut groups: BTreeMap<i64, Vec<ChangeRecord>> = BTreeMap::new();
        let batches = table
            .scan()
            .build()
            .map_err(storage)?
            .to_arrow()
            .await
            .map_err(storage)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(storage)?;
        let n = self.generation.schema().fields().len();
        for batch in batches {
            if batch.num_columns() != n + 8 {
                return Err(CdcError::InvalidRow);
            }
            fn column<T: Array + 'static>(batch: &RecordBatch, i: usize) -> Result<&T, CdcError> {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<T>()
                    .ok_or(CdcError::InvalidRow)
            }
            let op = column::<Int32Array>(&batch, n)?;
            let update = column::<BooleanArray>(&batch, n + 1)?;
            let weight = column::<Int64Array>(&batch, n + 2)?;
            let row_id = column::<StringArray>(&batch, n + 3)?;
            let batch_id = column::<StringArray>(&batch, n + 4)?;
            let seq = column::<Int64Array>(&batch, n + 5)?;
            let ordinal = column::<Int64Array>(&batch, n + 6)?;
            let time = column::<TimestampMicrosecondArray>(&batch, n + 7)?;
            for r in 0..batch.num_rows() {
                if [
                    op as &dyn Array,
                    update,
                    weight,
                    batch_id,
                    seq,
                    ordinal,
                    time,
                ]
                .iter()
                .any(|a| a.is_null(r))
                {
                    return Err(CdcError::InvalidRow);
                }
                let e = usize::try_from(seq.value(r))
                    .ok()
                    .and_then(|i| i.checked_sub(1))
                    .and_then(|i| envelopes.get(i))
                    .ok_or(CdcError::InvalidRow)?;
                if batch_id.value(r) != e.batch_id || time.value(r) != e.prepared_at_us {
                    return Err(CdcError::InvalidRow);
                }
                if e.batch_seq <= cursor.batch_seq {
                    continue;
                }
                let mut payload = Vec::new();
                for (i, f) in self.generation.schema().fields().iter().enumerate() {
                    payload.push(if batch.column(i).is_null(r) {
                        Value::Null
                    } else {
                        match f.data_type {
                            PayloadType::Long => {
                                Value::Int(column::<Int64Array>(&batch, i)?.value(r))
                            }
                            PayloadType::String => {
                                Value::Str(column::<StringArray>(&batch, i)?.value(r).to_string())
                            }
                        }
                    });
                }
                groups.entry(e.batch_seq).or_default().push(ChangeRecord {
                    payload,
                    op_type: op.value(r),
                    is_update: update.value(r),
                    weight: weight.value(r),
                    row_id: if row_id.is_null(r) {
                        None
                    } else {
                        Some(row_id.value(r).to_string())
                    },
                    ordinal: ordinal.value(r),
                });
            }
        }
        let mut out = Vec::new();
        let mut relation = seed.clone();
        for envelope in envelopes
            .into_iter()
            .filter(|e| e.batch_seq > cursor.batch_seq)
        {
            let mut rows = groups.remove(&envelope.batch_seq).unwrap_or_default();
            rows.sort_by_key(|r| r.ordinal);
            let batch = PreparedBatch::from_records(&self.generation, envelope, rows)?;
            relation = batch.apply_to(&self.generation, &relation)?;
            out.push(batch);
        }
        if !groups.is_empty() {
            return Err(CdcError::InvalidRow);
        }
        Ok(out)
    }
    /// Emit with instrumentation at real durability boundaries. Observer errors after commit
    /// exercise lost-ack resolution; a blocking observer lets a separate process kill the writer.
    pub async fn emit_with_observer(
        &mut self,
        batch: &PreparedBatch,
        observer: &(dyn Fn(CommitStage) -> Result<(), CdcError> + Send + Sync),
    ) -> Result<CommitEnvelope, CdcError> {
        if self._owner.is_none() {
            return Err(CdcError::Ownership);
        }
        self.emit_many_with_observer(std::slice::from_ref(batch), observer)
            .await?
            .pop()
            .ok_or(CdcError::InvalidEnvelope)
    }
    /// Publish contiguous logical batches in one physical snapshot, including empty batches.
    pub async fn emit_many(
        &mut self,
        batches: &[PreparedBatch],
    ) -> Result<Vec<CommitEnvelope>, CdcError> {
        self.emit_many_with_observer(batches, &|_| Ok(())).await
    }
    /// Batch publication with instrumentation at the real durability boundaries.
    pub async fn emit_many_with_observer(
        &mut self,
        batches: &[PreparedBatch],
        observer: &(dyn Fn(CommitStage) -> Result<(), CdcError> + Send + Sync),
    ) -> Result<Vec<CommitEnvelope>, CdcError> {
        if self._owner.is_none() {
            return Err(CdcError::Ownership);
        }
        if batches.is_empty() {
            return Err(CdcError::InvalidEnvelope);
        }
        let identity: Vec<_> = batches
            .iter()
            .map(|b| {
                (
                    b.envelope().batch_id.clone(),
                    b.envelope().delta_hash.clone(),
                )
            })
            .collect();
        if self.pending.as_ref().is_some_and(|p| p != &identity) {
            return Err(CdcError::Conflict);
        }
        for (i, batch) in batches.iter().enumerate() {
            batch.envelope().validate_generation(&self.generation)?;
            if i > 0 {
                batch.envelope().follows(Some(batches[i - 1].envelope()))?;
            }
        }
        let existing = self.read_batches().await?;
        let mut skip = 0;
        for batch in batches {
            if let Some(found) = existing
                .iter()
                .find(|b| b.envelope().batch_seq == batch.envelope().batch_seq)
            {
                if !same_logical_batch(found, batch) {
                    return Err(CdcError::DigestMismatch);
                }
                skip += 1;
            } else {
                break;
            }
        }
        if skip == batches.len() {
            self.pending = None;
            return batches
                .iter()
                .map(|batch| {
                    existing
                        .iter()
                        .find(|b| same_logical_batch(b, batch))
                        .map(|b| b.envelope().clone())
                        .ok_or(CdcError::UnknownCommit)
                })
                .collect();
        }
        let remaining = &batches[skip..];
        remaining[0]
            .envelope()
            .follows(existing.last().map(|b| b.envelope()))?;
        let mut relation = Multiset::new();
        for batch in existing.iter().chain(remaining.iter()) {
            relation = batch.apply_to(&self.generation, &relation)?;
        }
        self.pending = Some(identity);
        let table = self.load().await?;
        let mut files = Vec::new();
        for batch in remaining {
            files.extend(write_batch_files(&table, record_batch(&self.generation, batch)?).await?);
        }
        observer(CommitStage::FilesWritten)?;
        let entries: Vec<_> = remaining.iter().map(|b| b.envelope().clone()).collect();
        let last = entries.last().ok_or(CdcError::InvalidEnvelope)?;
        let props = HashMap::from([
            ("optd.protocol".into(), PROTOCOL.into()),
            ("optd.commit".into(), serde_json::to_string(last)?),
            (
                "optd.commits".into(),
                encode_index("optd-commit-index/1", &entries)?,
            ),
        ]);
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(files)
            .set_snapshot_properties(props)
            .apply(tx)
            .map_err(storage)?;
        let outcome = match tx.commit(self.catalog.as_ref()).await.map_err(storage) {
            Ok(_) => observer(CommitStage::CatalogCommitted),
            Err(e) => Err(e),
        };
        let committed = self.read_batches().await?;
        let found = batches
            .iter()
            .all(|batch| committed.iter().any(|b| same_logical_batch(b, batch)));
        if found {
            self.pending = None;
            return batches
                .iter()
                .map(|batch| {
                    committed
                        .iter()
                        .find(|b| same_logical_batch(b, batch))
                        .map(|b| b.envelope().clone())
                        .ok_or(CdcError::UnknownCommit)
                })
                .collect();
        }
        outcome?;
        Err(CdcError::UnknownCommit)
    }
}

/// Read-only log access that can coexist with the producer's lifetime ownership.
pub struct CdcLogReader {
    inner: IcebergCdcSink,
}
impl CdcLogReader {
    /// Open an existing output, checking its UUID, schema and definition without creating it.
    pub async fn open(
        catalog: Arc<LocalCatalog>,
        ident: TableIdent,
        generation: Generation,
    ) -> Result<Self, CdcError> {
        let table = catalog.load_table(&ident).await.map_err(storage)?;
        let inner = IcebergCdcSink {
            catalog,
            ident,
            generation,
            table_uuid: table.metadata().uuid().to_string(),
            _owner: None,
            pending: None,
        };
        inner.validate_table(&table)?;
        inner.envelopes(&table)?;
        Ok(Self { inner })
    }
    /// Read a validated, immutable observation of the log.
    pub async fn read_batches(&self) -> Result<Vec<PreparedBatch>, CdcError> {
        self.inner.read_batches().await
    }
    /// Validate and replay only a suffix from a trusted destination checkpoint.
    /// The cursor must still resolve in the envelope archive. Physical scans remain full-table.
    pub async fn read_since(
        &self,
        cursor: &crate::consumer::Cursor,
        state: &Multiset,
    ) -> Result<Vec<PreparedBatch>, CdcError> {
        let _scan = self
            .inner
            .catalog
            .reader_lock(&format!("scan-{}", self.table_uuid()))?;
        self.inner.read_since_unlocked(cursor, state).await
    }
    /// Stable output table identity.
    pub fn table_uuid(&self) -> &str {
        self.inner.table_uuid()
    }
    /// Immutable generation definition.
    pub fn generation(&self) -> &Generation {
        self.inner.generation()
    }
    pub(crate) fn catalog(&self) -> &LocalCatalog {
        &self.inner.catalog
    }
    pub(crate) fn table_name(&self) -> String {
        self.inner.ident.to_string()
    }
}
impl DeltaSink for IcebergCdcSink {
    async fn last_checkpoint(&self) -> Result<Option<CommitEnvelope>, CdcError> {
        Ok(self
            .read_batches()
            .await?
            .last()
            .map(|b| b.envelope().clone()))
    }
    async fn emit(&mut self, batch: &PreparedBatch) -> Result<CommitEnvelope, CdcError> {
        self.emit_with_observer(batch, &|_| Ok(())).await
    }
}

/// Recoverable single-view LOAD/CATCH-UP loop. Memory advances only after sink confirmation.
pub struct CdcLoop {
    catalog: Arc<LocalCatalog>,
    source_ident: TableIdent,
    source_id: crate::change::TableId,
    sink: IcebergCdcSink,
    view: crate::engine::View,
    previous: Option<CommitEnvelope>,
    pending: Option<(PreparedBatch, Option<crate::engine::PreparedRefresh>)>,
    writer_id: String,
}
impl CdcLoop {
    /// Rebuild at the durable checkpoint, or prepare a full bootstrap from pinned input.
    pub async fn recover(
        catalog: Arc<LocalCatalog>,
        source_ident: TableIdent,
        output_ident: TableIdent,
        generation: Generation,
        plan: crate::plan::IncPlanNode,
        writer_id: String,
    ) -> Result<Self, CdcError> {
        let expected = Generation::new(
            generation.view_id().into(),
            generation.generation_id().into(),
            generation.schema().clone(),
            &plan,
            generation.bindings().clone(),
        )?;
        if expected.properties()? != generation.properties()? {
            return Err(CdcError::InvalidCheckpoint);
        }
        let source_id = *generation
            .bindings()
            .keys()
            .next()
            .ok_or(CdcError::InvalidCheckpoint)?;
        let sink = IcebergCdcSink::open(catalog.clone(), output_ident, generation.clone()).await?;
        let previous = sink.last_checkpoint().await?;
        let table = catalog.load_table(&source_ident).await.map_err(storage)?;
        if table.metadata().uuid().to_string() != generation.bindings()[&source_id].table_id {
            return Err(CdcError::InvalidCheckpoint);
        }
        let observation = crate::iceberg_source::IcebergSource::observe(
            source_id,
            &table,
            previous.as_ref().map(|e| &e.source_to[0]),
        )
        .await
        .map_err(storage)?;
        let position = match &previous {
            Some(e) => u64::try_from(e.source_to[0].sequence_number.unwrap_or(0))
                .map_err(|_| CdcError::InvalidCheckpoint)?,
            None => crate::source::DeltaSource::current_position(&observation),
        };
        let view = crate::engine::View::load_at(plan, &observation, vec![source_id], position)?;
        let pending = if previous.is_none() {
            Some((
                PreparedBatch::from_endpoints(
                    &generation,
                    None,
                    vec![observation.checkpoint().clone()],
                    &Multiset::new(),
                    view.contents(),
                    now_us()?,
                    writer_id.clone(),
                )?,
                None,
            ))
        } else {
            let mut folded = Multiset::new();
            for batch in sink.read_batches().await? {
                folded = folded
                    .try_union(&batch.delta()?)
                    .map_err(|_| CdcError::Overflow)?;
            }
            if &folded != view.contents() {
                return Err(CdcError::DigestMismatch);
            }
            None
        };
        Ok(Self {
            catalog,
            source_ident,
            source_id,
            sink,
            view,
            previous,
            pending,
            writer_id,
        })
    }
    /// Current in-memory view, still at the predecessor when an output attempt fails.
    pub fn view(&self) -> &crate::engine::View {
        &self.view
    }
    /// Inspect durable output without consuming or advancing it.
    pub fn sink(&self) -> &IcebergCdcSink {
        &self.sink
    }
    /// Durable producer checkpoint held after successful read-back.
    pub fn checkpoint(&self) -> Option<&CommitEnvelope> {
        self.previous.as_ref()
    }
    /// Publish bootstrap or one new source range; None means no source position advanced.
    pub async fn step(&mut self) -> Result<Option<CommitEnvelope>, CdcError> {
        self.step_with_observer(&|_| Ok(())).await
    }
    /// Step with instrumentation at the actual file and catalog boundaries.
    pub async fn step_with_observer(
        &mut self,
        observer: &(dyn Fn(CommitStage) -> Result<(), CdcError> + Send + Sync),
    ) -> Result<Option<CommitEnvelope>, CdcError> {
        if self.pending.is_none() {
            let previous = self.previous.as_ref().ok_or(CdcError::InvalidCheckpoint)?;
            let table = self
                .catalog
                .load_table(&self.source_ident)
                .await
                .map_err(storage)?;
            let observation = crate::iceberg_source::IcebergSource::observe(
                self.source_id,
                &table,
                Some(&previous.source_to[0]),
            )
            .await
            .map_err(storage)?;
            let Some(candidate) = self.view.prepare_refresh(&observation)? else {
                return Ok(None);
            };
            let batch = PreparedBatch::from_endpoints(
                self.sink.generation(),
                Some(previous),
                vec![observation.checkpoint().clone()],
                self.view.contents(),
                candidate.contents(),
                now_us()?,
                self.writer_id.clone(),
            )?;
            self.pending = Some((batch, Some(candidate)));
        }
        let (batch, candidate) = self.pending.as_ref().ok_or(CdcError::InvalidEnvelope)?;
        let committed = self.sink.emit_with_observer(batch, observer).await?;
        if let Some(candidate) = candidate {
            self.view.acknowledge(candidate)?;
        }
        self.previous = Some(committed.clone());
        self.pending = None;
        Ok(Some(committed))
    }
}
fn now_us() -> Result<i64, CdcError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(storage)?
            .as_micros(),
    )
    .map_err(|_| CdcError::Overflow)
}

fn encode_index(domain: &str, envelopes: &[CommitEnvelope]) -> Result<String, CdcError> {
    if envelopes.is_empty() {
        return Err(CdcError::InvalidEnvelope);
    }
    let checksum = crate::cdc::fingerprint(&serde_json::json!([domain, envelopes]))?;
    Ok(serde_json::to_string(
        &serde_json::json!({"envelopes":envelopes,"checksum":checksum}),
    )?)
}
fn decode_index(domain: &str, input: &str) -> Result<Vec<CommitEnvelope>, CdcError> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Index {
        envelopes: Vec<CommitEnvelope>,
        checksum: String,
    }
    let index: Index = serde_json::from_value(crate::cdc::parse_unique_json(input)?)?;
    if index.envelopes.is_empty() {
        return Err(CdcError::InvalidEnvelope);
    }
    if crate::cdc::fingerprint(&serde_json::json!([domain, index.envelopes]))? != index.checksum {
        return Err(CdcError::DigestMismatch);
    }
    Ok(index.envelopes)
}

impl IcebergCdcSink {
    /// Rewrite physical files while preserving all logical rows and envelope coordinates.
    pub async fn compact(&mut self) -> Result<(), CdcError> {
        self.compact_with_observer(&|_| Ok(())).await
    }
    /// Compact with crash boundaries before and after the atomic catalog publication.
    pub async fn compact_with_observer(
        &mut self,
        observer: &(dyn Fn(CommitStage) -> Result<(), CdcError> + Send + Sync),
    ) -> Result<(), CdcError> {
        use iceberg::spec::{ManifestListWriter, ManifestWriterBuilder, Snapshot, Summary};
        if self._owner.is_none() {
            return Err(CdcError::Ownership);
        }
        if self.pending.is_some() {
            return Err(CdcError::Conflict);
        }
        let _scan = self
            .catalog
            .writer_lock(&format!("scan-{}", self.table_uuid))?;
        let batches = self.read_batches_unlocked().await?;
        if batches.is_empty() {
            return Ok(());
        }
        let table = self.load().await?;
        let envelopes: Vec<_> = batches.iter().map(|b| b.envelope().clone()).collect();
        let arrow_batches: Vec<_> = batches
            .iter()
            .map(|b| record_batch(&self.generation, b))
            .collect::<Result<_, _>>()?;
        let schema = arrow_batches[0].schema();
        let merged = arrow::compute::concat_batches(&schema, &arrow_batches).map_err(storage)?;
        let files = write_batch_files(&table, merged).await?;
        let seq = table.metadata().next_sequence_number();
        let mut id = now_us()?;
        while table.metadata().snapshot_by_id(id).is_some() {
            id = id.checked_add(1).ok_or(CdcError::Overflow)?;
        }
        // Each attempt reserves a fresh directory, including retries after an uncommitted crash.
        let base = std::path::Path::new(table.metadata().location()).join("metadata");
        let mut attempt = id;
        let dir = loop {
            let dir = base.join(format!("compact-{attempt}"));
            match std::fs::create_dir(&dir) {
                Ok(()) => break dir,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    attempt = attempt.checked_add(1).ok_or(CdcError::Overflow)?;
                }
                Err(e) => return Err(e.into()),
            }
        };
        let path = |name: &str| -> Result<String, CdcError> {
            Ok(dir
                .join(name)
                .to_str()
                .ok_or(CdcError::InvalidSchema)?
                .to_string())
        };
        let mut manifests = Vec::new();
        if !files.is_empty() {
            let output = table
                .file_io()
                .new_output(path("data.avro")?)
                .map_err(storage)?;
            let mut manifest = ManifestWriterBuilder::new(
                output,
                Some(id),
                table.current_schema_ref(),
                table.metadata().default_partition_spec().as_ref().clone(),
            )
            .build_v2_data();
            for file in files {
                manifest.add_file(file, seq).map_err(storage)?;
            }
            manifests.push(manifest.write_manifest_file().await.map_err(storage)?);
        }
        let list = path("list.avro")?;
        let writer = table
            .file_io()
            .new_output(&list)
            .map_err(storage)?
            .writer()
            .await
            .map_err(storage)?;
        let mut list_writer =
            ManifestListWriter::v2(writer, id, table.metadata().current_snapshot_id(), seq);
        list_writer
            .add_manifests(manifests.into_iter())
            .map_err(storage)?;
        list_writer.close().await.map_err(storage)?;
        observer(CommitStage::FilesWritten)?;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(table.metadata().current_snapshot_id())
            .with_sequence_number(seq)
            .with_manifest_list(list)
            .with_timestamp_ms(now_us()? / 1000)
            .with_schema_id(table.metadata().current_schema_id())
            .with_summary(Summary {
                operation: Operation::Replace,
                additional_properties: HashMap::from([
                    ("optd.protocol".into(), PROTOCOL.into()),
                    (
                        "optd.envelopes".into(),
                        encode_index("optd-envelope-archive/1", &envelopes)?,
                    ),
                    (
                        "optd.commit".into(),
                        serde_json::to_string(envelopes.last().ok_or(CdcError::InvalidEnvelope)?)?,
                    ),
                ]),
            })
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(table.metadata_location().map(str::to_string))
            .set_branch_snapshot(snapshot, "main")
            .map_err(storage)?
            .build()
            .map_err(storage)?
            .metadata;
        self.catalog
            .publish_maintenance(&table, metadata)
            .await
            .map_err(storage)?;
        let outcome = observer(CommitStage::CatalogCommitted);
        let readback = self.read_batches_unlocked().await?;
        if !same_batches(&batches, &readback) {
            return Err(CdcError::DigestMismatch);
        }
        let _ = outcome;
        Ok(())
    }
    /// Expire old output snapshots only after a verified current compaction archive.
    /// All logical CDC rows and all files remain retained; source history is untouched.
    pub async fn expire_snapshots(&mut self) -> Result<usize, CdcError> {
        if self._owner.is_none() {
            return Err(CdcError::Ownership);
        }
        if self.pending.is_some() {
            return Err(CdcError::Conflict);
        }
        let _scan = self
            .catalog
            .writer_lock(&format!("scan-{}", self.table_uuid))?;
        let before = self.read_batches_unlocked().await?;
        let table = self.load().await?;
        let current = table
            .metadata()
            .current_snapshot()
            .ok_or(CdcError::InvalidCheckpoint)?;
        let archive = current
            .summary()
            .additional_properties
            .get("optd.envelopes")
            .ok_or(CdcError::InvalidCheckpoint)?;
        let entries = decode_index("optd-envelope-archive/1", archive)?;
        if entries
            != before
                .iter()
                .map(|b| b.envelope().clone())
                .collect::<Vec<_>>()
        {
            return Err(CdcError::InvalidEnvelope);
        }
        let expired: Vec<_> = table
            .metadata()
            .snapshots()
            .map(|s| s.snapshot_id())
            .filter(|id| *id != current.snapshot_id())
            .collect();
        if expired.is_empty() {
            return Ok(0);
        }
        let metadata = table
            .metadata()
            .clone()
            .into_builder(table.metadata_location().map(str::to_string))
            .remove_snapshots(&expired)
            .build()
            .map_err(storage)?
            .metadata;
        self.catalog
            .publish_maintenance(&table, metadata)
            .await
            .map_err(storage)?;
        if !same_batches(&before, &self.read_batches_unlocked().await?) {
            return Err(CdcError::DigestMismatch);
        }
        Ok(expired.len())
    }
}
fn same_batches(a: &[PreparedBatch], b: &[PreparedBatch]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(a, b)| a.envelope() == b.envelope() && a.rows() == b.rows())
}

fn same_logical_batch(a: &PreparedBatch, b: &PreparedBatch) -> bool {
    a.envelope().batch_id == b.envelope().batch_id
        && a.envelope().delta_hash == b.envelope().delta_hash
        && a.rows() == b.rows()
}
