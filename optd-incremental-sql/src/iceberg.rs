//! Typed, explicitly pinned endpoints from the qualified local Iceberg catalog.
//!
//! Changed inputs are full snapshot differences, not a native changelog. A
//! complete vector selects a tuple of source states; it does not establish an
//! upstream cross-table transaction.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::iceberg::arrow::schema_to_arrow_schema;
use ::iceberg::spec::{DataFileFormat, FormatVersion, ManifestContentType, ManifestList, Schema};
use ::iceberg::table::Table;
use ::iceberg::{Catalog, TableIdent};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use futures::TryStreamExt;
use optd_incremental::local_catalog::{LocalCatalog, WriterLock};
use serde_json::json;

use crate::bag::Bag;
use crate::source_vector::{
    arrow_layout_digest, canonical_arrow_layout, iceberg_schema_digest, snapshot_descriptor_digest,
    CatalogBinding, PinnedSnapshot, PinnedSource, PinnedSourceVector,
};
use crate::{Error, Result, TableChange, MAX_MATERIALIZED_ROWS};

const MAX_ENDPOINT_BYTES: usize = 256 * 1024 * 1024;
const MAX_MANIFEST_LIST_BYTES: u64 = 16 * 1024 * 1024;
const MAX_SCAN_TASKS: usize = 100_000;

/// An explicit input selection; there is deliberately no moving-head variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotSelection {
    /// Admit a newly selected table only if it has never had a snapshot.
    Empty,
    /// Read this exact retained mainline snapshot.
    Snapshot(i64),
}

/// Source acquisition work, independent of SQL operator and checkpoint work.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceWork {
    /// Tables whose cursor changes, including physically rewritten equal data.
    pub changed_sources: usize,
    /// Actual complete nonempty snapshot endpoint reads.
    pub endpoint_scans: usize,
    /// Actual Arrow record batches decoded across those endpoint reads.
    pub decoded_batches: usize,
    /// Decoded rows across all actual endpoint reads, before consolidation.
    pub decoded_rows: usize,
    /// Sum of decoded Arrow array memory sizes across those reads.
    pub decoded_bytes: usize,
    /// Distinct consolidated signed input rows submitted to SQL.
    pub signed_change_rows: usize,
}

/// Fixed SQL source names bound to one canonical local Iceberg catalog.
pub struct LocalIcebergSources {
    root: PathBuf,
    catalog: LocalCatalog,
    bindings: BTreeMap<String, TableIdent>,
}

pub(crate) struct Observation {
    tables: BTreeMap<String, Table>,
    // Cooperating maintenance cannot remove files during an admitted refresh.
    // This is not a lease against arbitrary external writers or vacuums.
    _pins: Vec<WriterLock>,
}

pub(crate) struct SourceRefresh {
    pub changes: Vec<TableChange>,
    pub tables: BTreeMap<String, RecordBatch>,
    pub work: SourceWork,
}

impl LocalIcebergSources {
    /// Open the qualified local catalog with a complete fixed SQL source map.
    pub fn open(root: impl AsRef<Path>, bindings: BTreeMap<String, TableIdent>) -> Result<Self> {
        if bindings.is_empty() || bindings.keys().any(|name| name.trim().is_empty()) {
            return Err(invalid("Iceberg source bindings must be nonempty"));
        }
        let catalog = LocalCatalog::open(root.as_ref())?;
        let root = std::fs::canonicalize(root.as_ref())?;
        if root.to_str().is_none() {
            return Err(invalid("Iceberg catalog root must be UTF-8"));
        }
        Ok(Self {
            root,
            catalog,
            bindings,
        })
    }

    /// Freeze identities for every explicit selection, without reading rows.
    ///
    /// Metadata observations are independent across tables. The returned tuple
    /// is not a distributed database snapshot. Later heads never enter this tuple.
    pub async fn pin(
        &self,
        selections: &BTreeMap<String, SnapshotSelection>,
    ) -> Result<PinnedSourceVector> {
        if selections.keys().ne(self.bindings.keys()) {
            return Err(invalid(
                "Iceberg selection must name every bound SQL source exactly once",
            ));
        }
        let mut sources = BTreeMap::new();
        for (name, ident) in &self.bindings {
            let table = self.catalog.load_table(ident).await?;
            let selected = selections
                .get(name)
                .ok_or_else(|| invalid("missing source selection"))?;
            let snapshot = match selected {
                SnapshotSelection::Empty => {
                    if table.metadata().current_snapshot_id().is_some()
                        || table.metadata().snapshots().next().is_some()
                    {
                        return Err(invalid(
                            "new Empty selection requires a table with no snapshots",
                        ));
                    }
                    PinnedSnapshot::Empty
                }
                SnapshotSelection::Snapshot(id) => {
                    require_mainline(&table, *id)?;
                    snapshot_identity(&table, *id)?
                }
            };
            sources.insert(name.clone(), self.describe(&table, snapshot)?);
        }
        let vector = PinnedSourceVector {
            version: 1,
            sources,
        };
        vector.validate_for(&empty_tables(&vector)?)?;
        Ok(vector)
    }

    pub(crate) async fn observe(
        &self,
        target: &PinnedSourceVector,
        before: Option<&PinnedSourceVector>,
    ) -> Result<Observation> {
        if target.sources.keys().ne(self.bindings.keys()) {
            return Err(invalid(
                "source vector does not match the fixed SQL bindings",
            ));
        }
        target.validate_for(&empty_tables(target)?)?;
        if let Some(previous) = before {
            previous.validate_for(&empty_tables(previous)?)?;
            if target != previous {
                target.validate_successor(previous)?;
            }
        }
        let mut tables = BTreeMap::new();
        let mut pins = Vec::new();
        for (name, ident) in &self.bindings {
            let table = self.catalog.load_table(ident).await?;
            pins.push(
                self.catalog
                    .reader_lock(&table.metadata().uuid().to_string())
                    .map_err(|error| invalid(format!("Iceberg source read ownership: {error}")))?,
            );
            let source = target
                .sources
                .get(name)
                .ok_or_else(|| invalid("missing source"))?;
            let previous = before.and_then(|vector| vector.sources.get(name));
            let expected = self.describe(&table, source.snapshot.clone())?;
            if &expected != source {
                return Err(invalid(
                    "Iceberg source identity, schema or snapshot descriptor changed",
                ));
            }
            match &source.snapshot {
                PinnedSnapshot::Empty => {
                    if previous.is_none()
                        && (table.metadata().current_snapshot_id().is_some()
                            || table.metadata().snapshots().next().is_some())
                    {
                        return Err(invalid(
                            "new Empty endpoint no longer names an initial empty table",
                        ));
                    }
                }
                PinnedSnapshot::Snapshot { id, .. } => require_mainline(&table, *id)?,
            }
            if let Some(previous) = previous {
                if self.describe(&table, previous.snapshot.clone())? != *previous {
                    return Err(invalid(
                        "committed Iceberg endpoint is unavailable or changed",
                    ));
                }
                require_successor(&table, &previous.snapshot, &source.snapshot)?;
            }
            tables.insert(name.clone(), table);
        }
        Ok(Observation {
            tables,
            _pins: pins,
        })
    }

    fn describe(&self, table: &Table, snapshot: PinnedSnapshot) -> Result<PinnedSource> {
        let metadata = table.metadata();
        if metadata.format_version() != FormatVersion::V2 {
            return Err(invalid("typed Iceberg input requires format v2"));
        }
        let schema = match &snapshot {
            PinnedSnapshot::Empty => metadata.current_schema().clone(),
            PinnedSnapshot::Snapshot { id, .. } => {
                let actual = snapshot_identity(table, *id)?;
                if actual != snapshot {
                    return Err(invalid(
                        "pinned Iceberg snapshot descriptor does not match metadata",
                    ));
                }
                let retained = metadata
                    .snapshot_by_id(*id)
                    .ok_or_else(|| invalid("pinned Iceberg snapshot has expired"))?;
                let schema_id = retained
                    .schema_id()
                    .ok_or_else(|| invalid("snapshot has no explicit schema ID"))?;
                metadata
                    .schema_by_id(schema_id)
                    .ok_or_else(|| invalid("snapshot schema has expired"))?
                    .clone()
            }
        };
        // Fixed schema includes unused columns, field IDs, order and defaults.
        let iceberg_schema = serde_json::to_value(schema.as_ref())?;
        if iceberg_schema != serde_json::to_value(metadata.current_schema().as_ref())? {
            return Err(invalid(
                "schema evolution is outside the typed Iceberg profile",
            ));
        }
        let arrow_schema = schema_to_arrow_schema(&schema)?;
        let arrow_layout = canonical_arrow_layout(&arrow_schema)?;
        Ok(PinnedSource {
            catalog: CatalogBinding {
                kind: "local/1".into(),
                canonical_root: self
                    .root
                    .to_str()
                    .ok_or_else(|| invalid("non-UTF-8 catalog root"))?
                    .into(),
            },
            namespace: table.identifier().namespace().iter().cloned().collect(),
            name: table.identifier().name().into(),
            table_uuid: metadata.uuid().to_string(),
            reference: "main".into(),
            snapshot,
            schema_id: metadata.current_schema_id(),
            iceberg_schema_sha256: iceberg_schema_digest(&iceberg_schema)?,
            iceberg_schema,
            arrow_layout_sha256: arrow_layout_digest(&arrow_layout)?,
            arrow_layout,
            normalizer_version: 1,
        })
    }

    pub(crate) async fn read_all(
        &self,
        observed: &Observation,
        vector: &PinnedSourceVector,
    ) -> Result<(BTreeMap<String, RecordBatch>, SourceWork)> {
        let mut work = SourceWork::default();
        let mut tables = BTreeMap::new();
        for (name, source) in &vector.sources {
            let table = observed
                .tables
                .get(name)
                .ok_or_else(|| invalid("missing observed table"))?;
            tables.insert(name.clone(), read_endpoint(table, source, &mut work).await?);
        }
        vector.validate_for(&tables)?;
        Ok((tables, work))
    }

    pub(crate) async fn changes(
        &self,
        observed: &Observation,
        before: &PinnedSourceVector,
        target: &PinnedSourceVector,
        cache: &BTreeMap<String, RecordBatch>,
    ) -> Result<SourceRefresh> {
        before.validate_for(cache)?;
        let mut tables = cache.clone();
        let mut changes = Vec::new();
        let mut work = SourceWork::default();
        for (name, source) in &target.sources {
            let previous = before
                .sources
                .get(name)
                .ok_or_else(|| invalid("missing committed source"))?;
            if source.snapshot == previous.snapshot {
                continue;
            }
            work.changed_sources = work.changed_sources.checked_add(1).ok_or(Error::Overflow)?;
            let table = observed
                .tables
                .get(name)
                .ok_or_else(|| invalid("missing observed source"))?;
            let old = read_endpoint(table, previous, &mut work).await?;
            let cached = cache
                .get(name)
                .ok_or_else(|| invalid("missing committed source cache"))?;
            if Bag::from_batch(&old)? != Bag::from_batch(cached)? {
                return Err(invalid(
                    "exact committed Iceberg endpoint differs from its source cache",
                ));
            }
            let next = read_endpoint(table, source, &mut work).await?;
            let delta = Bag::from_batch(&next)?.difference(&Bag::from_batch(&old)?)?;
            work.signed_change_rows = work
                .signed_change_rows
                .checked_add(delta.len())
                .ok_or(Error::Overflow)?;
            let weighted = delta.weighted_batch()?;
            let weights = weighted
                .column(weighted.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| invalid("invalid typed source weights"))?
                .values()
                .to_vec();
            let rows = RecordBatch::try_new(
                next.schema(),
                weighted.columns()[..next.num_columns()].to_vec(),
            )?;
            changes.push(TableChange {
                table: name.clone(),
                rows,
                weights,
            });
            tables.insert(name.clone(), next);
        }
        target.validate_for(&tables)?;
        Ok(SourceRefresh {
            changes,
            tables,
            work,
        })
    }
}

pub(crate) fn same_tables(
    expected: &BTreeMap<String, RecordBatch>,
    actual: &BTreeMap<String, RecordBatch>,
) -> Result<bool> {
    if expected.keys().ne(actual.keys()) {
        return Ok(false);
    }
    for (name, batch) in expected {
        let other = actual
            .get(name)
            .ok_or_else(|| invalid("source cache name mismatch"))?;
        if Bag::from_batch(batch)? != Bag::from_batch(other)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn empty_tables(vector: &PinnedSourceVector) -> Result<BTreeMap<String, RecordBatch>> {
    vector
        .sources
        .iter()
        .map(|(name, source)| {
            let schema: Schema = serde_json::from_value(source.iceberg_schema.clone())?;
            Ok((
                name.clone(),
                RecordBatch::new_empty(Arc::new(schema_to_arrow_schema(&schema)?)),
            ))
        })
        .collect()
}

fn snapshot_identity(table: &Table, id: i64) -> Result<PinnedSnapshot> {
    let snapshot = table
        .metadata()
        .snapshot_by_id(id)
        .ok_or_else(|| invalid("pinned Iceberg snapshot is not retained"))?;
    if snapshot.schema_id().is_none() || snapshot.sequence_number() <= 0 {
        return Err(invalid(
            "snapshot requires an explicit schema ID and positive v2 sequence",
        ));
    }
    let descriptor = json!({
        "snapshot_id":snapshot.snapshot_id(), "parent_snapshot_id":snapshot.parent_snapshot_id(),
        "sequence_number":snapshot.sequence_number(), "timestamp_ms":snapshot.timestamp_ms(),
        "manifest_list":snapshot.manifest_list(), "summary":snapshot.summary(),
        "schema_id":snapshot.schema_id(), "encryption_key_id":snapshot.encryption_key_id(),
        "first_row_id":snapshot.first_row_id(), "added_rows_count":snapshot.added_rows_count()
    });
    Ok(PinnedSnapshot::Snapshot {
        id,
        sequence_number: snapshot.sequence_number(),
        descriptor_sha256: snapshot_descriptor_digest(&descriptor)?,
    })
}

fn require_mainline(table: &Table, selected: i64) -> Result<()> {
    let mut current = table.metadata().current_snapshot_id();
    let mut visited = HashSet::new();
    while let Some(id) = current {
        if !visited.insert(id) {
            return Err(invalid("Iceberg ancestry contains a cycle"));
        }
        let snapshot = table
            .metadata()
            .snapshot_by_id(id)
            .ok_or_else(|| invalid("Iceberg mainline ancestry is missing"))?;
        if id == selected {
            return Ok(());
        }
        current = snapshot.parent_snapshot_id();
    }
    Err(invalid(
        "selected Iceberg snapshot is not on the observed mainline",
    ))
}

fn require_successor(
    table: &Table,
    before: &PinnedSnapshot,
    target: &PinnedSnapshot,
) -> Result<()> {
    if before == target {
        return Ok(());
    }
    let mut current = match target {
        PinnedSnapshot::Empty => return Err(invalid("Iceberg source cannot roll back to Empty")),
        PinnedSnapshot::Snapshot { id, .. } => Some(*id),
    };
    let mut visited = HashSet::new();
    while let Some(id) = current {
        if !visited.insert(id) {
            return Err(invalid("Iceberg ancestry contains a cycle"));
        }
        let snapshot = table
            .metadata()
            .snapshot_by_id(id)
            .ok_or_else(|| invalid("required Iceberg ancestor has expired"))?;
        if matches!(before, PinnedSnapshot::Snapshot { id: old, .. } if *old == id) {
            return Ok(());
        }
        current = snapshot.parent_snapshot_id();
    }
    if matches!(before, PinnedSnapshot::Empty) {
        Ok(())
    } else {
        Err(invalid(
            "target Iceberg snapshot is not a descendant of the committed cursor",
        ))
    }
}

async fn read_endpoint(
    table: &Table,
    source: &PinnedSource,
    work: &mut SourceWork,
) -> Result<RecordBatch> {
    let schema: Schema = serde_json::from_value(source.iceberg_schema.clone())?;
    let schema = Arc::new(schema_to_arrow_schema(&schema)?);
    let PinnedSnapshot::Snapshot { id, .. } = source.snapshot else {
        return Ok(RecordBatch::new_empty(schema));
    };
    let snapshot = table
        .metadata()
        .snapshot_by_id(id)
        .ok_or_else(|| invalid("missing scan snapshot"))?;
    // Inspect delete manifests as well as attached tasks: a delete-only manifest
    // can exist even when the current data-task stream is empty.
    let input = table.file_io().new_input(snapshot.manifest_list())?;
    if input.metadata().await?.size > MAX_MANIFEST_LIST_BYTES {
        return Err(invalid(
            "Iceberg manifest list exceeds the local metadata limit",
        ));
    }
    let bytes = input.read().await?;
    if u64::try_from(bytes.len()).map_err(|_| Error::Overflow)? > MAX_MANIFEST_LIST_BYTES {
        return Err(invalid(
            "Iceberg manifest list exceeds the local metadata limit",
        ));
    }
    let list = ManifestList::parse_with_version(&bytes, FormatVersion::V2)?;
    if list
        .entries()
        .iter()
        .any(|manifest| manifest.content == ManifestContentType::Deletes)
    {
        return Err(invalid(
            "delete manifests are not qualified for typed Iceberg input",
        ));
    }
    let scan = table
        .scan()
        .snapshot_id(id)
        .select_all()
        .with_batch_size(Some(8192))
        .with_concurrency_limit(1)
        .with_data_file_concurrency_limit(1)
        .with_manifest_entry_concurrency_limit(1)
        .build()?;
    let mut tasks = scan.plan_files().await?;
    let mut count = 0_usize;
    while let Some(task) = tasks.try_next().await? {
        count = count.checked_add(1).ok_or(Error::Overflow)?;
        if count > MAX_SCAN_TASKS
            || task.data_file_format() != DataFileFormat::Parquet
            || !task.deletes().is_empty()
        {
            return Err(invalid(
                "unqualified Iceberg scan task, format or delete content",
            ));
        }
    }
    work.endpoint_scans = work.endpoint_scans.checked_add(1).ok_or(Error::Overflow)?;
    let mut stream = scan.to_arrow().await?;
    let mut endpoint = Bag::empty(schema);
    let mut rows = 0_usize;
    let mut decoded_bytes = 0_usize;
    while let Some(batch) = stream.try_next().await? {
        work.decoded_batches = work.decoded_batches.checked_add(1).ok_or(Error::Overflow)?;
        rows = rows.checked_add(batch.num_rows()).ok_or(Error::Overflow)?;
        decoded_bytes = decoded_bytes
            .checked_add(batch.get_array_memory_size())
            .ok_or(Error::Overflow)?;
        // For the admitted scalar profile the Arrow bytes contain all variable
        // payloads. Also charge every decoded row's scalar/vector storage before
        // growing the cumulative bag, even when duplicates later consolidate.
        let scalar_storage = rows
            .checked_mul(batch.num_columns())
            .and_then(|count| count.checked_mul(std::mem::size_of::<ScalarValue>()))
            .and_then(|bytes| {
                rows.checked_mul(std::mem::size_of::<Vec<ScalarValue>>() + 16)
                    .and_then(|row_bytes| bytes.checked_add(row_bytes))
            })
            .and_then(|bytes| bytes.checked_add(decoded_bytes))
            .ok_or(Error::Overflow)?;
        if rows > MAX_MATERIALIZED_ROWS
            || decoded_bytes > MAX_ENDPOINT_BYTES
            || scalar_storage > MAX_ENDPOINT_BYTES
        {
            return Err(invalid(
                "Iceberg endpoint exceeds local row/byte materialization limits",
            ));
        }
        if canonical_arrow_layout(batch.schema().as_ref())? != source.arrow_layout
            || batch
                .schema()
                .fields()
                .iter()
                .zip(batch.columns())
                .any(|(field, column)| !field.is_nullable() && column.null_count() != 0)
        {
            return Err(invalid(
                "Iceberg batch differs from its complete pinned Arrow layout",
            ));
        }
        endpoint = endpoint.add(&Bag::from_batch(&batch)?)?;
    }
    work.decoded_rows = work.decoded_rows.checked_add(rows).ok_or(Error::Overflow)?;
    work.decoded_bytes = work
        .decoded_bytes
        .checked_add(decoded_bytes)
        .ok_or(Error::Overflow)?;
    endpoint.to_batch()
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
