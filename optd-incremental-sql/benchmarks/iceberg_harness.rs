//! Real fixture snapshots and independent exact-endpoint SQL qualification.
//!
//! This is a bounded fixture writer for a new local catalog, not a production
//! multi-table transaction API. Whole-file replacement intentionally exercises
//! retractions without implying native changelog or incremental IO.

use super::*;
use ::iceberg::arrow::schema_to_arrow_schema;
use ::iceberg::spec::{
    FormatVersion, ManifestListWriter, ManifestWriterBuilder, NestedField, Operation,
    PrimitiveType, Schema as IcebergSchema, Snapshot, Summary, Type,
};
use ::iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use datafusion::arrow::datatypes::DataType;
use futures::TryStreamExt;
use optd_incremental::iceberg_sink::write_batch_files;
use optd_incremental::local_catalog::LocalCatalog;
use optd_incremental_sql::coordinator::{IcebergRefresh, IcebergSqlView};
use optd_incremental_sql::iceberg::{LocalIcebergSources, SnapshotSelection, SourceWork};
use optd_incremental_sql::source_vector::{PinnedSnapshot, PinnedSourceVector};
use optd_incremental_sql::store::{CheckpointStore, SourceSnapshot};

pub(super) fn prepare_root(root: &Path) -> Result<()> {
    if !root.is_absolute()
        || root
            .components()
            .any(|p| matches!(p, std::path::Component::ParentDir))
        || root == Path::new("/tmp")
    {
        return Err("Iceberg artifacts require an absolute child directory of /tmp".into());
    }
    let parent = fs::canonicalize(root.parent().ok_or("missing artifact parent")?)?;
    if !parent.starts_with("/tmp") {
        return Err("Iceberg artifact parent must be an existing directory under /tmp".into());
    }
    if !root.exists() {
        fs::create_dir(root)?;
    }
    if fs::symlink_metadata(root)?.file_type().is_symlink()
        || !fs::canonicalize(root)?.starts_with("/tmp")
        || !root.is_dir()
    {
        return Err("invalid Iceberg artifact root".into());
    }
    Ok(())
}

fn ident(name: &str) -> Result<TableIdent> {
    Ok(TableIdent::new(
        NamespaceIdent::new("qualification".into()),
        name.into(),
    ))
}

fn normalize_fixture(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if field.data_type() == &DataType::Utf8View {
            fields.push(Arc::new(
                field.as_ref().clone().with_data_type(DataType::Utf8),
            ));
            columns.push(datafusion::arrow::compute::cast(column, &DataType::Utf8)?);
        } else {
            fields.push(Arc::clone(field));
            columns.push(Arc::clone(column));
        }
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )?)
}

fn input_schema(batch: &RecordBatch) -> Result<IcebergSchema> {
    let mut fields = Vec::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        let primitive = match field.data_type() {
            DataType::Int32 => PrimitiveType::Int,
            DataType::Int64 => PrimitiveType::Long,
            DataType::Utf8 => PrimitiveType::String,
            DataType::Boolean => PrimitiveType::Boolean,
            DataType::Date32 => PrimitiveType::Date,
            DataType::Decimal128(precision, scale) if *scale >= 0 => PrimitiveType::Decimal {
                precision: u32::from(*precision),
                scale: u32::try_from(*scale)?,
            },
            other => return Err(format!("unqualified TPC fixture input type: {other}").into()),
        };
        let id = i32::try_from(index + 1)?;
        fields.push(Arc::new(if field.is_nullable() {
            NestedField::optional(id, field.name(), Type::Primitive(primitive))
        } else {
            NestedField::required(id, field.name(), Type::Primitive(primitive))
        }));
    }
    Ok(IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()?)
}

fn sources(root: &Path, tables: &Tables) -> Result<LocalIcebergSources> {
    let bindings = tables
        .keys()
        .map(|name| Ok((name.clone(), ident(name)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(LocalIcebergSources::open(root, bindings)?)
}

async fn pin(catalog: &LocalCatalog, root: &Path, tables: &Tables) -> Result<PinnedSourceVector> {
    let mut selections = BTreeMap::new();
    for name in tables.keys() {
        let table = catalog.load_table(&ident(name)?).await?;
        selections.insert(
            name.clone(),
            table
                .metadata()
                .current_snapshot_id()
                .map(SnapshotSelection::Snapshot)
                .unwrap_or(SnapshotSelection::Empty),
        );
    }
    Ok(sources(root, tables)?.pin(&selections).await?)
}

async fn replace(catalog: &LocalCatalog, name: &str, input: &RecordBatch) -> Result<()> {
    let table = catalog.load_table(&ident(name)?).await?;
    let sequence = table.metadata().next_sequence_number();
    let id = sequence;
    if table.metadata().snapshot_by_id(id).is_some() {
        return Err("fixture snapshot identity already exists".into());
    }
    let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
    // Rebind only field IDs/metadata to the newly created Iceberg schema. Exact
    // data types and all array values must already match; no scalar cast.
    let input = RecordBatch::try_new(schema, input.columns().to_vec())?;
    let files = write_batch_files(&table, input).await?;
    let mut manifests = Vec::new();
    if !files.is_empty() {
        let output = table.file_io().new_output(format!(
            "{}/metadata/qualification-{id}.avro",
            table.metadata().location()
        ))?;
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(id),
            table.current_schema_ref(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        for file in files {
            writer.add_file(file, sequence)?;
        }
        manifests.push(writer.write_manifest_file().await?);
    }
    let list = format!(
        "{}/metadata/qualification-list-{id}.avro",
        table.metadata().location()
    );
    let output = table.file_io().new_output(&list)?.writer().await?;
    let mut writer =
        ManifestListWriter::v2(output, id, table.metadata().current_snapshot_id(), sequence);
    writer.add_manifests(manifests.into_iter())?;
    writer.close().await?;
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let snapshot = Snapshot::builder()
        .with_snapshot_id(id)
        .with_parent_snapshot_id(table.metadata().current_snapshot_id())
        .with_sequence_number(sequence)
        .with_manifest_list(list)
        .with_timestamp_ms(now.max(table.metadata().last_updated_ms() + 1))
        .with_schema_id(table.metadata().current_schema_id())
        .with_summary(Summary {
            operation: Operation::Overwrite,
            additional_properties: Default::default(),
        })
        .build();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(table.metadata_location().map(str::to_owned))
        .set_branch_snapshot(snapshot, "main")?
        .build()?
        .metadata;
    // This fixture owns a fresh, unshared catalog. Write a complete new metadata
    // record and switch its local pointer; all old snapshots remain retained.
    let table_root = Path::new(table.metadata().location());
    let path = PathBuf::from(
        fs::read_to_string(table_root.join("current"))?
            .parse::<::iceberg::MetadataLocation>()?
            .with_next_version()
            .to_string()
            .trim_start_matches("file://"),
    );
    fs::write(&path, serde_json::to_vec(&metadata)?)?;
    fs::File::open(&path)?.sync_all()?;
    fs::File::open(path.parent().ok_or("metadata parent missing")?)?.sync_all()?;
    let pointer = table_root.join(format!("qualification-pointer-{id}"));
    fs::write(
        &pointer,
        path.to_str().ok_or("fixture metadata path is not UTF-8")?,
    )?;
    fs::File::open(&pointer)?.sync_all()?;
    fs::rename(pointer, table_root.join("current"))?;
    fs::File::open(table_root)?.sync_all()?;
    Ok(())
}

async fn independently_scan(catalog: &LocalCatalog, vector: &PinnedSourceVector) -> Result<Tables> {
    let mut result = Tables::new();
    for (name, source) in &vector.sources {
        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::from_vec(source.namespace.clone())?,
                source.name.clone(),
            ))
            .await?;
        let schema: IcebergSchema = serde_json::from_value(source.iceberg_schema.clone())?;
        let schema = Arc::new(schema_to_arrow_schema(&schema)?);
        let batch = match source.snapshot {
            PinnedSnapshot::Empty => RecordBatch::new_empty(schema),
            PinnedSnapshot::Snapshot { id, .. } => {
                let batches = table
                    .scan()
                    .snapshot_id(id)
                    .select_all()
                    .build()?
                    .to_arrow()
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;
                concat_batches(&schema, &batches)?
            }
        };
        result.insert(name.clone(), batch);
    }
    Ok(result)
}

fn verify_sources(actual: &Tables, expected: &Tables) -> Result<()> {
    if actual.keys().ne(expected.keys()) {
        return Err("source names differ".into());
    }
    for (name, batch) in expected {
        equal(&actual[name], batch)?;
    }
    Ok(())
}

fn work_json(work: &SourceWork) -> Value {
    json!({"changed_sources":work.changed_sources,"endpoint_scans":work.endpoint_scans,
        "decoded_batches":work.decoded_batches,"decoded_rows":work.decoded_rows,"decoded_bytes":work.decoded_bytes,
        "signed_change_rows":work.signed_change_rows})
}

fn load_checkpoint(store: &Path) -> Result<SourceSnapshot> {
    CheckpointStore::open(store)?
        .load_source()?
        .ok_or_else(|| "missing source checkpoint".into())
}

fn verify_replay(store: &Path, expected: &RecordBatch) -> Result<()> {
    let history = CheckpointStore::open(store)?.history_source()?;
    let mut folded = Bag::new();
    for (epoch, checkpoint) in history.into_iter().enumerate() {
        if checkpoint.snapshot.epoch != u64::try_from(epoch)? {
            return Err("noncontiguous replay epochs".into());
        }
        let committed_output = checkpoint.snapshot.output;
        let delta = checkpoint.snapshot.delta;
        let index = delta
            .num_columns()
            .checked_sub(1)
            .ok_or("missing replay weights")?;
        let weights = delta
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("bad replay weights")?;
        if weights.null_count() != 0 {
            return Err("NULL replay weights".into());
        }
        let converter = RowConverter::new(
            delta.schema().fields()[..index]
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?;
        let rows = converter.convert_columns(&delta.columns()[..index])?;
        for (index, row) in rows.iter().enumerate() {
            let value = folded.entry(row.as_ref().to_vec()).or_insert(0_i64);
            *value = value
                .checked_add(weights.value(index))
                .ok_or("replay overflow")?;
        }
        folded.retain(|_, count| *count != 0);
        if folded.values().any(|count| *count < 0) {
            return Err("negative replay multiplicity".into());
        }
        if folded != bag(&committed_output)? {
            return Err("committed output differs from its replay prefix".into());
        }
    }
    if folded != bag(expected)? {
        return Err("full committed output log replay mismatch".into());
    }
    Ok(())
}

pub(super) async fn run(sql: &str, complete: &Tables, root: &Path) -> Result<Value> {
    fs::create_dir(root)?;
    let normalized = complete
        .iter()
        .map(|(name, batch)| Ok((name.clone(), normalize_fixture(batch)?)))
        .collect::<Result<Tables>>()?;
    let complete = &normalized;
    let catalog_root = root.join("catalog");
    let store = root.join("store");
    let catalog = LocalCatalog::open(&catalog_root)?;
    let namespace = NamespaceIdent::new("qualification".into());
    catalog
        .create_namespace(&namespace, Default::default())
        .await?;
    for (name, batch) in complete {
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(name.clone())
                    .schema(input_schema(batch)?)
                    .format_version(FormatVersion::V2)
                    .properties([("commit.retry.num-retries".into(), "0".into())])
                    .build(),
            )
            .await?;
        replace(&catalog, name, &batch.slice(0, batch.num_rows() / 2)).await?;
    }
    let initial_vector = pin(&catalog, &catalog_root, complete).await?;
    let initial_sources = independently_scan(&catalog, &initial_vector).await?;
    let original_initial = complete
        .iter()
        .map(|(name, batch)| (name.clone(), batch.slice(0, batch.num_rows() / 2)))
        .collect();
    verify_sources(&initial_sources, &original_initial)?;
    let initial = oracle(sql, &initial_sources).await?;
    let mut previous = initial.rows;
    let mut view = IcebergSqlView::bootstrap(
        &store,
        sql,
        sources(&catalog_root, complete)?,
        initial_vector.clone(),
    )
    .await?;
    equal(&view.view().contents()?, &previous)?;
    let qualification = super::plan_qualification(view.view());
    drop(view);
    let initial_checkpoint = load_checkpoint(&store)?;
    if initial_checkpoint.vector != initial_vector || initial_checkpoint.snapshot.epoch != 0 {
        return Err("bootstrap checkpoint differs from its exact source vector or epoch".into());
    }
    verify_sources(&initial_checkpoint.snapshot.tables, &initial_sources)?;
    verify_delta(
        &RecordBatch::new_empty(previous.schema()),
        &previous,
        &initial_checkpoint.snapshot.delta,
    )?;
    view = IcebergSqlView::reopen(&store, sql, sources(&catalog_root, complete)?).await?;
    if view.source_vector() != &initial_vector || view.view().epoch() != 0 {
        return Err("bootstrap strict reopen changed its source vector or epoch".into());
    }
    verify_sources(&view.view().tables()?, &initial_sources)?;
    let initial_rows = previous.num_rows();
    let mut rounds = Vec::new();
    for (round, stage) in [
        "insert_remaining_half",
        "delete_first_row",
        "restore_first_row",
    ]
    .into_iter()
    .enumerate()
    {
        let mut original_target = Tables::new();
        for (name, batch) in complete {
            let rows = if stage == "delete_first_row" && batch.num_rows() > 0 {
                batch.slice(1, batch.num_rows() - 1)
            } else {
                batch.clone()
            };
            replace(&catalog, name, &rows).await?;
            original_target.insert(name.clone(), rows);
        }
        let target = pin(&catalog, &catalog_root, complete).await?;
        let target_sources = independently_scan(&catalog, &target).await?;
        verify_sources(&target_sources, &original_target)?;
        let expected = oracle(sql, &target_sources).await?;
        let result = view.refresh(target.clone()).await?;
        let (source_work, sql_stats) = match result {
            IcebergRefresh::Committed {
                source_work,
                sql_stats,
            } => (source_work, sql_stats),
            IcebergRefresh::Unchanged { .. } => {
                return Err("new fixture snapshots failed to advance epoch".into())
            }
        };
        equal(&view.view().contents()?, &expected.rows)?;
        drop(view);
        let checkpoint = load_checkpoint(&store)?;
        let epoch = u64::try_from(round + 1)?;
        if checkpoint.vector != target || checkpoint.snapshot.epoch != epoch {
            return Err("durable vector or epoch differs from target".into());
        }
        verify_sources(&checkpoint.snapshot.tables, &target_sources)?;
        verify_delta(&previous, &expected.rows, &checkpoint.snapshot.delta)?;
        view = IcebergSqlView::reopen(&store, sql, sources(&catalog_root, complete)?).await?;
        if view.source_vector() != &target || view.view().epoch() != epoch {
            return Err("strict reopen changed its source vector or epoch".into());
        }
        verify_sources(&view.view().tables()?, &target_sources)?;
        equal(&view.view().contents()?, &expected.rows)?;
        rounds.push(json!({"stage":stage,"status":"pass","output_rows":expected.rows.num_rows(),
            "oracle_schema":expected.schema,"signed_delta_rows":checkpoint.snapshot.delta.num_rows(),
            "stats":format!("{sql_stats:?}"),"sql_stats":sql_stats,"source_work":work_json(&source_work),"committed_epoch":epoch,
            "source_vector":target,"source_vector_sha256":checkpoint.vector.digest()?,"strict_reopen_verified":true,
            "storage_work":null,"recovery_work":null,"affected_scope_qualified":null,"performance_qualified":false}));
        previous = expected.rows;
    }
    drop(view);
    verify_replay(&store, &previous)?;
    let nonempty = initial_rows > 0
        || rounds
            .iter()
            .any(|r| r["output_rows"].as_u64().unwrap_or(0) > 0);
    let changed = rounds
        .iter()
        .any(|r| r["signed_delta_rows"].as_u64().unwrap_or(0) > 0);
    Ok(
        json!({"status":if !nonempty {"verified_empty_only"} else if !changed {"verified_unchanged_output"} else {"pass"},
        "reason":null,"plan_qualification":qualification,"registered_tables":complete.keys().collect::<Vec<_>>(),"initial_rows":initial_rows,
        "bootstrap_oracle_schema":initial.schema,"bootstrap_source_vector":initial_vector,
        "bootstrap_source_vector_sha256":initial_checkpoint.vector.digest()?,"last_verified_rows":previous.num_rows(),
        "final_rows":previous.num_rows(),"completed_rounds":3,"nonempty_result_seen":nonempty,"output_delta_seen":changed,
        "all_rounds_empty":rounds.iter().all(|r| r["output_rows"]==0),"rounds":rounds,"artifact_directory":root,
        "oracle":"independent_DataFusion_full_SQL_over_exact_Iceberg_endpoints",
        "comparison":"typed_bag_and_signed_endpoint_delta","ordered_presentation_checked":false,
        "bootstrap_committed_epoch":0,"bootstrap_strict_reopen_verified":true,"strict_reopen_verified":true,"full_output_log_replay_verified":true,
        "fixture_representation_normalization":"Utf8View_to_Utf8_only_no_numeric_or_date_casts",
        "source_consistency":"explicit_tuple_not_distributed_transaction","source_acquisition":"full_endpoint_difference"}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, StringViewArray};

    #[test]
    fn fixture_string_representation_preserves_unicode_empty_null_and_duplicates() -> Result<()> {
        let values = vec![Some("中文🦀"), None, Some(""), Some("中文🦀")];
        let input = RecordBatch::try_from_iter(vec![(
            "text",
            Arc::new(StringViewArray::from(values.clone())) as Arc<dyn Array>,
        )])?;
        let actual = normalize_fixture(&input)?;
        assert_eq!(actual.schema().field(0).data_type(), &DataType::Utf8);
        let array = actual
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("not Utf8")?;
        assert_eq!(array.iter().collect::<Vec<_>>(), values);
        Ok(())
    }
}
