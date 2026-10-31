//! Reproducible query qualification; the oracle never reads incremental caches.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::DFParser;
use optd_incremental_sql::{SqlView, TableChange};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[cfg(feature = "iceberg")]
#[path = "iceberg_harness.rs"]
mod iceberg_harness;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type Tables = BTreeMap<String, RecordBatch>;
type Bag = BTreeMap<Vec<u8>, i64>;

#[path = "query_mode.rs"]
mod query_mode;

#[path = "database_harness.rs"]
mod database_harness;

fn plan_qualification(view: &SqlView) -> Value {
    use optd_incremental_sql::Strategy;
    let strategies = view.strategies();
    let blockers = strategies
        .iter()
        .filter(|node| node.strategy == Strategy::OperatorRecompute)
        .map(|node| {
            json!({"id":node.id,"operator":node.operator,"rule":node.rule,
            "reason":view.incremental_plan().nodes()[node.id].reason})
        })
        .collect::<Vec<_>>();
    json!({"strict_plan_eligible":blockers.is_empty(),"complete_recompute_blockers":blockers,
        "planned_nodes":strategies,"work_scope":"operator_local_excludes_full_cache_source_and_checkpoint_costs"})
}

struct OracleOutput {
    rows: RecordBatch,
    schema: Value,
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .ok_or_else(|| format!("missing string field {field}").into())
}

fn check_digest(path: &Path, expected: &str) -> Result<()> {
    let digest = format!("{:x}", Sha256::digest(fs::read(path)?));
    if digest != expected {
        return Err(format!("fixture digest mismatch: {}", path.display()).into());
    }
    Ok(())
}

fn literal_null_pattern(marker: &str) -> String {
    let mut expression = String::from("^");
    for character in marker.chars() {
        if matches!(
            character,
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|'
        ) {
            expression.push('\\');
        }
        expression.push(character);
    }
    expression.push('$');
    expression
}

async fn load_tables(manifest: &Value, directory: &Path, repository: &Path) -> Result<Tables> {
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let schema_path = directory.join(required_string(manifest, "schema")?);
    check_digest(&schema_path, required_string(manifest, "schema_sha256")?)?;
    for statement in DFParser::parse_sql(&fs::read_to_string(schema_path)?)? {
        let plan = context.state().statement_to_plan(statement).await?;
        context.execute_logical_plan(plan).await?.collect().await?;
    }
    let csv_root = if manifest["csv_path_base"] == "repository_root" {
        repository
    } else {
        directory
    };
    let mut tables = BTreeMap::new();
    for table in manifest["tables"].as_array().ok_or("missing table list")? {
        let name = required_string(table, "name")?;
        let schema = Arc::new(context.table(name).await?.schema().as_arrow().clone());
        let path = csv_root.join(required_string(table, "csv")?);
        check_digest(&path, required_string(table, "sha256")?)?;
        // DataFusion 55 forwards null_regex during schema inference, but its
        // CsvSource reader omits it. Read the checked fixture with Arrow's
        // typed parser directly so NULL markers reach the actual decoder.
        let mut reader = ReaderBuilder::new(Arc::clone(&schema))
            .with_header(false)
            .with_delimiter(b'|');
        if let Some(marker) = manifest["csv_null"]
            .as_str()
            .filter(|marker| !marker.is_empty())
        {
            reader = reader.with_null_regex(literal_null_pattern(marker).parse()?);
        }
        let batches = reader
            .build(fs::File::open(&path)?)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let batch = concat_batches(&schema, &batches)?;
        if let Some(expected) = table["rows"].as_u64() {
            if batch.num_rows() as u64 != expected {
                return Err(format!("fixture row count mismatch for {name}").into());
            }
        }
        tables.insert(name.to_owned(), batch);
    }
    Ok(tables)
}

async fn oracle(sql: &str, tables: &Tables) -> Result<OracleOutput> {
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for (name, rows) in tables {
        context.register_table(
            name.as_str(),
            Arc::new(MemTable::try_new(rows.schema(), vec![vec![rows.clone()]])?),
        )?;
    }
    let frame = context.sql(sql).await?;
    let logical_types: Vec<_> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().to_string())
        .collect();
    let physical = frame.create_physical_plan().await?;
    let schema = physical.schema();
    let physical_types: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| field.data_type().to_string())
        .collect();
    let changed = logical_types != physical_types;
    let batches = datafusion::physical_plan::collect(physical, context.task_ctx()).await?;
    Ok(OracleOutput {
        rows: concat_batches(&schema, &batches)?,
        schema: json!({"logical_types":logical_types,"physical_types":physical_types,"logical_physical_types_differ":changed}),
    })
}

fn bag(rows: &RecordBatch) -> Result<Bag> {
    let converter = RowConverter::new(
        rows.schema()
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )?;
    let encoded = converter.convert_columns(rows.columns())?;
    let mut bag = BTreeMap::new();
    for row in encoded.iter() {
        *bag.entry(row.as_ref().to_vec()).or_default() += 1;
    }
    Ok(bag)
}

fn equal(left: &RecordBatch, right: &RecordBatch) -> Result<()> {
    let types = |batch: &RecordBatch| {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().clone(), field.data_type().clone()))
            .collect::<Vec<_>>()
    };
    if types(left) != types(right) || bag(left)? != bag(right)? {
        return Err(format!(
            "typed result mismatch: incremental {} rows {:?}, independent SQL {} rows {:?}",
            left.num_rows(),
            types(left),
            right.num_rows(),
            types(right)
        )
        .into());
    }
    Ok(())
}

fn verify_delta(before: &RecordBatch, after: &RecordBatch, delta: &RecordBatch) -> Result<()> {
    let index = delta
        .num_columns()
        .checked_sub(1)
        .ok_or("missing delta weight column")?;
    if delta.schema().field(index).name() != "__optd_weight" {
        return Err("unexpected delta weight column name".into());
    }
    let weights = delta
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("delta weight type is not Int64")?;
    if weights.null_count() != 0 {
        return Err("NULL delta weight".into());
    }
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(delta.schema().fields()[..index].to_vec())),
        delta.columns()[..index].to_vec(),
    )?;
    if rows.num_columns() != after.num_columns()
        || rows
            .schema()
            .fields()
            .iter()
            .zip(after.schema().fields())
            .any(|(left, right)| {
                left.name() != right.name() || left.data_type() != right.data_type()
            })
    {
        return Err(
            "signed output delta column names/types differ from the independent result".into(),
        );
    }
    let converter = RowConverter::new(
        rows.schema()
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )?;
    let encoded = converter.convert_columns(rows.columns())?;
    let mut actual: Bag = BTreeMap::new();
    for (index, row) in encoded.iter().enumerate() {
        *actual.entry(row.as_ref().to_vec()).or_default() += weights.value(index);
    }
    actual.retain(|_, count| *count != 0);
    let mut expected = bag(after)?;
    for (row, count) in bag(before)? {
        *expected.entry(row).or_default() -= count;
    }
    expected.retain(|_, count| *count != 0);
    if actual != expected {
        return Err("signed output delta differs from independent endpoint difference".into());
    }
    Ok(())
}

async fn referenced_tables(sql: &str, complete: &Tables) -> Result<Tables> {
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for (name, rows) in complete {
        context.register_batch(name, rows.clone())?;
    }
    let plan = context.state().create_logical_plan(sql).await?;
    let mut names = BTreeSet::new();
    // Analyzed plans retain scans inside scalar/EXISTS/IN subqueries. Ordinary
    // child-only traversal would silently omit their base-table dependencies.
    plan.apply_with_subqueries(|plan| {
        if let LogicalPlan::TableScan(scan) = plan {
            names.insert(scan.table_name.table().to_owned());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    names
        .into_iter()
        .map(|name| {
            complete
                .get(&name)
                .cloned()
                .map(|rows| (name.clone(), rows))
                .ok_or_else(|| format!("resolved query table is not in the fixture: {name}").into())
        })
        .collect()
}

async fn run_query(sql: &str, complete: &Tables, iceberg_root: Option<&Path>) -> Result<Value> {
    let referenced = referenced_tables(sql, complete).await?;
    if let Some(root) = iceberg_root {
        #[cfg(feature = "iceberg")]
        return iceberg_harness::run(sql, &referenced, root).await;
        #[cfg(not(feature = "iceberg"))]
        {
            let _ = root;
            return Err("Iceberg qualification requires --features iceberg".into());
        }
    }
    let registered_tables: Vec<_> = referenced.keys().cloned().collect();
    let complete = &referenced;
    let mut current: Tables = complete
        .iter()
        .map(|(name, batch)| (name.clone(), batch.slice(0, batch.num_rows() / 2)))
        .collect();
    let mut view = SqlView::new(sql, current.clone())
        .await
        .map_err(|error| format!("view_new: {error}"))?;
    let qualification = plan_qualification(&view);
    let bootstrap = oracle(sql, &current)
        .await
        .map_err(|error| format!("oracle_bootstrap: {error}"))?;
    let mut previous = bootstrap.rows;
    let bootstrap_schema = bootstrap.schema;
    equal(&view.contents()?, &previous).map_err(|error| format!("compare_bootstrap: {error}"))?;
    let initial_rows = previous.num_rows();
    let mut rounds = Vec::new();
    for stage in [
        "insert_remaining_half",
        "delete_first_row",
        "restore_first_row",
    ] {
        let mut changes = Vec::new();
        for (name, batch) in complete {
            let length = batch.num_rows();
            if length == 0 {
                continue;
            }
            let (rows, weight, next) = match stage {
                "insert_remaining_half" => {
                    let split = length / 2;
                    (batch.slice(split, length - split), 1, batch.clone())
                }
                "delete_first_row" => (batch.slice(0, 1), -1, batch.slice(1, length - 1)),
                _ => (batch.slice(0, 1), 1, batch.clone()),
            };
            changes.push(TableChange {
                table: name.clone(),
                weights: vec![weight; rows.num_rows()],
                rows,
            });
            current.insert(name.clone(), next);
        }
        let result: Result<(RecordBatch, Value)> = async {
            let candidate = view.prepare(&changes).await?;
            let delta = candidate.delta()?;
            let strategy = format!("{:?}", candidate.stats());
            let sql_stats = candidate.stats().clone();
            let expected = oracle(sql, &current).await.map_err(|error| format!("oracle_{stage}: {error}"))?;
            verify_delta(&previous, &expected.rows, &delta).map_err(|error| format!("compare_delta_{stage}: {error}"))?;
            view.acknowledge(candidate)?;
            equal(&view.contents()?, &expected.rows).map_err(|error| format!("compare_result_{stage}: {error}"))?;
            Ok((expected.rows.clone(), json!({"stage":stage,"status":"pass","output_rows":expected.rows.num_rows(),"oracle_schema":expected.schema,"signed_delta_rows":delta.num_rows(),"stats":strategy,"sql_stats":sql_stats,"storage_work":null,"recovery_work":null,"source_work":null,"affected_scope_qualified":null,"performance_qualified":false})))
        }
        .await;
        match result {
            Ok((expected, record)) => {
                rounds.push(record);
                previous = expected;
            }
            Err(error) => {
                rounds.push(json!({"stage":stage,"status":"fail","reason":error.to_string()}));
                break;
            }
        }
    }
    let nonempty_result_seen = initial_rows > 0
        || rounds
            .iter()
            .any(|round| round["output_rows"].as_u64().unwrap_or(0) > 0);
    let output_delta_seen = rounds
        .iter()
        .any(|round| round["signed_delta_rows"].as_u64().unwrap_or(0) > 0);
    let all_rounds_empty = rounds
        .iter()
        .all(|round| round["output_rows"].as_u64().unwrap_or(0) == 0);
    let failure = rounds
        .iter()
        .find(|round| round["status"] == "fail")
        .map(|round| round["reason"].clone());
    let completed_rounds = rounds
        .iter()
        .filter(|round| round["status"] == "pass")
        .count();
    let status = if failure.is_some() {
        "fail"
    } else if !nonempty_result_seen {
        "verified_empty_only"
    } else if !output_delta_seen {
        "verified_unchanged_output"
    } else {
        "pass"
    };
    Ok(
        json!({"status":status,"reason":failure,"plan_qualification":qualification,"registered_tables":registered_tables,"initial_rows":initial_rows,"bootstrap_oracle_schema":bootstrap_schema,"last_verified_rows":previous.num_rows(),"final_rows":if completed_rounds==3 {Some(previous.num_rows())} else {None},"completed_rounds":completed_rounds,"nonempty_result_seen":nonempty_result_seen,"output_delta_seen":output_delta_seen,"all_rounds_empty":all_rounds_empty,"rounds":rounds,"oracle":"independent_DataFusion_full_SQL","comparison":"typed_bag_and_signed_endpoint_delta","ordered_presentation_checked":false}),
    )
}

/// Execute every selected manifest entry and retain failures in structured output.
pub async fn run_manifest(path: &Path, selected: Option<u64>) -> Result<Value> {
    let query_mode =
        std::env::var("OPTD_SQL_BENCH_QUERY_MODE").unwrap_or_else(|_| "original".into());
    query_mode::prepare("SELECT 1", &query_mode)?;
    let database_root = std::env::var_os("OPTD_SQL_BENCH_DATABASE_ROOT").map(PathBuf::from);
    if let Some(root) = &database_root {
        fs::create_dir(root)?;
    }
    let iceberg_root = std::env::var_os("OPTD_SQL_BENCH_ICEBERG_ROOT").map(PathBuf::from);
    if database_root.is_some() && iceberg_root.is_some() {
        return Err("SQL database and Iceberg benchmark backends are mutually exclusive".into());
    }
    if let Some(root) = &iceberg_root {
        #[cfg(not(feature = "iceberg"))]
        return Err(format!(
            "Iceberg qualification requires --features iceberg: {}",
            root.display()
        )
        .into());
        #[cfg(feature = "iceberg")]
        iceberg_harness::prepare_root(root)?;
    }
    let timeout_seconds = std::env::var("OPTD_SQL_BENCH_QUERY_TIMEOUT_SECS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(120);
    if !(1..=3600).contains(&timeout_seconds) {
        return Err("OPTD_SQL_BENCH_QUERY_TIMEOUT_SECS must be in 1..3600".into());
    }
    let manifest: Value = serde_json::from_slice(&fs::read(path)?)?;
    let directory = path.parent().ok_or("manifest has no parent directory")?;
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("crate has no repository parent")?
        .to_path_buf();
    let loaded = load_tables(&manifest, directory, &repository).await;
    let fixture_error = loaded.as_ref().err().map(ToString::to_string);
    let mut records = Vec::new();
    for query in manifest["queries"].as_array().ok_or("missing query list")? {
        let number = query["query_id"].as_u64().ok_or("invalid query number")?;
        if selected.is_some_and(|id| id != number) {
            continue;
        }
        for variant in query["variants"].as_array().ok_or("missing variants")? {
            let variant_id = required_string(variant, "variant_id")?;
            if !variant_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                return Err("unsafe query variant directory name".into());
            }
            let source_root = iceberg_root
                .as_ref()
                .map(|root| root.join(format!("query-{variant_id}")));
            eprintln!(
                "BENCH start {} q{variant_id}",
                required_string(&manifest, "suite")?
            );
            let result = tokio::time::timeout(Duration::from_secs(timeout_seconds), async {
                let complete = loaded.as_ref().map_err(|error| error.to_string())?;
                let file = directory.join(required_string(variant, "sql")?);
                check_digest(&file, required_string(variant, "sha256")?)?;
                let original = fs::read_to_string(file)?;
                let (sql, transformation) = query_mode::prepare(&original, &query_mode)?;
                let mut result = if let Some(root) = &database_root {
                    database_harness::run(&sql, complete, &root.join(format!("query-{variant_id}")))
                        .await?
                } else {
                    run_query(&sql, complete, source_root.as_deref()).await?
                };
                result["query_transformation"] = transformation;
                Ok::<_, Box<dyn Error + Send + Sync>>(result)
            })
            .await;
            let mut record = match result {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => json!({"status":"fail","reason":error.to_string(),"rounds":[]}),
                Err(_) => {
                    json!({"status":"fail","reason":format!("query exceeded {timeout_seconds}s async deadline"),"timed_out":true,"completed_rounds":null,"rounds":[],"verification_progress":"unknown_after_timeout"})
                }
            };
            eprintln!(
                "BENCH done {} q{variant_id} {}",
                required_string(&manifest, "suite")?,
                record["status"].as_str().unwrap_or("unknown")
            );
            record["query_id"] = json!(number);
            record["variant_id"] = variant["variant_id"].clone();
            record["source"] = query["source"].clone();
            record["qualification"] = query["qualification"].clone();
            record["source_backend"] = json!(if database_root.is_some() {
                "sql_database"
            } else if iceberg_root.is_some() {
                "typed_iceberg_endpoint_vector"
            } else {
                "typed_arrow"
            });
            records.push(record);
        }
    }
    if records.is_empty() {
        return Err("selection contains no manifest queries".into());
    }
    let passed = records
        .iter()
        .filter(|record| record["status"] == "pass")
        .count();
    let nonempty = records
        .iter()
        .filter(|record| record["final_rows"].as_u64().unwrap_or(0) > 0)
        .count();
    let failed = records
        .iter()
        .filter(|record| record["status"] == "fail")
        .count();
    let empty_only = records
        .iter()
        .filter(|record| record["status"] == "verified_empty_only")
        .count();
    let unchanged = records
        .iter()
        .filter(|record| record["status"] == "verified_unchanged_output")
        .count();
    let manifest_digest = format!("{:x}", Sha256::digest(fs::read(path)?));
    let bootstrap_verified = records
        .iter()
        .filter(|record| record["initial_rows"].is_number())
        .count();
    let complete_rounds_verified = records
        .iter()
        .filter(|record| record["completed_rounds"] == 3)
        .count();
    let mut report = json!({"format_version":1,"suite":manifest["suite"],"manifest":path,"manifest_sha256":manifest_digest,"command":std::env::args().collect::<Vec<_>>(),"host_arch":std::env::consts::ARCH,"host_os":std::env::consts::OS,"query_timeout_seconds":timeout_seconds,"timeout_scope":"async_query_future","session_configuration":{"target_partitions":1,"comparison":"exact_typed_bag"},"source":manifest["source"],"schema_sha256":manifest["schema_sha256"],"data_status":manifest["data_status"],"fixture_qualification":manifest["fixture_qualification"],"fixture_error":fixture_error,"executed_variants":records.len(),"bootstrap_verified_variants":bootstrap_verified,"three_rounds_verified_variants":complete_rounds_verified,"passed_variants":passed,"failed_variants":failed,"verified_empty_only_variants":empty_only,"verified_unchanged_output_variants":unchanged,"incomplete_variants":records.len()-passed,"nonempty_final_variants":nonempty,"scale_or_performance_qualification":false,"queries":records});
    report["source_backend"] = json!(if database_root.is_some() {
        "sql_database"
    } else if iceberg_root.is_some() {
        "typed_iceberg_endpoint_vector"
    } else {
        "typed_arrow"
    });
    report["iceberg_artifact_root"] = json!(iceberg_root);
    report["database_artifact_root"] = json!(database_root);
    report["unsupported_strict_plan_variants"] = json!(report["queries"]
        .as_array()
        .ok_or("missing records")?
        .iter()
        .filter(|r| r["status"] == "unsupported_strict_plan")
        .count());
    report["query_mode"] = json!(query_mode);
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn referenced_tables_include_expression_subqueries_and_exclude_unused_inputs(
    ) -> Result<()> {
        let rows = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn Array>,
        )])?;
        let complete = ["a", "b", "c", "unused"]
            .into_iter()
            .map(|name| (name.to_owned(), rows.clone()))
            .collect();
        let selected = referenced_tables(
            "SELECT a.id, (SELECT MAX(c.id) FROM c) AS m FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id)",
            &complete,
        )
        .await?;
        assert_eq!(
            selected.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert_eq!(selected["c"].num_rows(), rows.num_rows());
        Ok(())
    }

    #[tokio::test]
    async fn csv_loader_uses_the_literal_manifest_null_marker_for_typed_columns() -> Result<()> {
        for marker in [r"\N", r"\\N", "NULL.+"] {
            let directory = tempfile::tempdir()?;
            let schema = directory.path().join("schema.sql");
            fs::write(&schema, "CREATE TABLE input (id INTEGER, day DATE, amount DECIMAL(15,2), description VARCHAR);")?;
            let csv = directory.path().join("input.csv");
            fs::write(
                &csv,
                format!("1|{marker}|{marker}|{marker}\n2|2000-01-01|1.50|prefix{marker}\n"),
            )?;
            let manifest = json!({
                "schema":"schema.sql", "schema_sha256":format!("{:x}", Sha256::digest(fs::read(&schema)?)),
                "csv_path_base":"manifest_directory", "csv_null":marker,
                "tables":[{"name":"input","csv":"input.csv","rows":2,"sha256":format!("{:x}",Sha256::digest(fs::read(&csv)?))}]
            });
            let tables = load_tables(&manifest, directory.path(), directory.path()).await?;
            let rows = &tables["input"];
            assert_eq!(rows.num_rows(), 2);
            assert_eq!(rows.column(0).null_count(), 0);
            for column in 1..4 {
                assert_eq!(
                    rows.column(column).null_count(),
                    1,
                    "literal marker {marker:?}, column {column}"
                );
                assert!(rows.column(column).is_null(0));
                assert!(!rows.column(column).is_null(1));
            }
        }
        Ok(())
    }
}
