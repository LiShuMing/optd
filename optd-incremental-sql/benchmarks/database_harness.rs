//! SQL frontend qualification with independently constructed source endpoints.
use super::*;
use datafusion::arrow::array::{ArrayRef, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use optd_incremental_sql::database::SqlSession;
use std::time::Instant;

struct StageTracer {
    path: PathBuf,
    run_start: Instant,
    active_stage: Option<(String, Instant)>,
    completed: Vec<Value>,
}

impl StageTracer {
    fn new(root: &Path) -> Self {
        Self {
            path: root.join("progress.json"),
            run_start: Instant::now(),
            active_stage: None,
            completed: Vec::new(),
        }
    }

    fn persist(&self, status: &str) -> Result<()> {
        let active = self.active_stage.as_ref().map(|(name, started)| {
            json!({
                "stage": name,
                "started_at_ms": started.saturating_duration_since(self.run_start).as_millis() as u64,
            })
        });
        let payload = json!({
            "format_version": 1,
            "status": status,
            "elapsed_total_ms": self.run_start.elapsed().as_millis() as u64,
            "active_stage": active,
            "completed_stages": self.completed,
        });
        let tmp = self.path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(&payload)?)?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    fn record_instant(
        &mut self,
        stage: &str,
        elapsed_ms: u64,
        detail: Option<Value>,
    ) -> Result<()> {
        let mut entry = json!({
            "stage": stage,
            "elapsed_ms": elapsed_ms,
            "status": "ok",
        });
        if let Some(detail) = detail {
            entry["detail"] = detail;
        }
        self.completed.push(entry);
        self.persist("in_progress")
    }

    fn begin(&mut self, stage: impl Into<String>) -> Result<()> {
        self.active_stage = Some((stage.into(), Instant::now()));
        self.persist("in_progress")
    }

    fn finish(&mut self, detail: Option<Value>) -> Result<()> {
        let Some((stage, started)) = self.active_stage.take() else {
            return Ok(());
        };
        let mut entry = json!({
            "stage": stage,
            "elapsed_ms": started.elapsed().as_millis() as u64,
            "status": "ok",
        });
        if let Some(detail) = detail {
            entry["detail"] = detail;
        }
        self.completed.push(entry);
        self.persist("in_progress")
    }

    fn complete(&mut self) -> Result<()> {
        self.active_stage = None;
        self.persist("complete")
    }
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn sql_type(t: &DataType) -> Result<String> {
    Ok(match t {
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        _ => return Err(format!("SQL fixture literal type unsupported: {t}").into()),
    })
}
fn literal(a: &ArrayRef, i: usize) -> Result<String> {
    let scalar = ScalarValue::try_from_array(a, i)?;
    let value = if scalar.is_null() {
        "NULL".into()
    } else {
        format!("'{}'", scalar.to_string().replace('\'', "''"))
    };
    Ok(format!("CAST({value} AS {})", sql_type(a.data_type())?))
}
fn insert(name: &str, b: &RecordBatch) -> Result<Vec<String>> {
    const MAX_INSERT_ROWS: usize = 1024;
    const MAX_INSERT_CELLS: usize = 8192;
    let mut statements = Vec::new();
    // Each cell adds a typed CAST expression. Bound width as well as rows so wide
    // fixtures respect the binder budget without changing the transaction boundary.
    let chunk_rows = MAX_INSERT_ROWS.min((MAX_INSERT_CELLS / b.num_columns().max(1)).max(1));
    for start in (0..b.num_rows()).step_by(chunk_rows) {
        let rows = (start..(start + chunk_rows).min(b.num_rows()))
            .map(|i| {
                Ok(format!(
                    "({})",
                    b.columns()
                        .iter()
                        .map(|a| literal(a, i))
                        .collect::<Result<Vec<_>>>()?
                        .join(",")
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        statements.push(format!(
            "INSERT INTO {} VALUES {}",
            quote(name),
            rows.join(",")
        ));
    }
    Ok(statements)
}
fn selected(b: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let indices = UInt32Array::from(indices.to_vec());
    Ok(RecordBatch::try_new(
        b.schema(),
        b.columns()
            .iter()
            .map(|a| take(a, &indices, None))
            .collect::<std::result::Result<_, _>>()?,
    )?)
}
fn conjunction(terms: &[String]) -> String {
    if terms.len() == 1 {
        return terms[0].clone();
    }
    let middle = terms.len() / 2;
    format!(
        "({} AND {})",
        conjunction(&terms[..middle]),
        conjunction(&terms[middle..])
    )
}
fn mutation(name: &str, b: &RecordBatch, update: bool) -> Result<(String, RecordBatch)> {
    let old = b
        .columns()
        .iter()
        .map(|a| ScalarValue::try_from_array(a, 0))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let predicates = b
        .schema()
        .fields()
        .iter()
        .zip(b.columns())
        .map(|(f, a)| {
            Ok(format!(
                "({} IS NOT DISTINCT FROM {})",
                quote(f.name()),
                literal(a, 0)?
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let predicate = conjunction(&predicates);
    let mut matching = Vec::new();
    let mut keep = Vec::new();
    for i in 0..b.num_rows() {
        let row = b
            .columns()
            .iter()
            .map(|a| ScalarValue::try_from_array(a, i))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if row == old {
            matching.push(i as u32);
        } else {
            keep.push(i as u32);
        }
    }
    let kept = selected(b, &keep)?;
    if !update {
        return Ok((
            format!("DELETE FROM {} WHERE {predicate}", quote(name)),
            kept,
        ));
    }
    let (column, value) = old
        .iter()
        .enumerate()
        .find_map(|(i, v)| match v {
            ScalarValue::Int32(Some(n)) => {
                n.checked_add(1).map(|n| (i, ScalarValue::Int32(Some(n))))
            }
            ScalarValue::Int64(Some(n)) => {
                n.checked_add(1).map(|n| (i, ScalarValue::Int64(Some(n))))
            }
            _ => None,
        })
        .ok_or("fixture UPDATE requires a non-NULL integer field")?;
    let mut arrays = selected(b, &matching)?.columns().to_vec();
    arrays[column] = value.to_array_of_size(matching.len())?;
    let replaced = RecordBatch::try_new(b.schema(), arrays)?;
    let after = concat_batches(&b.schema(), [&kept, &replaced])?;
    Ok((
        format!(
            "UPDATE {} SET {}={}+1 WHERE {predicate}",
            quote(name),
            quote(b.schema().field(column).name()),
            quote(b.schema().field(column).name())
        ),
        after,
    ))
}
async fn verify(
    db: &mut SqlSession,
    sql: &str,
    current: &Tables,
    numerical_columns: &[usize],
) -> Result<RecordBatch> {
    for (name, expected) in current {
        let actual = db
            .execute(&format!("SELECT * FROM {}", quote(name)))
            .await?
            .rows
            .ok_or("missing source rows")?;
        equal(&actual, expected)?;
    }
    let expected = oracle(sql, current).await?.rows;
    let actual = db
        .execute("SELECT * FROM maintained_result")
        .await?
        .rows
        .ok_or("missing view result")?;
    equal_result(&actual, &expected, numerical_columns)?;
    db.verify_output_history()?;
    Ok(actual)
}

// Streaming statements keeps one commit boundary without accumulating a >16MiB
// request string. Callers discard the session if statement execution fails.
async fn execute_batch(
    db: &mut SqlSession,
    statements: &[String],
) -> Result<Option<optd_incremental_sql::database::CommitReport>> {
    if statements.is_empty() {
        return Ok(None);
    }
    db.execute("BEGIN").await?;
    for statement in statements {
        db.execute(statement).await?;
    }
    Ok(Some(
        db.execute("COMMIT")
            .await?
            .commit
            .ok_or("missing commit report")?,
    ))
}

pub(super) async fn run(sql: &str, complete: &Tables, root: &Path) -> Result<Value> {
    let preflight_start = Instant::now();
    let referenced = referenced_tables(sql, complete).await?;
    let mut current: Tables = referenced
        .iter()
        .map(|(n, b)| (n.clone(), b.slice(0, b.num_rows() / 2)))
        .collect();
    let probe = SqlView::preflight(
        sql,
        current
            .iter()
            .map(|(name, batch)| (name.clone(), batch.schema()))
            .collect(),
        optd_incremental_sql::planner::IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .map_err(|e| format!("plan_preflight: {e}"))?;
    let preflight_ms = preflight_start.elapsed().as_millis() as u64;
    let blockers = probe
        .blockers
        .iter()
        .map(|blocker| {
            let mut value = json!(blocker);
            // Preserve the existing report's rule identity object for audit readers.
            value["rule"] = probe
                .planned_nodes
                .iter()
                .find(|node| node.id == blocker.id)
                .and_then(|node| node.rule.as_ref())
                .map_or_else(|| json!({"name": blocker.rule}), |rule| json!(rule));
            value
        })
        .collect::<Vec<_>>();
    let qualification = json!({
        "strict_plan_eligible": probe.strict_plan_eligible,
        "complete_recompute_blockers": blockers,
        "planned_nodes": probe.planned_nodes,
        "numerical_columns": probe.numerical_columns,
        "work_scope": "schema_bound_planning_only_no_bootstrap_execution",
    });
    if qualification["strict_plan_eligible"] != true {
        return Ok(
            json!({"status":"unsupported_strict_plan","plan_qualification":qualification,"completed_rounds":0,"rounds":[]}),
        );
    }
    // Per-query roots must be fresh; never reset another run's committed state.
    fs::create_dir(root)?;
    let mut tracer = StageTracer::new(root);
    tracer.record_instant("preflight", preflight_ms, None)?;
    let mut script = String::new();
    tracer.begin("bootstrap_ddl")?;
    let mut db = SqlSession::open(root.join("warehouse")).await?;
    for (name, b) in &current {
        let columns = b
            .schema()
            .fields()
            .iter()
            .map(|f| {
                Ok(format!(
                    "{} {}{}",
                    quote(f.name()),
                    sql_type(f.data_type())?,
                    if f.is_nullable() { "" } else { " NOT NULL" }
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let ddl = format!("CREATE TABLE {} ({})", quote(name), columns.join(","));
        db.execute(&ddl)
            .await
            .map_err(|e| format!("bootstrap_ddl: {e}"))?;
        script.push_str(&format!("{ddl};\n"));
    }
    tracer.finish(Some(json!({"tables": current.len()})))?;
    tracer.begin("bootstrap_import")?;
    let initial = current
        .iter()
        .map(|(name, b)| insert(name, b))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    script.push_str(&format!("BEGIN;\n{};\nCOMMIT;\n", initial.join(";\n")));
    fs::write(root.join("input.sql"), &script)?;
    execute_batch(&mut db, &initial)
        .await
        .map_err(|e| format!("bootstrap_rows: {e}"))?;
    tracer.finish(Some(json!({"statements": initial.len()})))?;
    tracer.begin("bootstrap_mv")?;
    let ddl = format!("CREATE MATERIALIZED VIEW maintained_result AS {sql}");
    db.execute(&ddl)
        .await
        .map_err(|e| format!("bootstrap_ddl: {e}"))?;
    script.push_str(&format!("{ddl};\n"));
    tracer.finish(None)?;
    tracer.begin("bootstrap_verify")?;
    let mut previous = verify(&mut db, sql, &current, &probe.numerical_columns).await?;
    let initial_rows = previous.num_rows();
    let bootstrap_epoch = db.epoch();
    tracer.finish(Some(
        json!({"initial_rows": initial_rows, "bootstrap_epoch": bootstrap_epoch}),
    ))?;
    let mut nonempty = initial_rows > 0;
    let mut output_changed = false;
    let mut rounds = Vec::new();
    for stage in [
        "insert_remaining_half",
        "delete_matching_rows",
        "update_keys",
    ] {
        let mut statements = Vec::new();
        for (name, complete) in &referenced {
            let before = &current[name];
            if stage == "insert_remaining_half" {
                let split = complete.num_rows() / 2;
                statements.extend(insert(
                    name,
                    &complete.slice(split, complete.num_rows() - split),
                )?);
                current.insert(name.clone(), complete.clone());
            } else if before.num_rows() > 0 {
                let (statement, after) = mutation(name, before, stage == "update_keys")?;
                statements.push(statement);
                current.insert(name.clone(), after);
            }
        }
        let body = statements.join(";\n");
        script.push_str(&format!("BEGIN;\n{body};\nCOMMIT;\n"));
        fs::write(root.join("input.sql"), &script)?;
        tracer.begin(format!("round:{stage}:dml_execute"))?;
        let storage_before = db.store_work()?;
        let report = execute_batch(&mut db, &statements)
            .await
            .map_err(|e| format!("{stage}: {e}"))?;
        let storage_after = db.store_work()?;
        let storage_work = storage_after.since(&storage_before)?;
        tracer.finish(Some(json!({"statements": statements.len()})))?;
        tracer.begin(format!("round:{stage}:verify_before_reopen"))?;
        let expected = verify(&mut db, sql, &current, &probe.numerical_columns).await?;
        let verification_before = db.store_work()?.since(&storage_after)?;
        tracer.finish(Some(json!({"output_rows": expected.num_rows()})))?;
        nonempty |= expected.num_rows() > 0;
        output_changed |= bag(&previous)? != bag(&expected)?;
        tracer.begin(format!("round:{stage}:reopen"))?;
        drop(db);
        db = SqlSession::open(root.join("warehouse")).await?;
        let recovery_work = db.recovery_work().clone();
        let reopened = db.store_work()?;
        tracer.finish(Some(json!({"rebuilt_views": recovery_work.rebuilt_views})))?;
        tracer.begin(format!("round:{stage}:verify_after_reopen"))?;
        verify(&mut db, sql, &current, &probe.numerical_columns).await?;
        let verification_after = db.store_work()?.since(&reopened)?;
        tracer.finish(None)?;
        rounds.push(
            json!({"stage":stage,"status":"pass","output_rows":expected.num_rows(),"commit":report,
            "storage_work":storage_work,"recovery_work":recovery_work,
            "verification_store_work_before_reopen":verification_before,
            "verification_store_work_after_reopen":verification_after,
            "source_work":null,"affected_scope_qualified":null,"performance_qualified":false,
            "reopen_verified":true,"base_tables_verified":true,"output_history_verified":true}),
        );
        previous = expected;
    }
    tracer.complete()?;
    Ok(
        json!({"status":if !nonempty{"verified_empty_only"}else if !output_changed{"verified_unchanged_output"}else{"pass"},
        "plan_qualification":qualification,"bootstrap_epoch":bootstrap_epoch,"initial_rows":initial_rows,"final_rows":previous.num_rows(),"nonempty_result_seen":nonempty,"output_delta_seen":output_changed,
        "completed_rounds":rounds.len(),"rounds":rounds,"stage_timings":tracer.completed,"progress_file":tracer.path,"oracle":"independent_DataFusion_full_SQL_over_independent_source_endpoints","comparison":"exact_sources_and_output_log_replay_with_declared_numerical_MV_lineage","numerical_contract":{"columns":probe.numerical_columns,"absolute":numeric_comparison::ABSOLUTE,"relative":numeric_comparison::RELATIVE},"sql_script":root.join("input.sql"),"warehouse":root.join("warehouse"),"performance_qualified":false}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wide_import_preserves_one_commit_results_and_reopen() -> Result<()> {
        let expressions = (0..64)
            .map(|i| format!("CAST({i} AS BIGINT) AS c{i}"))
            .collect::<Vec<_>>();
        let row = oracle(&format!("SELECT {}", expressions.join(",")), &Tables::new())
            .await?
            .rows;
        // More than one old row chunk with enough columns to exceed its binder budget.
        let rows = concat_batches(&row.schema(), std::iter::repeat_n(&row, 1025))?;
        let columns = row
            .schema()
            .fields()
            .iter()
            .map(|field| {
                Ok(format!(
                    "{} {}",
                    quote(field.name()),
                    sql_type(field.data_type())?
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let root = tempfile::tempdir()?;
        let mut db = SqlSession::open(root.path()).await?;
        db.execute(&format!("CREATE TABLE input({})", columns.join(",")))
            .await?;
        db.execute("CREATE MATERIALIZED VIEW maintained_result AS SELECT COUNT(*) AS n,SUM(c63) AS total FROM input")
            .await?;
        let report = execute_batch(&mut db, &insert("input", &rows)?)
            .await?
            .ok_or("missing import commit")?;
        assert_eq!(report.epoch, 3);
        assert_eq!(report.affected_rows, 1025);
        let expected = oracle(
            "SELECT CAST(1025 AS BIGINT) AS n,CAST(64575 AS BIGINT) AS total",
            &Tables::new(),
        )
        .await?
        .rows;
        for reopened in [false, true] {
            if reopened {
                drop(db);
                db = SqlSession::open(root.path()).await?;
            }
            let source = db
                .execute("SELECT * FROM input")
                .await?
                .rows
                .ok_or("source")?;
            equal(&source, &rows)?;
            for sql in [
                "SELECT COUNT(*) AS n,SUM(c63) AS total FROM input",
                "SELECT * FROM maintained_result",
            ] {
                let actual = db.execute(sql).await?.rows.ok_or("result")?;
                equal(&actual, &expected)?;
            }
            db.verify_output_history()?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn streaming_transaction_accepts_large_total_input_with_bounded_statements() -> Result<()>
    {
        let root = tempfile::tempdir()?;
        let mut db = SqlSession::open(root.path()).await?;
        db.execute("CREATE TABLE input(v VARCHAR)").await?;
        let payload = "x".repeat(8 * 1024 * 1024 + 1);
        let statement = format!("INSERT INTO input VALUES ('{payload}')");
        let statements = vec![statement.clone(), statement];
        assert!(statements.iter().map(String::len).sum::<usize>() > 16 * 1024 * 1024);
        let report = execute_batch(&mut db, &statements).await?.ok_or("commit")?;
        assert_eq!(report.epoch, 2);
        assert_eq!(report.affected_rows, 2);
        let count = db
            .execute("SELECT COUNT(*) AS n FROM input")
            .await?
            .rows
            .ok_or("count")?;
        equal(
            &count,
            &oracle("SELECT CAST(2 AS BIGINT) AS n", &Tables::new())
                .await?
                .rows,
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn generated_wide_sql_preserves_duplicates_nulls_literals_and_replays_as_a_script(
    ) -> Result<()> {
        let mut expressions = vec!["CAST(1 AS BIGINT) AS k".to_owned()];
        for i in 1..60 {
            expressions.push(format!("CAST({i} AS INT) AS c{i}"));
        }
        expressions.extend([
            "CAST('O''Brien' AS VARCHAR) AS text".into(),
            "CAST(NULL AS BIGINT) AS nullable".into(),
            "CAST('2001-02-03' AS DATE) AS day".into(),
            "CAST('1.25' AS DECIMAL(15,2)) AS price".into(),
        ]);
        let row = oracle(&format!("SELECT {}", expressions.join(",")), &Tables::new())
            .await?
            .rows;
        // Duplicate full rows are all matched; a distinct row remains for UPDATE.
        let mut arrays = row.columns().to_vec();
        arrays[0] = ScalarValue::Int64(Some(3)).to_array_of_size(1)?;
        let other = RecordBatch::try_new(row.schema(), arrays)?;
        let rows = concat_batches(
            &row.schema(),
            std::iter::repeat_n(&row, 130).chain([&other]),
        )?;
        let complete = Tables::from([("input".into(), rows)]);
        let dir = tempfile::tempdir()?;
        let result = run(
            "SELECT k,COUNT(*) AS n,SUM(price) AS total FROM input GROUP BY k",
            &complete,
            &dir.path().join("qualification"),
        )
        .await?;
        assert_eq!(result["status"], "pass");
        assert_eq!(result["completed_rounds"], 3);
        for round in result["rounds"].as_array().ok_or("rounds")? {
            assert_eq!(round["storage_work"]["manifest_writes"]["files"], 1);
            assert_eq!(round["storage_work"]["tables"]["read"]["files"], 0);
            assert_eq!(round["recovery_work"]["rebuilt_views"], 0);
            assert_eq!(round["recovery_work"]["restored_views"], 1);
            assert!(
                round["verification_store_work_before_reopen"]["manifest_reads"]["files"]
                    .as_u64()
                    .ok_or("manifest read count")?
                    > 0
            );
            assert!(
                round["verification_store_work_after_reopen"]["tables"]["read"]["files"]
                    .as_u64()
                    .ok_or("table read count")?
                    > 0
            );
            assert_eq!(round["source_work"], Value::Null);
            assert_eq!(round["performance_qualified"], false);
            assert!(round["commit"]["dml"]
                .as_array()
                .ok_or("DML work")?
                .iter()
                .all(|w| w["execution"].is_object()));
        }
        // Initial data is published once, independently of INSERT chunking.
        assert_eq!(result["bootstrap_epoch"], 3);
        let progress_path = Path::new(result["progress_file"].as_str().ok_or("progress path")?);
        let progress: Value = serde_json::from_slice(&fs::read(progress_path)?)?;
        assert_eq!(progress["status"], "complete");
        assert!(progress["active_stage"].is_null());
        let completed_stages = progress["completed_stages"]
            .as_array()
            .ok_or("completed stages")?;
        let stage_names: Vec<&str> = completed_stages
            .iter()
            .map(|entry| entry["stage"].as_str().unwrap_or_default())
            .collect();
        assert_eq!(
            stage_names,
            vec![
                "preflight",
                "bootstrap_ddl",
                "bootstrap_import",
                "bootstrap_mv",
                "bootstrap_verify",
                "round:insert_remaining_half:dml_execute",
                "round:insert_remaining_half:verify_before_reopen",
                "round:insert_remaining_half:reopen",
                "round:insert_remaining_half:verify_after_reopen",
                "round:delete_matching_rows:dml_execute",
                "round:delete_matching_rows:verify_before_reopen",
                "round:delete_matching_rows:reopen",
                "round:delete_matching_rows:verify_after_reopen",
                "round:update_keys:dml_execute",
                "round:update_keys:verify_before_reopen",
                "round:update_keys:reopen",
                "round:update_keys:verify_after_reopen",
            ]
        );
        assert_eq!(result["stage_timings"], progress["completed_stages"]);
        let script = fs::read_to_string(result["sql_script"].as_str().ok_or("script path")?)?;
        let mut db = SqlSession::open(dir.path().join("script-replay")).await?;
        db.execute_script(&script).await?;
        let result = db
            .execute("SELECT k,n,total FROM maintained_result")
            .await?
            .rows
            .ok_or("result")?;
        equal(&result,&oracle("SELECT CAST(4 AS BIGINT) AS k,COUNT(*) AS n,SUM(CAST('1.25' AS DECIMAL(15,2))) AS total",&Tables::new()).await?.rows)?;
        db.verify_output_history()?;
        Ok(())
    }
}
