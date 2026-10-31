//! SQL frontend qualification with independently constructed source endpoints.
use super::*;
use datafusion::arrow::array::{ArrayRef, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use optd_incremental_sql::database::SqlSession;

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
    let mut statements = Vec::new();
    // Keep binder expression counts and SQL buffers bounded even for larger fixtures.
    for start in (0..b.num_rows()).step_by(1024) {
        let rows = (start..(start + 1024).min(b.num_rows()))
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
async fn verify(db: &mut SqlSession, sql: &str, current: &Tables) -> Result<RecordBatch> {
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
    equal(&actual, &expected)?;
    db.verify_output_history()?;
    Ok(expected)
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
    let referenced = referenced_tables(sql, complete).await?;
    let mut current: Tables = referenced
        .iter()
        .map(|(n, b)| (n.clone(), b.slice(0, b.num_rows() / 2)))
        .collect();
    let probe = SqlView::new(sql, current.clone())
        .await
        .map_err(|e| format!("plan_probe: {e}"))?;
    let qualification = plan_qualification(&probe);
    if qualification["strict_plan_eligible"] != true {
        return Ok(
            json!({"status":"unsupported_strict_plan","plan_qualification":qualification,"completed_rounds":0,"rounds":[]}),
        );
    }
    drop(probe);
    // Per-query roots must be fresh; never reset another run's committed state.
    fs::create_dir(root)?;
    let mut script = String::new();
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
    let ddl = format!("CREATE MATERIALIZED VIEW maintained_result AS {sql}");
    db.execute(&ddl)
        .await
        .map_err(|e| format!("bootstrap_ddl: {e}"))?;
    script.push_str(&format!("{ddl};\n"));
    let mut previous = verify(&mut db, sql, &current).await?;
    let initial_rows = previous.num_rows();
    let bootstrap_epoch = db.epoch();
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
        let storage_before = db.store_work()?;
        let report = execute_batch(&mut db, &statements)
            .await
            .map_err(|e| format!("{stage}: {e}"))?;
        let storage_after = db.store_work()?;
        let storage_work = storage_after.since(&storage_before)?;
        let expected = verify(&mut db, sql, &current).await?;
        let verification_before = db.store_work()?.since(&storage_after)?;
        nonempty |= expected.num_rows() > 0;
        output_changed |= bag(&previous)? != bag(&expected)?;
        drop(db);
        db = SqlSession::open(root.join("warehouse")).await?;
        let recovery_work = db.recovery_work().clone();
        let reopened = db.store_work()?;
        verify(&mut db, sql, &current).await?;
        let verification_after = db.store_work()?.since(&reopened)?;
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
    Ok(
        json!({"status":if !nonempty{"verified_empty_only"}else if !output_changed{"verified_unchanged_output"}else{"pass"},
        "plan_qualification":qualification,"bootstrap_epoch":bootstrap_epoch,"initial_rows":initial_rows,"final_rows":previous.num_rows(),"nonempty_result_seen":nonempty,"output_delta_seen":output_changed,
        "completed_rounds":rounds.len(),"rounds":rounds,"oracle":"independent_DataFusion_full_SQL_over_independent_source_endpoints","comparison":"exact_typed_bag_sources_and_MV_plus_output_log_replay","sql_script":root.join("input.sql"),"warehouse":root.join("warehouse"),"performance_qualified":false}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
            assert_eq!(round["recovery_work"]["rebuilt_views"], 1);
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
