//! Measure Q6 write and result-read times in a single warm SQL session.
use optd_incremental_sql::database::{display_rows, SqlSession};
use serde_json::{json, Value};
use std::{env, fs, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
async fn read(session: &mut SqlSession, sql: &str) -> Result<(u128, Value)> {
    let timer = Instant::now();
    let response = session.execute(sql).await?;
    let us = timer.elapsed().as_micros();
    let batch = response.rows.ok_or("missing query result")?;
    let schema: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().to_string(), f.is_nullable()))
        .collect();
    Ok((
        us,
        json!({"rows":display_rows(&batch)?, "schema":schema,
        "base_read":response.table_reads}),
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    let [_, root, workload, mode] = args.as_slice() else {
        return Err("usage: benchmark_q6_write WAREHOUSE WORKLOAD.json incremental|full".into());
    };
    let incremental = match mode.as_str() {
        "incremental" => true,
        "full" => false,
        _ => return Err("expected incremental or full".into()),
    };
    let mut db = SqlSession::open(root).await?;
    let work: Value = serde_json::from_slice(&fs::read(workload)?)?;
    let query = work["query"].as_str().ok_or("missing Q6")?;
    let mut bootstrap_us = None;
    if incremental {
        let timer = Instant::now();
        db.execute(&format!("CREATE MATERIALIZED VIEW q6_revenue AS {query}"))
            .await?;
        bootstrap_us = Some(timer.elapsed().as_micros());
    }
    let selection = if incremental {
        "SELECT * FROM q6_revenue"
    } else {
        query
    };
    let (_, initial) = read(&mut db, selection).await?;
    let mut rounds = Vec::new();
    for _ in 0..work["cycles"].as_u64().ok_or("missing cycles")? {
        for stage in work["stages"].as_array().ok_or("missing stages")? {
            let sql = stage["sql"].as_str().ok_or("missing DML")?;
            let timer = Instant::now();
            let change = db.execute(sql).await?;
            let dml_us = timer.elapsed().as_micros();
            let (read_us, mut result) = read(&mut db, selection).await?;
            let commit = change.commit.ok_or("missing commit")?;
            result["stage"] = stage["stage"].clone();
            result["affected"] = json!(commit.affected_rows);
            result["dml_us"] = json!(dml_us);
            result["read_us"] = json!(read_us);
            result["total_us"] = json!(dml_us + read_us);
            result["dml"] = json!(commit.dml);
            result["maintenance"] = json!(commit.maintenance);
            rounds.push(result);
        }
    }
    println!(
        "{}",
        json!({"bootstrap_us":bootstrap_us,"initial":initial,"rounds":rounds})
    );
    Ok(())
}
