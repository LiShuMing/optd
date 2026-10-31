//! Qualify SQL DDL/DML and durable materialized views against benchmark queries.
//!
//! `cargo run -p optd-incremental-sql --example benchmark_sql_database -- MANIFEST OUTPUT [QUERY]`

#[path = "../benchmarks/harness.rs"]
mod harness;

use std::error::Error;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    if std::env::var_os("OPTD_SQL_BENCH_DATABASE_ROOT").is_none() {
        return Err(
            "set OPTD_SQL_BENCH_DATABASE_ROOT to a fresh SQL database artifact directory".into(),
        );
    }
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if !(2..=3).contains(&arguments.len()) {
        return Err(
            "usage: benchmark_sql_database <manifest.json> <coverage.json> [logical_query_id]"
                .into(),
        );
    }
    let selected = arguments.get(2).map(|value| value.parse()).transpose()?;
    let result = harness::run_manifest(Path::new(&arguments[0]), selected).await?;
    std::fs::write(&arguments[1], serde_json::to_vec_pretty(&result)?)?;
    println!(
        "{}",
        serde_json::json!({"output":arguments[1],"suite":result["suite"],"executed":result["executed_variants"],"passed":result["passed_variants"],"failed":result["failed_variants"],"unsupported_strict_plan":result["unsupported_strict_plan_variants"],"empty_only":result["verified_empty_only_variants"],"unchanged_output":result["verified_unchanged_output_variants"],"incomplete":result["incomplete_variants"],"nonempty_final":result["nonempty_final_variants"]})
    );
    if result["incomplete_variants"].as_u64().unwrap_or(1) != 0 {
        std::process::exit(1);
    }
    Ok(())
}
