//! SQL-only materialized-view loop with one durable database transaction authority.

use std::path::Path;

use optd_incremental_sql::database::{display_rows, SqlResponse, SqlSession};
use optd_incremental_sql::{Error, Result};
use serde_json::{json, Value};

fn response(result: SqlResponse) -> Result<Value> {
    let output = result.rows.as_ref().map(|batch| -> Result<Value> {
        Ok(json!({"schema":batch.schema().fields().iter().map(|f|json!({"name":f.name(),"type":f.data_type().to_string(),"nullable":f.is_nullable()})).collect::<Vec<_>>(),
            "rows":display_rows(batch)?}))
    }).transpose()?;
    let executed = result.commit.as_ref().filter(|_| !result.replayed);
    Ok(
        json!({"epoch":result.epoch,"output":output,"commit":result.commit,"replayed":result.replayed,
        "execution_work": {"dml":executed.map(|r| &r.dml),
            "maintenance":executed.map(|r| &r.maintenance),
            "receipt_scope":if result.replayed {"original_request"} else if executed.is_some() {"current_execution"} else {"unmeasured"},
            "read_selector":null,"create_mv_bootstrap_internal":null}}),
    )
}

fn script(path: &str) -> Result<String> {
    if std::fs::metadata(path)?.len() > 16 * 1024 * 1024 {
        return Err(Error::Invalid("SQL script exceeds 16MiB budget".into()));
    }
    let text = std::fs::read_to_string(path)?;
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Invalid("SQL script exceeds 16MiB budget".into()));
    }
    Ok(text)
}

pub(crate) async fn run(args: &[String]) -> Result<Value> {
    let Some(root) = args.first() else {
        return Err(Error::Invalid("usage: sql_database <warehouse> <script.sql> | --request <id> <script.sql> | --transaction <id> <script.sql> | --replay | --explain <view>".into()));
    };
    let mut session = SqlSession::open(Path::new(root)).await?;
    let before = session.store_work()?;
    let recovery = session.recovery_work().clone();
    let mut result = match &args[1..] {
        [path] if path == "--replay" => {
            session.verify_output_history()?;
            Ok(json!({"epoch":session.epoch(),"output_history_verified":true}))
        }
        [option, name] if option == "--explain" => {
            Ok(json!({"epoch":session.epoch(),"plan":session.explain_view(name)?}))
        }
        [option, id, path] if option == "--request" => {
            response(session.execute_request(id, &script(path)?).await?)
        }
        [option, id, path] if option == "--transaction" => {
            response(session.execute_transaction(id, &script(path)?).await?)
        }
        [path] => {
            let mut results = Vec::new();
            for result in session.execute_script(&script(path)?).await? {
                results.push(response(result)?);
            }
            Ok(json!({"results":results,"committed_epoch":session.epoch()}))
        }
        _ => Err(Error::Invalid("invalid SQL database arguments".into())),
    }?;
    result["work"] = json!({"storage":session.store_work()?.since(&before)?,
        "recovery":recovery,"semantic_verified":null,"affected_scope_qualified":null,
        "performance_qualified":false,"allocator_bytes":null,"physical_device_io":null});
    Ok(result)
}

#[cfg(not(test))]
#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match run(&args).await {
        Ok(result) => println!("{result}"),
        Err(error) => {
            eprintln!("{}", json!({"error":error.to_string()}));
            std::process::exit(1);
        }
    }
}
