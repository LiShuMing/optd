//! Run a complete typed SQL checkpoint loop; see docs/src/incremental/sql_cli.md.

#[path = "sql_loop/mod.rs"]
mod command;

#[tokio::main]
async fn main() {
    let args = std::env::args_os()
        .skip(1)
        .map(|value| {
            value.into_string().map_err(|_| {
                optd_incremental_sql::Error::Invalid("arguments must be valid UTF-8".into())
            })
        })
        .collect::<optd_incremental_sql::Result<Vec<_>>>();
    let result = match args {
        Ok(args) => command::run(&args).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(value) => println!("{value}"),
        Err(error) => {
            eprintln!("{}", serde_json::json!({"error": error.to_string()}));
            std::process::exit(1);
        }
    }
}
