//! Command-local work excludes successful open and original replayed receipt work.

#[path = "../examples/sql_database.rs"]
mod command;

use serde_json::Value;

async fn run(root: &std::path::Path, args: &[&str]) -> Value {
    let mut owned = vec![root.to_str().unwrap().to_owned()];
    owned.extend(args.iter().map(|s| s.to_string()));
    command::run(&owned).await.unwrap()
}

#[tokio::test]
async fn sql_command_separates_retry_recovery_and_verification_work() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("db");
    let script = temp.path().join("input.sql");
    std::fs::write(&script,
        "CREATE TABLE t(k BIGINT);INSERT INTO t VALUES (1),(1),(NULL);CREATE MATERIALIZED VIEW v AS SELECT k FROM t;")
        .unwrap();
    let initial = run(&root, &[script.to_str().unwrap()]).await;
    assert_eq!(initial["work"]["storage"]["manifest_writes"]["files"], 3);
    assert_eq!(initial["work"]["recovery"]["rebuilt_views"], 0);
    assert_eq!(
        initial["results"][0]["execution_work"]["create_mv_bootstrap_internal"],
        Value::Null
    );
    assert_eq!(initial["work"]["semantic_verified"], Value::Null);

    std::fs::write(&script, "INSERT INTO t VALUES (2)").unwrap();
    let args = ["--request", "one", script.to_str().unwrap()];
    let original = run(&root, &args).await;
    assert_eq!(
        original["execution_work"]["receipt_scope"],
        "current_execution"
    );
    assert_eq!(
        original["execution_work"]["dml"][0]["execution"]["selector"]["source_rows"],
        0
    );
    assert_eq!(original["work"]["storage"]["tables"]["written"]["rows"], 4);
    let retry = run(&root, &args).await;
    assert_eq!(retry["commit"], original["commit"]);
    assert_eq!(retry["replayed"], true);
    assert_eq!(retry["execution_work"]["receipt_scope"], "original_request");
    assert_eq!(retry["execution_work"]["dml"], Value::Null);
    assert_eq!(retry["execution_work"]["maintenance"], Value::Null);
    assert_eq!(retry["work"]["storage"]["tables"]["written"]["rows"], 0);
    assert_eq!(retry["work"]["storage"]["manifest_reads"]["files"], 4);
    assert_eq!(
        retry["work"]["recovery"]["store"]["manifest_reads"]["files"],
        5
    );
    assert_eq!(retry["work"]["recovery"]["rebuilt_views"], 1);
    let replay = run(&root, &["--replay"]).await;
    assert_eq!(replay["output_history_verified"], true);
    assert_eq!(replay["work"]["storage"]["manifest_reads"]["files"], 4);
    assert_eq!(replay["work"]["performance_qualified"], false);
}
