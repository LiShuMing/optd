//! Compile and dump text incremental plans for every query in a benchmark manifest (TPC-H / TPC-DS).
//!
//! Pure schema-only compilation: no CSV/data rows are loaded. Supports both `original` SQL and
//! `relational_core` (outer ORDER BY / LIMIT / FETCH removed) so original SQL and benchmark
//! variants remain strictly separated.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use optd_incremental_sql::planner::{IncrementalOptimizer, Strategy};
use optd_incremental_sql::SqlView;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct Manifest {
    suite: String,
    schema: String,
    queries: Vec<QueryEntry>,
}

#[derive(Deserialize)]
struct QueryEntry {
    query_id: usize,
    variants: Vec<VariantEntry>,
}

#[derive(Deserialize)]
struct VariantEntry {
    variant_id: String,
    sql: String,
}

fn prepare_mode(sql: &str, mode: &str) -> Result<(String, Vec<&'static str>), String> {
    let mut statements = DFParser::parse_sql(sql).map_err(|e| e.to_string())?;
    if statements.len() != 1 {
        return Err("expected exactly one query statement".into());
    }
    let Some(DFStatement::Statement(statement)) = statements.pop_front() else {
        return Err("expected a SQL query statement".into());
    };
    let SqlStatement::Query(mut query) = *statement else {
        return Err("expected SELECT query".into());
    };
    let mut removed = Vec::new();
    if mode == "relational_core" {
        if query.order_by.take().is_some() {
            removed.push("outer_order_by");
        }
        if query.limit_clause.take().is_some() {
            removed.push("outer_limit_offset");
        }
        if query.fetch.take().is_some() {
            removed.push("outer_fetch");
        }
    }
    let prepared = if mode == "original" {
        sql.to_owned()
    } else {
        query.to_string()
    };
    Ok((prepared, removed))
}

async fn load_schemas(schema_path: &Path) -> Result<BTreeMap<String, SchemaRef>, String> {
    let ddl = fs::read_to_string(schema_path)
        .map_err(|e| format!("failed to read {}: {e}", schema_path.display()))?;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let statements = DFParser::parse_sql(&ddl).map_err(|e| e.to_string())?;
    let mut schemas = BTreeMap::new();
    for stmt in statements {
        let DFStatement::Statement(sql_stmt) = stmt else {
            return Err("unexpected non-standard statement in schema.sql".into());
        };
        let plan = ctx
            .state()
            .statement_to_plan(DFStatement::Statement(sql_stmt))
            .await
            .map_err(|e| e.to_string())?;
        let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(create)) = plan else {
            return Err("expected CREATE TABLE statement in schema.sql".into());
        };
        let name = create.name.table().to_lowercase();
        let schema: SchemaRef = Arc::new(create.input.schema().as_arrow().clone());
        schemas.insert(name, schema);
    }
    Ok(schemas)
}

fn strategy_name(strategy: Strategy) -> &'static str {
    match strategy {
        Strategy::NativeDelta => "NativeDelta",
        Strategy::IndexedDelta => "IndexedDelta",
        Strategy::MatchCountDelta => "MatchCountDelta",
        Strategy::KeyedRecompute => "KeyedRecompute",
        Strategy::AccumulatorDelta => "AccumulatorDelta",
        Strategy::RankDelta => "RankDelta",
        Strategy::ChangedGroupRecompute => "ChangedGroupRecompute",
        Strategy::OperatorRecompute => "OperatorRecompute",
    }
}

fn file_stem(query_id: usize, variant_id: &str, multi_variant: bool) -> String {
    if multi_variant {
        format!("q{query_id:02}_{variant_id}")
    } else {
        format!("q{query_id:02}")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        eprintln!(
            "usage: explain_benchmarks <manifest.json> <output_dir> [--mode original|relational_core|both] [--detailed]"
        );
        std::process::exit(2);
    }
    let manifest_path = PathBuf::from(&args[0]);
    let output_root = PathBuf::from(&args[1]);
    let mut mode_arg = "both".to_owned();
    let mut detailed = false;
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" => {
                i += 1;
                mode_arg = args.get(i).cloned().ok_or("missing value for --mode")?;
            }
            "--detailed" => detailed = true,
            other => return Err(format!("unknown argument: {other}").into()),
        }
        i += 1;
    }
    let modes: Vec<&str> = match mode_arg.as_str() {
        "original" => vec!["original"],
        "relational_core" => vec!["relational_core"],
        "both" => vec!["original", "relational_core"],
        other => return Err(format!("invalid --mode {other}").into()),
    };

    let manifest_dir = manifest_path
        .parent()
        .ok_or("manifest path has no parent directory")?;
    let manifest: Manifest = serde_json::from_str(&fs::read_to_string(&manifest_path)?)?;
    let schemas = load_schemas(&manifest_dir.join(&manifest.schema)).await?;
    let suite_dir = output_root.join(&manifest.suite);
    fs::create_dir_all(&suite_dir)?;

    let mut mode_summaries = BTreeMap::<String, Vec<serde_json::Value>>::new();

    for mode in &modes {
        let mode_dir = suite_dir.join(mode);
        fs::create_dir_all(&mode_dir)?;
        let mut records = Vec::new();
        let mut strict_count = 0usize;
        let mut error_count = 0usize;

        for query in &manifest.queries {
            let multi = query.variants.len() > 1;
            for variant in &query.variants {
                let sql_path = manifest_dir.join(&variant.sql);
                let raw_sql = fs::read_to_string(&sql_path)?;
                let stem = file_stem(query.query_id, &variant.variant_id, multi);
                let out_path = mode_dir.join(format!("{stem}.txt"));

                let (prepared_sql, removed) = match prepare_mode(&raw_sql, mode) {
                    Ok(pair) => pair,
                    Err(err) => {
                        error_count += 1;
                        fs::write(
                            &out_path,
                            format!(
                                "Suite: {} | Query: {} | Mode: {}\nParse Error: {err}\n",
                                manifest.suite, stem, mode
                            ),
                        )?;
                        records.push(json!({
                            "query_id": query.query_id,
                            "variant_id": variant.variant_id,
                            "stem": stem,
                            "mode": mode,
                            "removed_clauses": [],
                            "status": "parse_error",
                            "strict_eligible": false,
                            "error": err,
                        }));
                        continue;
                    }
                };

                match SqlView::explain_query(
                    &prepared_sql,
                    schemas.clone(),
                    IncrementalOptimizer::default(),
                )
                .await
                {
                    Ok(explained) => {
                        if explained.preflight.strict_plan_eligible {
                            strict_count += 1;
                        }
                        let mut strategies = BTreeMap::<String, usize>::new();
                        for node in &explained.preflight.planned_nodes {
                            let label = node
                                .strategy
                                .map_or("CompileFailed", strategy_name)
                                .to_owned();
                            *strategies.entry(label).or_default() += 1;
                        }
                        let state_count = explained.plan.as_ref().map_or(0, |p| {
                            p.programs().iter().filter(|pr| pr.state.is_some()).count()
                        });
                        let snapshot_count: usize = explained
                            .preflight
                            .planned_nodes
                            .iter()
                            .map(|n| n.snapshot_dependencies.len())
                            .sum();
                        let blocker_reasons: Vec<String> = explained
                            .preflight
                            .blockers
                            .iter()
                            .map(|b| format!("#{} {}: {}", b.id, b.rule, b.reason))
                            .collect();
                        let rendered = if detailed {
                            explained.explain_detailed()
                        } else {
                            explained.explain_tree()
                        };
                        let header = format!(
                            "Suite: {} | Query: {} | Mode: {} | Removed Clauses: {:?}\n{}\n",
                            manifest.suite,
                            stem,
                            mode,
                            removed,
                            "=".repeat(78)
                        );
                        fs::write(&out_path, format!("{header}{rendered}"))?;
                        records.push(json!({
                            "query_id": query.query_id,
                            "variant_id": variant.variant_id,
                            "stem": stem,
                            "mode": mode,
                            "removed_clauses": removed,
                            "status": if explained.preflight.strict_plan_eligible { "strict_eligible" } else { "blocked" },
                            "strict_eligible": explained.preflight.strict_plan_eligible,
                            "nodes": explained.preflight.planned_nodes.len(),
                            "states": state_count,
                            "snapshots": snapshot_count,
                            "blockers": explained.preflight.blockers.len(),
                            "strategies": strategies,
                            "blocker_reasons": blocker_reasons,
                            "numerical_columns": explained.preflight.numerical_columns,
                        }));
                    }
                    Err(err) => {
                        error_count += 1;
                        fs::write(
                            &out_path,
                            format!(
                                "Suite: {} | Query: {} | Mode: {} | Removed Clauses: {:?}\nCompile Error: {err}\n",
                                manifest.suite, stem, mode, removed
                            ),
                        )?;
                        records.push(json!({
                            "query_id": query.query_id,
                            "variant_id": variant.variant_id,
                            "stem": stem,
                            "mode": mode,
                            "removed_clauses": removed,
                            "status": "compile_error",
                            "strict_eligible": false,
                            "error": err.to_string(),
                        }));
                    }
                }
            }
        }

        let summary_json = json!({
            "suite": manifest.suite,
            "mode": mode,
            "detailed": detailed,
            "total_variants": records.len(),
            "strict_eligible": strict_count,
            "blocked": records.len() - strict_count - error_count,
            "errors": error_count,
            "queries": records,
        });
        fs::write(
            mode_dir.join("summary.json"),
            serde_json::to_string_pretty(&summary_json)?,
        )?;
        println!(
            "suite={} mode={} total={} strict_eligible={} blocked={} errors={} dir={}",
            manifest.suite,
            mode,
            records.len(),
            strict_count,
            records.len() - strict_count - error_count,
            error_count,
            mode_dir.display()
        );
        mode_summaries.insert((*mode).to_owned(), records);
    }

    let mut md = String::new();
    let _ = writeln!(
        md,
        "# {} Incremental Plan Summary\n",
        manifest.suite.to_uppercase()
    );
    for (mode, records) in &mode_summaries {
        let strict = records
            .iter()
            .filter(|r| r["strict_eligible"].as_bool() == Some(true))
            .count();
        let errors = records
            .iter()
            .filter(|r| {
                r["status"].as_str() == Some("compile_error")
                    || r["status"].as_str() == Some("parse_error")
            })
            .count();
        let blocked = records.len() - strict - errors;
        let _ = writeln!(
            md,
            "## Mode: `{mode}` — strict_eligible={strict}/{}, blocked={blocked}, errors={errors}\n",
            records.len()
        );
        let _ = writeln!(
            md,
            "| Query | Status | Nodes | States | Snapshots | Blockers | Strategies | Notes |"
        );
        let _ = writeln!(md, "| --- | --- | ---: | ---: | ---: | ---: | --- | --- |");
        for rec in records {
            let stem = rec["stem"].as_str().unwrap_or("?");
            let status = rec["status"].as_str().unwrap_or("?");
            let nodes = rec["nodes"]
                .as_u64()
                .map_or("-".to_owned(), |v| v.to_string());
            let states = rec["states"]
                .as_u64()
                .map_or("-".to_owned(), |v| v.to_string());
            let snaps = rec["snapshots"]
                .as_u64()
                .map_or("-".to_owned(), |v| v.to_string());
            let blockers = rec["blockers"]
                .as_u64()
                .map_or("-".to_owned(), |v| v.to_string());
            let strategies = rec["strategies"].as_object().map_or("-".to_owned(), |map| {
                map.iter()
                    .map(|(k, v)| format!("{k}={}", v.as_u64().unwrap_or(0)))
                    .collect::<Vec<_>>()
                    .join(", ")
            });
            let note = if let Some(err) = rec["error"].as_str() {
                err.replace('\n', " ")
            } else if let Some(reasons) = rec["blocker_reasons"].as_array() {
                reasons
                    .iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            } else {
                String::new()
            };
            let _ = writeln!(
                md,
                "| `{stem}` | {status} | {nodes} | {states} | {snaps} | {blockers} | {strategies} | {note} |"
            );
        }
        let _ = writeln!(md);
    }
    fs::write(suite_dir.join("SUMMARY.md"), md)?;
    Ok(())
}
