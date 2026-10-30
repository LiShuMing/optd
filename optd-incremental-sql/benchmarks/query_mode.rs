//! Explicit benchmark variants; production SQL planning never drops clauses.

use super::{json, Result, Value};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use sha2::{Digest, Sha256};

pub(super) fn prepare(sql: &str, mode: &str) -> Result<(String, Value)> {
    if !matches!(mode, "original" | "relational_core") {
        return Err("OPTD_SQL_BENCH_QUERY_MODE must be original or relational_core".into());
    }
    let mut statements = DFParser::parse_sql(sql)?;
    if statements.len() != 1 {
        return Err("benchmark query mode requires exactly one query statement".into());
    }
    let Some(Statement::Statement(statement)) = statements.pop_front() else {
        return Err("benchmark query mode requires a SQL query".into());
    };
    let SqlStatement::Query(mut query) = *statement else {
        return Err("benchmark query mode rejects non-query statements".into());
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
    let executed = if mode == "original" {
        sql.to_owned()
    } else {
        query.to_string()
    };
    let metadata = json!({
        "mode":mode,"removed_clauses":removed,
        "original_sql_sha256":format!("{:x}",Sha256::digest(sql.as_bytes())),
        "executed_sql_sha256":format!("{:x}",Sha256::digest(executed.as_bytes())),
        "executed_sql":executed,
        "original_query_semantics_preserved":removed.is_empty(),
        "qualification":if removed.is_empty() {"original_query"} else {"relational_core_excludes_outer_order_and_row_bounds"},
    });
    Ok((executed, metadata))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_only_outer_presentation_and_records_changed_membership() {
        let sql = "SELECT k, row_number() OVER (PARTITION BY k ORDER BY v) AS r FROM (SELECT * FROM t ORDER BY v LIMIT 3) x ORDER BY k LIMIT 2 OFFSET 1";
        let (core, meta) = prepare(sql, "relational_core").unwrap();
        assert!(core.contains("PARTITION BY k ORDER BY v"));
        assert!(core.contains("ORDER BY v LIMIT 3"));
        assert!(!core.contains("ORDER BY k"));
        assert_eq!(
            meta["removed_clauses"],
            json!(["outer_order_by", "outer_limit_offset"])
        );
        assert_eq!(meta["original_query_semantics_preserved"], false);
        let (unchanged, meta) = prepare(sql, "original").unwrap();
        assert_eq!(unchanged, sql);
        assert_eq!(meta["original_sql_sha256"], meta["executed_sql_sha256"]);
    }

    #[test]
    fn preserves_literals_and_rejects_non_queries() {
        let (core, _) = prepare(
            "SELECT 'order by limit' AS x ORDER BY x FETCH FIRST 2 ROWS ONLY",
            "relational_core",
        )
        .unwrap();
        assert!(core.contains("'order by limit'"));
        assert!(!core.contains("FETCH FIRST"));
        assert!(prepare("DELETE FROM t", "relational_core").is_err());
        assert!(prepare("SELECT 1; SELECT 2", "relational_core").is_err());
        assert!(prepare("SELECT 1", "unknown").is_err());
    }
}
