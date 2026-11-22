//! Tests for readable text incremental plan EXPLAIN rendering and SQL `EXPLAIN` statements.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use optd_incremental_sql::database::SqlSession;
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::SqlView;
use tempfile::tempdir;

fn lineitem_schemas() -> BTreeMap<String, Arc<Schema>> {
    let mut schemas = BTreeMap::new();
    schemas.insert(
        "orders".into(),
        Arc::new(Schema::new(vec![
            Field::new("o_key", DataType::Int64, false),
            Field::new("o_cust", DataType::Int64, false),
            Field::new("o_amount", DataType::Int64, false),
        ])),
    );
    schemas.insert(
        "customers".into(),
        Arc::new(Schema::new(vec![
            Field::new("c_key", DataType::Int64, false),
            Field::new("c_region", DataType::Utf8, false),
        ])),
    );
    schemas
}

#[tokio::test]
async fn explain_query_renders_unicode_dag_tree_states_and_detailed_steps() {
    let sql = "SELECT c.c_region, SUM(o.o_amount) AS total, COUNT(o.o_key) AS cnt \
               FROM orders o INNER JOIN customers c ON o.o_cust = c.c_key \
               WHERE o.o_amount > 10 GROUP BY c.c_region";
    let explained =
        SqlView::explain_query(sql, lineitem_schemas(), IncrementalOptimizer::default())
            .await
            .expect("schema-only explain must succeed");
    assert!(explained.preflight.strict_plan_eligible);
    assert!(explained.plan.is_some());

    let tree = explained.explain_tree();
    assert!(tree.contains("strict_eligible=true"), "{tree}");
    assert!(tree.contains("Logical DAG:"), "{tree}");
    assert!(tree.contains("AccumulatorDelta"), "{tree}");
    assert!(tree.contains("state=Accumulator("), "{tree}");
    assert!(tree.contains("state=Keyed("), "{tree}");
    assert!(tree.contains("├─") || tree.contains("└─"), "{tree}");
    assert!(!tree.contains("Physical Programs (DAG steps):"), "{tree}");

    let detailed = explained.explain_detailed();
    assert!(
        detailed.contains("Physical Programs (DAG steps):"),
        "{detailed}"
    );
    assert!(detailed.contains("UpdateAccumulator"), "{detailed}");
    assert!(detailed.contains("DeltaRead("), "{detailed}");
}

#[tokio::test]
async fn explain_query_renders_shared_dag_nodes_without_exponential_duplication() {
    let sql = "WITH high_orders AS (SELECT o_cust, o_amount FROM orders WHERE o_amount > 100) \
               SELECT a.o_amount, b.o_amount AS b_amount \
               FROM high_orders a INNER JOIN high_orders b ON a.o_cust = b.o_cust";
    let explained =
        SqlView::explain_query(sql, lineitem_schemas(), IncrementalOptimizer::default())
            .await
            .expect("shared CTE explain must succeed");
    let tree = explained.explain_tree();
    assert!(tree.contains("strict_eligible=true"), "{tree}");
    assert!(
        tree.contains("(shared, see above)"),
        "shared CTE node should be referenced instead of duplicated:\n{tree}"
    );
}

#[tokio::test]
async fn explain_query_renders_blocked_nodes_inline_and_in_footer() {
    let sql = "SELECT o_key, o_amount FROM orders ORDER BY o_amount DESC LIMIT 5";
    let explained =
        SqlView::explain_query(sql, lineitem_schemas(), IncrementalOptimizer::default())
            .await
            .expect("blocked query explain must succeed without failing");
    assert!(!explained.preflight.strict_plan_eligible);
    assert!(!explained.preflight.blockers.is_empty());

    let tree = explained.explain_tree();
    assert!(tree.contains("strict_eligible=false"), "{tree}");
    assert!(tree.contains("BLOCKED:"), "{tree}");
    assert!(tree.contains("Blockers ("), "{tree}");

    let preflight_tree = explained.preflight.explain_tree();
    assert!(
        preflight_tree.contains("strict_eligible=false"),
        "{preflight_tree}"
    );
    assert!(preflight_tree.contains("BLOCKED:"), "{preflight_tree}");
}

#[tokio::test]
async fn sql_session_supports_sql_explain_verbose_and_view_explain() {
    let dir = tempdir().unwrap();
    let mut session = SqlSession::open(dir.path()).await.unwrap();
    session
        .execute("CREATE TABLE orders (o_key BIGINT, o_cust BIGINT, o_amount BIGINT)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO orders VALUES (1, 10, 50), (2, 10, 75)")
        .await
        .unwrap();
    session
        .execute(
            "CREATE MATERIALIZED VIEW mv_sum AS \
             SELECT o_cust, SUM(o_amount) AS total FROM orders GROUP BY o_cust",
        )
        .await
        .unwrap();

    // 1. SQL EXPLAIN SELECT ... (read-only, zero base-table reads)
    let resp = session
        .execute("EXPLAIN SELECT o_cust, SUM(o_amount) FROM orders GROUP BY o_cust")
        .await
        .unwrap();
    assert!(resp.table_reads.is_none());
    let batch = resp
        .rows
        .expect("EXPLAIN must return a single-column plan batch");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "plan");
    let text = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert!(text.contains("strict_eligible=true"), "{text}");
    assert!(text.contains("AccumulatorDelta"), "{text}");
    assert!(!text.contains("Physical Programs (DAG steps):"), "{text}");

    // 2. SQL EXPLAIN VERBOSE SELECT ...
    let verbose = session
        .execute("EXPLAIN VERBOSE SELECT o_cust, SUM(o_amount) FROM orders GROUP BY o_cust")
        .await
        .unwrap();
    let verbose_text = verbose
        .rows
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_owned();
    assert!(
        verbose_text.contains("Physical Programs (DAG steps):"),
        "{verbose_text}"
    );

    // 3. Session explain_view_tree / explain_view_detailed match SQL EXPLAIN output
    let view_tree = session.explain_view_tree("mv_sum").unwrap();
    assert!(view_tree.contains("AccumulatorDelta"), "{view_tree}");
    assert!(view_tree.contains("state=Accumulator("), "{view_tree}");
    assert!(session
        .explain_view_detailed("mv_sum")
        .unwrap()
        .contains("Physical Programs (DAG steps):"));

    // 4. EXPLAIN works inside explicit transaction without mutating state
    session.execute("BEGIN").await.unwrap();
    let tx_resp = session
        .execute("EXPLAIN SELECT o_key FROM orders WHERE o_amount > 0")
        .await
        .unwrap();
    assert!(tx_resp.rows.is_some());
    session.execute("ROLLBACK").await.unwrap();

    // 5. EXPLAIN ANALYZE is explicitly refused
    let err = session
        .execute("EXPLAIN ANALYZE SELECT o_key FROM orders")
        .await
        .err()
        .expect("EXPLAIN ANALYZE must be rejected");
    assert!(
        err.to_string().contains("EXPLAIN ANALYZE is not supported"),
        "unexpected error: {err}"
    );
}
