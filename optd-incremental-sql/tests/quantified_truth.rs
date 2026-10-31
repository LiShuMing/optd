//! Independent three-valued membership and observable UNKNOWN over SQL writes.

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use optd_incremental_sql::database::{display_rows, SqlSession};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{SqlView, Strategy, TableChange};
use std::collections::BTreeMap;
use std::sync::Arc;

const PROJECTED: &str =
    "SELECT k,k IN (SELECT k FROM b) AS m,k NOT IN (SELECT k FROM b) AS n FROM a";

fn batch(values: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

fn tables(left: &[Option<i64>], right: &[Option<i64>]) -> BTreeMap<String, RecordBatch> {
    BTreeMap::from([("a".into(), batch(left)), ("b".into(), batch(right))])
}

type Rows = BTreeMap<Vec<String>, i64>;

fn add(rows: &mut Rows, row: Vec<String>, weight: i64) {
    let next = rows.get(&row).copied().unwrap_or_default() + weight;
    if next == 0 {
        rows.remove(&row);
    } else {
        rows.insert(row, next);
    }
}

fn values(batch: &RecordBatch, signed: bool) -> Rows {
    let mut result = Rows::new();
    for mut row in display_rows(batch).unwrap() {
        let weight = if signed {
            row.pop().unwrap().parse().unwrap()
        } else {
            1
        };
        add(&mut result, row, weight);
    }
    result
}

const QUERIES: [&str; 6] = [
    PROJECTED,
    "SELECT k FROM a WHERE k=9 OR k NOT IN (SELECT k FROM b)",
    "SELECT k FROM a WHERE NOT (k=9 OR k IN (SELECT k FROM b))",
    "SELECT k FROM a WHERE (k IN (SELECT k FROM b)) IS FALSE",
    "SELECT k FROM a WHERE (k IN (SELECT k FROM b)) = FALSE",
    "SELECT k FROM a WHERE CASE WHEN k IN (SELECT k FROM b) THEN FALSE ELSE TRUE END",
];

/// Scalar three-valued logic, independent of the SQL optimizer under test.
fn expected_query(mode: usize, left: &[Option<i64>], right: &[Option<i64>]) -> Rows {
    let mut result = Rows::new();
    if mode == 0 {
        for row in expected(left, right) {
            add(&mut result, row, 1);
        }
        return result;
    }
    for k in left {
        let m = membership(*k, right);
        let keep = match mode {
            1 => *k == Some(9) || m == Some(false),
            2 => k.is_some() && *k != Some(9) && m == Some(false),
            3 | 4 => m == Some(false),
            5 => m != Some(true),
            _ => unreachable!("test query index"),
        };
        if keep {
            add(&mut result, vec![text(*k)], 1);
        }
    }
    result
}

#[tokio::test]
async fn signed_truth_prefixes_cover_null_empty_and_two_source_transitions() {
    for (mode, query) in QUERIES.iter().enumerate() {
        let mut data = [vec![Some(1), Some(2), Some(2), Some(9), None], vec![]];
        let mut view = SqlView::new_with_optimizer(
            query,
            tables(&data[0], &data[1]),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let mut replay = values(&view.contents().unwrap(), false);
        assert_eq!(replay, expected_query(mode, &data[0], &data[1]));
        // Explicitly cross first/last NULL, duplicate NULL/value and empty boundaries.
        let directed = [
            (None, 1),
            (None, 1),
            (Some(1), 1),
            (Some(1), 1),
            (None, -1),
            (None, -1),
            (Some(1), -1),
            (Some(1), -1),
        ];
        let mut seed = 0x92b_u64;
        for round in 0..32 {
            let before = view.contents().unwrap();
            let mut changes = Vec::new();
            for side in 0..2 {
                if side == 0 && round < 8 {
                    continue;
                }
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let (value, weight) =
                    if let Some(change) = directed.get(round).filter(|_| side == 1) {
                        *change
                    } else if !data[side].is_empty() && seed.is_multiple_of(3) {
                        (data[side][(seed as usize >> 12) % data[side].len()], -1)
                    } else {
                        let k = (seed >> 20) % 5;
                        ((k != 4).then_some(k as i64), 1)
                    };
                if weight == 1 {
                    data[side].push(value);
                } else {
                    let position = data[side].iter().position(|v| *v == value).unwrap();
                    data[side].remove(position);
                }
                changes.push(TableChange {
                    table: ["a", "b"][side].into(),
                    rows: batch(&[value]),
                    weights: vec![weight],
                });
            }
            let expected = expected_query(mode, &data[0], &data[1]);
            let discarded = view.prepare(&changes).await.unwrap();
            assert_eq!(
                values(discarded.contents(), false),
                expected,
                "discard mode {mode} round {round}"
            );
            drop(discarded);
            assert_eq!(view.contents().unwrap(), before);
            let candidate = view.prepare(&changes).await.unwrap();
            for (row, weight) in values(&candidate.delta().unwrap(), true) {
                add(&mut replay, row, weight);
            }
            assert_eq!(replay, expected, "prefix mode {mode} round {round}");
            assert_eq!(values(candidate.contents(), false), expected);
            assert!(candidate
                .stats()
                .nodes
                .iter()
                .all(|n| n.strategy != Strategy::OperatorRecompute));
            let stale = view.prepare(&changes).await.unwrap();
            view.acknowledge(candidate).unwrap();
            assert!(view.acknowledge(stale).is_err());
        }
    }
}

#[tokio::test]
async fn strict_projected_truth_maintains_global_flags() {
    let left = vec![Some(1), Some(2), Some(2), None];
    let mut view = SqlView::new_with_optimizer(
        PROJECTED,
        tables(&left, &[Some(1)]),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap();
    assert!(view
        .incremental_plan()
        .programs()
        .iter()
        .all(|n| n.strategy() != Strategy::OperatorRecompute));
    let candidate = view
        .prepare(&[TableChange {
            table: "b".into(),
            rows: batch(&[None]),
            weights: vec![1],
        }])
        .await
        .unwrap();
    let mut rows = display_rows(candidate.contents()).unwrap();
    rows.sort();
    assert_eq!(rows, expected(&left, &[Some(1), None]));
    view.acknowledge(candidate).unwrap();
}

fn text(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".into(), |v| v.to_string())
}

/// SQL scalar IN: a TRUE match wins, then UNKNOWN comparisons, else FALSE.
fn membership(left: Option<i64>, right: &[Option<i64>]) -> Option<bool> {
    if right.iter().any(|v| left.is_some() && *v == left) {
        Some(true)
    } else if right.iter().any(|v| left.is_none() || v.is_none()) {
        None
    } else {
        Some(false)
    }
}

fn expected(left: &[Option<i64>], right: &[Option<i64>]) -> Vec<Vec<String>> {
    let flag = |v: Option<bool>| v.map_or_else(|| "NULL".into(), |v| v.to_string());
    let mut result = left
        .iter()
        .map(|v| {
            let m = membership(*v, right);
            vec![text(*v), flag(m), flag(m.map(|v| !v))]
        })
        .collect::<Vec<_>>();
    result.sort();
    result
}

#[tokio::test]
async fn projected_membership_and_negative_disjunction_preserve_unknown() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(k BIGINT);CREATE TABLE b(k BIGINT);INSERT INTO a VALUES (1),(2),(2),(NULL);INSERT INTO b VALUES (1),(NULL),(NULL)").await.unwrap();
    let output = db
        .execute("SELECT k,k IN (SELECT k FROM b) AS m,k NOT IN (SELECT k FROM b) AS n FROM a")
        .await
        .unwrap()
        .rows
        .unwrap();
    let mut rows = display_rows(&output).unwrap();
    rows.sort();
    assert_eq!(
        rows,
        expected(&[Some(1), Some(2), Some(2), None], &[Some(1), None, None])
    );
    let result = db
        .execute("SELECT k FROM a WHERE k=9 OR k NOT IN (SELECT k FROM b)")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(result.num_rows(), 0);
    use datafusion::arrow::datatypes::DataType;
    for index in [1, 2] {
        assert_eq!(output.schema().field(index).data_type(), &DataType::Boolean);
        assert!(output.schema().field(index).is_nullable());
    }
}

#[tokio::test]
async fn duplicate_counts_cancel_flags_but_first_null_affects_unchanged_keys() {
    for untouched in [8, 512] {
        let left = (1..=untouched).map(Some).chain([None]).collect::<Vec<_>>();
        let mut view = SqlView::new_with_optimizer(
            PROJECTED,
            tables(&left, &[Some(1), None]),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let duplicate = view
            .prepare(&[TableChange {
                table: "b".into(),
                rows: batch(&[Some(1), None]),
                weights: vec![1, 1],
            }])
            .await
            .unwrap();
        assert_eq!(duplicate.delta().unwrap().num_rows(), 0);
        for node in &duplicate.stats().nodes {
            if node.operator.contains("Join:") {
                assert_eq!(node.delta_rows, 0);
            }
            if let Some(work) = &node.keyed_work {
                assert_eq!(work.full_input_scans, 0);
                assert_eq!(
                    work.snapshot_evaluations_before + work.snapshot_evaluations_after,
                    0
                );
                if let Some(work) = &work.match_join_work {
                    assert_eq!(work.peer_entries_visited, 0);
                }
            }
        }
        view.acknowledge(duplicate).unwrap();
        let no_null = view
            .prepare(&[TableChange {
                table: "b".into(),
                rows: batch(&[None]),
                weights: vec![-2],
            }])
            .await
            .unwrap();
        assert_eq!(
            values(no_null.contents(), false),
            expected_query(0, &left, &[Some(1), Some(1)])
        );
        // Every nonmatching non-NULL key changes UNKNOWN -> FALSE/TRUE.
        assert_eq!(
            no_null.delta().unwrap().num_rows(),
            2 * (untouched as usize - 1)
        );
        for node in &no_null.stats().nodes {
            if let Some(work) = &node.keyed_work {
                assert_eq!(work.full_input_scans, 0);
            }
        }
        view.acknowledge(no_null).unwrap();
        let first_null = view
            .prepare(&[TableChange {
                table: "b".into(),
                rows: batch(&[None]),
                weights: vec![1],
            }])
            .await
            .unwrap();
        assert_eq!(
            first_null.delta().unwrap().num_rows(),
            2 * (untouched as usize - 1)
        );
        assert_eq!(
            values(first_null.contents(), false),
            expected_query(0, &left, &[Some(1), Some(1), None])
        );
    }
}

/// SQL writes use an independent endpoint model, including both sides in one epoch.
#[tokio::test]
async fn sql_truth_views_writes_retry_reopen_and_history_match_independent_oracle() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(k BIGINT);CREATE TABLE b(k BIGINT);INSERT INTO a VALUES (1),(2),(2),(NULL);INSERT INTO b VALUES (1)").await.unwrap();
    let mut data = [vec![Some(1), Some(2), Some(2), None], vec![Some(1)]];
    for (mode, query) in QUERIES.iter().enumerate() {
        db.execute(&format!("CREATE MATERIALIZED VIEW truth_{mode} AS {query}"))
            .await
            .unwrap();
    }
    for round in 0..8 {
        let sql = match round {
            0 => {
                data[1].extend([None, None]);
                "INSERT INTO b VALUES (NULL),(NULL)"
            }
            1 => {
                data[1].retain(Option::is_some);
                "DELETE FROM b WHERE k IS NULL"
            }
            2 => {
                data[1].clear();
                "DELETE FROM b"
            }
            3 => {
                data[1] = vec![Some(1), Some(2), Some(2), None];
                "INSERT INTO b VALUES (1),(2),(2),(NULL)"
            }
            4 => {
                for value in &mut data[1] {
                    if *value == Some(1) {
                        *value = Some(3);
                    }
                }
                "UPDATE b SET k=3 WHERE k=1"
            }
            5 => {
                for value in &mut data[0] {
                    if *value == Some(1) {
                        *value = Some(3);
                    }
                }
                data[0].push(None);
                data[1].retain(Option::is_some);
                "UPDATE a SET k=3 WHERE k=1;DELETE FROM b WHERE k IS NULL;INSERT INTO a VALUES (NULL)"
            }
            6 => {
                data[0].clear();
                data[1].clear();
                "DELETE FROM a;DELETE FROM b"
            }
            7 => {
                data = [vec![Some(1), Some(2), None], vec![None]];
                "INSERT INTO a VALUES (1),(2),(NULL);INSERT INTO b VALUES (NULL)"
            }
            _ => unreachable!("test round"),
        };
        let id = format!("truth-{round}");
        let epoch = db.epoch();
        let response = db.execute_transaction(&id, sql).await.unwrap();
        assert!(!response.replayed);
        assert_eq!(response.epoch, epoch + 1);
        let receipt = serde_json::to_value(response.commit.unwrap()).unwrap();
        for mode in 0..QUERIES.len() {
            let output = db
                .execute(&format!("SELECT * FROM truth_{mode}"))
                .await
                .unwrap()
                .rows
                .unwrap();
            assert_eq!(
                values(&output, false),
                expected_query(mode, &data[0], &data[1]),
                "mode {mode} round {round}"
            );
            let read = db.execute(QUERIES[mode]).await.unwrap().rows.unwrap();
            assert_eq!(output.schema(), read.schema());
            assert_eq!(values(&read, false), values(&output, false));
            let nodes = receipt["maintenance"][format!("truth_{mode}")]["nodes"]
                .as_array()
                .unwrap();
            assert!(nodes.iter().all(|n| n["strategy"] != "OperatorRecompute"));
        }
        db.verify_output_history().unwrap();
        drop(db);
        db = SqlSession::open(root.path()).await.unwrap();
        let retry = db.execute_transaction(&id, sql).await.unwrap();
        assert!(retry.replayed);
        assert_eq!(retry.epoch, epoch + 1);
        assert_eq!(
            serde_json::to_value(retry.commit.unwrap()).unwrap(),
            receipt
        );
        for mode in 0..QUERIES.len() {
            let output = db
                .execute(&format!("SELECT * FROM truth_{mode}"))
                .await
                .unwrap()
                .rows
                .unwrap();
            assert_eq!(
                values(&output, false),
                expected_query(mode, &data[0], &data[1])
            );
        }
        db.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn dml_selectors_and_insert_select_use_the_same_truth_relations() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(k BIGINT);CREATE TABLE b(k BIGINT);CREATE TABLE target(k BIGINT,m BOOLEAN,n BOOLEAN);INSERT INTO a VALUES (1),(2),(2),(NULL);INSERT INTO b VALUES (1),(NULL)").await.unwrap();
    db.execute(&format!("INSERT INTO target {PROJECTED}"))
        .await
        .unwrap();
    let output = db
        .execute("SELECT * FROM target")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(
        values(&output, false),
        expected_query(0, &[Some(1), Some(2), Some(2), None], &[Some(1), None])
    );
    db.execute("DELETE FROM a WHERE k=9 OR k NOT IN (SELECT k FROM b)")
        .await
        .unwrap();
    assert_eq!(
        db.execute("SELECT * FROM a")
            .await
            .unwrap()
            .rows
            .unwrap()
            .num_rows(),
        4
    );
    db.execute("DELETE FROM b WHERE k IS NULL").await.unwrap();
    db.execute("UPDATE a SET k=7 WHERE k=9 OR k NOT IN (SELECT k FROM b)")
        .await
        .unwrap();
    let output = db.execute("SELECT * FROM a").await.unwrap().rows.unwrap();
    let reference = batch(&[Some(1), Some(7), Some(7), None]);
    assert_eq!(values(&output, false), values(&reference, false));
    db.execute("DELETE FROM b").await.unwrap();
    db.execute("DELETE FROM a WHERE k=9 OR k NOT IN (SELECT k FROM b)")
        .await
        .unwrap();
    assert_eq!(
        db.execute("SELECT * FROM a")
            .await
            .unwrap()
            .rows
            .unwrap()
            .num_rows(),
        0
    );
}

#[tokio::test]
async fn alias_collisions_and_nonnullable_truth_keep_the_analyzed_schema() {
    use datafusion::prelude::SessionContext;
    for nullable in [false, true] {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "__optd_quantified_0",
            DataType::Int64,
            nullable,
        )
        .with_metadata(std::collections::HashMap::from([(
            "owner".into(),
            "truth-test".into(),
        )]))]));
        let input =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 2]))]).unwrap();
        let data = BTreeMap::from([("a".into(), input.clone()), ("b".into(), input.clone())]);
        let query="SELECT \"__optd_quantified_1\".*,\"__optd_quantified_1\".\"__optd_quantified_0\" IN (SELECT \"__optd_quantified_0\" FROM b AS \"__optd_quantified_2\") AS \"__optd_quantified_3\" FROM a AS \"__optd_quantified_1\"";
        let ctx = SessionContext::new();
        ctx.register_batch("a", input.clone()).unwrap();
        ctx.register_batch("b", input).unwrap();
        let original = ctx.state().create_logical_plan(query).await.unwrap();
        let mut view = SqlView::new_with_optimizer(
            query,
            data,
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let before = view.contents().unwrap();
        assert_eq!(before.schema().as_ref(), original.schema().as_arrow());
        assert_eq!(before.schema().field(1).is_nullable(), nullable);
        let delete = view
            .prepare(&[TableChange {
                table: "b".into(),
                rows: view.tables().unwrap()["b"].clone(),
                weights: vec![-1, -1, -1],
            }])
            .await
            .unwrap();
        let reference = Rows::from([
            (vec!["1".into(), "false".into()], 1),
            (vec!["2".into(), "false".into()], 2),
        ]);
        assert_eq!(values(delete.contents(), false), reference);
        assert_eq!(delete.contents().schema(), before.schema());
        view.acknowledge(delete).unwrap();
    }
}

#[tokio::test]
async fn nested_observed_memberships_preserve_nullable_boolean_comparisons() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(k BIGINT);CREATE TABLE b(k BIGINT);CREATE TABLE flags(v BOOLEAN);INSERT INTO a VALUES (1),(2),(NULL);INSERT INTO b VALUES (1),(NULL);INSERT INTO flags VALUES (TRUE)").await.unwrap();
    let query = "SELECT k,(k IN (SELECT k FROM b)) IN (SELECT v FROM flags) AS m FROM a";
    db.execute(&format!("CREATE MATERIALIZED VIEW nested AS {query}"))
        .await
        .unwrap();
    let actual = db
        .execute("SELECT * FROM nested")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(
        values(&actual, false),
        Rows::from([
            (vec!["1".into(), "true".into()], 1),
            (vec!["2".into(), "NULL".into()], 1),
            (vec!["NULL".into(), "NULL".into()], 1)
        ])
    );
    db.execute("DELETE FROM flags").await.unwrap();
    let actual = db
        .execute("SELECT * FROM nested")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert!(display_rows(&actual)
        .unwrap()
        .iter()
        .all(|row| row[1] == "false"));
    assert_eq!(actual.num_rows(), 3);
}

#[tokio::test]
async fn volatile_subqueries_and_expansion_overflow_refuse_without_publication() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(k BIGINT);CREATE TABLE b(k BIGINT);INSERT INTO a VALUES (1);INSERT INTO b VALUES (1)").await.unwrap();
    let before = db.epoch();
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    for query in [
        "SELECT k IN (SELECT k FROM b WHERE random()>0) AS m FROM a".to_string(),
        format!(
            "SELECT {} FROM a",
            (0..65)
                .map(|i| format!("k IN (SELECT k FROM b) AS m{i}"))
                .collect::<Vec<_>>()
                .join(",")
        ),
    ] {
        let error = db
            .execute(&format!("CREATE MATERIALIZED VIEW refused AS {query}"))
            .await
            .err()
            .unwrap();
        let message = error.to_string();
        assert!(
            message.contains("immutable") || message.contains("budget"),
            "{message}"
        );
        assert_eq!(db.epoch(), before);
        assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
        assert!(db.explain_view("refused").is_err());
    }
}

#[tokio::test]
async fn subquery_evaluation_failure_does_not_leak_candidate_sources_or_state() {
    let query = "SELECT k,k IN (SELECT 100/k FROM b) AS m FROM a";
    let mut view = SqlView::new_with_optimizer(
        query,
        tables(&[Some(1), Some(100)], &[Some(1)]),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap();
    let before = view.contents().unwrap();
    let sources = view.tables().unwrap();
    assert!(view
        .prepare(&[
            TableChange {
                table: "a".into(),
                rows: batch(&[Some(10)]),
                weights: vec![1]
            },
            TableChange {
                table: "b".into(),
                rows: batch(&[Some(0)]),
                weights: vec![1]
            }
        ])
        .await
        .is_err());
    assert_eq!(view.epoch(), 0);
    assert_eq!(view.contents().unwrap(), before);
    assert_eq!(view.tables().unwrap(), sources);
    let candidate = view
        .prepare(&[TableChange {
            table: "b".into(),
            rows: batch(&[Some(10)]),
            weights: vec![1],
        }])
        .await
        .unwrap();
    assert_eq!(
        values(candidate.contents(), false),
        Rows::from([
            (vec!["1".into(), "false".into()], 1),
            (vec!["100".into(), "true".into()], 1)
        ])
    );
    view.acknowledge(candidate).unwrap();
}
