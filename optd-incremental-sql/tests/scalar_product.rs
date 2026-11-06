//! Scalar broadcasts preserve SQL cardinality and expose their real snapshot scope.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::prelude::SessionContext;
use optd_incremental_sql::database::{display_rows, SqlSession};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{SqlView, Strategy, TableChange};

const SQL: &str = "SELECT k,(SELECT count(*) FROM b) AS n FROM a";

fn batch(rows: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(rows.to_vec()))],
    )
    .unwrap()
}

fn tables(left: &[Option<i64>], right: &[Option<i64>]) -> BTreeMap<String, RecordBatch> {
    BTreeMap::from([("a".into(), batch(left)), ("b".into(), batch(right))])
}

type Rows = HashMap<Vec<ScalarValue>, i64>;

fn rows(batch: &RecordBatch, signed: bool) -> Rows {
    let columns = batch.num_columns() - usize::from(signed);
    let mut result = Rows::new();
    for index in 0..batch.num_rows() {
        let key = batch.columns()[..columns]
            .iter()
            .map(|array| ScalarValue::try_from_array(array, index).unwrap())
            .collect();
        let weight = if signed {
            let ScalarValue::Int64(Some(weight)) =
                ScalarValue::try_from_array(batch.column(columns), index).unwrap()
            else {
                panic!("delta requires an Int64 weight")
            };
            weight
        } else {
            1
        };
        *result.entry(key).or_default() += weight;
    }
    result.retain(|_, weight| *weight != 0);
    result
}

#[tokio::test]
async fn scalar_aggregate_subquery_has_a_strict_incremental_plan() {
    let view = SqlView::new_with_optimizer(
        SQL,
        tables(&[Some(1), Some(1), None], &[]),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap();
    assert!(view
        .strategies()
        .iter()
        .all(|node| node.strategy != Strategy::OperatorRecompute));
    assert!(view
        .incremental_plan()
        .explain()
        .contains("builtin.scalar_product"));
    assert_eq!(display_rows(&view.contents().unwrap()).unwrap().len(), 3);
}

async fn full(sql: &str, data: BTreeMap<String, RecordBatch>) -> RecordBatch {
    let ctx = SessionContext::new();
    for (name, batch) in data {
        ctx.register_batch(name, batch).unwrap();
    }
    let frame = ctx.sql(sql).await.unwrap();
    datafusion::arrow::compute::concat_batches(
        &Arc::new(frame.schema().as_arrow().clone()),
        &frame.collect().await.unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn two_sided_signed_changes_match_full_sql_integer_oracle_and_delta_replay() {
    let mut left = vec![Some(1), Some(1), None];
    let mut right = Vec::new();
    let mut view = SqlView::new_with_optimizer(
        SQL,
        tables(&left, &right),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap();
    // Repeated values exercise multiplicity; the last deletion restores an empty RHS.
    let directed = [
        (None, 1),
        (Some(2), 1),
        (Some(2), 1),
        (None, -1),
        (Some(2), -1),
        (Some(2), -1),
    ];
    for (round, (value, weight)) in directed.into_iter().cycle().take(36).enumerate() {
        let before = rows(&view.contents().unwrap(), false);
        let mut changes = vec![TableChange {
            table: "b".into(),
            rows: batch(&[value]),
            weights: vec![weight],
        }];
        if weight > 0 {
            right.push(value);
        } else {
            right.remove(right.iter().position(|row| *row == value).unwrap());
        }
        // Replacement images make both deltas nonzero in the same atomic batch.
        let moved = Some(100 + round as i64);
        changes.push(TableChange {
            table: "a".into(),
            rows: batch(&[left[0], moved]),
            weights: vec![-1, 1],
        });
        left[0] = moved;
        let expected = rows(&full(SQL, tables(&left, &right)).await, false);
        let mut integer_oracle = Rows::new();
        for key in &left {
            *integer_oracle
                .entry(vec![
                    ScalarValue::Int64(*key),
                    ScalarValue::Int64(Some(right.len() as i64)),
                ])
                .or_default() += 1;
        }
        assert_eq!(expected, integer_oracle);
        let candidate = view.prepare(&changes).await.unwrap();
        assert_eq!(rows(candidate.contents(), false), expected);
        let mut replay = before.clone();
        for (row, weight) in rows(&candidate.delta().unwrap(), true) {
            *replay.entry(row).or_default() += weight;
        }
        replay.retain(|_, weight| *weight != 0);
        assert_eq!(replay, expected);
        drop(candidate);
        assert_eq!(rows(&view.contents().unwrap(), false), before);
        let candidate = view.prepare(&changes).await.unwrap();
        view.acknowledge(candidate).unwrap();
        let invalid = TableChange {
            table: "b".into(),
            rows: batch(&[Some(999)]),
            weights: vec![-1],
        };
        assert!(view.prepare(&[invalid]).await.is_err());
        assert_eq!(rows(&view.contents().unwrap(), false), expected);
    }
}

#[tokio::test]
async fn delta_guards_avoid_untouched_left_scans_and_measure_necessary_broadcast() {
    for size in [32, 4096] {
        let left = (0..size).map(Some).collect::<Vec<_>>();
        let data = tables(&left, &[Some(7)]);
        let report = SqlView::preflight(
            SQL,
            data.iter()
                .map(|(name, batch)| (name.clone(), batch.schema()))
                .collect(),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let node = report
            .planned_nodes
            .iter()
            .find(|node| {
                node.rule
                    .as_ref()
                    .is_some_and(|r| r.name == "builtin.scalar_product")
            })
            .unwrap();
        assert_eq!(node.snapshot_dependencies.len(), 2);
        assert!(node
            .snapshot_dependencies
            .iter()
            .all(|read| read.trigger.is_some()));
        let mut view = SqlView::new_with_optimizer(
            SQL,
            data,
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let candidate = view
            .prepare(&[TableChange {
                table: "a".into(),
                rows: batch(&[Some(-1)]),
                weights: vec![1],
            }])
            .await
            .unwrap();
        let work = &candidate.stats().work.cache_work.guarded_snapshots;
        assert_eq!((work.reads, work.skipped_reads, work.rows), (1, 1, 1));
        assert_eq!(candidate.delta().unwrap().num_rows(), 1);
        view.acknowledge(candidate).unwrap();
        let candidate = view
            .prepare(&[TableChange {
                table: "b".into(),
                rows: batch(&[None]),
                weights: vec![1],
            }])
            .await
            .unwrap();
        let work = &candidate.stats().work.cache_work.guarded_snapshots;
        assert_eq!(
            (work.reads, work.skipped_reads, work.rows),
            (1, 1, (size + 1) as usize)
        );
        assert_eq!(
            candidate.delta().unwrap().num_rows(),
            2 * (size + 1) as usize
        );
    }
}

#[tokio::test]
async fn nullable_scalar_sum_and_count_survive_sql_updates_retry_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    let query = "SELECT k,(SELECT sum(k) FROM b) AS s,(SELECT count(*) FROM b) AS n FROM a";
    db.execute_script(&format!(
        "CREATE TABLE a(k BIGINT); CREATE TABLE b(k BIGINT); CREATE MATERIALIZED VIEW m AS {query}"
    ))
    .await
    .unwrap();
    for (round, body) in [
        "INSERT INTO a VALUES (1),(1),(NULL)",
        "INSERT INTO b VALUES (2),(2),(NULL)",
        "UPDATE a SET k=5 WHERE k=1; UPDATE b SET k=k+1 WHERE k IS NOT NULL",
        "DELETE FROM b",
        "DELETE FROM a",
        "INSERT INTO b VALUES (9); INSERT INTO a VALUES (8),(NULL)",
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("round-{round}");
        let commit = db.execute_transaction(&key, body).await.unwrap();
        let epoch = db.epoch();
        let expected = db.execute(query).await.unwrap().rows.unwrap();
        assert_eq!(
            rows(
                &db.execute("SELECT * FROM m").await.unwrap().rows.unwrap(),
                false
            ),
            rows(&expected, false)
        );
        if let Some(report) = commit.commit {
            assert!(report.maintenance["m"]["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|node| node["strategy"] != "OperatorRecompute"));
        }
        db.verify_output_history().unwrap();
        drop(db);
        db = SqlSession::open(root.path()).await.unwrap();
        assert_eq!(db.epoch(), epoch);
        assert!(db.execute_transaction(&key, body).await.unwrap().replayed);
        assert_eq!(db.epoch(), epoch);
        assert_eq!(
            rows(
                &db.execute("SELECT * FROM m").await.unwrap().rows.unwrap(),
                false
            ),
            rows(&expected, false)
        );
        db.verify_output_history().unwrap();
    }
}
