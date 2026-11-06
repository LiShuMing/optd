//! Differential rank changes expose necessary suffix work and preserved prefixes.

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{DurableSqlView, SqlView, TableChange};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

type Rows = HashMap<Vec<ScalarValue>, i64>;
fn batch(values: &[(i64, Option<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                values.iter().map(|v| v.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                values.iter().map(|v| v.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}
fn data(values: &[(i64, Option<i64>)]) -> BTreeMap<String, RecordBatch> {
    BTreeMap::from([("t".into(), batch(values))])
}
fn rows(batch: &RecordBatch, signed: bool) -> Rows {
    let n = batch.num_columns() - usize::from(signed);
    let mut result = Rows::new();
    for i in 0..batch.num_rows() {
        let row = batch.columns()[..n]
            .iter()
            .map(|a| ScalarValue::try_from_array(a, i).unwrap())
            .collect();
        let w = if signed {
            let ScalarValue::Int64(Some(w)) =
                ScalarValue::try_from_array(batch.column(n), i).unwrap()
            else {
                panic!("weight")
            };
            w
        } else {
            1
        };
        *result.entry(row).or_default() += w;
    }
    result.retain(|_, w| *w != 0);
    result
}
async fn full(sql: &str, source: &[(i64, Option<i64>)]) -> Rows {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_batch("t", batch(source)).unwrap();
    let frame = ctx.sql(sql).await.unwrap();
    rows(
        &datafusion::arrow::compute::concat_batches(
            &Arc::new(frame.schema().as_arrow().clone()),
            &frame.collect().await.unwrap(),
        )
        .unwrap(),
        false,
    )
}
#[tokio::test]
async fn rank_matches_full_sql_and_signed_replay_through_updates_and_recovery() {
    for order in [
        "v ASC NULLS FIRST",
        "v DESC NULLS LAST",
        "v ASC NULLS LAST,id DESC",
    ] {
        let sql = format!("SELECT id,v,rank() OVER (ORDER BY {order}) AS r FROM t");
        let root = tempfile::tempdir().unwrap();
        let mut source = vec![
            (1, None),
            (2, Some(2)),
            (3, Some(2)),
            (3, Some(2)),
            (4, Some(4)),
        ];
        let mut view = DurableSqlView::open_with_optimizer(
            root.path(),
            &sql,
            data(&source),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        for round in 0..12 {
            let old = source.remove(round % source.len());
            let new = (
                100 + round as i64,
                if round % 3 == 0 {
                    None
                } else {
                    Some((round % 5) as i64)
                },
            );
            source.push(new);
            let before = rows(&view.view().contents().unwrap(), false);
            let changes = [TableChange {
                table: "t".into(),
                rows: batch(&[old, new]),
                weights: vec![-1, 1],
            }];
            let candidate = view.view().prepare(&changes).await.unwrap();
            let expected = full(&sql, &source).await;
            assert_eq!(rows(candidate.contents(), false), expected);
            let mut replay = before.clone();
            for (row, w) in rows(&candidate.delta().unwrap(), true) {
                *replay.entry(row).or_default() += w;
            }
            replay.retain(|_, w| *w != 0);
            assert_eq!(replay, expected);
            drop(candidate);
            assert_eq!(rows(&view.view().contents().unwrap(), false), before);
            view.refresh(&changes).await.unwrap();
            drop(view);
            view = DurableSqlView::open_with_optimizer(
                root.path(),
                &sql,
                BTreeMap::new(),
                IncrementalOptimizer::default().require_incremental(),
            )
            .await
            .unwrap();
            assert_eq!(rows(&view.view().contents().unwrap(), false), expected);
        }
    }
}
#[tokio::test]
async fn rank_at_end_avoids_prefix_reads_and_at_start_reports_broadcast() {
    for size in [32, 4096] {
        let source = (0..size).map(|v| (v, Some(v))).collect::<Vec<_>>();
        let mut view = SqlView::new_with_optimizer(
            "SELECT id,v,rank() OVER (ORDER BY v) AS r FROM t",
            data(&source),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let changes = [TableChange {
            table: "t".into(),
            rows: batch(&[(size, Some(size))]),
            weights: vec![1],
        }];
        let candidate = view.prepare(&changes).await.unwrap();
        assert_eq!(candidate.delta().unwrap().num_rows(), 1);
        let work = candidate
            .stats()
            .nodes
            .iter()
            .find_map(|n| n.rank_work.as_ref())
            .unwrap();
        assert_eq!(
            (
                work.changed_peers,
                work.visited_peers,
                work.historical_input_rows
            ),
            (1, 1, 0)
        );
        view.acknowledge(candidate).unwrap();
        // Two content-only changes leave all intervening peer ranks unchanged.
        // Growing that gap must not turn its metadata seek into a historical scan.
        let candidate = view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: batch(&[
                    (0, Some(0)),
                    (-10, Some(0)),
                    (size, Some(size)),
                    (-11, Some(size)),
                ]),
                weights: vec![-1, 1, -1, 1],
            }])
            .await
            .unwrap();
        let work = candidate
            .stats()
            .nodes
            .iter()
            .find_map(|n| n.rank_work.as_ref())
            .unwrap();
        assert_eq!(
            (
                work.changed_peers,
                work.visited_peers,
                work.historical_input_rows,
                work.output_delta_rows,
            ),
            (2, 2, 2, 4)
        );
        drop(candidate);
        let candidate = view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: batch(&[(-1, Some(-1))]),
                weights: vec![1],
            }])
            .await
            .unwrap();
        assert_eq!(
            candidate.delta().unwrap().num_rows(),
            2 * (size + 1) as usize + 1
        );
        let work = candidate
            .stats()
            .nodes
            .iter()
            .find_map(|n| n.rank_work.as_ref())
            .unwrap();
        assert_eq!(work.historical_input_rows, (size + 1) as usize);
    }
}

#[tokio::test]
async fn floating_rank_peers_match_sql_total_order_on_signed_zero_and_nan_payloads() {
    use datafusion::arrow::array::Float64Array;
    let make = |values: &[Option<f64>]| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)])),
            vec![Arc::new(Float64Array::from(values.to_vec()))],
        )
        .unwrap()
    };
    for direction in ["ASC NULLS LAST", "DESC NULLS FIRST"] {
        let sql = format!("SELECT v,rank() OVER (ORDER BY v {direction}) AS r FROM t");
        let mut source = vec![
            Some(-0.0),
            Some(0.0),
            None,
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0x7ff8_0000_0000_0002)),
            Some(f64::NEG_INFINITY),
        ];
        let mut view = SqlView::new_with_optimizer(
            &sql,
            BTreeMap::from([("t".into(), make(&source))]),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        for _ in 0..6 {
            let removed = source.remove(0);
            let candidate = view
                .prepare(&[TableChange {
                    table: "t".into(),
                    rows: make(&[removed]),
                    weights: vec![-1],
                }])
                .await
                .unwrap();
            let ctx =
                SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
            ctx.register_batch("t", make(&source)).unwrap();
            let frame = ctx.sql(&sql).await.unwrap();
            let result = datafusion::arrow::compute::concat_batches(
                &Arc::new(frame.schema().as_arrow().clone()),
                &frame.collect().await.unwrap(),
            )
            .unwrap();
            assert_eq!(rows(candidate.contents(), false), rows(&result, false));
            view.acknowledge(candidate).unwrap();
        }
    }
}
