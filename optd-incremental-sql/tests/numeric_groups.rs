//! Exact SQL oracles for grouping identity and the qualified integral AVG domain.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{DurableSqlView, SqlView, Strategy, TableChange};

type Rows = HashMap<Vec<ScalarValue>, i64>;

fn rows(batch: &RecordBatch, signed: bool) -> Rows {
    let n = batch.num_columns() - usize::from(signed);
    let mut result = Rows::new();
    for i in 0..batch.num_rows() {
        let row = batch.columns()[..n]
            .iter()
            .map(|a| ScalarValue::try_from_array(a, i).unwrap())
            .collect();
        let weight = if signed {
            let ScalarValue::Int64(Some(weight)) =
                ScalarValue::try_from_array(batch.column(n), i).unwrap()
            else {
                panic!("invalid delta")
            };
            weight
        } else {
            1
        };
        *result.entry(row).or_default() += weight;
    }
    result.retain(|_, weight| *weight != 0);
    result
}

fn data(batch: RecordBatch) -> BTreeMap<String, RecordBatch> {
    BTreeMap::from([("t".into(), batch)])
}

async fn oracle(sql: &str, batch: RecordBatch) -> Rows {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_batch("t", batch).unwrap();
    let frame = ctx.sql(sql).await.unwrap();
    let batch = datafusion::arrow::compute::concat_batches(
        &Arc::new(frame.schema().as_arrow().clone()),
        &frame.collect().await.unwrap(),
    )
    .unwrap();
    rows(&batch, false)
}

fn floats(values: &[Option<f64>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Float64, true),
            Field::new("k", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Float64Array::from(values.to_vec())),
            Arc::new(Int64Array::from(vec![1; values.len()])),
        ],
    )
    .unwrap()
}

fn ints(values: &[(i64, Option<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                values.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                values.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn floating_distinct_preserves_sql_representatives_and_exact_delete_identity() {
    let nan1 = f64::from_bits(0x7ff8_0000_0000_0001);
    let nan2 = f64::from_bits(0x7ff8_0000_0000_0002);
    // Exercise both primitive and compound grouping implementations.
    for sql in [
        "SELECT DISTINCT v FROM t",
        "SELECT DISTINCT v,k FROM t",
        "SELECT DISTINCT CAST(v AS REAL) AS v,k FROM t",
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut values = vec![Some(-0.0), Some(0.0), Some(nan1), Some(nan2), None, None];
        let mut view = DurableSqlView::open_with_optimizer(
            root.path(),
            sql,
            data(floats(&values)),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        assert_eq!(
            rows(&view.view().contents().unwrap(), false),
            oracle(sql, floats(&values)).await
        );
        assert!(view
            .view()
            .strategies()
            .iter()
            .all(|n| n.strategy != Strategy::OperatorRecompute));
        for value in [Some(-0.0), Some(nan1), None, Some(0.0), Some(nan2), None] {
            let index = values
                .iter()
                .position(|v| v.map(f64::to_bits) == value.map(f64::to_bits))
                .unwrap();
            values.remove(index);
            let changes = [TableChange {
                table: "t".into(),
                rows: floats(&[value]),
                weights: vec![-1],
            }];
            let before = rows(&view.view().contents().unwrap(), false);
            let candidate = view.view().prepare(&changes).await.unwrap();
            let expected = oracle(sql, floats(&values)).await;
            assert_eq!(rows(candidate.contents(), false), expected);
            let mut replay = before.clone();
            for (row, weight) in rows(&candidate.delta().unwrap(), true) {
                *replay.entry(row).or_default() += weight;
            }
            replay.retain(|_, weight| *weight != 0);
            assert_eq!(replay, expected);
            drop(candidate);
            assert_eq!(rows(&view.view().contents().unwrap(), false), before);
            let stats = view.refresh(&changes).await.unwrap();
            let work = stats
                .nodes
                .iter()
                .find_map(|n| n.accumulator_work.as_ref())
                .unwrap();
            assert_eq!((work.touched_groups, work.historical_input_rows), (1, 0));
            drop(view);
            view = DurableSqlView::open_with_optimizer(
                root.path(),
                sql,
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
async fn integer_average_is_exact_across_cancellation_global_empty_and_reopen() {
    for sql in [
        "SELECT k,avg(v) AS a,count(v) AS n FROM t GROUP BY k",
        "SELECT avg(v) AS a,count(v) AS n FROM t",
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut source = vec![(1, Some(1_000_000)), (1, Some(-999_999)), (1, None)];
        let mut view = DurableSqlView::open_with_optimizer(
            root.path(),
            sql,
            data(ints(&source)),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        for (value, weight) in [
            (Some(3), 1),
            (Some(1_000_000), -1),
            (Some(-999_999), -1),
            (Some(3), -1),
            (None, -1),
            (Some(11), 1),
        ] {
            if weight > 0 {
                source.push((1, value));
            } else {
                source.remove(source.iter().position(|r| *r == (1, value)).unwrap());
            }
            let changes = [TableChange {
                table: "t".into(),
                rows: ints(&[(1, value)]),
                weights: vec![weight],
            }];
            let candidate = view.view().prepare(&changes).await.unwrap();
            assert_eq!(
                rows(candidate.contents(), false),
                oracle(sql, ints(&source)).await
            );
            drop(candidate);
            view.refresh(&changes).await.unwrap();
            drop(view);
            view = DurableSqlView::open_with_optimizer(
                root.path(),
                sql,
                BTreeMap::new(),
                IncrementalOptimizer::default().require_incremental(),
            )
            .await
            .unwrap();
            assert_eq!(
                rows(&view.view().contents().unwrap(), false),
                oracle(sql, ints(&source)).await
            );
        }
    }
}

#[tokio::test]
async fn numeric_state_touches_only_one_of_growing_untouched_groups() {
    for size in [32, 4096] {
        let source = (0..size).map(|k| (k, Some(3))).collect::<Vec<_>>();
        let mut view = SqlView::new_with_optimizer(
            "SELECT k,avg(v) FROM t GROUP BY k",
            data(ints(&source)),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let candidate = view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: ints(&[(0, Some(5))]),
                weights: vec![1],
            }])
            .await
            .unwrap();
        let work = candidate
            .stats()
            .nodes
            .iter()
            .find_map(|n| n.accumulator_work.as_ref())
            .unwrap();
        assert_eq!(
            (
                work.touched_groups,
                work.delta_input_rows,
                work.historical_input_rows
            ),
            (1, 1, 0)
        );
        view.acknowledge(candidate).unwrap();
    }
}

#[tokio::test]
async fn exact_average_domain_rejects_overflow_and_general_floats_atomically() {
    let sql = "SELECT avg(v) FROM t";
    let mut view = SqlView::new_with_optimizer(
        sql,
        data(ints(&[(1, Some(1i64 << 53))])),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap();
    let before = view.contents().unwrap();
    let invalid = TableChange {
        table: "t".into(),
        rows: ints(&[(1, Some(1))]),
        weights: vec![1],
    };
    assert!(view
        .prepare(&[invalid])
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("exact integer average"));
    assert_eq!(view.contents().unwrap(), before);
    let candidate = view
        .prepare(&[TableChange {
            table: "t".into(),
            rows: ints(&[(1, Some(1i64 << 53)), (1, Some(7))]),
            weights: vec![-1, 1],
        }])
        .await
        .unwrap();
    view.acknowledge(candidate).unwrap();
    assert_eq!(
        rows(&view.contents().unwrap(), false),
        oracle(sql, ints(&[(1, Some(7))])).await
    );
    assert!(SqlView::new_with_optimizer(
        sql,
        data(floats(&[Some(0.1)])),
        IncrementalOptimizer::default().require_incremental()
    )
    .await
    .is_err());
}

#[tokio::test]
async fn variance_full_sql_is_order_dependent_and_stable_state_uses_a_separate_contract() {
    // Identical exact source bags can yield different native Welford results.
    // A stable retractable formula alone cannot satisfy a bitwise SQL oracle.
    let a = [Some(1e16), Some(1e16 + 2.0), Some(1e16 + 4.0)];
    let b = [a[0], a[2], a[1]];
    assert_eq!(rows(&floats(&a), false), rows(&floats(&b), false));
    let sql = "SELECT stddev(v) AS s FROM t";
    let x = oracle(sql, floats(&a)).await;
    let y = oracle(sql, floats(&b)).await;
    eprintln!("native variance permutation witness: {x:?} versus {y:?}");
    assert_ne!(
        x, y,
        "revisit the pinned numerical contract if native variance changes"
    );
    for source in [a, b] {
        let view = SqlView::new_with_optimizer(
            sql,
            data(floats(&source)),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        assert_eq!(
            rows(&view.contents().unwrap(), false),
            Rows::from([(vec![ScalarValue::Float64(Some(2.0))], 1)])
        );
    }
}
