//! Stable numeric oracles, exact replay and host-local numerical state reconstruction.

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{DurableSqlView, SqlView, TableChange};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

const SQL: &str =
    "SELECT k,count(v) AS n,avg(v) AS a,stddev_samp(v) AS s,stddev_pop(v) AS p FROM t GROUP BY k";
fn batch(values: &[(i64, Option<f64>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                values.iter().map(|v| v.0).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                values.iter().map(|v| v.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}
fn data(values: &[(i64, Option<f64>)]) -> BTreeMap<String, RecordBatch> {
    BTreeMap::from([("t".into(), batch(values))])
}
fn close(actual: &ScalarValue, expected: Option<f64>) {
    let ScalarValue::Float64(actual) = actual else {
        panic!("floating result required")
    };
    match (*actual, expected) {
        (None, None) => {}
        (Some(a), Some(b)) => assert!((a - b).abs() <= 1e-10 + 1e-9 * b.abs(), "{a} != {b}"),
        other => panic!("null mismatch {other:?}"),
    }
}
fn exact_rows(batch: &RecordBatch, signed: bool) -> HashMap<Vec<ScalarValue>, i64> {
    let n = batch.num_columns() - usize::from(signed);
    let mut result = HashMap::new();
    for i in 0..batch.num_rows() {
        let row = batch.columns()[..n]
            .iter()
            .map(|a| ScalarValue::try_from_array(a, i).unwrap())
            .collect();
        let weight = if signed {
            let ScalarValue::Int64(Some(w)) =
                ScalarValue::try_from_array(batch.column(n), i).unwrap()
            else {
                panic!("weight")
            };
            w
        } else {
            1
        };
        *result.entry(row).or_default() += weight;
    }
    result.retain(|_, w| *w != 0);
    result
}
fn check(batch: &RecordBatch, origin: f64, offsets: &[Option<i64>]) {
    if offsets.is_empty() {
        assert_eq!(batch.num_rows(), 0);
        return;
    }
    assert_eq!(batch.num_rows(), 1);
    let values = offsets.iter().flatten().copied().collect::<Vec<_>>();
    let n = values.len();
    let sum = values.iter().map(|v| *v as i128).sum::<i128>();
    let squares = values.iter().map(|v| (*v as i128).pow(2)).sum::<i128>();
    let corrected = if n == 0 {
        0.0
    } else {
        (squares * n as i128 - sum * sum) as f64 / n as f64
    };
    assert_eq!(
        ScalarValue::try_from_array(batch.column(1), 0).unwrap(),
        ScalarValue::Int64(Some(n as i64))
    );
    close(
        &ScalarValue::try_from_array(batch.column(2), 0).unwrap(),
        (n > 0).then(|| origin + sum as f64 / n as f64),
    );
    close(
        &ScalarValue::try_from_array(batch.column(3), 0).unwrap(),
        (n > 1).then(|| (corrected / (n - 1) as f64).sqrt()),
    );
    close(
        &ScalarValue::try_from_array(batch.column(4), 0).unwrap(),
        (n > 0).then(|| (corrected / n as f64).sqrt()),
    );
}
#[tokio::test]
async fn centered_moments_retract_duplicates_large_offsets_and_survive_reopen() {
    for origin in [0.0, 1e12, 1e16] {
        let root = tempfile::tempdir().unwrap();
        let mut offsets = vec![Some(0), Some(2), Some(4), Some(4), None];
        let mut view = DurableSqlView::open_with_optimizer(
            root.path(),
            SQL,
            data(
                &offsets
                    .iter()
                    .map(|v| (1, v.map(|v| origin + v as f64)))
                    .collect::<Vec<_>>(),
            ),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        assert_eq!(view.view().numerical_columns().unwrap(), vec![2, 3, 4]);
        check(&view.view().contents().unwrap(), origin, &offsets);
        for round in 0..18 {
            let removed = offsets.remove(0);
            let added = if round % 4 == 0 {
                None
            } else {
                Some(2 * (round % 5) as i64)
            };
            offsets.push(added);
            let values = [
                (1, removed.map(|v| origin + v as f64)),
                (1, added.map(|v| origin + v as f64)),
            ];
            let changes = [TableChange {
                table: "t".into(),
                rows: batch(&values),
                weights: vec![-1, 1],
            }];
            let before = exact_rows(&view.view().contents().unwrap(), false);
            let candidate = view.view().prepare(&changes).await.unwrap();
            check(candidate.contents(), origin, &offsets);
            let expected = exact_rows(candidate.contents(), false);
            let mut replay = before.clone();
            for (row, w) in exact_rows(&candidate.delta().unwrap(), true) {
                *replay.entry(row).or_default() += w;
            }
            replay.retain(|_, w| *w != 0);
            assert_eq!(replay, expected);
            drop(candidate);
            assert_eq!(exact_rows(&view.view().contents().unwrap(), false), before);
            view.refresh(&changes).await.unwrap();
            drop(view);
            view = DurableSqlView::open_with_optimizer(
                root.path(),
                SQL,
                BTreeMap::new(),
                IncrementalOptimizer::default().require_incremental(),
            )
            .await
            .unwrap();
            assert_eq!(
                exact_rows(&view.view().contents().unwrap(), false),
                expected
            );
        }
        while let Some(value) = offsets.pop() {
            view.refresh(&[TableChange {
                table: "t".into(),
                rows: batch(&[(1, value.map(|v| origin + v as f64))]),
                weights: vec![-1],
            }])
            .await
            .unwrap();
            check(&view.view().contents().unwrap(), origin, &offsets);
        }
    }
}
#[tokio::test]
async fn numeric_rebases_visit_only_the_changed_groups_counted_values() {
    for size in [32, 4096] {
        let source = (0..size)
            .flat_map(|k| [(k, Some(2.0)), (k, Some(4.0))])
            .collect::<Vec<_>>();
        let view = SqlView::new_with_optimizer(
            SQL,
            data(&source),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let candidate = view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: batch(&[(0, Some(6.0))]),
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
        let numeric = work.numeric_work.as_ref().unwrap();
        assert_eq!(
            (numeric.observations, numeric.rebases, numeric.rebase_values),
            (3, 3, 9)
        );
    }
}
#[tokio::test]
async fn nonfinite_candidates_are_rejected_and_numeric_keys_remain_exact() {
    let view = SqlView::new_with_optimizer(
        SQL,
        data(&[(1, Some(2.0)), (1, Some(4.0))]),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap();
    let before = view.contents().unwrap();
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: batch(&[(1, Some(value))]),
                weights: vec![1]
            }])
            .await
            .is_err());
        assert_eq!(view.contents().unwrap(), before);
    }
    let sql="SELECT CAST(k AS DOUBLE) AS key,stddev(v) AS s,count(*) AS n,stddev(v)/avg(v) AS cv FROM t GROUP BY CAST(k AS DOUBLE)";
    let view = SqlView::new_with_optimizer(
        sql,
        data(&[(1, Some(2.0)), (1, Some(4.0))]),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap();
    assert_eq!(view.numerical_columns().unwrap(), vec![1, 3]);

    // The integer key's exact AVG shares a node with moments of another argument.
    let view = SqlView::new_with_optimizer(
        "SELECT avg(k) AS exact_average,stddev(v) AS s,avg(v) AS a FROM t",
        data(&[(1, Some(2.0)), (2, Some(4.0))]),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap();
    assert_eq!(view.numerical_columns().unwrap(), vec![1, 2]);
}
