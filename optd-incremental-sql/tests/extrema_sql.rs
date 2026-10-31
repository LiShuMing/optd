//! Ordered extrema must execute through the existing aggregate delta plan.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{SqlView, Strategy, TableChange};

type InputRow = (Option<i64>, Option<i64>);
type Multiset = HashMap<Vec<ScalarValue>, i64>;

fn batch(rows: &[(Option<i64>, Option<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn ordinary_global_grouped_and_mixed_extrema_select_the_accumulator_plan() {
    for sql in [
        "SELECT min(v), max(v) FROM t",
        "SELECT k, count(*), sum(v), min(v), max(v) FROM t GROUP BY k",
    ] {
        let view = SqlView::new(
            sql,
            BTreeMap::from([(
                "t".into(),
                batch(&[(Some(1), Some(5)), (Some(1), None), (None, Some(3))]),
            )]),
        )
        .await
        .unwrap();
        let aggregates = view
            .strategies()
            .into_iter()
            .filter(|n| n.operator.to_lowercase().contains("aggregate"))
            .collect::<Vec<_>>();
        assert_eq!(aggregates.len(), 1);
        assert_eq!(
            aggregates[0].strategy,
            Strategy::AccumulatorDelta,
            "{sql}: {aggregates:?}"
        );
    }
}

fn multiset(batch: &RecordBatch) -> Multiset {
    let mut rows = HashMap::new();
    for index in 0..batch.num_rows() {
        let row = batch
            .columns()
            .iter()
            .map(|array| ScalarValue::try_from_array(array, index).unwrap())
            .collect();
        *rows.entry(row).or_default() += 1;
    }
    rows
}

fn replay(mut rows: Multiset, delta: &RecordBatch) -> Multiset {
    let count = delta.num_columns() - 1;
    assert_eq!(delta.schema().field(count).name(), "__optd_weight");
    for index in 0..delta.num_rows() {
        let row = delta.columns()[..count]
            .iter()
            .map(|array| ScalarValue::try_from_array(array, index).unwrap())
            .collect::<Vec<_>>();
        let ScalarValue::Int64(Some(weight)) =
            ScalarValue::try_from_array(delta.column(count), index).unwrap()
        else {
            panic!("signed delta weight");
        };
        let next = rows.get(&row).copied().unwrap_or_default() + weight;
        assert!(next >= 0);
        if next == 0 {
            rows.remove(&row);
        } else {
            rows.insert(row, next);
        }
    }
    rows
}

async fn oracle(sql: &str, input: RecordBatch) -> RecordBatch {
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    context.register_batch("t", input).unwrap();
    let frame = context.sql(sql).await.unwrap();
    let schema = Arc::new(frame.schema().as_arrow().clone());
    concat_batches(&schema, &frame.collect().await.unwrap()).unwrap()
}

async fn strict(sql: &str, input: RecordBatch) -> SqlView {
    SqlView::new_with_optimizer(
        sql,
        BTreeMap::from([("t".into(), input)]),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap()
}

fn change(edits: &[(InputRow, i64)]) -> TableChange {
    TableChange {
        table: "t".into(),
        rows: batch(&edits.iter().map(|(row, _)| *row).collect::<Vec<_>>()),
        weights: edits.iter().map(|(_, weight)| *weight).collect(),
    }
}

fn edit_model(model: &mut Vec<InputRow>, edits: &[(InputRow, i64)]) {
    for (row, weight) in edits {
        if *weight == 1 {
            model.push(*row);
        } else {
            assert_eq!(*weight, -1);
            let index = model.iter().position(|value| value == row).unwrap();
            model.remove(index);
        }
    }
}

#[tokio::test]
async fn arbitrary_extremum_deletions_nulls_empty_and_mixed_states_match_full_sql_and_deltas() {
    for sql in [
        "SELECT count(*), count(v), min(v), max(v) FROM t",
        "SELECT k, count(*), min(v), max(v) FROM t GROUP BY k",
        "SELECT k, count(*), sum(v), avg(CAST(v AS DECIMAL(18,2))), min(v), max(v) FROM t GROUP BY k",
    ] {
        let mut model = vec![(Some(1),Some(5)), (Some(1),Some(5)), (Some(1),Some(9)), (Some(1),Some(3)), (Some(1),None), (Some(2),None), (None,Some(7)), (None,Some(7)), (None,Some(100))];
        let mut view = strict(sql, batch(&model)).await;
        let mut rounds = vec![vec![((Some(1),Some(9)),-1)], vec![((Some(1),Some(5)),-1)], vec![((Some(1),Some(5)),-1)], vec![((Some(1),Some(-10)),1)], vec![((None,Some(7)),-1),((Some(3),Some(7)),1)]];
        let mut endpoint = model.clone();
        for edits in &rounds { edit_model(&mut endpoint, edits); }
        rounds.push(endpoint.iter().map(|row| (*row,-1)).collect());
        rounds.push(vec![((Some(1),None),1),((Some(1),None),1),((None,Some(8)),1)]);
        for edits in rounds {
            let before = view.contents().unwrap();
            let old_tables = view.tables().unwrap();
            edit_model(&mut model, &edits);
            let expected = oracle(sql, batch(&model)).await;
            let candidate = view.prepare(&[change(&edits)]).await.unwrap();
            assert_eq!(candidate.contents().schema(), expected.schema(), "{sql}");
            assert_eq!(multiset(candidate.contents()), multiset(&expected), "{sql} {edits:?}");
            assert_eq!(replay(multiset(&before), &candidate.delta().unwrap()), multiset(&expected));
            for node in &candidate.stats().nodes {
                if let Some(work) = &node.accumulator_work {
                    assert_eq!(work.historical_input_rows, 0);
                    assert!(work.value_index_work.is_some());
                }
            }
            assert_eq!(view.contents().unwrap(), before);
            assert_eq!(view.tables().unwrap(), old_tables);
            drop(candidate);
            view.acknowledge(view.prepare(&[change(&edits)]).await.unwrap()).unwrap();
        }
    }
}

#[tokio::test]
async fn unchanged_groups_and_a_growing_hot_value_index_have_bounded_copy_work() {
    for n in [1024usize, 8192] {
        for hot_group in [false, true] {
            let rows = if hot_group {
                (0..n)
                    .map(|v| (Some(0), Some(v as i64)))
                    .collect::<Vec<_>>()
            } else {
                (0..n)
                    .flat_map(|k| {
                        [
                            (Some(k as i64), Some(k as i64)),
                            (Some(k as i64), Some(-(k as i64))),
                        ]
                    })
                    .collect()
            };
            let sql = "SELECT k, min(v), max(v) FROM t GROUP BY k";
            let view = strict(sql, batch(&rows)).await;
            let edits = if hot_group {
                vec![
                    ((Some(0), Some(n as i64 - 1)), -1),
                    ((Some(0), Some(n as i64 + 5)), 1),
                ]
            } else {
                vec![((Some(0), Some(0)), -1), ((Some(0), Some(-1)), 1)]
            };
            let candidate = view.prepare(&[change(&edits)]).await.unwrap();
            let work = candidate
                .stats()
                .nodes
                .iter()
                .find_map(|node| node.accumulator_work.as_ref())
                .unwrap();
            let values = work.value_index_work.as_ref().unwrap();
            assert_eq!(work.historical_input_rows, 0);
            assert_eq!(work.touched_groups, 1);
            assert_eq!(values.input_rows, 4);
            assert_eq!(values.value_updates, 4);
            assert_eq!(values.extremum_lookups, 2);
            assert!(
                values.directory_entries_copied
                    <= if hot_group {
                        12 * (n.ilog2() as usize + 1)
                    } else {
                        8
                    },
                "n={n} hot={hot_group} {values:?}"
            );
            assert!(
                candidate.stats().work.state_copies.accumulator_entries
                    <= 4 * (n.ilog2() as usize + 1)
            );
            let mut expected = rows;
            edit_model(&mut expected, &edits);
            assert_eq!(
                multiset(candidate.contents()),
                multiset(&oracle(sql, batch(&expected)).await)
            );
        }
    }
}

#[tokio::test]
async fn projected_value_cancellation_emits_nothing_and_does_not_copy_value_paths() {
    let sql = "SELECT k % 2 AS g, min(v), max(v), count(*) FROM t GROUP BY k % 2";
    let mut view = strict(sql, batch(&[(Some(0), Some(10)), (Some(1), Some(20))])).await;
    let edits = vec![((Some(0), Some(10)), -1), ((Some(2), Some(10)), 1)];
    let candidate = view.prepare(&[change(&edits)]).await.unwrap();
    let values = candidate
        .stats()
        .nodes
        .iter()
        .find_map(|n| n.accumulator_work.as_ref())
        .unwrap()
        .value_index_work
        .as_ref()
        .unwrap();
    assert_eq!(values.input_rows, 4);
    assert_eq!(values.value_updates, 0);
    assert_eq!(values.directory_entries_copied, 0);
    assert_eq!(candidate.delta().unwrap().num_rows(), 0);
    view.acknowledge(candidate).unwrap();
    let candidate = view
        .prepare(&[change(&[((Some(2), Some(10)), -1)])])
        .await
        .unwrap();
    assert_eq!(
        multiset(candidate.contents()),
        multiset(&oracle(sql, batch(&[(Some(1), Some(20))])).await)
    );
}

#[tokio::test]
async fn deterministic_sql_epochs_match_an_independent_integer_group_oracle_and_replay() {
    let sql = "SELECT k, count(*), min(v), max(v) FROM t GROUP BY k";
    let mut model = (0..200)
        .map(|i| (Some(i % 19), Some(i % 31 - 15)))
        .collect::<Vec<_>>();
    let mut view = strict(sql, batch(&model)).await;
    let mut seed = 104729u64;
    for step in 0..80 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let index = seed as usize % model.len();
        let old = model[index];
        let new = (
            if step % 7 == 0 {
                None
            } else {
                Some((seed % 23) as i64)
            },
            if step % 5 == 0 {
                None
            } else {
                Some((seed % 41) as i64 - 20)
            },
        );
        let edits = [(old, -1), (new, 1)];
        edit_model(&mut model, &edits);
        let mut groups = BTreeMap::<Option<i64>, Vec<Option<i64>>>::new();
        for (key, value) in &model {
            groups.entry(*key).or_default().push(*value);
        }
        let expected = groups
            .iter()
            .map(|(key, values)| {
                (
                    vec![
                        ScalarValue::Int64(*key),
                        ScalarValue::Int64(Some(values.len() as i64)),
                        ScalarValue::Int64(values.iter().flatten().copied().min()),
                        ScalarValue::Int64(values.iter().flatten().copied().max()),
                    ],
                    1,
                )
            })
            .collect::<Multiset>();
        let before = multiset(&view.contents().unwrap());
        let candidate = view.prepare(&[change(&edits)]).await.unwrap();
        assert_eq!(multiset(candidate.contents()), expected, "step={step}");
        assert_eq!(replay(before, &candidate.delta().unwrap()), expected);
        if step % 8 == 0 {
            assert_eq!(
                multiset(candidate.contents()),
                multiset(&oracle(sql, batch(&model)).await)
            );
        }
        let rival = if step % 9 == 0 {
            Some(view.prepare(&[change(&edits)]).await.unwrap())
        } else {
            None
        };
        view.acknowledge(candidate).unwrap();
        if let Some(rival) = rival {
            assert!(view.acknowledge(rival).is_err());
            assert_eq!(multiset(&view.contents().unwrap()), expected);
        }
        let before = view.contents().unwrap();
        assert!(view
            .prepare(&[change(&[((Some(99999), Some(99999)), -1)])])
            .await
            .is_err());
        assert_eq!(view.contents().unwrap(), before);
    }
}

#[tokio::test]
async fn admitted_extremum_types_match_native_result_types_nulls_and_retractions() {
    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, Int16Array, Int32Array,
        Int8Array, LargeStringArray, NullArray, StringArray, StringViewArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    };
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(NullArray::new(4)),
        Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(true),
        ])),
        Arc::new(Int8Array::from(vec![Some(-3), Some(9), None, Some(9)])),
        Arc::new(Int16Array::from(vec![Some(-3), Some(9), None, Some(9)])),
        Arc::new(Int32Array::from(vec![Some(-3), Some(9), None, Some(9)])),
        Arc::new(Int64Array::from(vec![Some(-3), Some(9), None, Some(9)])),
        Arc::new(UInt8Array::from(vec![Some(0), Some(9), None, Some(9)])),
        Arc::new(UInt16Array::from(vec![Some(0), Some(9), None, Some(9)])),
        Arc::new(UInt32Array::from(vec![Some(0), Some(9), None, Some(9)])),
        Arc::new(UInt64Array::from(vec![
            Some(0),
            Some(u64::MAX),
            None,
            Some(u64::MAX),
        ])),
        Arc::new(
            Decimal128Array::from(vec![Some(-300), Some(900), None, Some(900)])
                .with_precision_and_scale(18, 2)
                .unwrap(),
        ),
        Arc::new(Date32Array::from(vec![Some(-3), Some(9), None, Some(9)])),
        Arc::new(Date64Array::from(vec![
            Some(-86_400_000),
            Some(86_400_000),
            None,
            Some(86_400_000),
        ])),
        Arc::new(StringArray::from(vec![
            Some("A"),
            Some("é"),
            None,
            Some("e\u{301}"),
        ])),
        Arc::new(LargeStringArray::from(vec![
            Some("A"),
            Some("é"),
            None,
            Some("e\u{301}"),
        ])),
        Arc::new(StringViewArray::from(vec![
            Some("A"),
            Some("é"),
            None,
            Some("e\u{301}"),
        ])),
    ];
    let sql = "SELECT min(v), max(v) FROM t";
    for array in arrays {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            array.data_type().clone(),
            true,
        )]));
        let input = RecordBatch::try_new(schema.clone(), vec![array.clone()]).unwrap();
        let mut view = strict(sql, input.clone()).await;
        for (offset, length) in [(3, 1), (1, 1), (0, 1), (2, 1)] {
            let before = view.contents().unwrap();
            let current = view.tables().unwrap()["t"].clone();
            let remove = input.slice(offset, length);
            let candidate = view
                .prepare(&[TableChange {
                    table: "t".into(),
                    rows: remove,
                    weights: vec![-1],
                }])
                .await
                .unwrap();
            // Independently track retained original positions instead of relying on the index.
            let retained = match offset {
                3 => input.slice(0, 3),
                1 => datafusion::arrow::compute::concat_batches(
                    &schema,
                    &[input.slice(0, 1), input.slice(2, 1)],
                )
                .unwrap(),
                0 => input.slice(2, 1),
                _ => RecordBatch::new_empty(schema.clone()),
            };
            let expected = oracle(sql, retained).await;
            assert_eq!(
                candidate.contents().schema(),
                expected.schema(),
                "{:?}",
                array.data_type()
            );
            assert_eq!(
                multiset(candidate.contents()),
                multiset(&expected),
                "{:?}",
                array.data_type()
            );
            assert_eq!(
                replay(multiset(&before), &candidate.delta().unwrap()),
                multiset(&expected)
            );
            assert_eq!(view.tables().unwrap()["t"], current);
            view.acknowledge(candidate).unwrap();
        }
    }
    let sql = "SELECT min(CAST(v AS DECIMAL(60,5))), max(CAST(v AS DECIMAL(60,5))) FROM t";
    let input = batch(&[(None, Some(-3)), (None, Some(9)), (None, None)]);
    let view = strict(sql, input.clone()).await;
    let candidate = view
        .prepare(&[change(&[((None, Some(9)), -1)])])
        .await
        .unwrap();
    assert_eq!(
        multiset(candidate.contents()),
        multiset(&oracle(sql, batch(&[(None, Some(-3)), (None, None)])).await)
    );
}

#[tokio::test]
async fn sql_writes_retry_and_reopen_preserve_duplicate_extrema_and_output_history() {
    use optd_incremental_sql::database::SqlSession;

    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute("CREATE TABLE t(k BIGINT, v BIGINT)")
        .await
        .unwrap();
    let mut model = vec![
        (Some(1), Some(9)),
        (Some(1), Some(9)),
        (Some(1), Some(3)),
        (Some(2), None),
        (None, Some(7)),
    ];
    db.execute("INSERT INTO t VALUES (1,9),(1,9),(1,3),(2,NULL),(NULL,7)")
        .await
        .unwrap();
    let queries = [
        (
            "g",
            "SELECT k, count(*) AS n, min(v) AS lo, max(v) AS hi FROM t GROUP BY k",
        ),
        (
            "a",
            "SELECT count(*) AS n, sum(v) AS s, min(v) AS lo, max(v) AS hi FROM t",
        ),
    ];
    for (name, sql) in queries {
        db.execute(&format!("CREATE MATERIALIZED VIEW {name} AS {sql}"))
            .await
            .unwrap();
    }
    let rounds = [
        (
            "DELETE FROM t WHERE k=1 AND v=9",
            vec![((Some(1), Some(9)), -1), ((Some(1), Some(9)), -1)],
        ),
        (
            "INSERT INTO t VALUES (1,11),(1,11)",
            vec![((Some(1), Some(11)), 1), ((Some(1), Some(11)), 1)],
        ),
        (
            "UPDATE t SET k=3, v=-2 WHERE k=1 AND v=3",
            vec![((Some(1), Some(3)), -1), ((Some(3), Some(-2)), 1)],
        ),
        (
            "DELETE FROM t WHERE k=1",
            vec![((Some(1), Some(11)), -1), ((Some(1), Some(11)), -1)],
        ),
        (
            "UPDATE t SET v=5 WHERE v IS NULL",
            vec![((Some(2), None), -1), ((Some(2), Some(5)), 1)],
        ),
        (
            "DELETE FROM t",
            vec![
                ((Some(2), Some(5)), -1),
                ((Some(3), Some(-2)), -1),
                ((None, Some(7)), -1),
            ],
        ),
        (
            "INSERT INTO t VALUES (NULL,NULL),(NULL,-10)",
            vec![((None, None), 1), ((None, Some(-10)), 1)],
        ),
    ];
    for (epoch, (sql, edits)) in rounds.into_iter().enumerate() {
        let id = format!("extrema-{epoch}");
        let response = db.execute_request(&id, sql).await.unwrap();
        assert!(!response.replayed);
        let commit = serde_json::to_value(response.commit.unwrap()).unwrap();
        for name in ["a", "g"] {
            let nodes = commit["maintenance"][name]["nodes"].as_array().unwrap();
            let work = nodes
                .iter()
                .find_map(|node| node.get("accumulator_work"))
                .unwrap();
            assert_eq!(work["historical_input_rows"], 0);
            assert!(work["value_index_work"].is_object());
        }
        edit_model(&mut model, &edits);
        for (name, query) in queries {
            let actual = db
                .execute(&format!("SELECT * FROM {name}"))
                .await
                .unwrap()
                .rows
                .unwrap();
            assert_eq!(
                multiset(&actual),
                multiset(&oracle(query, batch(&model)).await)
            );
        }
        db.verify_output_history().unwrap();
        drop(db);
        db = SqlSession::open(root.path()).await.unwrap();
        let retry = db.execute_request(&id, sql).await.unwrap();
        assert!(retry.replayed);
        assert_eq!(serde_json::to_value(retry.commit.unwrap()).unwrap(), commit);
        for (name, query) in queries {
            let actual = db
                .execute(&format!("SELECT * FROM {name}"))
                .await
                .unwrap()
                .rows
                .unwrap();
            assert_eq!(
                multiset(&actual),
                multiset(&oracle(query, batch(&model)).await)
            );
        }
        db.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn unqualified_numeric_types_and_modifiers_do_not_select_indexed_accumulators() {
    for sql in [
        "SELECT min(CAST(v AS DOUBLE)), max(CAST(v AS DOUBLE)) FROM t",
        "SELECT sum(CAST(v AS DOUBLE)), min(v) FROM t",
        "SELECT avg(v), max(v) FROM t",
        "SELECT k, min(v) FILTER (WHERE v > 0) FROM t GROUP BY k",
    ] {
        let view = SqlView::new(
            sql,
            BTreeMap::from([("t".into(), batch(&[(Some(1), Some(2)), (Some(1), Some(5))]))]),
        )
        .await
        .unwrap();
        assert!(
            view.strategies()
                .iter()
                .filter(|n| n.operator.to_lowercase().contains("aggregate"))
                .all(|n| n.strategy != Strategy::AccumulatorDelta),
            "{sql}"
        );
        let candidate = view
            .prepare(&[change(&[((Some(1), Some(5)), -1)])])
            .await
            .unwrap();
        assert_eq!(
            multiset(candidate.contents()),
            multiset(&oracle(sql, batch(&[(Some(1), Some(2))])).await)
        );
    }
}
