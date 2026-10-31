//! DISTINCT aggregates consume only counted membership zero crossings.

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
type Rows = HashMap<Vec<ScalarValue>, i64>;

fn batch(rows: &[InputRow]) -> RecordBatch {
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

const QUERIES: &[&str] = &[
    "SELECT count(DISTINCT v), count(*), sum(v), avg(CAST(v AS DECIMAL(18,2))) FROM t",
    "SELECT k, count(DISTINCT v), sum(DISTINCT v), avg(DISTINCT CAST(v AS DECIMAL(18,2))), count(*) FROM t GROUP BY k",
    "SELECT DISTINCT k, v FROM t",
    "SELECT count(DISTINCT k), count(DISTINCT v), sum(v) FROM t",
];

#[tokio::test]
async fn global_grouped_and_mixed_distinct_select_only_accumulator_aggregate_plans() {
    for sql in QUERIES {
        let view = SqlView::new(
            sql,
            BTreeMap::from([(
                "t".into(),
                batch(&[(Some(1), Some(7)), (Some(1), Some(7)), (None, None)]),
            )]),
        )
        .await
        .unwrap();
        let aggregates = view
            .strategies()
            .into_iter()
            .filter(|n| n.operator.to_lowercase().contains("aggregate"))
            .collect::<Vec<_>>();
        assert!(!aggregates.is_empty());
        assert!(
            aggregates
                .iter()
                .all(|n| n.strategy == Strategy::AccumulatorDelta),
            "{sql}: {aggregates:?}"
        );
    }
}

fn bag(batch: &RecordBatch) -> Rows {
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

fn replay(mut rows: Rows, delta: &RecordBatch) -> Rows {
    let columns = delta.num_columns() - 1;
    assert_eq!(delta.schema().field(columns).name(), "__optd_weight");
    for index in 0..delta.num_rows() {
        let row = delta.columns()[..columns]
            .iter()
            .map(|array| ScalarValue::try_from_array(array, index).unwrap())
            .collect::<Vec<_>>();
        let ScalarValue::Int64(Some(weight)) =
            ScalarValue::try_from_array(delta.column(columns), index).unwrap()
        else {
            panic!("signed output weight");
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

fn change(edits: &[(InputRow, i64)]) -> TableChange {
    TableChange {
        table: "t".into(),
        rows: batch(&edits.iter().map(|(row, _)| *row).collect::<Vec<_>>()),
        weights: edits.iter().map(|(_, weight)| *weight).collect(),
    }
}

fn edit(model: &mut Vec<InputRow>, edits: &[(InputRow, i64)]) {
    for (row, weight) in edits {
        if *weight == 1 {
            model.push(*row);
        } else {
            assert_eq!(*weight, -1);
            let index = model.iter().position(|r| r == row).unwrap();
            model.remove(index);
        }
    }
}

async fn oracle(sql: &str, input: RecordBatch) -> RecordBatch {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_batch("t", input).unwrap();
    let frame = ctx.sql(sql).await.unwrap();
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

#[tokio::test]
async fn arbitrary_duplicate_and_null_changes_match_full_sql_schema_and_signed_replay() {
    for sql in QUERIES.iter().copied().chain([
        "SELECT count(DISTINCT v), sum(DISTINCT v), avg(DISTINCT CAST(v AS DECIMAL(18,2))), min(DISTINCT v), max(DISTINCT v) FROM t",
        "SELECT k % 2, count(DISTINCT v % 3), sum(DISTINCT v % 3), min(v), max(v), sum(v) FROM t GROUP BY k % 2",
    ]) {
        let mut model=vec![(Some(0),Some(9)),(Some(0),Some(9)),(Some(0),Some(3)),(Some(1),None),(None,Some(5)),(None,Some(5)),(None,None)];
        let mut view=strict(sql,batch(&model)).await;
        let mut rounds=vec![
            vec![((Some(0),Some(9)),-1)],
            vec![((Some(0),Some(9)),-1)],
            vec![((None,Some(5)),-1),((Some(2),Some(5)),1)],
            vec![((Some(0),Some(3)),-1),((Some(0),Some(-10)),1)],
            vec![((Some(2),Some(5)),-1),((Some(4),Some(5)),1)],
        ];
        let mut endpoint=model.clone();for edits in &rounds {edit(&mut endpoint,edits);}
        rounds.push(endpoint.iter().map(|row|(*row,-1)).collect());
        rounds.push(vec![((None,None),1),((None,None),1)]);
        rounds.push(vec![((Some(0),Some(11)),1),((Some(0),Some(11)),1)]);
        for edits in rounds {
            let before=view.contents().unwrap();let sources=view.tables().unwrap();
            edit(&mut model,&edits);
            let expected=oracle(sql,batch(&model)).await;
            let candidate=view.prepare(&[change(&edits)]).await.unwrap();
            assert_eq!(candidate.contents().schema(),expected.schema(),"{sql}");
            assert_eq!(bag(candidate.contents()),bag(&expected),"{sql} {edits:?}");
            assert_eq!(replay(bag(&before),&candidate.delta().unwrap()),bag(&expected));
            for node in &candidate.stats().nodes {
                if let Some(work)=&node.accumulator_work {assert_eq!(work.historical_input_rows,0);}
            }
            assert_eq!(view.contents().unwrap(),before);assert_eq!(view.tables().unwrap(),sources);
            drop(candidate);
            let candidate=view.prepare(&[change(&edits)]).await.unwrap();
            let stale=view.prepare(&[change(&edits)]).await.unwrap();
            view.acknowledge(candidate).unwrap();assert!(view.acknowledge(stale).is_err());
            assert_eq!(bag(&view.contents().unwrap()),bag(&expected));
        }
    }
}

#[tokio::test]
async fn native_distinct_inputs_are_only_zero_crossings_with_growing_untouched_and_hot_state() {
    let sql="SELECT k, count(*), count(DISTINCT v), sum(DISTINCT v), avg(DISTINCT CAST(v AS DECIMAL(18,2))) FROM t GROUP BY k";
    for n in [1024usize, 8192] {
        for hot in [false, true] {
            let mut model = if hot {
                (0..n)
                    .map(|v| (Some(0), Some(v as i64)))
                    .collect::<Vec<_>>()
            } else {
                (0..n).map(|k| (Some(k as i64), Some(k as i64))).collect()
            };
            model.push((Some(0), Some(0)));
            let mut view = strict(sql, batch(&model)).await;
            for (edits, entering, leaving) in [
                (vec![((Some(0), Some(0)), -1)], 0, 0),
                (
                    vec![((Some(0), Some(0)), -1), ((Some(0), Some(-1)), 1)],
                    3,
                    3,
                ),
            ] {
                let candidate = view.prepare(&[change(&edits)]).await.unwrap();
                let work = candidate
                    .stats()
                    .nodes
                    .iter()
                    .find_map(|n| n.accumulator_work.as_ref())
                    .unwrap();
                let distinct = work.distinct_work.as_ref().unwrap();
                assert_eq!(work.historical_input_rows, 0);
                assert_eq!(work.touched_groups, 1);
                assert_eq!(distinct.entering_values, entering);
                assert_eq!(distinct.leaving_values, leaving);
                assert_eq!(distinct.input_rows, edits.len() * 3);
                assert_eq!(distinct.value_updates, edits.len() * 3);
                assert!(
                    distinct.directory_entries_copied <= 18 * (n.ilog2() as usize + 1),
                    "{distinct:?}"
                );
                edit(&mut model, &edits);
                assert_eq!(
                    bag(candidate.contents()),
                    bag(&oracle(sql, batch(&model)).await)
                );
                view.acknowledge(candidate).unwrap();
            }
        }
    }
}

#[tokio::test]
async fn projected_distinct_cancellation_copies_no_value_paths_or_native_inputs() {
    let sql = "SELECT k % 2, count(DISTINCT v), sum(DISTINCT v), count(*) FROM t GROUP BY k % 2";
    let mut view = strict(sql, batch(&[(Some(0), Some(10)), (Some(1), Some(20))])).await;
    let candidate = view
        .prepare(&[change(&[
            ((Some(0), Some(10)), -1),
            ((Some(2), Some(10)), 1),
        ])])
        .await
        .unwrap();
    let work = candidate
        .stats()
        .nodes
        .iter()
        .find_map(|n| n.accumulator_work.as_ref())
        .unwrap()
        .distinct_work
        .as_ref()
        .unwrap();
    assert_eq!(work.input_rows, 4);
    assert_eq!(work.value_updates, 0);
    assert_eq!(work.directory_entries_copied, 0);
    assert_eq!(work.entering_values, 0);
    assert_eq!(work.leaving_values, 0);
    assert_eq!(candidate.delta().unwrap().num_rows(), 0);
    view.acknowledge(candidate).unwrap();
    let candidate = view
        .prepare(&[change(&[((Some(2), Some(10)), -1)])])
        .await
        .unwrap();
    assert_eq!(
        bag(candidate.contents()),
        bag(&oracle(sql, batch(&[(Some(1), Some(20))])).await)
    );
}

#[tokio::test]
async fn deterministic_epochs_match_independent_integer_sets_and_delta_prefixes() {
    use std::collections::BTreeSet;
    let sql = "SELECT k, count(*), count(DISTINCT v), sum(DISTINCT v) FROM t GROUP BY k";
    let mut model = (0..240)
        .map(|i| (Some(i % 17), Some(i % 23 - 11)))
        .collect::<Vec<_>>();
    let mut view = strict(sql, batch(&model)).await;
    let mut seed = 65537u64;
    for step in 0..120 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let old = model[seed as usize % model.len()];
        let new = (
            if step % 9 == 0 {
                None
            } else {
                Some((seed % 19) as i64)
            },
            if step % 7 == 0 {
                None
            } else {
                Some((seed % 29) as i64 - 14)
            },
        );
        let edits = [(old, -1), (new, 1)];
        edit(&mut model, &edits);
        let mut groups = BTreeMap::<Option<i64>, Vec<Option<i64>>>::new();
        for (key, value) in &model {
            groups.entry(*key).or_default().push(*value);
        }
        let expected = groups
            .iter()
            .map(|(key, values)| {
                let distinct = values.iter().flatten().copied().collect::<BTreeSet<_>>();
                (
                    vec![
                        ScalarValue::Int64(*key),
                        ScalarValue::Int64(Some(values.len() as i64)),
                        ScalarValue::Int64(Some(distinct.len() as i64)),
                        ScalarValue::Int64((!distinct.is_empty()).then(|| distinct.iter().sum())),
                    ],
                    1,
                )
            })
            .collect::<Rows>();
        let before = bag(&view.contents().unwrap());
        let candidate = view.prepare(&[change(&edits)]).await.unwrap();
        assert_eq!(bag(candidate.contents()), expected, "step={step}");
        assert_eq!(replay(before, &candidate.delta().unwrap()), expected);
        if step % 10 == 0 {
            assert_eq!(
                bag(candidate.contents()),
                bag(&oracle(sql, batch(&model)).await)
            );
        }
        if step % 7 == 0 {
            drop(candidate);
            view.acknowledge(view.prepare(&[change(&edits)]).await.unwrap())
                .unwrap();
        } else {
            view.acknowledge(candidate).unwrap();
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
async fn sql_duplicate_zero_crossings_survive_reopen_retry_and_output_history() {
    use optd_incremental_sql::database::SqlSession;
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute("CREATE TABLE t(k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,9),(3,9),(5,3),(2,NULL),(NULL,7)")
        .await
        .unwrap();
    let mut model = vec![
        (Some(1), Some(9)),
        (Some(3), Some(9)),
        (Some(5), Some(3)),
        (Some(2), None),
        (None, Some(7)),
    ];
    let queries=[
        ("g","SELECT k % 2 AS g, count(*) AS n, count(DISTINCT v) AS d, sum(DISTINCT v) AS s, avg(DISTINCT CAST(v AS DECIMAL(18,2))) AS a FROM t GROUP BY k % 2"),
        ("a","SELECT count(*) AS n, count(DISTINCT v) AS d, sum(v) AS s, avg(CAST(v AS DECIMAL(18,2))) AS a FROM t"),
    ];
    for (name, query) in queries {
        db.execute(&format!("CREATE MATERIALIZED VIEW {name} AS {query}"))
            .await
            .unwrap();
    }
    let rounds = [
        ("DELETE FROM t WHERE k=1", vec![((Some(1), Some(9)), -1)]),
        (
            "INSERT INTO t VALUES (1,11),(7,11)",
            vec![((Some(1), Some(11)), 1), ((Some(7), Some(11)), 1)],
        ),
        (
            "UPDATE t SET k=9,v=-2 WHERE k=3",
            vec![((Some(3), Some(9)), -1), ((Some(9), Some(-2)), 1)],
        ),
        ("DELETE FROM t WHERE k=1", vec![((Some(1), Some(11)), -1)]),
        ("DELETE FROM t WHERE k=7", vec![((Some(7), Some(11)), -1)]),
        (
            "DELETE FROM t",
            vec![
                ((Some(9), Some(-2)), -1),
                ((Some(5), Some(3)), -1),
                ((Some(2), None), -1),
                ((None, Some(7)), -1),
            ],
        ),
        (
            "INSERT INTO t VALUES (NULL,NULL),(NULL,NULL)",
            vec![((None, None), 1), ((None, None), 1)],
        ),
    ];
    for (round, (sql, edits)) in rounds.into_iter().enumerate() {
        let id = format!("distinct-{round}");
        let response = db.execute_request(&id, sql).await.unwrap();
        assert!(!response.replayed);
        let receipt = serde_json::to_value(response.commit.unwrap()).unwrap();
        for name in ["g", "a"] {
            let nodes = receipt["maintenance"][name]["nodes"].as_array().unwrap();
            assert!(nodes
                .iter()
                .any(|node| node["accumulator_work"]["distinct_work"].is_object()));
            for work in nodes.iter().filter_map(|node| node.get("accumulator_work")) {
                assert_eq!(work["historical_input_rows"], 0);
            }
        }
        edit(&mut model, &edits);
        for (name, query) in queries {
            let actual = db
                .execute(&format!("SELECT * FROM {name}"))
                .await
                .unwrap()
                .rows
                .unwrap();
            assert_eq!(bag(&actual), bag(&oracle(query, batch(&model)).await));
        }
        db.verify_output_history().unwrap();
        drop(db);
        db = SqlSession::open(root.path()).await.unwrap();
        let retry = db.execute_request(&id, sql).await.unwrap();
        assert!(retry.replayed);
        assert_eq!(
            serde_json::to_value(retry.commit.unwrap()).unwrap(),
            receipt
        );
        for (name, query) in queries {
            let actual = db
                .execute(&format!("SELECT * FROM {name}"))
                .await
                .unwrap()
                .rows
                .unwrap();
            assert_eq!(bag(&actual), bag(&oracle(query, batch(&model)).await));
        }
        db.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn normalized_distinct_duplicates_use_group_counts_without_reading_the_bucket() {
    for copies in [1024usize, 8192] {
        let sql = "SELECT DISTINCT k,v FROM t";
        let mut view = strict(sql, batch(&vec![(Some(1), Some(7)); copies])).await;
        let candidate = view
            .prepare(&[change(&[((Some(1), Some(7)), -1)])])
            .await
            .unwrap();
        let work = candidate
            .stats()
            .nodes
            .iter()
            .find_map(|node| node.accumulator_work.as_ref())
            .unwrap();
        assert_eq!(work.delta_input_rows, 1);
        assert_eq!(work.historical_input_rows, 0);
        assert_eq!(work.touched_groups, 1);
        assert!(work.value_index_work.is_none());
        assert!(work.distinct_work.is_none());
        assert_eq!(candidate.stats().work.state_copies.accumulator_entries, 1);
        assert_eq!(candidate.delta().unwrap().num_rows(), 0);
        view.acknowledge(candidate).unwrap();
        let candidate = view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: batch(&[(Some(1), Some(7))]),
                weights: vec![-(copies as i64 - 1)],
            }])
            .await
            .unwrap();
        assert_eq!(candidate.contents().num_rows(), 0);
        assert_eq!(
            replay(bag(&view.contents().unwrap()), &candidate.delta().unwrap()),
            Rows::new()
        );
    }
}

#[tokio::test]
async fn counted_distinct_ordered_types_match_native_sql_nulls_and_last_occurrences() {
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
            Some("é"),
        ])),
        Arc::new(LargeStringArray::from(vec![
            Some("A"),
            Some("é"),
            None,
            Some("é"),
        ])),
        Arc::new(StringViewArray::from(vec![
            Some("A"),
            Some("é"),
            None,
            Some("é"),
        ])),
    ];
    let sql = "SELECT count(*), count(DISTINCT v) FROM t";
    for array in arrays {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            array.data_type().clone(),
            true,
        )]));
        let input = RecordBatch::try_new(schema.clone(), vec![array.clone()]).unwrap();
        let mut view = strict(sql, input.clone()).await;
        let mut retained = vec![0usize, 1, 2, 3];
        for offset in [3, 1, 0, 2] {
            let before = view.contents().unwrap();
            let candidate = view
                .prepare(&[TableChange {
                    table: "t".into(),
                    rows: input.slice(offset, 1),
                    weights: vec![-1],
                }])
                .await
                .unwrap();
            retained.retain(|i| *i != offset);
            let slices = retained
                .iter()
                .map(|i| input.slice(*i, 1))
                .collect::<Vec<_>>();
            let endpoint = concat_batches(&schema, &slices).unwrap();
            let expected = oracle(sql, endpoint).await;
            assert_eq!(
                candidate.contents().schema(),
                expected.schema(),
                "{:?}",
                array.data_type()
            );
            assert_eq!(
                bag(candidate.contents()),
                bag(&expected),
                "{:?}",
                array.data_type()
            );
            assert_eq!(
                replay(bag(&before), &candidate.delta().unwrap()),
                bag(&expected)
            );
            view.acknowledge(candidate).unwrap();
        }
    }
    let sql="SELECT count(*), count(DISTINCT CAST(v AS DECIMAL(60,5))), sum(DISTINCT CAST(v AS DECIMAL(60,5))), avg(DISTINCT CAST(v AS DECIMAL(60,5))) FROM t";
    let input = batch(&[
        (None, Some(-3)),
        (None, Some(9)),
        (None, None),
        (None, Some(9)),
    ]);
    let mut view = strict(sql, input.clone()).await;
    for length in (0..4).rev() {
        let before = view.contents().unwrap();
        let candidate = view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: input.slice(length, 1),
                weights: vec![-1],
            }])
            .await
            .unwrap();
        let expected = oracle(sql, input.slice(0, length)).await;
        assert_eq!(candidate.contents().schema(), expected.schema());
        assert_eq!(bag(candidate.contents()), bag(&expected));
        assert_eq!(
            replay(bag(&before), &candidate.delta().unwrap()),
            bag(&expected)
        );
        view.acknowledge(candidate).unwrap();
    }
}
