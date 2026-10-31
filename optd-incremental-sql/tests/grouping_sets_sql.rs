//! Grouping-set normalization must preserve typed identities and ordinary delta plans.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use optd_incremental_sql::{SqlView, Strategy};

fn input() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("p", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), None])),
            Arc::new(Int64Array::from(vec![Some(2), None, Some(2)])),
            Arc::new(Int64Array::from(vec![Some(5), Some(7), None])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn rollup_cube_and_duplicate_sets_select_ordinary_accumulator_branches() {
    for sql in [
        "SELECT k, sum(v), grouping(k) FROM t GROUP BY ROLLUP(k)",
        "SELECT k, sum(v) FROM t GROUP BY GROUPING SETS ((k),())",
        "SELECT k,p,count(*),sum(v),grouping(k,p) FROM t GROUP BY CUBE(k,p)",
        "SELECT k,p,count(*),grouping(k,p) FROM t GROUP BY GROUPING SETS ((k),(p),(),(k))",
        "SELECT k FROM t GROUP BY GROUPING SETS ((k),())",
        "SELECT k,p,sum(v),grouping(p) FROM t GROUP BY k,ROLLUP(p)",
    ] {
        let view = SqlView::new(sql, BTreeMap::from([("t".into(), input())]))
            .await
            .unwrap();
        let aggregates = view
            .strategies()
            .into_iter()
            .filter(|node| node.operator.starts_with("Aggregate"))
            .collect::<Vec<_>>();
        assert!(!aggregates.is_empty());
        assert!(
            aggregates
                .iter()
                .all(|node| node.strategy == Strategy::AccumulatorDelta),
            "{sql}: {aggregates:?}"
        );
        assert!(
            view.strategies()
                .iter()
                .any(|node| node.operator.starts_with("Union")),
            "{sql}"
        );
    }
}

use datafusion::arrow::compute::concat_batches;
use datafusion::common::ScalarValue;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::TableChange;
use std::collections::HashMap;

type InputRow = (Option<i64>, Option<i64>, Option<i64>);
type Rows = HashMap<Vec<ScalarValue>, i64>;

fn batch(rows: &[InputRow]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("p", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
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
            let index = model.iter().position(|value| value == row).unwrap();
            model.remove(index);
        }
    }
}

async fn oracle(sql: &str, input: RecordBatch) -> RecordBatch {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_batch("t", input).unwrap();
    let frame = ctx.sql(sql).await.unwrap();
    // GROUPING is lowered before physical planning and can become nonnullable.
    // Compare actual full-query result schemas, not the pre-optimization schema.
    let plan = frame.create_physical_plan().await.unwrap();
    let schema = plan.schema();
    concat_batches(
        &schema,
        &datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap(),
    )
    .unwrap()
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
async fn null_identities_duplicate_sets_empty_totals_and_key_moves_match_full_sql() {
    for sql in [
        "SELECT k,p,count(*),sum(v),min(v),max(v),grouping(k,p),grouping(p,k) FROM t GROUP BY ROLLUP(k,p)",
        "SELECT k,p,count(*),count(DISTINCT v),sum(DISTINCT v),grouping(k,p) FROM t GROUP BY CUBE(k,p)",
        "SELECT k,p,count(*),sum(v),grouping(p,k) FROM t GROUP BY GROUPING SETS ((k),(p),(),(k),(p,k),(k,p),())",
        "SELECT k,p,sum(v),grouping(p) FROM t GROUP BY k,ROLLUP(p) HAVING sum(v)>0 OR grouping(p)=1",
        "SELECT k%2 AS a,p+1 AS b,count(*),grouping(p+1,k%2) FROM t GROUP BY ROLLUP(k%2,p+1)",
        "SELECT k FROM t GROUP BY GROUPING SETS ((k),())",
        "SELECT 1 AS c FROM t GROUP BY GROUPING SETS ((),())",
        "SELECT k,count(*),grouping(k) FROM t GROUP BY GROUPING SETS ((k))",
        "SELECT count(*),sum(v) FROM t GROUP BY GROUPING SETS (())",
    ] {
        let mut model=vec![(Some(1),Some(2),Some(5)),(Some(1),Some(2),Some(5)),(Some(1),None,Some(-4)),(None,Some(2),Some(7)),(None,None,None)];
        let mut view=strict(sql,batch(&model)).await;
        assert_eq!(bag(&view.contents().unwrap()),bag(&oracle(sql,batch(&model)).await),"bootstrap: {sql}");
        let mut rounds=vec![
            vec![((Some(1),Some(2),Some(5)),-1)],
            vec![((Some(1),Some(2),Some(5)),-1),((Some(2),Some(3),Some(10)),1)],
            vec![((None,Some(2),Some(7)),-1),((Some(1),None,Some(7)),1)],
        ];
        let mut endpoint=model.clone();for edits in &rounds {edit(&mut endpoint,edits);}rounds.push(endpoint.iter().map(|row|(*row,-1)).collect());
        rounds.push(vec![((None,None,None),1),((None,None,None),1)]);
        rounds.push(vec![((Some(1),Some(1),Some(9)),1)]);
        for edits in rounds {
            let before=view.contents().unwrap();let sources=view.tables().unwrap();edit(&mut model,&edits);
            let expected=oracle(sql,batch(&model)).await;
            let candidate=view.prepare(&[change(&edits)]).await.unwrap();
            assert_eq!(candidate.contents().schema(),expected.schema(),"{sql}");assert_eq!(bag(candidate.contents()),bag(&expected),"{sql} {edits:?}");
            assert_eq!(replay(bag(&before),&candidate.delta().unwrap()),bag(&expected));
            assert_eq!(view.contents().unwrap(),before);assert_eq!(view.tables().unwrap(),sources);drop(candidate);
            let candidate=view.prepare(&[change(&edits)]).await.unwrap();let stale=view.prepare(&[change(&edits)]).await.unwrap();view.acknowledge(candidate).unwrap();assert!(view.acknowledge(stale).is_err());
        }
    }
}

#[tokio::test]
async fn not_null_grouping_keys_keep_the_original_nullable_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("p", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![3, 4])),
            Arc::new(Int64Array::from(vec![Some(5), None])),
        ],
    )
    .unwrap();
    for sql in [
        "SELECT k,p,count(*),sum(v),grouping(k,p) FROM t GROUP BY CUBE(k,p)",
        "SELECT k,p FROM t GROUP BY GROUPING SETS ((k),(p),())",
    ] {
        let view = strict(sql, data.clone()).await;
        let expected = oracle(sql, data.clone()).await;
        assert_eq!(view.contents().unwrap().schema(), expected.schema());
        assert_eq!(bag(&view.contents().unwrap()), bag(&expected));
        let candidate = view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: data.clone(),
                weights: vec![-1, -1],
            }])
            .await
            .unwrap();
        let expected = oracle(sql, RecordBatch::new_empty(schema.clone())).await;
        assert_eq!(candidate.contents().schema(), expected.schema());
        assert_eq!(bag(candidate.contents()), bag(&expected));
    }
}

#[tokio::test]
async fn one_key_change_updates_only_each_rollup_branch_state_at_growing_scale() {
    let sql = "SELECT k,p,sum(v),grouping(k,p) FROM t GROUP BY ROLLUP(k,p)";
    for n in [1024usize, 8192] {
        let mut model = (0..n)
            .map(|key| (Some(key as i64), Some(0), Some(1)))
            .collect::<Vec<_>>();
        let view = strict(sql, batch(&model)).await;
        let edits = [
            ((Some(0), Some(0), Some(1)), -1),
            ((Some(0), Some(0), Some(2)), 1),
        ];
        let candidate = view.prepare(&[change(&edits)]).await.unwrap();
        let work = candidate
            .stats()
            .nodes
            .iter()
            .filter_map(|node| node.accumulator_work.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(work.len(), 3);
        assert_eq!(work.iter().map(|w| w.touched_groups).sum::<usize>(), 3);
        assert!(work
            .iter()
            .all(|w| w.historical_input_rows == 0 && w.delta_input_rows == 2));
        assert!(
            candidate.stats().work.state_copies.accumulator_entries
                <= 12 * (n.ilog2() as usize + 1)
        );
        edit(&mut model, &edits);
        assert_eq!(
            bag(candidate.contents()),
            bag(&oracle(sql, batch(&model)).await)
        );
    }
}

#[tokio::test]
async fn not_null_grouping_key_metadata_survives_logical_and_physical_boundaries() {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("k", DataType::Int64, false)
                .with_metadata(HashMap::from([("meaning".into(), "grouping-key".into())])),
            Field::new("v", DataType::Int64, true),
        ],
        HashMap::from([("origin".into(), "grouping-test".into())]),
    ));
    let input = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![Some(3), None])),
        ],
    )
    .unwrap();
    let sql = "SELECT k,sum(v),grouping(k) FROM t GROUP BY ROLLUP(k)";
    let view = strict(sql, input.clone()).await;
    let expected = oracle(sql, input.clone()).await;
    assert_eq!(view.contents().unwrap().schema(), expected.schema());
    assert_eq!(bag(&view.contents().unwrap()), bag(&expected));
    let candidate = view
        .prepare(&[TableChange {
            table: "t".into(),
            rows: input.clone(),
            weights: vec![-1, -1],
        }])
        .await
        .unwrap();
    let expected = oracle(sql, RecordBatch::new_empty(input.schema())).await;
    assert_eq!(candidate.contents().schema(), expected.schema());
    assert_eq!(bag(candidate.contents()), bag(&expected));
}

#[tokio::test]
async fn randomized_rollup_deltas_match_an_independent_grouping_identity_oracle() {
    let sql="SELECT k,p,grouping(k,p) AS g,grouping(p,k) AS r,count(*) AS n,sum(v) AS s FROM t GROUP BY ROLLUP(k,p)";
    let mut model = (0..120)
        .map(|i| (Some(i % 11), Some(i % 7), Some(i % 23 - 11)))
        .collect::<Vec<_>>();
    let mut view = strict(sql, batch(&model)).await;
    let mut seed = 524287u64;
    for step in 0..80 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let old = model[seed as usize % model.len()];
        let new = (
            if step % 5 == 0 {
                None
            } else {
                Some((seed % 13) as i64)
            },
            if step % 7 == 0 {
                None
            } else {
                Some((seed % 9) as i64)
            },
            if step % 11 == 0 {
                None
            } else {
                Some((seed % 31) as i64 - 15)
            },
        );
        let edits = [(old, -1), (new, 1)];
        edit(&mut model, &edits);
        let mut groups = BTreeMap::<(Option<i64>, Option<i64>, i32), (i64, Option<i64>)>::from([(
            (None, None, 3),
            (0, None),
        )]);
        for (k, p, v) in &model {
            for key in [(*k, *p, 0), (*k, None, 1), (None, None, 3)] {
                let entry = groups.entry(key).or_default();
                entry.0 += 1;
                if let Some(value) = v {
                    entry.1 = Some(entry.1.unwrap_or_default() + value);
                }
            }
        }
        let expected = groups
            .into_iter()
            .map(|((k, p, g), (count, sum))| {
                (
                    vec![
                        ScalarValue::Int64(k),
                        ScalarValue::Int64(p),
                        ScalarValue::Int32(Some(g)),
                        ScalarValue::Int32(Some(((g & 1) << 1) | ((g & 2) >> 1))),
                        ScalarValue::Int64(Some(count)),
                        ScalarValue::Int64(sum),
                    ],
                    1,
                )
            })
            .collect::<Rows>();
        let before = view.contents().unwrap();
        let candidate = view.prepare(&[change(&edits)]).await.unwrap();
        assert_eq!(bag(candidate.contents()), expected, "step={step}");
        assert_eq!(replay(bag(&before), &candidate.delta().unwrap()), expected);
        if step % 8 == 0 {
            assert_eq!(
                bag(candidate.contents()),
                bag(&oracle(sql, batch(&model)).await)
            );
        }
        if step % 11 == 0 {
            drop(candidate);
            view.acknowledge(view.prepare(&[change(&edits)]).await.unwrap())
                .unwrap();
        } else {
            view.acknowledge(candidate).unwrap();
        }
        let before = view.contents().unwrap();
        assert!(view
            .prepare(&[change(&[((Some(9999), None, None), -1)])])
            .await
            .is_err());
        assert_eq!(view.contents().unwrap(), before);
    }
}

#[tokio::test]
async fn sql_rollup_mixed_distinct_states_reopen_retry_and_replay_atomically() {
    use optd_incremental_sql::database::SqlSession;
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute("CREATE TABLE t(k BIGINT,p BIGINT,v BIGINT)")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,2,5),(1,2,5),(1,NULL,7),(NULL,2,9),(NULL,NULL,NULL)")
        .await
        .unwrap();
    let mut model = vec![
        (Some(1), Some(2), Some(5)),
        (Some(1), Some(2), Some(5)),
        (Some(1), None, Some(7)),
        (None, Some(2), Some(9)),
        (None, None, None),
    ];
    let sql="SELECT k,p,grouping(k,p) AS g,count(*) AS n,count(DISTINCT v) AS d,sum(v) AS s,min(v) AS lo,max(v) AS hi FROM t GROUP BY ROLLUP(k,p)";
    db.execute(&format!("CREATE MATERIALIZED VIEW m AS {sql}"))
        .await
        .unwrap();
    let rounds = [
        (
            "UPDATE t SET k=3,p=4 WHERE k=1 AND p=2",
            vec![
                ((Some(1), Some(2), Some(5)), -1),
                ((Some(1), Some(2), Some(5)), -1),
                ((Some(3), Some(4), Some(5)), 1),
                ((Some(3), Some(4), Some(5)), 1),
            ],
        ),
        (
            "DELETE FROM t WHERE k=3",
            vec![
                ((Some(3), Some(4), Some(5)), -1),
                ((Some(3), Some(4), Some(5)), -1),
            ],
        ),
        (
            "DELETE FROM t",
            vec![
                ((Some(1), None, Some(7)), -1),
                ((None, Some(2), Some(9)), -1),
                ((None, None, None), -1),
            ],
        ),
        (
            "INSERT INTO t VALUES (NULL,NULL,10),(1,2,3)",
            vec![
                ((None, None, Some(10)), 1),
                ((Some(1), Some(2), Some(3)), 1),
            ],
        ),
    ];
    for (epoch, (dml, edits)) in rounds.into_iter().enumerate() {
        let id = format!("rollup-{epoch}");
        let response = db.execute_request(&id, dml).await.unwrap();
        assert!(!response.replayed);
        let receipt = serde_json::to_value(response.commit.unwrap()).unwrap();
        let nodes = receipt["maintenance"]["m"]["nodes"].as_array().unwrap();
        let work = nodes
            .iter()
            .filter_map(|node| node.get("accumulator_work"))
            .collect::<Vec<_>>();
        assert_eq!(work.len(), 3);
        assert!(work.iter().all(|w| w["historical_input_rows"] == 0));
        edit(&mut model, &edits);
        let expected = oracle(sql, batch(&model)).await;
        let actual = db.execute("SELECT * FROM m").await.unwrap().rows.unwrap();
        assert_eq!(actual.schema(), expected.schema());
        assert_eq!(bag(&actual), bag(&expected));
        db.verify_output_history().unwrap();
        drop(db);
        db = SqlSession::open(root.path()).await.unwrap();
        let retry = db.execute_request(&id, dml).await.unwrap();
        assert!(retry.replayed);
        assert_eq!(
            serde_json::to_value(retry.commit.unwrap()).unwrap(),
            receipt
        );
        let actual = db.execute("SELECT * FROM m").await.unwrap().rows.unwrap();
        assert_eq!(bag(&actual), bag(&expected));
        db.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn duplicate_set_ordinals_cross_the_eight_bit_mask_boundary_without_losing_rows() {
    let names = (0..8).map(|i| format!("k{i}")).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let input = RecordBatch::try_new(
        schema,
        names
            .iter()
            .enumerate()
            .map(|(i, _)| {
                Arc::new(Int64Array::from(vec![i as i64, i as i64 + 1]))
                    as datafusion::arrow::array::ArrayRef
            })
            .collect(),
    )
    .unwrap();
    let keys = names.join(",");
    let sql=format!("SELECT {keys},grouping({keys}) AS g,count(*) AS n FROM t GROUP BY GROUPING SETS (({keys}),({keys}),())");
    let mut view = strict(&sql, input.clone()).await;
    let expected = oracle(&sql, input.clone()).await;
    assert_eq!(view.contents().unwrap().schema(), expected.schema());
    assert_eq!(bag(&view.contents().unwrap()), bag(&expected));
    assert!(bag(&expected).values().any(|count| *count == 2));
    for index in 0..2 {
        let before = view.contents().unwrap();
        let candidate = view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: input.slice(index, 1),
                weights: vec![-1],
            }])
            .await
            .unwrap();
        let expected = oracle(&sql, input.slice(index + 1, 1 - index)).await;
        assert_eq!(candidate.contents().schema(), expected.schema());
        assert_eq!(bag(candidate.contents()), bag(&expected));
        assert_eq!(
            replay(bag(&before), &candidate.delta().unwrap()),
            bag(&expected)
        );
        view.acknowledge(candidate).unwrap();
    }
}

#[tokio::test]
async fn normalization_keeps_unqualified_floating_aggregate_refusal_explicit() {
    let sql = "SELECT k,avg(v) FROM t GROUP BY ROLLUP(k)";
    let view = SqlView::new(sql, BTreeMap::from([("t".into(), input())]))
        .await
        .unwrap();
    assert!(view
        .strategies()
        .iter()
        .any(|node| node.strategy == Strategy::OperatorRecompute));
    let error = SqlView::new_with_optimizer(
        sql,
        BTreeMap::from([("t".into(), input())]),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(
        error,
        optd_incremental_sql::Error::Rewrite {
            phase: "policy",
            ..
        }
    ));
    let expected = oracle(sql, input()).await;
    assert_eq!(bag(&view.contents().unwrap()), bag(&expected));
}
