//! SQL semantics checked against a fresh, independent DataFusion full plan.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Date32Array, Decimal128Array, Int64Array};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_incremental_sql::{RefreshStats, SqlView, Strategy, TableChange};

type Rows = HashMap<Vec<ScalarValue>, i64>;
type IntRow = (Option<i64>, Option<i64>);

fn ints(rows: &[IntRow]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn change(table: &str, rows: &[IntRow], weights: &[i64]) -> TableChange {
    TableChange {
        table: table.into(),
        rows: ints(rows),
        weights: weights.to_vec(),
    }
}

fn scalar_row(batch: &RecordBatch, row: usize, columns: usize) -> Vec<ScalarValue> {
    batch
        .columns()
        .iter()
        .take(columns)
        .map(|array| ScalarValue::try_from_array(array.as_ref(), row).unwrap())
        .collect()
}

fn add(rows: &mut Rows, row: Vec<ScalarValue>, weight: i64) {
    let next = rows
        .get(&row)
        .copied()
        .unwrap_or(0)
        .checked_add(weight)
        .unwrap();
    if next == 0 {
        rows.remove(&row);
    } else {
        rows.insert(row, next);
    }
}

fn bag(batch: &RecordBatch) -> Rows {
    let mut rows = HashMap::new();
    for index in 0..batch.num_rows() {
        add(&mut rows, scalar_row(batch, index, batch.num_columns()), 1);
    }
    rows
}

fn apply_output_delta(mut rows: Rows, delta: &RecordBatch) -> Rows {
    assert_eq!(
        delta.schema().fields().last().unwrap().name(),
        "__optd_weight"
    );
    for index in 0..delta.num_rows() {
        let weight =
            ScalarValue::try_from_array(delta.column(delta.num_columns() - 1).as_ref(), index)
                .unwrap();
        let ScalarValue::Int64(Some(weight)) = weight else {
            panic!("delta weight must be nonnull Int64");
        };
        assert_ne!(weight, 0);
        add(
            &mut rows,
            scalar_row(delta, index, delta.num_columns() - 1),
            weight,
        );
    }
    rows
}

async fn oracle(sql: &str, tables: BTreeMap<String, RecordBatch>) -> RecordBatch {
    // This context has no incremental graph, relation cache or custom planner.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for (name, batch) in tables {
        ctx.register_batch(name.as_str(), batch).unwrap();
    }
    let frame = ctx.sql(sql).await.unwrap();
    let physical = frame.create_physical_plan().await.unwrap();
    let schema = physical.schema();
    concat_batches(&schema, &collect(physical, ctx.task_ctx()).await.unwrap()).unwrap()
}

async fn assert_oracle(view: &SqlView) {
    let actual = view.contents().unwrap();
    let expected = oracle(view.sql(), view.tables().unwrap()).await;
    // DataFusion's scalar-subquery-to-left-join rewrite conservatively widens
    // COUNT's nullability, while the independent physical scalar plan retains
    // nonnull COUNT. Compare names/types and every actual NULL/value exactly;
    // each maintained plan's own complete schema must remain stable below.
    assert_eq!(
        actual.num_columns(),
        expected.num_columns(),
        "{}",
        view.sql()
    );
    for (actual, expected) in actual
        .schema()
        .fields()
        .iter()
        .zip(expected.schema().fields())
    {
        assert_eq!(actual.name(), expected.name(), "{}", view.sql());
        assert_eq!(actual.data_type(), expected.data_type(), "{}", view.sql());
    }
    assert_eq!(bag(&actual), bag(&expected), "{}", view.sql());
}

async fn refresh(view: &mut SqlView, changes: &[TableChange]) -> RefreshStats {
    let epoch = view.epoch();
    let before_output = view.contents().unwrap();
    let before_tables = view.tables().unwrap();
    let mut expected_sources: BTreeMap<String, Rows> = before_tables
        .iter()
        .map(|(name, batch)| (name.clone(), bag(batch)))
        .collect();
    // Independently check that source application itself has not lost a row or
    // applied a shared/self-join scan more than once.
    for change in changes {
        let rows = expected_sources.get_mut(&change.table).unwrap();
        for (index, weight) in change.weights.iter().enumerate() {
            add(
                rows,
                scalar_row(&change.rows, index, change.rows.num_columns()),
                *weight,
            );
        }
    }
    assert!(expected_sources
        .values()
        .all(|rows| rows.values().all(|weight| *weight > 0)));
    let candidate = view.prepare(changes).await.unwrap();
    assert_eq!(view.epoch(), epoch);
    assert_eq!(bag(&view.contents().unwrap()), bag(&before_output));
    for (name, batch) in view.tables().unwrap() {
        assert_eq!(bag(&batch), bag(&before_tables[&name]));
    }
    assert_eq!(candidate.epoch(), epoch + 1);
    assert_eq!(
        candidate.contents().schema(),
        before_output.schema(),
        "the maintained output schema must be stable across epochs"
    );
    assert_eq!(
        apply_output_delta(bag(&before_output), &candidate.delta().unwrap()),
        bag(candidate.contents()),
        "output changes must reconstruct the candidate relation"
    );
    let stats = candidate.stats().clone();
    view.acknowledge(candidate).unwrap();
    assert_eq!(view.epoch(), epoch + 1);
    assert_eq!(view.contents().unwrap().schema(), before_output.schema());
    for (name, batch) in view.tables().unwrap() {
        assert_eq!(bag(&batch), expected_sources[&name], "source {name}");
    }
    assert_oracle(view).await;
    stats
}

#[tokio::test]
async fn native_inner_join_handles_nulls_duplicates_and_two_sided_changes() {
    let sql = "SELECT l.k, l.v, r.v AS rv FROM l JOIN r ON l.k = r.k WHERE l.v >= 0";
    let tables = BTreeMap::from([
        (
            "l".into(),
            ints(&[
                (Some(1), Some(10)),
                (Some(1), Some(10)),
                (Some(2), Some(20)),
                (None, Some(99)),
            ]),
        ),
        (
            "r".into(),
            ints(&[(Some(1), Some(100)), (Some(2), None), (None, Some(1000))]),
        ),
    ]);
    let mut view = SqlView::new(sql, tables).await.unwrap();
    assert_eq!(view.contents().unwrap().num_rows(), 3);
    assert_oracle(&view).await;
    let stats = refresh(
        &mut view,
        &[
            change(
                "l",
                &[
                    (Some(1), Some(10)),
                    (Some(1), Some(11)),
                    (Some(3), Some(30)),
                ],
                &[-1, 1, 1],
            ),
            change(
                "r",
                &[
                    (Some(1), Some(100)),
                    (Some(1), Some(101)),
                    (Some(3), Some(300)),
                ],
                &[-1, 1, 1],
            ),
        ],
    )
    .await;
    assert!(stats
        .nodes
        .iter()
        .any(|node| node.operator.to_lowercase().contains("join")
            && node.strategy == Strategy::IndexedDelta));
    assert_eq!(view.contents().unwrap().num_rows(), 4);
    refresh(
        &mut view,
        &[
            change("l", &[(Some(1), Some(10)), (None, Some(7))], &[-1, 1]),
            change("r", &[(Some(1), Some(101)), (None, Some(8))], &[-1, 1]),
        ],
    )
    .await;
}

#[tokio::test]
async fn self_join_uses_one_source_epoch_for_both_edges() {
    let mut view = SqlView::new(
        "SELECT a.k, a.v AS av, b.v AS bv FROM t a JOIN t b ON a.k = b.k WHERE a.v < b.v",
        BTreeMap::from([(
            "t".into(),
            ints(&[
                (Some(1), Some(10)),
                (Some(1), Some(10)),
                (Some(1), Some(20)),
                (Some(2), Some(5)),
                (None, Some(30)),
            ]),
        )]),
    )
    .await
    .unwrap();
    assert_eq!(view.contents().unwrap().num_rows(), 2);
    assert_oracle(&view).await;
    refresh(
        &mut view,
        &[change(
            "t",
            &[
                (Some(1), Some(10)),
                (Some(1), Some(20)),
                (Some(1), Some(30)),
                (Some(2), Some(15)),
            ],
            &[-1, -1, 1, 1],
        )],
    )
    .await;
    assert_eq!(view.contents().unwrap().num_rows(), 2);
    refresh(
        &mut view,
        &[change(
            "t",
            &[(Some(1), Some(10)), (Some(1), Some(30))],
            &[-1, 1],
        )],
    )
    .await;
    assert_eq!(view.contents().unwrap().num_rows(), 1);
}

fn decimals(rows: &[(i64, Option<i128>, i32)]) -> RecordBatch {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(
            Decimal128Array::from(rows.iter().map(|row| row.1).collect::<Vec<_>>())
                .with_precision_and_scale(18, 2)
                .unwrap(),
        ),
        Arc::new(Date32Array::from(
            rows.iter().map(|row| row.2).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(18, 2), true),
            Field::new("d", DataType::Date32, false),
        ])),
        arrays,
    )
    .unwrap()
}

#[tokio::test]
async fn decimal_arithmetic_and_date_filters_retain_native_sql_types() {
    let sqls = [
        "SELECT k, amount * CAST(1.25 AS DECIMAL(3,2)) AS taxed, d + INTERVAL '2 days' AS shifted FROM t WHERE amount IS NOT NULL AND d >= DATE '2024-01-01'",
        "SELECT COUNT(amount) AS present, SUM(amount) AS total, AVG(amount) AS mean FROM t WHERE d >= DATE '2024-01-01'",
    ];
    for sql in sqls {
        let mut view = SqlView::new(
            sql,
            BTreeMap::from([(
                "t".into(),
                decimals(&[
                    (1, Some(12345), 19723),
                    (2, Some(-500), 19724),
                    (3, None, 19724),
                    (4, Some(100), 19720),
                ]),
            )]),
        )
        .await
        .unwrap();
        assert_oracle(&view).await;
        refresh(
            &mut view,
            &[TableChange {
                table: "t".into(),
                rows: decimals(&[
                    (1, Some(12345), 19723),
                    (1, Some(23456), 19725),
                    (5, Some(1), 19723),
                ]),
                weights: vec![-1, 1, 1],
            }],
        )
        .await;
        assert!(view
            .contents()
            .unwrap()
            .schema()
            .fields()
            .iter()
            .any(|field| matches!(
                field.data_type(),
                DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
            )));
    }
}

#[tokio::test]
async fn nested_aggregates_having_and_distinct_refresh_operator_local_state() {
    let sql = "SELECT band, SUM(total) AS overall, COUNT(*) AS groups FROM (SELECT k % 2 AS band, k, SUM(v) AS total FROM t GROUP BY k % 2, k HAVING SUM(v) >= 10) a GROUP BY band HAVING SUM(total) > 0";
    let mut view = SqlView::new(
        sql,
        BTreeMap::from([(
            "t".into(),
            ints(&[
                (Some(1), Some(8)),
                (Some(1), Some(8)),
                (Some(2), Some(9)),
                (Some(3), Some(12)),
                (Some(4), None),
                (None, Some(20)),
            ]),
        )]),
    )
    .await
    .unwrap();
    assert_oracle(&view).await;
    let stats = refresh(
        &mut view,
        &[change(
            "t",
            &[
                (Some(1), Some(8)),
                (Some(2), Some(2)),
                (Some(3), Some(12)),
                (Some(4), Some(30)),
            ],
            &[-1, 1, -1, 1],
        )],
    )
    .await;
    assert!(
        stats
            .nodes
            .iter()
            .filter(|node| node.operator.to_lowercase().contains("aggregate")
                && node.strategy == Strategy::AccumulatorDelta
                && node
                    .accumulator_work
                    .as_ref()
                    .is_some_and(|w| w.historical_input_rows == 0))
            .count()
            >= 2
    );
    refresh(
        &mut view,
        &[change(
            "t",
            &[(Some(1), Some(8)), (Some(2), Some(9)), (Some(2), Some(2))],
            &[-1, -1, -1],
        )],
    )
    .await;
}

#[tokio::test]
async fn empty_global_aggregates_distinct_and_null_multiplicities_match_sql() {
    let initial = ints(&[
        (Some(1), Some(10)),
        (Some(1), Some(10)),
        (Some(1), None),
        (None, None),
    ]);
    let sqls = [
        "SELECT DISTINCT k, v FROM t",
        "SELECT COUNT(*) AS n, COUNT(v) AS present, SUM(v) AS s, MIN(v) AS lo, MAX(v) AS hi, AVG(v) AS av FROM t",
        "SELECT k, COUNT(DISTINCT v) AS distinct_values FROM t GROUP BY k",
    ];
    for sql in sqls {
        let mut view = SqlView::new(sql, BTreeMap::from([("t".into(), initial.clone())]))
            .await
            .unwrap();
        assert_oracle(&view).await;
        refresh(
            &mut view,
            &[TableChange {
                table: "t".into(),
                rows: initial.clone(),
                weights: vec![-1; initial.num_rows()],
            }],
        )
        .await;
        if sql.contains("COUNT(*)") {
            assert_eq!(view.contents().unwrap().num_rows(), 1);
        } else {
            assert_eq!(view.contents().unwrap().num_rows(), 0);
        }
        refresh(
            &mut view,
            &[change(
                "t",
                &[(Some(2), None), (Some(2), Some(0)), (Some(2), Some(0))],
                &[1, 1, 1],
            )],
        )
        .await;
    }
}

#[tokio::test]
async fn outer_semi_and_anti_joins_handle_last_match_retractions() {
    let sqls = [
        "SELECT l.k, l.v, r.v AS rv FROM l LEFT JOIN r ON l.k = r.k",
        "SELECT COALESCE(l.k,r.k) AS k, l.v, r.v AS rv FROM l FULL JOIN r ON l.k = r.k",
        "SELECT l.k, l.v FROM l LEFT SEMI JOIN r ON l.k = r.k",
        "SELECT l.k, l.v FROM l LEFT ANTI JOIN r ON l.k = r.k",
    ];
    for sql in sqls {
        let tables = BTreeMap::from([
            (
                "l".into(),
                ints(&[
                    (Some(1), Some(10)),
                    (Some(1), Some(10)),
                    (Some(2), Some(20)),
                    (None, Some(5)),
                ]),
            ),
            (
                "r".into(),
                ints(&[(Some(1), Some(100)), (Some(3), Some(300)), (None, Some(6))]),
            ),
        ]);
        let mut view = SqlView::new(sql, tables).await.unwrap();
        assert_oracle(&view).await;
        let stats = refresh(
            &mut view,
            &[change(
                "r",
                &[(Some(1), Some(100)), (Some(2), Some(200))],
                &[-1, 1],
            )],
        )
        .await;
        assert!(stats
            .nodes
            .iter()
            .any(|node| node.operator.to_lowercase().contains("join")
                && node.strategy == Strategy::KeyedRecompute));
        refresh(
            &mut view,
            &[
                change("l", &[(Some(2), Some(20)), (Some(3), Some(30))], &[-1, 1]),
                change("r", &[(Some(3), Some(300)), (Some(1), Some(101))], &[-1, 1]),
            ],
        )
        .await;
    }
}

fn sequence(batch: &RecordBatch) -> Vec<Vec<ScalarValue>> {
    (0..batch.num_rows())
        .map(|row| scalar_row(batch, row, batch.num_columns()))
        .collect()
}

#[tokio::test]
async fn topk_preserves_order_through_alias_and_projection_boundaries() {
    let sql = "SELECT renamed, v + 100 AS adjusted FROM (SELECT k AS renamed, v FROM t ORDER BY v DESC, k ASC LIMIT 4) q LIMIT 2 OFFSET 1";
    let mut view = SqlView::new(
        sql,
        BTreeMap::from([(
            "t".into(),
            ints(&[
                (Some(3), Some(40)),
                (Some(1), Some(70)),
                (Some(2), Some(20)),
                (Some(4), Some(60)),
                (Some(5), Some(10)),
            ]),
        )]),
    )
    .await
    .unwrap();
    assert_eq!(
        sequence(&view.contents().unwrap()),
        sequence(&oracle(sql, view.tables().unwrap()).await)
    );
    for changes in [
        vec![change(
            "t",
            &[(Some(1), Some(70)), (Some(6), Some(100))],
            &[-1, 1],
        )],
        vec![change(
            "t",
            &[(Some(4), Some(60)), (Some(7), Some(65))],
            &[-1, 1],
        )],
        vec![change(
            "t",
            &[(Some(6), Some(100)), (Some(8), Some(30))],
            &[-1, 1],
        )],
    ] {
        let stats = refresh(&mut view, &changes).await;
        assert!(stats
            .nodes
            .iter()
            .any(|node| node.strategy == Strategy::OperatorRecompute));
        assert_eq!(
            sequence(&view.contents().unwrap()),
            sequence(&oracle(sql, view.tables().unwrap()).await)
        );
    }
}

#[tokio::test]
async fn reordered_projected_sequence_dirties_limit_even_when_its_bag_is_unchanged() {
    // The filter above the inner LIMIT prevents merging both LIMITs. Dropping
    // v makes the intermediate bag unchanged when only its ordering key changes.
    let sql = "SELECT renamed FROM (SELECT k AS renamed FROM t ORDER BY v DESC, k ASC LIMIT 4) q WHERE renamed > 0 LIMIT 2";
    let mut view = SqlView::new(
        sql,
        BTreeMap::from([(
            "t".into(),
            ints(&[
                (Some(1), Some(40)),
                (Some(2), Some(30)),
                (Some(3), Some(20)),
                (Some(4), Some(10)),
            ]),
        )]),
    )
    .await
    .unwrap();
    assert_eq!(
        sequence(&view.contents().unwrap()),
        vec![
            vec![ScalarValue::Int64(Some(1))],
            vec![ScalarValue::Int64(Some(2))]
        ]
    );
    refresh(
        &mut view,
        &[change(
            "t",
            &[(Some(4), Some(10)), (Some(4), Some(100))],
            &[-1, 1],
        )],
    )
    .await;
    assert_eq!(
        sequence(&view.contents().unwrap()),
        vec![
            vec![ScalarValue::Int64(Some(4))],
            vec![ScalarValue::Int64(Some(1))]
        ]
    );
}

#[tokio::test]
async fn window_ranks_and_running_aggregate_recompute_on_changed_membership() {
    let sql = "SELECT k, v, ROW_NUMBER() OVER (ORDER BY v, k) AS rn, SUM(v) OVER (ORDER BY v, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM t";
    let mut view = SqlView::new(
        sql,
        BTreeMap::from([(
            "t".into(),
            ints(&[
                (Some(1), Some(10)),
                (Some(2), Some(30)),
                (Some(3), Some(20)),
            ]),
        )]),
    )
    .await
    .unwrap();
    assert_oracle(&view).await;
    let stats = refresh(
        &mut view,
        &[change(
            "t",
            &[(Some(2), Some(30)), (Some(4), Some(15))],
            &[-1, 1],
        )],
    )
    .await;
    assert!(stats
        .nodes
        .iter()
        .any(|node| node.operator.to_lowercase().contains("window")
            && node.strategy == Strategy::OperatorRecompute));
}

#[tokio::test]
async fn nonimmutable_functions_are_rejected_even_inside_expression_subqueries() {
    let sqls = [
        "SELECT random() FROM t",
        "SELECT CURRENT_DATE FROM t",
        "SELECT (SELECT CURRENT_TIMESTAMP) AS unstable FROM t",
        "SELECT k FROM t WHERE EXISTS (SELECT 1 FROM t inner_t WHERE random() >= 0.0)",
    ];
    for sql in sqls {
        assert!(
            SqlView::new(
                sql,
                BTreeMap::from([("t".into(), ints(&[(Some(1), Some(10))]))])
            )
            .await
            .is_err(),
            "must reject nonimmutable SQL: {sql}"
        );
    }
}

#[tokio::test]
async fn scalar_aggregate_subqueries_in_where_having_and_select_track_their_sources() {
    let sqls = [
        "SELECT a.k, a.v FROM a WHERE a.v > (SELECT AVG(b.v) FROM b)",
        "SELECT a.k, SUM(a.v) AS total FROM a GROUP BY a.k HAVING SUM(a.v) > (SELECT COALESCE(SUM(b.v), 0) FROM b)",
        "SELECT a.k, (SELECT MAX(b.v) FROM b) AS threshold FROM a",
        "SELECT a.k, (SELECT COUNT(*) FROM a counted) AS source_count FROM a",
    ];
    for sql in sqls {
        let mut view = SqlView::new(
            sql,
            BTreeMap::from([
                (
                    "a".into(),
                    ints(&[
                        (Some(1), Some(15)),
                        (Some(1), Some(15)),
                        (Some(2), Some(60)),
                    ]),
                ),
                (
                    "b".into(),
                    ints(&[(Some(1), Some(10)), (Some(2), Some(30))]),
                ),
            ]),
        )
        .await
        .unwrap();
        assert_oracle(&view).await;
        // Both the outer relation and the scalar aggregate source change in
        // this epoch, exposing any residual read of the original providers.
        refresh(
            &mut view,
            &[
                change("a", &[(Some(2), Some(20)), (Some(3), Some(40))], &[1, 1]),
                change("b", &[(Some(2), Some(30)), (Some(2), Some(50))], &[-1, 1]),
            ],
        )
        .await;
        // MAX/AVG over no rows is one NULL scalar, not a zero-row relation.
        // COUNT and COALESCE intentionally exercise their different results.
        refresh(
            &mut view,
            &[change(
                "b",
                &[(Some(1), Some(10)), (Some(2), Some(50))],
                &[-1, -1],
            )],
        )
        .await;
        if sql.contains("threshold") {
            let result = view.contents().unwrap();
            assert_eq!(result.num_rows(), 5);
            assert_eq!(result.column(1).null_count(), 5);
        }
        refresh(
            &mut view,
            &[
                change("a", &[(Some(1), Some(15))], &[-1]),
                change("b", &[(Some(3), Some(5))], &[1]),
            ],
        )
        .await;
    }
}

#[tokio::test]
async fn invalid_changes_and_foreign_or_stale_candidates_preserve_visible_state() {
    let sql = "SELECT k, SUM(v) AS total FROM t GROUP BY k";
    let initial = BTreeMap::from([(
        "t".into(),
        ints(&[(Some(1), Some(10)), (Some(1), Some(10))]),
    )]);
    let mut view = SqlView::new(sql, initial.clone()).await.unwrap();
    let original = view.contents().unwrap();
    let bad = [
        vec![change("t", &[(Some(9), Some(9))], &[-1])],
        vec![
            change("t", &[(Some(2), Some(2))], &[1]),
            change("unknown", &[(Some(3), Some(3))], &[1]),
        ],
        vec![change("t", &[(Some(2), Some(2))], &[])],
        vec![change("t", &[(Some(1), Some(10))], &[i64::MAX])],
        vec![TableChange {
            table: "t".into(),
            rows: RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "other",
                    DataType::Int64,
                    true,
                )])),
                vec![Arc::new(Int64Array::from(vec![Some(1)]))],
            )
            .unwrap(),
            weights: vec![1],
        }],
    ];
    for changes in bad {
        assert!(view.prepare(&changes).await.is_err());
        assert_eq!(view.epoch(), 0);
        assert_eq!(bag(&view.contents().unwrap()), bag(&original));
        assert_eq!(bag(&view.tables().unwrap()["t"]), bag(&initial["t"]));
    }
    let other = SqlView::new(sql, initial).await.unwrap();
    let foreign = other.prepare(&[]).await.unwrap();
    assert!(view.acknowledge(foreign).is_err());
    let first = view
        .prepare(&[change("t", &[(Some(2), Some(20))], &[1])])
        .await
        .unwrap();
    let stale = view
        .prepare(&[change("t", &[(Some(3), Some(30))], &[1])])
        .await
        .unwrap();
    view.acknowledge(first).unwrap();
    let committed = view.contents().unwrap();
    assert!(view.acknowledge(stale).is_err());
    assert_eq!(view.epoch(), 1);
    assert_eq!(bag(&view.contents().unwrap()), bag(&committed));
    assert_oracle(&view).await;
    let stats = refresh(
        &mut view,
        &[
            change("t", &[(Some(4), Some(40))], &[-1]),
            change("t", &[(Some(4), Some(40))], &[1]),
        ],
    )
    .await;
    assert!(
        stats.nodes.is_empty(),
        "a net-zero atomic source batch must not dirty ancestors"
    );
}
