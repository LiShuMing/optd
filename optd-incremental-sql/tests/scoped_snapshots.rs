//! Indexed snapshot rules checked against independent full SQL and signed deltas.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_incremental_sql::planner::{
    DeltaExpr, IncrementalOptimizer, IncrementalRule, KeyScope, Maintenance, RuleContext,
    RuleRewrite, SnapshotVersion,
};
use optd_incremental_sql::{
    DurableSqlView, Error, RefreshStats, Result, SqlView, Strategy, TableChange,
};

type Row = (Option<i64>, Option<i64>);
type Sources = BTreeMap<String, Vec<Row>>;
type Rows = HashMap<Vec<ScalarValue>, i64>;

fn batch(rows: &[Row]) -> RecordBatch {
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

fn tables(sources: &Sources) -> BTreeMap<String, RecordBatch> {
    sources
        .iter()
        .map(|(name, rows)| (name.clone(), batch(rows)))
        .collect()
}

fn change(table: &str, rows: &[Row], weights: &[i64]) -> TableChange {
    TableChange {
        table: table.into(),
        rows: batch(rows),
        weights: weights.to_vec(),
    }
}

fn row(batch: &RecordBatch, index: usize, columns: usize) -> Vec<ScalarValue> {
    batch.columns()[..columns]
        .iter()
        .map(|array| ScalarValue::try_from_array(array.as_ref(), index).unwrap())
        .collect()
}

fn add(rows: &mut Rows, row: Vec<ScalarValue>, weight: i64) {
    let next = rows
        .get(&row)
        .copied()
        .unwrap_or_default()
        .checked_add(weight)
        .unwrap();
    if next == 0 {
        rows.remove(&row);
    } else {
        rows.insert(row, next);
    }
}

fn bag(batch: &RecordBatch) -> Rows {
    let mut rows = Rows::new();
    for index in 0..batch.num_rows() {
        add(&mut rows, row(batch, index, batch.num_columns()), 1);
    }
    rows
}

fn signed(batch: &RecordBatch) -> Rows {
    let mut rows = Rows::new();
    let columns = batch.num_columns() - 1;
    assert_eq!(batch.schema().field(columns).name(), "__optd_weight");
    for index in 0..batch.num_rows() {
        let ScalarValue::Int64(Some(weight)) =
            ScalarValue::try_from_array(batch.column(columns).as_ref(), index).unwrap()
        else {
            panic!("signed output requires nonnull Int64 weights");
        };
        assert_ne!(weight, 0);
        add(&mut rows, row(batch, index, columns), weight);
    }
    rows
}

fn apply_delta(mut before: Rows, delta: &RecordBatch) -> Rows {
    for (row, weight) in signed(delta) {
        add(&mut before, row, weight);
    }
    assert!(before.values().all(|weight| *weight > 0));
    before
}

fn apply_sources(sources: &mut Sources, changes: &[TableChange]) {
    for change in changes {
        let source = sources.get_mut(&change.table).unwrap();
        for (index, weight) in change.weights.iter().copied().enumerate() {
            let values = row(&change.rows, index, 2);
            let [ScalarValue::Int64(k), ScalarValue::Int64(v)] = values.as_slice() else {
                panic!("fixture source has two Int64 columns");
            };
            let value = (*k, *v);
            for _ in 0..weight.unsigned_abs() {
                if weight > 0 {
                    source.push(value);
                } else {
                    let found = source.iter().position(|r| *r == value).unwrap();
                    source.remove(found);
                }
            }
        }
    }
}

async fn oracle(sql: &str, sources: &Sources) -> RecordBatch {
    // This physical plan does not use the incremental planner, states or caches.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for (name, input) in tables(sources) {
        ctx.register_batch(name.as_str(), input).unwrap();
    }
    let physical = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    concat_batches(
        &physical.schema(),
        &collect(physical, ctx.task_ctx()).await.unwrap(),
    )
    .unwrap()
}

fn assert_sources(view: &SqlView, expected: &Sources) {
    let actual = view.tables().unwrap();
    assert_eq!(actual.len(), expected.len());
    for (name, rows) in expected {
        assert_eq!(bag(&actual[name]), bag(&batch(rows)), "source {name}");
    }
}

async fn refresh_checked(
    view: &mut SqlView,
    changes: &[TableChange],
    expected_sources: &Sources,
) -> RefreshStats {
    let epoch = view.epoch();
    let before = view.contents().unwrap();
    let before_sources = view.tables().unwrap();
    let candidate = view.prepare(changes).await.unwrap();
    let expected = oracle(view.sql(), expected_sources).await;
    assert_eq!(candidate.contents().schema(), before.schema());
    assert_eq!(candidate.contents().schema(), expected.schema());
    assert_eq!(bag(candidate.contents()), bag(&expected));
    assert_eq!(
        apply_delta(bag(&before), &candidate.delta().unwrap()),
        bag(&expected)
    );
    assert_eq!(view.epoch(), epoch);
    assert_eq!(view.contents().unwrap(), before);
    assert_eq!(view.tables().unwrap(), before_sources);
    let stats = candidate.stats().clone();
    view.acknowledge(candidate).unwrap();
    assert_eq!(view.epoch(), epoch + 1);
    assert_sources(view, expected_sources);
    assert_eq!(bag(&view.contents().unwrap()), bag(&expected));
    stats
}

fn strict() -> IncrementalOptimizer {
    IncrementalOptimizer::default().require_incremental()
}

fn sources() -> Sources {
    let mut a = (100..300)
        .map(|k| (Some(k), Some(k * 10)))
        .collect::<Vec<_>>();
    a.extend([
        (Some(1), Some(10)),
        (Some(1), Some(10)),
        (Some(2), Some(20)),
        (None, Some(7)),
    ]);
    let mut b = (100..300)
        .map(|k| (Some(k), Some(k * 100)))
        .collect::<Vec<_>>();
    b.extend([(Some(1), Some(100)), (Some(3), Some(30)), (None, Some(8))]);
    BTreeMap::from([("a".into(), a), ("b".into(), b)])
}

#[tokio::test]
async fn outer_semi_anti_two_sided_multiround_snapshots_are_indexed() {
    for kind in [
        "LEFT",
        "RIGHT",
        "FULL",
        "LEFT SEMI",
        "RIGHT SEMI",
        "LEFT ANTI",
        "RIGHT ANTI",
    ] {
        for condition in [
            "a.k=b.k",
            "a.k IS NOT DISTINCT FROM b.k",
            "a.k=b.k AND a.v < b.v",
        ] {
            let sql = format!("SELECT * FROM a {kind} JOIN b ON {condition}");
            let mut data = sources();
            let mut view = SqlView::new_with_optimizer(&sql, tables(&data), strict())
                .await
                .unwrap();
            assert!(
                view.strategies()
                    .iter()
                    .any(|n| n.strategy == Strategy::KeyedRecompute),
                "{sql}"
            );
            for changes in [
                vec![
                    change("a", &[(Some(1), Some(10)), (Some(2), Some(21))], &[-1, 1]),
                    change("b", &[(Some(1), Some(100)), (Some(2), Some(200))], &[-1, 1]),
                ],
                vec![
                    change(
                        "a",
                        &[(Some(2), Some(20)), (Some(3), Some(31)), (None, Some(9))],
                        &[-1, 1, 1],
                    ),
                    change("b", &[(Some(3), Some(30)), (None, Some(8))], &[-1, -1]),
                ],
                vec![change(
                    "b",
                    &[(Some(1), Some(101)), (None, Some(12))],
                    &[2, 1],
                )],
                vec![change("b", &[(Some(1), Some(101))], &[-2])],
            ] {
                apply_sources(&mut data, &changes);
                let stats = refresh_checked(&mut view, &changes, &data).await;
                let work = stats
                    .nodes
                    .iter()
                    .find_map(|n| n.keyed_work.as_ref())
                    .unwrap();
                assert_eq!(work.scope, KeyScope::JoinKeys);
                assert_eq!(work.full_input_scans, 0);
                assert!(work.total_keys_before.iter().all(|n| *n >= 200));
                assert!(work.touched_keys <= 4);
                assert!(work.snapshot_input_rows_before.iter().sum::<usize>() <= 12);
                assert!(work.snapshot_input_rows_after.iter().sum::<usize>() <= 12);
                assert_eq!(work.snapshot_evaluations_before, 1);
                assert_eq!(work.snapshot_evaluations_after, 1);
            }
            let no_op = view
                .prepare(&[change("a", &[(Some(1), Some(10))], &[0])])
                .await
                .unwrap();
            assert!(no_op.stats().nodes.is_empty());
        }
    }
}

#[tokio::test]
async fn composite_keys_and_self_join_include_old_and_new_buckets() {
    for sql in [
        "SELECT * FROM a FULL JOIN b ON a.k=b.k AND a.v=b.v",
        "SELECT * FROM a FULL JOIN a AS other ON a.k=other.k AND a.v < other.v",
    ] {
        let mut data = sources();
        let mut view = SqlView::new_with_optimizer(sql, tables(&data), strict())
            .await
            .unwrap();
        for changes in [
            vec![change(
                "a",
                &[(Some(1), Some(10)), (Some(2), Some(30))],
                &[-1, 1],
            )],
            vec![
                change("a", &[(None, Some(7)), (None, Some(17))], &[-1, 1]),
                change("b", &[(Some(2), Some(30))], &[1]),
            ],
        ] {
            apply_sources(&mut data, &changes);
            let stats = refresh_checked(&mut view, &changes, &data).await;
            assert!(stats
                .nodes
                .iter()
                .filter_map(|n| n.keyed_work.as_ref())
                .all(|w| w.full_input_scans == 0 && w.touched_keys <= 4));
        }
    }
}

#[tokio::test]
async fn windows_recompute_complete_affected_partitions_and_retract_cached_output() {
    for expressions in [
        "ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn, RANK() OVER (PARTITION BY k ORDER BY v) AS r",
        "SUM(v) OVER (PARTITION BY k ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s",
        "SUM(v) OVER (PARTITION BY k ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s",
        "LAG(v) OVER (PARTITION BY k ORDER BY v) AS p, LEAD(v) OVER (PARTITION BY k ORDER BY v) AS n",
    ] {
        let sql = format!("SELECT k,v,{expressions} FROM a");
        let mut data = BTreeMap::from([("a".into(), (100..300).map(|k| (Some(k),Some(k))).chain([
            (Some(1),Some(10)), (Some(1),Some(20)), (Some(2),Some(30)), (None,Some(4)), (None,Some(6))]).collect())]);
        let mut view = SqlView::new_with_optimizer(&sql,tables(&data),strict()).await.unwrap();
        for changes in [
            vec![change("a", &[(Some(1),Some(10)),(Some(2),Some(10))], &[-1,1])],
            vec![change("a", &[(None,Some(4)),(None,Some(8))], &[-1,1])],
            vec![change("a", &[(Some(1),Some(20))], &[-1])],
        ] {
            apply_sources(&mut data,&changes);
            let stats = refresh_checked(&mut view,&changes,&data).await;
            let work = stats.nodes.iter().find_map(|n| n.keyed_work.as_ref()).unwrap();
            assert_eq!(work.scope, KeyScope::WindowPartitions);
            assert_eq!(work.full_input_scans,0);
            assert!(work.touched_keys <= 2);
            assert_eq!(work.snapshot_input_rows_before,vec![0]);
            assert_eq!(work.snapshot_evaluations_before,0);
            assert_eq!(work.snapshot_evaluations_after,1);
            assert!(work.cached_before_output_rows <= 3);
            assert!(work.snapshot_input_rows_after[0] <= 3);
        }
    }
}

#[tokio::test]
async fn peer_sensitive_before_is_exact_committed_output() {
    let sql = "SELECT k,v,ROW_NUMBER() OVER (PARTITION BY k ORDER BY k) AS rn, LAG(v) OVER (PARTITION BY k ORDER BY k) AS p FROM a";
    let data = BTreeMap::from([(
        "a".into(),
        vec![
            (Some(1), Some(90)),
            (Some(1), Some(10)),
            (Some(1), Some(50)),
            (Some(2), Some(99)),
        ],
    )]);
    let mut view = SqlView::new_with_optimizer(sql, tables(&data), strict())
        .await
        .unwrap();
    for changes in [
        vec![change("a", &[(Some(1), Some(90))], &[-1])],
        vec![change("a", &[(Some(1), Some(70))], &[1])],
    ] {
        let before = bag(&view.contents().unwrap());
        let candidate = view.prepare(&changes).await.unwrap();
        assert_eq!(
            apply_delta(before, &candidate.delta().unwrap()),
            bag(candidate.contents())
        );
        let work = candidate
            .stats()
            .nodes
            .iter()
            .find_map(|n| n.keyed_work.as_ref())
            .unwrap();
        assert_eq!(work.snapshot_evaluations_before, 0);
        assert_eq!(work.snapshot_evaluations_after, 1);
        view.acknowledge(candidate).unwrap();
    }
}

#[tokio::test]
async fn distinct_is_indexed_membership_including_null_and_final_delete() {
    let sql = "SELECT DISTINCT k,v FROM a";
    let mut data = sources();
    let mut view = SqlView::new_with_optimizer(sql, tables(&data), strict())
        .await
        .unwrap();
    assert!(view
        .strategies()
        .iter()
        .any(|n| n.strategy == Strategy::AccumulatorDelta));
    assert!(!view
        .strategies()
        .iter()
        .any(|n| n.operator.starts_with("Distinct")));
    for changes in [
        vec![change(
            "a",
            &[(Some(1), Some(10)), (None, Some(7))],
            &[-1, 1],
        )],
        vec![change(
            "a",
            &[(Some(1), Some(10)), (None, Some(7))],
            &[-1, -2],
        )],
    ] {
        apply_sources(&mut data, &changes);
        let stats = refresh_checked(&mut view, &changes, &data).await;
        assert!(stats
            .nodes
            .iter()
            .filter_map(|n| n.accumulator_work.as_ref())
            .all(|w| w.touched_groups <= 2 && w.historical_input_rows == 0));
    }
}

#[derive(Clone)]
struct ScopedRule {
    after_only: bool,
    invalid: bool,
}
impl IncrementalRule for ScopedRule {
    fn name(&self) -> &str {
        "test.scoped"
    }
    fn priority(&self) -> i32 {
        1000
    }
    fn matches(&self, ctx: &RuleContext<'_>) -> Result<bool> {
        Ok(matches!(ctx.logical_plan(), LogicalPlan::Join(_)))
    }
    fn rewrite(&self, _: &RuleContext<'_>) -> Result<RuleRewrite> {
        let after = DeltaExpr::ScopedSnapshot {
            scope: KeyScope::JoinKeys,
            version: SnapshotVersion::After,
        };
        let before = DeltaExpr::ScopedSnapshot {
            scope: if self.invalid {
                KeyScope::WindowPartitions
            } else {
                KeyScope::JoinKeys
            },
            version: SnapshotVersion::Before,
        };
        Ok(RuleRewrite {
            maintenance: Maintenance::Delta(if self.after_only {
                after
            } else {
                DeltaExpr::Sum(vec![after, DeltaExpr::Negate(Box::new(before))])
            }),
            reason: "test custom execution".into(),
        })
    }
}
fn custom(after_only: bool, invalid: bool) -> IncrementalOptimizer {
    strict()
        .with_rule(Arc::new(ScopedRule {
            after_only,
            invalid,
        }))
        .unwrap()
}

#[tokio::test]
async fn selected_custom_snapshot_controls_execution_and_invalid_scope_fails() {
    let sql = "SELECT * FROM a FULL JOIN b ON a.k=b.k";
    let data = sources();
    let view = SqlView::new_with_optimizer(sql, tables(&data), custom(true, false))
        .await
        .unwrap();
    let before = bag(&view.contents().unwrap());
    let candidate = view
        .prepare(&[change("b", &[(Some(1), Some(101))], &[1])])
        .await
        .unwrap();
    let work = candidate
        .stats()
        .nodes
        .iter()
        .find_map(|n| n.keyed_work.as_ref())
        .unwrap();
    assert_eq!(work.snapshot_evaluations_before, 0);
    assert_eq!(work.snapshot_input_rows_before, vec![0, 0]);
    assert_eq!(work.snapshot_input_rows_after, vec![2, 2]);
    assert_eq!(work.snapshot_evaluations_after, 1);
    assert_eq!(
        apply_delta(before, &candidate.delta().unwrap()),
        bag(candidate.contents())
    );
    let mut correct_sources = data.clone();
    apply_sources(
        &mut correct_sources,
        &[change("b", &[(Some(1), Some(101))], &[1])],
    );
    assert_ne!(
        bag(candidate.contents()),
        bag(&oracle(sql, &correct_sources).await),
        "after-only must execute its actual custom algebra"
    );
    let err = SqlView::new_with_optimizer(sql, tables(&data), custom(false, true))
        .await
        .err()
        .unwrap();
    assert!(matches!(
        err,
        Error::Rewrite {
            phase: "verify",
            ..
        }
    ));
}

#[tokio::test]
async fn strict_rejects_remaining_complete_operator_recompute() {
    for sql in [
        "SELECT MIN(CAST(v AS DOUBLE)) FROM a",
        "SELECT * FROM a ORDER BY k",
        "SELECT ROW_NUMBER() OVER (ORDER BY v) FROM a",
        "SELECT * FROM a FULL JOIN b ON a.v < b.v",
        "SELECT * FROM a FULL JOIN b ON CAST(a.k AS DOUBLE)=CAST(b.k AS DOUBLE)",
        "SELECT ROW_NUMBER() OVER (PARTITION BY CAST(k AS DOUBLE) ORDER BY v) FROM a",
    ] {
        let err = SqlView::new_with_optimizer(sql, tables(&sources()), strict())
            .await
            .err()
            .unwrap();
        assert!(
            matches!(
                err,
                Error::Rewrite {
                    phase: "policy",
                    ..
                }
            ),
            "{sql}: {err}"
        );
    }
}

#[tokio::test]
async fn strict_custom_join_rebuilds_index_on_reopen_and_refreshes() {
    let sql = "SELECT * FROM a FULL JOIN b ON a.k=b.k";
    let root = tempfile::tempdir().unwrap();
    let mut data = sources();
    let mut durable =
        DurableSqlView::open_with_optimizer(root.path(), sql, tables(&data), custom(false, false))
            .await
            .unwrap();
    let changes = vec![change(
        "b",
        &[(Some(1), Some(100)), (Some(2), Some(22))],
        &[-1, 1],
    )];
    apply_sources(&mut data, &changes);
    durable.refresh(&changes).await.unwrap();
    assert_eq!(
        bag(&durable.view().contents().unwrap()),
        bag(&oracle(sql, &data).await)
    );
    drop(durable);
    let mut durable = DurableSqlView::open_with_optimizer(
        root.path(),
        sql,
        BTreeMap::new(),
        custom(false, false),
    )
    .await
    .unwrap();
    assert_eq!(durable.view().epoch(), 1);
    let changes = vec![change(
        "a",
        &[(Some(2), Some(20)), (Some(1), Some(12))],
        &[-1, 1],
    )];
    apply_sources(&mut data, &changes);
    let stats = durable.refresh(&changes).await.unwrap();
    assert!(stats.nodes.iter().any(|n| n.rule.name == "test.scoped"
        && n.keyed_work.as_ref().is_some_and(|w| w.touched_keys == 2)));
    assert_eq!(
        bag(&durable.view().contents().unwrap()),
        bag(&oracle(sql, &data).await)
    );
}

#[tokio::test]
async fn discarded_candidates_and_failed_deletions_preserve_installed_indexes() {
    for sql in [
        "SELECT * FROM a FULL JOIN b ON a.k=b.k",
        "SELECT k,v,SUM(v) OVER (PARTITION BY k) AS s FROM a",
    ] {
        let mut data = sources();
        let mut view = SqlView::new_with_optimizer(sql, tables(&data), strict())
            .await
            .unwrap();
        let before = view.contents().unwrap();
        let changes = vec![change("a", &[(Some(1), Some(10))], &[-1])];
        let discarded = view.prepare(&changes).await.unwrap();
        drop(discarded);
        assert_eq!(view.epoch(), 0);
        assert_eq!(view.contents().unwrap(), before);
        assert!(view
            .prepare(&[change("a", &[(Some(999), Some(1))], &[-1])])
            .await
            .is_err());
        apply_sources(&mut data, &changes);
        refresh_checked(&mut view, &changes, &data).await;
        let changes = vec![change(
            "a",
            &[(Some(1), Some(10)), (Some(2), Some(20))],
            &[-1, -1],
        )];
        apply_sources(&mut data, &changes);
        refresh_checked(&mut view, &changes, &data).await;
    }
}

#[tokio::test]
async fn partitioned_window_rebuilds_committed_outputs_on_durable_reopen() {
    let sql = "SELECT k,v,ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS r FROM a";
    let root = tempfile::tempdir().unwrap();
    let mut data = sources();
    let mut durable =
        DurableSqlView::open_with_optimizer(root.path(), sql, tables(&data), strict())
            .await
            .unwrap();
    let changes = vec![change(
        "a",
        &[(Some(2), Some(20)), (Some(1), Some(20))],
        &[-1, 1],
    )];
    apply_sources(&mut data, &changes);
    durable.refresh(&changes).await.unwrap();
    drop(durable);
    let mut durable =
        DurableSqlView::open_with_optimizer(root.path(), sql, BTreeMap::new(), strict())
            .await
            .unwrap();
    let changes = vec![change(
        "a",
        &[(Some(1), Some(20)), (None, Some(20))],
        &[-1, 1],
    )];
    apply_sources(&mut data, &changes);
    let stats = durable.refresh(&changes).await.unwrap();
    assert!(stats.nodes.iter().any(|n| n
        .keyed_work
        .as_ref()
        .is_some_and(|w| w.snapshot_evaluations_before == 0 && w.touched_keys == 2)));
    assert_eq!(
        bag(&durable.view().contents().unwrap()),
        bag(&oracle(sql, &data).await)
    );
}

#[tokio::test]
async fn constant_partition_optimized_to_global_is_rejected_by_strict_mode() {
    // DataFusion removes constant partition keys. The remaining global window
    // must not acquire an incremental label merely from the SQL spelling.
    let sql = "SELECT ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY 1) AS rn FROM a";
    let err = SqlView::new_with_optimizer(sql, tables(&sources()), strict())
        .await
        .err()
        .unwrap();
    assert!(matches!(
        err,
        Error::Rewrite {
            phase: "policy",
            ..
        }
    ));
}

#[tokio::test]
async fn indexed_inner_delta_reads_only_peer_buckets_and_counts_cross_term_once() {
    for condition in [
        "a.k=b.k",
        "a.k IS NOT DISTINCT FROM b.k",
        "a.k=b.k AND a.v<b.v",
    ] {
        let sql = format!("SELECT a.k,a.v,b.v AS bv FROM a INNER JOIN b ON {condition}");
        let mut data = sources();
        let mut view = SqlView::new_with_optimizer(&sql, tables(&data), strict())
            .await
            .unwrap();
        let changes = vec![
            change(
                "a",
                &[
                    (Some(1), Some(10)),
                    (Some(1), Some(12)),
                    (None, Some(7)),
                    (None, Some(9)),
                ],
                &[-1, 1, -1, 1],
            ),
            change(
                "b",
                &[
                    (Some(1), Some(100)),
                    (Some(1), Some(120)),
                    (None, Some(8)),
                    (None, Some(11)),
                ],
                &[-1, 1, -1, 1],
            ),
        ];
        apply_sources(&mut data, &changes);
        let stats = refresh_checked(&mut view, &changes, &data).await;
        let node = stats
            .nodes
            .iter()
            .find(|n| n.strategy == Strategy::IndexedDelta)
            .unwrap();
        let work = node.keyed_work.as_ref().unwrap();
        assert_eq!(work.touched_keys, 2);
        assert_eq!(work.full_input_scans, 0);
        assert_eq!(work.snapshot_evaluations_before, 0);
        assert_eq!(work.snapshot_evaluations_after, 0);
        assert!(work.snapshot_input_rows_before.iter().sum::<usize>() <= 3);
        assert!(work.snapshot_input_rows_after.iter().sum::<usize>() <= 4);
    }
}

#[tokio::test]
async fn grouped_and_global_accumulators_retract_without_historical_group_reads() {
    for sql in [
        "SELECT k,COUNT(*) AS n,SUM(v) AS s FROM a GROUP BY k",
        "SELECT COUNT(*) AS n,COUNT(v) AS nonnull,SUM(v) AS s FROM a",
        "SELECT k,SUM(CAST(v AS DECIMAL(18,2))) AS s,AVG(CAST(v AS DECIMAL(18,2))) AS av FROM a GROUP BY k",
        "SELECT SUM(CAST(v AS DECIMAL(18,2))) AS s,AVG(CAST(v AS DECIMAL(18,2))) AS av FROM a",
    ] {
        let mut data=Sources::from([("a".into(),vec![])]);
        let mut view=SqlView::new_with_optimizer(sql,tables(&data),strict()).await.unwrap();
        assert!(view.strategies().iter().any(|n|n.strategy==Strategy::AccumulatorDelta));
        for _ in 0..4 {
            let inserted=[(Some(1),Some(10)),(Some(1),Some(10)),(Some(2),None),(None,Some(-3))];
            for weight in [1,-1] {
                let changes=vec![change("a",&inserted,&[weight;4])];
                apply_sources(&mut data,&changes);
                let stats=refresh_checked(&mut view,&changes,&data).await;
                let work=stats.nodes.iter().find_map(|n|n.accumulator_work.as_ref()).unwrap();
                assert_eq!(work.delta_input_rows,4);
                assert_eq!(work.historical_input_rows,0);
                assert_eq!(work.touched_groups,if sql.contains("GROUP BY"){3}else{1});
            }
        }
    }
}

#[tokio::test]
async fn one_changed_row_in_a_large_group_uses_only_delta_accumulator_arguments() {
    let input = (0..2000).map(|v| (Some(1), Some(v))).collect();
    let mut data = Sources::from([("a".into(), input)]);
    let sql = "SELECT k,COUNT(*) AS n,SUM(v) AS s FROM a GROUP BY k";
    let mut view = SqlView::new_with_optimizer(sql, tables(&data), strict())
        .await
        .unwrap();
    let changes = vec![change(
        "a",
        &[(Some(1), Some(100)), (Some(1), Some(3000))],
        &[-1, 1],
    )];
    apply_sources(&mut data, &changes);
    let stats = refresh_checked(&mut view, &changes, &data).await;
    let work = stats
        .nodes
        .iter()
        .find_map(|n| n.accumulator_work.as_ref())
        .unwrap();
    assert_eq!(work.touched_groups, 1);
    assert_eq!(work.delta_input_rows, 2);
    assert_eq!(work.historical_input_rows, 0);
}

#[tokio::test]
async fn durable_accumulator_rebuild_and_failed_candidate_preserve_empty_global_semantics() {
    let root = tempfile::tempdir().unwrap();
    let sql = "SELECT COUNT(*) AS n,SUM(v) AS s FROM a";
    let mut data = Sources::from([("a".into(), vec![])]);
    let mut view = DurableSqlView::open_with_optimizer(root.path(), sql, tables(&data), strict())
        .await
        .unwrap();
    let insert = vec![change("a", &[(Some(1), Some(10)), (None, None)], &[1, 1])];
    apply_sources(&mut data, &insert);
    view.refresh(&insert).await.unwrap();
    drop(view);
    let mut recovered = DurableSqlView::open_with_optimizer(
        root.path(),
        sql,
        tables(&Sources::from([("a".into(), vec![])])),
        strict(),
    )
    .await
    .unwrap();
    assert_eq!(
        bag(&recovered.view().contents().unwrap()),
        bag(&oracle(sql, &data).await)
    );
    let before = recovered.view().contents().unwrap();
    assert!(recovered
        .view()
        .prepare(&[change("a", &[(Some(9), Some(9))], &[-1])])
        .await
        .is_err());
    assert_eq!(recovered.view().contents().unwrap(), before);
    let delete = vec![change("a", &[(Some(1), Some(10)), (None, None)], &[-1, -1])];
    apply_sources(&mut data, &delete);
    let stats = recovered.refresh(&delete).await.unwrap();
    assert_eq!(
        bag(&recovered.view().contents().unwrap()),
        bag(&oracle(sql, &data).await)
    );
    assert!(stats.nodes.iter().any(|n| n
        .accumulator_work
        .as_ref()
        .is_some_and(|w| w.historical_input_rows == 0)));
}
