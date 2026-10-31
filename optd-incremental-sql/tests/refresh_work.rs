//! Refresh accounting distinguishes local delta evaluation from full cache work.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::prelude::SessionContext;
use optd_incremental_sql::database::SqlSession;
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{PreparedSqlRefresh, RefreshWork, SqlView, TableChange};

fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
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

fn rows(n: usize) -> Vec<(i64, i64)> {
    (0..n as i64).map(|k| (k, k * 10)).collect()
}

fn tables(n: usize) -> BTreeMap<String, RecordBatch> {
    BTreeMap::from([("t".into(), batch(&rows(n)))])
}

fn change(values: &[(i64, i64)], weights: &[i64]) -> TableChange {
    TableChange {
        table: "t".into(),
        rows: batch(values),
        weights: weights.to_vec(),
    }
}

async fn view(sql: &str, tables: BTreeMap<String, RecordBatch>) -> SqlView {
    SqlView::new_with_optimizer(
        sql,
        tables,
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap()
}

fn bag(batch: &RecordBatch) -> HashMap<Vec<ScalarValue>, i64> {
    let mut out = HashMap::new();
    for i in 0..batch.num_rows() {
        let row = batch
            .columns()
            .iter()
            .map(|column| ScalarValue::try_from_array(column.as_ref(), i).unwrap())
            .collect::<Vec<_>>();
        *out.entry(row).or_insert(0) += 1;
    }
    out
}

async fn check_oracle(
    sql: &str,
    candidate: &PreparedSqlRefresh,
    expected_tables: BTreeMap<String, RecordBatch>,
) {
    // The oracle receives independently constructed endpoints, not view.tables().
    let ctx = SessionContext::new();
    for (name, input) in expected_tables {
        ctx.register_batch(name.as_str(), input).unwrap();
    }
    let frame = ctx.sql(sql).await.unwrap();
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let expected = concat_batches(&schema, &frame.collect().await.unwrap()).unwrap();
    assert_eq!(bag(candidate.contents()), bag(&expected));
}

#[tokio::test]
async fn fixed_delta_exposes_full_source_and_cache_rebuilds_as_untouched_rows_grow() {
    let sql = "SELECT k, v FROM t";
    for n in [32, 256] {
        let maintained = view(sql, tables(n)).await;
        let candidate = maintained
            .prepare(&[change(&[(n as i64, 7)], &[1])])
            .await
            .unwrap();
        let work = &candidate.stats().work;
        assert_eq!(work.sources.map_rebuilds, 1);
        assert_eq!(work.sources.old_row_entries_copied, n);
        assert_eq!(work.sources.materializations, 0);
        assert!(work.caches.map_rebuilds > 0);
        assert_eq!(work.caches.materializations, work.caches.map_rebuilds);
        assert_eq!(
            work.caches.old_row_entries_copied,
            n * work.caches.map_rebuilds
        );
        assert_eq!(
            work.caches.materialized_rows,
            (n + 1) * work.caches.materializations
        );
        assert!(work.caches.materialized_array_bytes > 0);
        let mut expected = rows(n);
        expected.push((n as i64, 7));
        check_oracle(
            sql,
            &candidate,
            BTreeMap::from([("t".into(), batch(&expected))]),
        )
        .await;
        assert_eq!(maintained.epoch(), 0);
        assert_eq!(maintained.contents().unwrap().num_rows(), n);
    }
}

#[tokio::test]
async fn copied_distinct_entries_and_expanded_materialized_rows_use_different_units() {
    let sql = "SELECT k, v FROM t";
    let input = vec![(1, 10); 8];
    let maintained = view(sql, BTreeMap::from([("t".into(), batch(&input))])).await;
    let candidate = maintained
        .prepare(&[change(&[(1, 10)], &[1])])
        .await
        .unwrap();
    let work = &candidate.stats().work;
    assert_eq!(work.sources.old_row_entries_copied, 1);
    assert_eq!(work.caches.old_row_entries_copied, work.caches.map_rebuilds);
    assert_eq!(
        work.caches.materialized_rows,
        9 * work.caches.materializations
    );
    check_oracle(
        sql,
        &candidate,
        BTreeMap::from([("t".into(), batch(&[(1, 10); 9]))]),
    )
    .await;
}

#[tokio::test]
async fn aggregate_directory_copies_are_separate_from_one_touched_group() {
    for n in [16, 128] {
        for (sql, accumulator) in [
            ("SELECT k, SUM(v) AS total FROM t GROUP BY k", true),
            ("SELECT k, MIN(v) AS lo FROM t GROUP BY k", false),
        ] {
            let maintained = view(sql, tables(n)).await;
            let candidate = maintained
                .prepare(&[change(&[(0, 3)], &[1])])
                .await
                .unwrap();
            let stats = candidate.stats();
            if accumulator {
                assert_eq!(stats.work.state_copies.accumulator_entries, 2 * n);
                assert_eq!(stats.work.state_copies.group_entries, 0);
                let work = stats
                    .nodes
                    .iter()
                    .find_map(|n| n.accumulator_work.as_ref())
                    .unwrap();
                assert_eq!(work.touched_groups, 1);
                assert_eq!(work.historical_input_rows, 0);
            } else {
                assert_eq!(stats.work.state_copies.group_entries, 2 * n);
                assert_eq!(stats.work.state_copies.accumulator_entries, 0);
                let work = stats
                    .nodes
                    .iter()
                    .find_map(|n| n.group_work.as_ref())
                    .unwrap();
                assert_eq!(work.touched_groups, 1);
                assert_eq!(work.aggregate_input_rows, 2);
            }
            let mut expected = rows(n);
            expected.push((0, 3));
            check_oracle(
                sql,
                &candidate,
                BTreeMap::from([("t".into(), batch(&expected))]),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn keyed_input_and_cached_window_output_directory_copies_are_visible() {
    let n = 24;
    let mut input = tables(n);
    input.insert("u".into(), batch(&rows(n)));
    let sql = "SELECT t.k, t.v, u.v AS uv FROM t LEFT JOIN u ON t.k=u.k";
    let maintained = view(sql, input.clone()).await;
    let candidate = maintained
        .prepare(&[change(&[(0, 3)], &[1])])
        .await
        .unwrap();
    assert_eq!(
        candidate.stats().work.state_copies.keyed_input_entries,
        4 * n
    );
    assert_eq!(candidate.stats().work.state_copies.window_output_entries, 0);
    let mut expected = rows(n);
    expected.push((0, 3));
    input.insert("t".into(), batch(&expected));
    check_oracle(sql, &candidate, input).await;

    let sql = "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM t";
    let maintained = view(sql, tables(n)).await;
    let candidate = maintained
        .prepare(&[change(&[(0, 3)], &[1])])
        .await
        .unwrap();
    assert_eq!(
        candidate.stats().work.state_copies.keyed_input_entries,
        2 * n
    );
    assert_eq!(
        candidate.stats().work.state_copies.window_output_entries,
        2 * n
    );
    check_oracle(
        sql,
        &candidate,
        BTreeMap::from([("t".into(), batch(&expected))]),
    )
    .await;
}

#[tokio::test]
async fn empty_and_consolidated_zero_changes_skip_relation_rebuilds() {
    let mut maintained = view("SELECT k, SUM(v) FROM t GROUP BY k", tables(12)).await;
    for changes in [vec![], vec![change(&[(0, 3), (0, 3)], &[1, -1])]] {
        let candidate = maintained.prepare(&changes).await.unwrap();
        let work = &candidate.stats().work;
        assert_eq!(work.sources, Default::default());
        assert_eq!(work.caches, Default::default());
        // The current implementation still eagerly copies the installed directory.
        assert_eq!(work.state_copies.accumulator_entries, 12);
        assert!(candidate.stats().nodes.is_empty());
        maintained.acknowledge(candidate).unwrap();
    }
    assert_eq!(maintained.epoch(), 2);
}

#[tokio::test]
async fn concurrent_and_failed_candidates_do_not_share_accounting_or_mutate_state() {
    let mut maintained = view("SELECT k, SUM(v) FROM t GROUP BY k", tables(12)).await;
    let a = [change(&[(0, 3)], &[1])];
    let b = [change(&[(1, 4)], &[1])];
    let (a, b) = tokio::join!(maintained.prepare(&a), maintained.prepare(&b));
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.stats().work, b.stats().work);
    let work = a.stats().work.clone();
    assert!(maintained
        .prepare(&[change(&[(99, 99)], &[-1])])
        .await
        .is_err());
    assert_eq!(maintained.epoch(), 0);
    assert_eq!(maintained.contents().unwrap().num_rows(), 12);
    assert_eq!(a.stats().work, work);
    maintained.acknowledge(a).unwrap();
    assert!(maintained.acknowledge(b).is_err());
    assert_eq!(maintained.epoch(), 1);
}

#[tokio::test]
async fn sql_commit_and_identified_retry_retain_additive_refresh_work_json() {
    let root = tempfile::tempdir().unwrap();
    let mut session = SqlSession::open(root.path()).await.unwrap();
    session.execute_script("CREATE TABLE t(k BIGINT NOT NULL, v BIGINT NOT NULL); INSERT INTO t VALUES (1,10),(2,20); CREATE MATERIALIZED VIEW m AS SELECT k,SUM(v) AS total FROM t GROUP BY k;").await.unwrap();
    let statement = "INSERT INTO t VALUES (1,3)";
    let result = session.execute_request("work-1", statement).await.unwrap();
    let report = result.commit.unwrap();
    let json = &report.maintenance["m"]["work"];
    assert_eq!(json["sources"]["map_rebuilds"], 1);
    assert_eq!(json["state_copies"]["accumulator_entries"], 4);
    let _public_type: RefreshWork = Default::default();
    drop(session);
    let mut reopened = SqlSession::open(root.path()).await.unwrap();
    let retry = reopened.execute_request("work-1", statement).await.unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.commit.unwrap().maintenance["m"]["work"], *json);
    reopened.verify_output_history().unwrap();
}

#[tokio::test]
async fn split_changes_rebuild_each_source_once_and_cancel_without_rebuilding() {
    for n in [32, 256] {
        let sql = "SELECT k, SUM(v) AS total FROM t GROUP BY k";
        let mut maintained = view(sql, tables(n)).await;
        let candidate = maintained
            .prepare(&[
                change(&[(0, 3)], &[1]),
                change(&[(1, 10)], &[-1]),
                change(&[(1, 14)], &[1]),
            ])
            .await
            .unwrap();
        assert_eq!(candidate.stats().work.sources.map_rebuilds, 1);
        assert_eq!(candidate.stats().work.sources.old_row_entries_copied, n);
        let mut expected = rows(n);
        expected[1] = (1, 14);
        expected.push((0, 3));
        check_oracle(
            sql,
            &candidate,
            BTreeMap::from([("t".into(), batch(&expected))]),
        )
        .await;
        maintained.acknowledge(candidate).unwrap();

        // Negative intermediate multiplicities are valid inside an atomic batch.
        // Cancellation across separate TableChanges must not rebuild a source.
        let candidate = maintained
            .prepare(&[
                change(&[(999, 7)], &[-1]),
                change(&[(0, 3)], &[2]),
                change(&[(999, 7)], &[1]),
                change(&[(0, 3)], &[-2]),
            ])
            .await
            .unwrap();
        assert_eq!(candidate.stats().work.sources, Default::default());
        assert_eq!(candidate.stats().work.caches, Default::default());
        assert!(candidate.stats().nodes.is_empty());
        assert_eq!(candidate.delta().unwrap().num_rows(), 0);
        check_oracle(
            sql,
            &candidate,
            BTreeMap::from([("t".into(), batch(&expected))]),
        )
        .await;
        maintained.acknowledge(candidate).unwrap();
        assert_eq!(maintained.epoch(), 2);
    }
}

#[tokio::test]
async fn interleaved_two_source_changes_share_one_rebuild_per_source() {
    let n = 24;
    let sql = "SELECT t.k, t.v, u.v AS uv FROM t LEFT JOIN u ON t.k=u.k";
    let mut initial = tables(n);
    initial.insert("u".into(), batch(&rows(n)));
    let mut maintained = view(sql, initial).await;
    let for_u = |values: &[(i64, i64)], weights: &[i64]| TableChange {
        table: "u".into(),
        ..change(values, weights)
    };
    let candidate = maintained
        .prepare(&[
            change(&[(0, 0)], &[-1]),
            for_u(&[(1, 10)], &[-1]),
            change(&[(0, 5)], &[1]),
            for_u(&[(1, 17)], &[1]),
        ])
        .await
        .unwrap();
    assert_eq!(candidate.stats().work.sources.map_rebuilds, 2);
    assert_eq!(candidate.stats().work.sources.old_row_entries_copied, 2 * n);
    let mut t = rows(n);
    t[0] = (0, 5);
    let mut u = rows(n);
    u[1] = (1, 17);
    let expected = BTreeMap::from([("t".into(), batch(&t)), ("u".into(), batch(&u))]);
    check_oracle(sql, &candidate, expected.clone()).await;
    maintained.acknowledge(candidate).unwrap();
    for (name, batch) in maintained.tables().unwrap() {
        assert_eq!(bag(&batch), bag(&expected[&name]));
    }
}

#[tokio::test]
async fn cancellation_does_not_hide_invalid_schema_names_weights_or_final_deletes() {
    let maintained = view("SELECT k,v FROM t", tables(3)).await;
    let wrong_schema = Arc::new(Schema::new(vec![
        Field::new("other", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let wrong = |values: &[(i64, i64)], weights: &[i64]| {
        let input = batch(values);
        TableChange {
            table: "t".into(),
            rows: RecordBatch::try_new(wrong_schema.clone(), input.columns().to_vec()).unwrap(),
            weights: weights.to_vec(),
        }
    };
    let invalid_batches = [
        vec![TableChange {
            table: "missing".into(),
            ..change(&[], &[])
        }],
        vec![wrong(&[], &[])],
        vec![wrong(&[(1, 10), (1, 10)], &[1, -1])],
        vec![change(&[(1, 10)], &[])],
        vec![change(&[(999, 7)], &[-1])],
        vec![change(&[(1, 10)], &[1]), wrong(&[], &[])],
    ];
    for changes in invalid_batches {
        assert!(maintained.prepare(&changes).await.is_err());
        assert_eq!(maintained.epoch(), 0);
        assert_eq!(
            bag(&maintained.tables().unwrap()["t"]),
            bag(&batch(&rows(3)))
        );
    }
}

#[tokio::test]
async fn later_cancellation_cannot_hide_source_or_delta_arithmetic_overflow() {
    let maintained = view("SELECT k,v FROM t", tables(3)).await;
    // The first two cases overflow intermediate source weights. The third
    // overflows the staged delta while intermediate source arithmetic still fits.
    for weights in [
        vec![i64::MAX, -i64::MAX],
        vec![i64::MAX - 1, 1, -i64::MAX],
        vec![-i64::MAX, -2, i64::MAX, 2],
    ] {
        let changes = weights
            .into_iter()
            .map(|w| change(&[(1, 10)], &[w]))
            .collect::<Vec<_>>();
        assert!(matches!(
            maintained.prepare(&changes).await,
            Err(optd_incremental_sql::Error::Overflow)
        ));
        assert_eq!(maintained.epoch(), 0);
        assert_eq!(bag(&maintained.contents().unwrap()), bag(&batch(&rows(3))));
    }
}

#[tokio::test]
async fn signed_prefixes_can_be_negative_when_the_final_relation_is_valid() {
    let sql = "SELECT k,v FROM t";
    let mut maintained = view(sql, tables(3)).await;
    let candidate = maintained
        .prepare(&[
            change(&[(999, 7)], &[-2]),
            change(&[(999, 7)], &[3]),
            change(&[(1, 10)], &[-2]),
            change(&[(1, 10)], &[1]),
        ])
        .await
        .unwrap();
    let expected = vec![(0, 0), (2, 20), (999, 7)];
    check_oracle(
        sql,
        &candidate,
        BTreeMap::from([("t".into(), batch(&expected))]),
    )
    .await;
    assert_eq!(candidate.stats().work.sources.map_rebuilds, 1);
    maintained.acknowledge(candidate).unwrap();
    assert_eq!(
        bag(&maintained.tables().unwrap()["t"]),
        bag(&batch(&expected))
    );
}

#[tokio::test]
async fn representable_extreme_prefixes_can_cancel_without_materialization() {
    let mut maintained = view("SELECT k,v FROM t", tables(3)).await;
    let candidate = maintained
        .prepare(&[
            change(&[(999, 7)], &[i64::MIN]),
            change(&[(999, 7)], &[i64::MAX]),
            change(&[(999, 7)], &[1]),
        ])
        .await
        .unwrap();
    assert_eq!(candidate.stats().work.sources, Default::default());
    assert_eq!(candidate.stats().work.caches, Default::default());
    assert!(candidate.stats().nodes.is_empty());
    assert_eq!(bag(candidate.contents()), bag(&batch(&rows(3))));
    maintained.acknowledge(candidate).unwrap();
    assert_eq!(maintained.epoch(), 1);
}
