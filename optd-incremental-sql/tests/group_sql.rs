//! Public SQL evidence for indexed group work, exact deltas and index rebuilding.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_incremental_sql::store::CheckpointStore;
use optd_incremental_sql::{
    DurableSqlView, GroupWork, RefreshStats, SqlView, Strategy, TableChange,
};

// Integer AVG is coerced to a floating argument by DataFusion 55 and remains
// a fallback. This dedicated witness qualifies the admitted Decimal AVG path.
const SQL: &str =
    "SELECT k, COUNT(*) AS n, SUM(v) AS total, AVG(CAST(v AS DECIMAL(18,2))) AS mean, \
    MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY k";
type IntRow = (i64, i64);
type Rows = HashMap<Vec<ScalarValue>, i64>;

fn batch(rows: &[IntRow]) -> RecordBatch {
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

fn change(rows: &[IntRow], weights: &[i64]) -> TableChange {
    TableChange {
        table: "t".into(),
        rows: batch(rows),
        weights: weights.to_vec(),
    }
}

fn scalar_row(batch: &RecordBatch, row: usize, columns: usize) -> Vec<ScalarValue> {
    batch.columns()[..columns]
        .iter()
        .map(|column| ScalarValue::try_from_array(column.as_ref(), row).unwrap())
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
    let mut result = Rows::new();
    for row in 0..batch.num_rows() {
        add(&mut result, scalar_row(batch, row, batch.num_columns()), 1);
    }
    result
}

fn apply_delta(mut rows: Rows, delta: &RecordBatch) -> Rows {
    let columns = delta.num_columns() - 1;
    assert_eq!(delta.schema().field(columns).name(), "__optd_weight");
    for row in 0..delta.num_rows() {
        let ScalarValue::Int64(Some(weight)) =
            ScalarValue::try_from_array(delta.column(columns).as_ref(), row).unwrap()
        else {
            panic!("output delta requires a nonnull Int64 weight");
        };
        assert_ne!(weight, 0);
        add(&mut rows, scalar_row(delta, row, columns), weight);
    }
    assert!(rows.values().all(|weight| *weight > 0));
    rows
}

async fn oracle(sql: &str, rows: &[IntRow]) -> RecordBatch {
    // Hand-maintained source rows and a fresh full physical plan do not consult
    // incremental tables, group buckets, output caches or strategy decisions.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_batch("t", batch(rows)).unwrap();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let schema = plan.schema();
    concat_batches(&schema, &collect(plan, ctx.task_ctx()).await.unwrap()).unwrap()
}

fn one_group_work(stats: &RefreshStats) -> &GroupWork {
    let grouped = stats
        .nodes
        .iter()
        .filter(|node| node.strategy == Strategy::ChangedGroupRecompute)
        .collect::<Vec<_>>();
    assert_eq!(grouped.len(), 1, "{stats:?}");
    grouped[0]
        .group_work
        .as_ref()
        .expect("indexed work must be reported")
}

async fn refresh_checked(
    view: &mut DurableSqlView,
    changes: &[TableChange],
    expected_rows: &[IntRow],
) -> RefreshStats {
    let epoch = view.view().epoch();
    let before = view.view().contents().unwrap();
    let source_before = view.view().tables().unwrap();
    let prepared = view.view().prepare(changes).await.unwrap();
    let expected = oracle(SQL, expected_rows).await;
    assert_eq!(prepared.contents().schema(), before.schema());
    assert_eq!(prepared.contents().schema(), expected.schema());
    assert_eq!(bag(prepared.contents()), bag(&expected));
    assert_eq!(
        apply_delta(bag(&before), &prepared.delta().unwrap()),
        bag(&expected)
    );
    assert_eq!(view.view().epoch(), epoch);
    assert_eq!(view.view().contents().unwrap(), before);
    assert_eq!(view.view().tables().unwrap(), source_before);
    drop(prepared);
    let stats = view.refresh(changes).await.unwrap();
    assert_eq!(view.view().epoch(), epoch + 1);
    assert_eq!(bag(&view.view().contents().unwrap()), bag(&expected));
    assert_eq!(
        bag(&view.view().tables().unwrap()["t"]),
        bag(&batch(expected_rows))
    );
    stats
}

#[tokio::test]
async fn one_of_100_groups_reports_actual_work_and_survives_delete_restart_reinsert() {
    let root = tempfile::tempdir().unwrap();
    let mut rows = (0..100)
        .flat_map(|key| [(key, key * 10), (key, 1)])
        .collect::<Vec<_>>();
    let mut expected_sources = vec![rows.clone()];
    let mut view = DurableSqlView::open(
        root.path(),
        SQL,
        BTreeMap::from([("t".into(), batch(&rows))]),
    )
    .await
    .unwrap();
    assert_eq!(
        bag(&view.view().contents().unwrap()),
        bag(&oracle(SQL, &rows).await)
    );

    rows.retain(|row| row != &(42, 1));
    rows.push((42, 5));
    let stats = refresh_checked(&mut view, &[change(&[(42, 1), (42, 5)], &[-1, 1])], &rows).await;
    let work = one_group_work(&stats);
    assert_eq!(work.total_groups_before, 100);
    assert_eq!(work.total_groups_after, 100);
    assert_eq!(work.touched_groups, 1);
    assert_eq!(work.changed_input_rows, 2);
    assert_eq!(work.aggregate_input_rows, 2);
    assert_eq!(work.aggregate_output_rows, 1);
    expected_sources.push(rows.clone());

    rows.retain(|row| row.0 != 42);
    let stats = refresh_checked(
        &mut view,
        &[change(&[(42, 420), (42, 5)], &[-1, -1])],
        &rows,
    )
    .await;
    let work = one_group_work(&stats);
    assert_eq!(work.total_groups_before, 100);
    assert_eq!(work.total_groups_after, 99);
    assert_eq!(work.touched_groups, 1);
    assert_eq!(work.changed_input_rows, 2);
    assert_eq!(work.aggregate_input_rows, 0);
    assert_eq!(work.aggregate_output_rows, 0);
    expected_sources.push(rows.clone());

    drop(view);
    let mut recovered = DurableSqlView::open(root.path(), SQL, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(recovered.view().epoch(), 2);
    assert_eq!(
        bag(&recovered.view().contents().unwrap()),
        bag(&oracle(SQL, &rows).await)
    );
    rows.extend([(42, 100), (42, 200)]);
    let stats = refresh_checked(
        &mut recovered,
        &[change(&[(42, 100), (42, 200)], &[1, 1])],
        &rows,
    )
    .await;
    let work = one_group_work(&stats);
    assert_eq!(work.total_groups_before, 99);
    assert_eq!(work.total_groups_after, 100);
    assert_eq!(work.touched_groups, 1);
    assert_eq!(work.changed_input_rows, 2);
    assert_eq!(work.aggregate_input_rows, 2);
    assert_eq!(work.aggregate_output_rows, 1);
    expected_sources.push(rows);
    drop(recovered);

    let store = CheckpointStore::open(root.path()).unwrap();
    let history = store.history().unwrap();
    assert_eq!(history.len(), expected_sources.len());
    let mut replayed = Rows::new();
    for (epoch, (snapshot, source)) in history.iter().zip(expected_sources).enumerate() {
        assert_eq!(snapshot.epoch, epoch as u64);
        assert_eq!(snapshot.sql, SQL);
        assert_eq!(bag(&snapshot.tables["t"]), bag(&batch(&source)));
        let expected = oracle(SQL, &source).await;
        assert_eq!(snapshot.output.schema(), expected.schema());
        assert_eq!(bag(&snapshot.output), bag(&expected));
        replayed = apply_delta(replayed, &snapshot.delta);
        assert_eq!(replayed, bag(&expected));
    }
}

#[tokio::test]
async fn float_keys_float_arguments_grouping_sets_and_global_aggregates_report_fallback() {
    for sql in [
        "SELECT SUM(v) AS total FROM t",
        "SELECT k, SUM(v) AS total FROM t GROUP BY GROUPING SETS ((k), ())",
        "SELECT CAST(k AS DOUBLE) AS fk, SUM(v) AS total FROM t GROUP BY CAST(k AS DOUBLE)",
        "SELECT k, SUM(CAST(v AS DOUBLE)) AS total FROM t GROUP BY k",
        // Coercion changes the aggregate argument type even without a SQL cast.
        "SELECT k, AVG(v) AS mean FROM t GROUP BY k",
    ] {
        let mut rows = vec![(1, 3), (1, 5), (2, 4)];
        let mut view = SqlView::new(sql, BTreeMap::from([("t".into(), batch(&rows))]))
            .await
            .unwrap();
        let aggregates = view
            .strategies()
            .into_iter()
            .filter(|node| node.operator.to_lowercase().contains("aggregate"))
            .collect::<Vec<_>>();
        assert!(!aggregates.is_empty(), "{sql}");
        assert!(
            aggregates
                .iter()
                .all(|node| node.strategy == Strategy::OperatorRecompute),
            "{sql}: {aggregates:?}"
        );
        rows.remove(0);
        rows.push((3, 7));
        let before = view.contents().unwrap();
        let candidate = view
            .prepare(&[change(&[(1, 3), (3, 7)], &[-1, 1])])
            .await
            .unwrap();
        assert!(
            candidate
                .stats()
                .nodes
                .iter()
                .all(|node| node.group_work.is_none()),
            "{sql}"
        );
        let expected = oracle(sql, &rows).await;
        assert_eq!(candidate.contents().schema(), expected.schema(), "{sql}");
        assert_eq!(bag(candidate.contents()), bag(&expected), "{sql}");
        assert_eq!(
            apply_delta(bag(&before), &candidate.delta().unwrap()),
            bag(&expected),
            "{sql}"
        );
        view.acknowledge(candidate).unwrap();
        assert_eq!(bag(&view.contents().unwrap()), bag(&expected), "{sql}");
    }
}
