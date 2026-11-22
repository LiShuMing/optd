//! Regression tests for dead intermediate relation cache elimination (RFC TODO-S1).

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use optd_incremental_sql::database::{IncompatibleStatePolicy, OpenOptions, SqlSession};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{SqlView, TableChange};
use tempfile::tempdir;

fn make_batch(ids: &[i64], groups: &[i64], amounts: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Int64, false),
        Field::new("amt", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(groups.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(amounts.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn pure_delta_and_accumulator_pipeline_retains_only_root_relation() {
    let sql = "SELECT grp, SUM(amt) AS total, COUNT(id) AS cnt \
               FROM items WHERE amt > 0 GROUP BY grp";
    for n in [1_024_i64, 8_192_i64] {
        let ids: Vec<i64> = (1..=n).collect();
        let grps: Vec<i64> = (1..=n).map(|i| i % 4).collect();
        let amts: Vec<i64> = (1..=n).map(|i| (i % 100) + 1).collect();
        let mut tables = BTreeMap::new();
        tables.insert("items".into(), make_batch(&ids, &grps, &amts));

        let mut view = SqlView::new(sql, tables).await.unwrap();
        let total_nodes = view.incremental_plan().nodes().len();
        assert!(
            total_nodes >= 4,
            "expected multi-node pipeline, got {total_nodes}"
        );
        // Only the root output relation is retained; Scan/Filter/Projection intermediates are dropped.
        assert_eq!(view.retained_relation_count(), 1);
        let retained = view.incremental_plan().retained_relations();
        assert_eq!(retained.iter().filter(|&&r| r).count(), 1);
        assert!(retained[view.incremental_plan().root()]);

        // Inserting 1 row into a 4-group aggregate must touch ONLY the 4-row root cache,
        // NOT the 1,024 / 8,192 intermediate Scan/Filter/Projection relations!
        let candidate = view
            .prepare(&[TableChange {
                table: "items".into(),
                rows: make_batch(&[n + 1], &[1], &[500]),
                weights: vec![1],
            }])
            .await
            .unwrap();
        let work = &candidate.stats().work;
        assert_eq!(
            work.caches.map_rebuilds, 1,
            "only the root relation should rebuild on a 1-row insert (n={n}): {work:?}"
        );
        assert!(
            work.caches.old_row_entries_copied <= 4,
            "dead intermediate 256-row leaves must not be copied (n={n}): {work:?}"
        );
        assert!(
            work.cache_work.leaf_validation.rows <= 6,
            "leaf validation must touch only the 4-group root leaf (n={n}): {work:?}"
        );
        view.acknowledge(candidate).unwrap();
    }
}

#[tokio::test]
async fn guarded_scalar_product_retains_required_snapshot_inputs_across_reopen() {
    let dir = tempdir().unwrap();
    let opts = OpenOptions {
        checkpoint_interval: 1,
        incompatible_state: IncompatibleStatePolicy::Fail,
    };
    let mut session = SqlSession::open_with_options(dir.path(), opts)
        .await
        .unwrap();
    session
        .execute("CREATE TABLE items (id BIGINT, grp BIGINT, amt BIGINT)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO items VALUES (1, 1, 10), (2, 1, 20), (3, 2, 100)")
        .await
        .unwrap();
    let view_sql = "SELECT id, amt FROM items WHERE amt > (SELECT AVG(amt) FROM items)";
    session
        .execute(&format!("CREATE MATERIALIZED VIEW above_avg AS {view_sql}"))
        .await
        .unwrap();

    // Update triggers scalar aggregate change, exercising SnapshotReadWhenChanged on retained nodes.
    session
        .execute("INSERT INTO items VALUES (4, 2, 200)")
        .await
        .unwrap();
    let expected_before = session
        .execute(&format!("SELECT * FROM ({view_sql}) t ORDER BY id"))
        .await
        .unwrap()
        .rows
        .unwrap();
    let actual_before = session
        .execute("SELECT * FROM above_avg ORDER BY id")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(actual_before, expected_before);

    drop(session);

    // Reopen from persisted operator state (with dead intermediates omitted) and continue mutating.
    let mut reopened = SqlSession::open_with_options(dir.path(), opts)
        .await
        .unwrap();
    assert_eq!(reopened.recovery_work().restored_views, Some(1));
    assert_eq!(reopened.recovery_work().rebuilt_views, 0);

    reopened
        .execute("DELETE FROM items WHERE id = 4")
        .await
        .unwrap();
    let expected_after = reopened
        .execute(&format!("SELECT * FROM ({view_sql}) t ORDER BY id"))
        .await
        .unwrap()
        .rows
        .unwrap();
    let actual_after = reopened
        .execute("SELECT * FROM above_avg ORDER BY id")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(actual_after, expected_after);
}

#[tokio::test]
async fn ordered_recompute_view_retains_snapshot_and_ordered_nodes() {
    let sql = "SELECT id, amt FROM items ORDER BY amt DESC LIMIT 2";
    let mut tables = BTreeMap::new();
    tables.insert(
        "items".into(),
        make_batch(&[1, 2, 3], &[1, 1, 2], &[10, 50, 30]),
    );
    let mut view = SqlView::new_with_optimizer(sql, tables, IncrementalOptimizer::default())
        .await
        .unwrap();
    assert!(
        view.retained_relation_count() >= 2,
        "ordered recompute must retain snapshot input and ordered output"
    );
    let candidate = view
        .prepare(&[TableChange {
            table: "items".into(),
            rows: make_batch(&[4], &[2], &[100]),
            weights: vec![1],
        }])
        .await
        .unwrap();
    view.acknowledge(candidate).unwrap();
    let out = view.contents().unwrap();
    let ids = out
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values();
    assert_eq!(ids, &[4, 2]);
}
