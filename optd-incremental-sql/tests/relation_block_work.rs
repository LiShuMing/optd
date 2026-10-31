//! Positive source/intermediate updates must avoid complete relation copies.

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{SqlView, TableChange};
use std::collections::BTreeMap;
use std::sync::Arc;

fn batch(n: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(
            (0..n as i64).collect::<Vec<_>>(),
        ))],
    )
    .unwrap()
}

#[tokio::test]
async fn fixed_delta_and_fixed_root_output_copy_only_touched_cache_leaves() {
    let sql = "SELECT SUM(v) AS n FROM t WHERE v>=0";
    for n in [1024usize, 8192] {
        let view = SqlView::new_with_optimizer(
            sql,
            BTreeMap::from([("t".into(), batch(n))]),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let candidate = view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: batch(2).slice(1, 1),
                weights: vec![1],
            }])
            .await
            .unwrap();
        let work = &candidate.stats().work;
        assert!(work.sources.old_row_entries_copied <= 1, "n={n} {work:?}");
        assert_eq!(work.cache_work.source_state.updates, 1);
        assert_eq!(work.cache_work.source_state.validation_probes, 1);
        assert_eq!(work.cache_work.source_state.touched_buckets, 1);
        assert_eq!(work.cache_work.source_state.snapshot_collections, 0);
        assert!(
            work.cache_work.source_state.directory_entries_copied <= 3 * (n.ilog2() as usize + 1)
        );
        assert!(
            work.caches.old_row_entries_copied <= 3 * 256 + 1,
            "n={n} {work:?}"
        );
        assert!(work.caches.materialized_rows == 0, "n={n} {work:?}");
        assert_eq!(work.cache_work.demanded_leaves.materialized_rows, 1);
        assert!(work.cache_work.leaf_validation.rows <= 3 * 257 + 1);
        assert_eq!(work.cache_work.complete_caches.rows, 1);
        assert_eq!(work.cache_work.complete_caches.misses, 1);
        eprintln!("cache-block-work n={n} {work:?}");
        let ctx = SessionContext::new();
        let full = concat_batches(&batch(n).schema(), &[batch(n), batch(2).slice(1, 1)]).unwrap();
        ctx.register_batch("t", full).unwrap();
        let frame = ctx.sql(sql).await.unwrap();
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let expected = concat_batches(&schema, &frame.collect().await.unwrap()).unwrap();
        assert_eq!(candidate.contents(), &expected);
        assert_eq!(view.epoch(), 0);
    }
}

#[tokio::test]
async fn unread_raw_sources_keep_their_existing_nonmaterialized_weight_contract() {
    // A source outside this SQL graph need not expand its complete multiplicity.
    // Applying SQL-table materialization budgets here would reject valid prepares.
    let mut view = SqlView::new("SELECT 7 AS n", BTreeMap::from([("t".into(), batch(1))]))
        .await
        .unwrap();
    let candidate = view
        .prepare(&[TableChange {
            table: "t".into(),
            rows: batch(1),
            weights: vec![i64::MAX - 1],
        }])
        .await
        .unwrap();
    assert_eq!(candidate.contents(), &view.contents().unwrap());
    view.acknowledge(candidate).unwrap();
    assert!(view.tables().is_err()); // A demanded expansion still rejects this size.
    let candidate = view
        .prepare(&[TableChange {
            table: "t".into(),
            rows: batch(1),
            weights: vec![-(i64::MAX - 1)],
        }])
        .await
        .unwrap();
    view.acknowledge(candidate).unwrap();
    assert_eq!(view.tables().unwrap()["t"], batch(1));
}

#[tokio::test]
async fn count_constant_projection_keeps_duplicate_leaf_weights_unexpanded() {
    let sql = "SELECT COUNT(*) AS n FROM t WHERE v>=0";
    for n in [1024, 8192] {
        let view = SqlView::new_with_optimizer(
            sql,
            BTreeMap::from([("t".into(), batch(n))]),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let candidate = view
            .prepare(&[TableChange {
                table: "t".into(),
                rows: batch(1),
                weights: vec![1],
            }])
            .await
            .unwrap();
        // The optimizer projects a constant below COUNT. Delta-only consumers
        // must retain its high weight without expanding historical duplicates.
        assert_eq!(candidate.stats().work.caches.materialized_rows, 0);
        assert_eq!(
            candidate
                .stats()
                .work
                .cache_work
                .demanded_leaves
                .materialized_rows,
            1
        );
        assert!(candidate.stats().work.cache_work.leaf_validation.rows <= 2 * 257 + 2);
        assert_eq!(candidate.stats().work.cache_work.complete_caches.rows, 1);
        let ctx = SessionContext::new();
        ctx.register_batch(
            "t",
            concat_batches(&batch(n).schema(), &[batch(n), batch(1)]).unwrap(),
        )
        .unwrap();
        let frame = ctx.sql(sql).await.unwrap();
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let expected = concat_batches(&schema, &frame.collect().await.unwrap()).unwrap();
        assert_eq!(candidate.contents(), &expected);
    }
}

#[tokio::test]
async fn fetched_source_records_demanded_complete_bag_collection() {
    // Equal duplicates give unordered LIMIT unambiguous row membership.
    let input = concat_batches(&batch(1).schema(), &[batch(1), batch(1), batch(1)]).unwrap();
    let view = SqlView::new(
        "SELECT v FROM t LIMIT 2",
        BTreeMap::from([("t".into(), input)]),
    )
    .await
    .unwrap();
    let candidate = view
        .prepare(&[TableChange {
            table: "t".into(),
            rows: batch(1),
            weights: vec![-2],
        }])
        .await
        .unwrap();
    assert_eq!(candidate.contents(), &batch(1));
    let work = &candidate.stats().work.cache_work.source_state;
    assert_eq!(work.snapshot_collections, 1);
    assert_eq!(work.snapshot_entries_collected, 1);
    assert_eq!(work.updates, 1);
    assert_eq!(view.contents().unwrap().num_rows(), 2);
}
