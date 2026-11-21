//! Differential and accounting tests for reusable physical execution templates (R09b).

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use optd_incremental_sql::{SqlView, TableChange};

fn schema_items() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
        Field::new("tag", DataType::Utf8, true),
    ]))
}

struct ItemRow<'a> {
    id: i64,
    k: Option<i64>,
    v: Option<i64>,
    tag: Option<&'a str>,
}

fn item(id: i64, k: Option<i64>, v: Option<i64>, tag: Option<&str>) -> ItemRow<'_> {
    ItemRow { id, k, v, tag }
}

fn items_batch(rows: &[ItemRow<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        schema_items(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.id))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.k))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.v))),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.tag))),
        ],
    )
    .unwrap()
}

async fn oracle(sql: &str, tables: &BTreeMap<String, RecordBatch>) -> RecordBatch {
    let ctx = SessionContext::new();
    for (name, batch) in tables {
        ctx.register_batch(name, batch.clone()).unwrap();
    }
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let schema = ctx.sql(sql).await.unwrap().schema().as_arrow().clone();
    datafusion::arrow::compute::concat_batches(&Arc::new(schema), &batches).unwrap()
}

fn sorted_rows(batch: &RecordBatch) -> Vec<Vec<String>> {
    let mut rows = optd_incremental_sql::database::display_rows(batch).unwrap();
    rows.sort();
    rows
}

#[tokio::test]
async fn linear_pipeline_and_union_use_direct_templates_with_empty_short_circuits() {
    let sql = "SELECT k, v + 10 AS v10, tag FROM a WHERE v > 0 \
               UNION ALL \
               SELECT k, v * 2 AS v10, tag FROM b WHERE tag IS NOT NULL";
    let mut tables = BTreeMap::from([
        (
            "a".to_string(),
            items_batch(&[
                item(1, Some(10), Some(5), Some("x")),
                item(2, Some(10), None, Some("y")),
                item(3, None, Some(-3), None),
            ]),
        ),
        (
            "b".to_string(),
            items_batch(&[
                item(10, Some(20), Some(7), Some("z")),
                item(11, Some(20), Some(9), None),
            ]),
        ),
    ]);

    let mut view = SqlView::new(sql, tables.clone()).await.unwrap();
    assert_eq!(
        sorted_rows(&view.contents().unwrap()),
        sorted_rows(&oracle(sql, &tables).await)
    );

    // Pure insertion on table `a`: negative branches must short-circuit with zero fallbacks.
    let insert_a = items_batch(&[
        item(4, Some(10), Some(8), Some("dup")),
        item(4, Some(10), Some(8), Some("dup")),
        item(5, None, None, Some("null_v")),
    ]);
    let candidate = view
        .prepare(&[TableChange {
            table: "a".into(),
            rows: insert_a.clone(),
            weights: vec![1, 1, 1],
        }])
        .await
        .unwrap();
    let template_work = &candidate.stats().work.cache_work.template_execution;
    assert!(
        template_work.direct_invocations > 0,
        "expected direct template executions, got {template_work:?}"
    );
    assert_eq!(
        template_work.fallback_invocations, 0,
        "linear projection/filter/scan/union should not invoke DataFusion planner fallback"
    );
    assert!(
        template_work.empty_short_circuits > 0,
        "pure insert should short-circuit empty negative linear branches"
    );
    view.acknowledge(candidate).unwrap();

    tables.insert(
        "a".into(),
        datafusion::arrow::compute::concat_batches(&schema_items(), [&tables["a"], &insert_a])
            .unwrap(),
    );
    assert_eq!(
        sorted_rows(&view.contents().unwrap()),
        sorted_rows(&oracle(sql, &tables).await)
    );

    // Pure deletion on table `b` plus mixed update on `a`.
    let del_b = items_batch(&[item(10, Some(20), Some(7), Some("z"))]);
    let upd_a = items_batch(&[
        item(1, Some(10), Some(5), Some("x")),
        item(1, Some(10), Some(20), Some("x2")),
    ]);
    let candidate = view
        .prepare(&[
            TableChange {
                table: "b".into(),
                rows: del_b,
                weights: vec![-1],
            },
            TableChange {
                table: "a".into(),
                rows: upd_a,
                weights: vec![-1, 1],
            },
        ])
        .await
        .unwrap();
    let template_work = &candidate.stats().work.cache_work.template_execution;
    assert!(template_work.direct_invocations > 0);
    assert_eq!(template_work.fallback_invocations, 0);
    view.acknowledge(candidate).unwrap();

    let next_a = items_batch(&[
        item(1, Some(10), Some(20), Some("x2")),
        item(2, Some(10), None, Some("y")),
        item(3, None, Some(-3), None),
        item(4, Some(10), Some(8), Some("dup")),
        item(4, Some(10), Some(8), Some("dup")),
        item(5, None, None, Some("null_v")),
    ]);
    let next_b = items_batch(&[item(11, Some(20), Some(9), None)]);
    tables.insert("a".into(), next_a);
    tables.insert("b".into(), next_b);
    assert_eq!(
        sorted_rows(&view.contents().unwrap()),
        sorted_rows(&oracle(sql, &tables).await)
    );
}

#[tokio::test]
async fn keyed_join_project_keys_uses_direct_templates_and_isolates_runtime_errors() {
    let sql = "SELECT a.k, a.v / a.id AS ratio, b.v AS bv \
               FROM a LEFT JOIN b ON a.k = b.k";
    let tables = BTreeMap::from([
        (
            "a".to_string(),
            items_batch(&[item(2, Some(1), Some(10), Some("ok"))]),
        ),
        (
            "b".to_string(),
            items_batch(&[item(1, Some(1), Some(100), Some("m"))]),
        ),
    ]);
    let mut view = SqlView::new(sql, tables.clone()).await.unwrap();
    let before = view.contents().unwrap();

    // Trigger division by zero in direct projection template: candidate must fail cleanly
    // without mutating committed view state.
    let bad = items_batch(&[item(0, Some(1), Some(10), Some("boom"))]);
    let err = view
        .prepare(&[TableChange {
            table: "a".into(),
            rows: bad,
            weights: vec![1],
        }])
        .await;
    assert!(err.is_err(), "division by zero must fail candidate prepare");
    assert_eq!(sorted_rows(&view.contents().unwrap()), sorted_rows(&before));

    // Subsequent valid update succeeds and uses direct templates for ProjectKeys + Projection.
    let good = items_batch(&[item(5, Some(1), Some(25), Some("good"))]);
    let candidate = view
        .prepare(&[TableChange {
            table: "a".into(),
            rows: good.clone(),
            weights: vec![1],
        }])
        .await
        .unwrap();
    let template_work = &candidate.stats().work.cache_work.template_execution;
    assert!(template_work.direct_invocations > 0);
    assert_eq!(template_work.fallback_invocations, 0);
    view.acknowledge(candidate).unwrap();

    let mut expected_tables = tables;
    expected_tables.insert(
        "a".into(),
        datafusion::arrow::compute::concat_batches(&schema_items(), [&expected_tables["a"], &good])
            .unwrap(),
    );
    assert_eq!(
        sorted_rows(&view.contents().unwrap()),
        sorted_rows(&oracle(sql, &expected_tables).await)
    );
}

#[tokio::test]
async fn direct_filter_preserves_zero_column_multiplicity() {
    let sql = "SELECT COUNT(*) AS n FROM a WHERE v > 0";
    let mut tables = BTreeMap::from([(
        "a".to_string(),
        items_batch(&[
            item(1, Some(1), Some(5), None),
            item(2, Some(1), Some(-1), None),
            item(3, Some(1), None, None),
        ]),
    )]);
    let mut view = SqlView::new(sql, tables.clone()).await.unwrap();
    let added = items_batch(&[
        item(4, Some(2), Some(1), None),
        item(4, Some(2), Some(1), None),
        item(5, Some(2), None, None),
    ]);
    let candidate = view
        .prepare(&[TableChange {
            table: "a".into(),
            rows: added.clone(),
            weights: vec![1, 1, 1],
        }])
        .await
        .unwrap();
    assert_eq!(
        candidate
            .stats()
            .work
            .cache_work
            .template_execution
            .fallback_invocations,
        0
    );
    view.acknowledge(candidate).unwrap();
    tables.insert(
        "a".into(),
        datafusion::arrow::compute::concat_batches(&schema_items(), [&tables["a"], &added])
            .unwrap(),
    );
    assert_eq!(
        sorted_rows(&view.contents().unwrap()),
        sorted_rows(&oracle(sql, &tables).await)
    );
}
