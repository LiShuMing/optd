use super::*;
use crate::bag::Bag;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;

fn batch(keys: Vec<Option<i64>>, payload: &str) -> RecordBatch {
    let count = keys.len();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(StringArray::from(vec![payload; count])),
        ],
    )
    .unwrap()
}

fn lazy(keys: Vec<Option<i64>>) -> Result<TableRows> {
    Ok(TableRows::from_relation_batch(batch(keys, "wide payload"))?.rows)
}

#[tokio::test]
async fn scan_counter_overflow_is_atomic_and_the_failed_stream_is_fused() -> Result<()> {
    let counter = Arc::new(TableReadCounter(Mutex::new(RowReadWork {
        leaf_chunks: usize::MAX,
        ..Default::default()
    })));
    let before = counter.snapshot()?;
    let context = SessionContext::new();
    let scan = RowTable::new(lazy(vec![Some(1)])?, Arc::clone(&counter))
        .scan(&context.state(), None, &[], None)
        .await?;
    let mut stream = scan.execute(0, context.task_ctx())?;
    assert!(stream.next().await.unwrap().is_err());
    assert!(stream.next().await.is_none());
    assert_eq!(counter.snapshot()?, before);
    Ok(())
}

#[tokio::test]
async fn registration_planning_count_and_eliminated_reads_demand_no_leaf() -> Result<()> {
    let rows = lazy((0..8192).map(Some).chain([None, None]).collect())?;
    let counter = Arc::new(TableReadCounter::default());
    let context = SessionContext::new();
    let provider = Arc::new(RowTable::new(rows, Arc::clone(&counter)));
    assert!(provider.statistics().is_none());
    context.register_table("t", provider)?;
    for (query, expected) in [
        ("SELECT count(*) FROM t", 8194),
        ("SELECT count(k) FROM t", 8192),
        ("SELECT 1 FROM t WHERE false", 0),
        ("SELECT k FROM t LIMIT 0", 0),
    ] {
        let frame = context.sql(query).await?;
        assert_eq!(counter.snapshot()?, RowReadWork::default());
        let result = frame.collect().await?;
        if query.contains("count") {
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        } else {
            assert_eq!(
                result.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected as usize
            );
        }
        assert_eq!(counter.snapshot()?, RowReadWork::default());
    }
    Ok(())
}

#[tokio::test]
async fn late_fetch_caps_duplicate_expansion_and_statistics_preserve_unknown_prefix_nulls(
) -> Result<()> {
    let before = lazy(vec![Some(7), None])?;
    let rows = before
        .apply(&Bag::from_weighted(
            &batch(vec![Some(7)], "wide payload"),
            &[899_998],
        )?)?
        .rows;
    let counter = Arc::new(TableReadCounter::default());
    let table = RowTable::new(rows, Arc::clone(&counter));
    let context = SessionContext::new();
    let scan = table
        .scan(&context.state(), Some(&vec![0]), &[], None)
        .await?;
    let full = datafusion::physical_plan::StatisticsContext::new()
        .compute(scan.as_ref(), &StatisticsArgs::new())?;
    assert_eq!(full.num_rows, Precision::Exact(900_000));
    assert_eq!(full.column_statistics[0].null_count, Precision::Exact(1));
    assert_eq!(full.total_byte_size, Precision::Absent);
    let limited = scan.with_fetch(Some(1)).unwrap();
    assert_eq!(limited.fetch(), Some(1));
    let stats = datafusion::physical_plan::StatisticsContext::new()
        .compute(limited.as_ref(), &StatisticsArgs::new())?;
    assert_eq!(stats.num_rows, Precision::Exact(1));
    assert_eq!(stats.column_statistics[0].null_count, Precision::Absent);
    assert!(scan.execute(1, context.task_ctx()).is_err());
    assert!(datafusion::physical_plan::StatisticsContext::new()
        .compute(
            scan.as_ref(),
            &StatisticsArgs::new().with_partition(Some(1))
        )
        .is_err());
    let output = datafusion::physical_plan::collect(limited, context.task_ctx()).await?;
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let work = counter.snapshot()?;
    assert_eq!(work.leaf_chunks, 1);
    assert_eq!(work.cache_hits, 0);
    assert_eq!(work.materialization.materialized_rows, 1);
    assert_eq!(work.stored_entries_visited, 1);
    assert_eq!(work.ordering_rows, 2);
    assert_eq!(
        work.materialization.materialized_array_bytes,
        output[0].get_array_memory_size()
    );
    assert_eq!(output[0].num_columns(), 1);
    // LIMIT 0, unpolled execution and dropped streams cannot trigger speculative work.
    drop(scan.execute(0, context.task_ctx())?);
    let zero = scan.with_fetch(Some(0)).unwrap();
    assert!(datafusion::physical_plan::collect(zero, context.task_ctx())
        .await?
        .is_empty());
    assert_eq!(counter.snapshot()?, work);
    Ok(())
}

#[tokio::test]
async fn projected_chunks_zero_columns_and_filter_limit_match_memtable() -> Result<()> {
    let input = batch(
        (0..700)
            .rev()
            .map(|i| if i % 13 == 0 { None } else { Some(i) })
            .collect(),
        "payload",
    );
    for query in [
        "SELECT payload, k, k AS again FROM t ORDER BY k NULLS FIRST",
        "SELECT k FROM t WHERE k > 600 ORDER BY k LIMIT 3",
        "SELECT 1 FROM t LIMIT 333",
        "SELECT k, (SELECT max(k) FROM t) AS m FROM t WHERE k < 3 ORDER BY k",
    ] {
        let context = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(17)
                .with_target_partitions(1),
        );
        let counter = Arc::new(TableReadCounter::default());
        context.register_table(
            "t",
            Arc::new(RowTable::new(
                TableRows::from_relation_batch(input.clone())?.rows,
                Arc::clone(&counter),
            )),
        )?;
        let reference = SessionContext::new();
        reference.register_batch("t", input.clone())?;
        let frame = context.sql(query).await?;
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let result = concat_batches(&schema, &frame.collect().await?)?;
        let expected = concat_batches(&schema, &reference.sql(query).await?.collect().await?)?;
        assert_eq!(result, expected, "{query}");
        let work = counter.snapshot()?;
        if query == "SELECT 1 FROM t LIMIT 333" {
            assert_eq!(work.materialization.materialized_rows, 333);
            assert_eq!(work.materialization.materialized_array_bytes, 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn independent_cursors_keep_their_immutable_epoch_and_partial_reads_do_not_fill_full_cache(
) -> Result<()> {
    let rows = lazy((0..600).map(Some).collect())?;
    let counter = Arc::new(TableReadCounter::default());
    let table = RowTable::new(rows.clone(), Arc::clone(&counter));
    let context = SessionContext::new();
    let scan = table
        .scan(&context.state(), Some(&vec![0]), &[], Some(1))
        .await?;
    let mut first = scan.execute(0, context.task_ctx())?;
    let mut second = scan.execute(0, context.task_ctx())?;
    // A later root is isolated from both old scan cursors.
    let changed = rows
        .apply(&Bag::from_weighted(
            &batch(vec![Some(0), Some(-1)], "wide payload"),
            &[-1, 1],
        )?)?
        .rows;
    assert_eq!(first.next().await.unwrap()?, second.next().await.unwrap()?);
    assert!(first.next().await.is_none());
    assert!(second.next().await.is_none());
    assert_eq!(counter.snapshot()?.materialization.materialized_rows, 2);
    assert_eq!(counter.snapshot()?.cache_hits, 0);
    let new_scan = RowTable::new(changed, Arc::clone(&counter))
        .scan(&context.state(), Some(&vec![0]), &[], Some(1))
        .await?;
    let output = datafusion::physical_plan::collect(new_scan, context.task_ctx()).await?;
    assert_eq!(
        output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -1
    );
    Ok(())
}
