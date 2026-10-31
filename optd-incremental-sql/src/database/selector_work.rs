//! Measure source delivery after physical planning without retaining input batches.

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Constraints, DataFusionError, Result as DataFusionResult, Statistics};
use datafusion::datasource::MemTable;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::{
    self, ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, StatisticsArgs,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::Stream;

use super::dml_work::SelectorWork;
use super::store::DatabaseSnapshot;
use crate::bag::Bag;
use crate::error::invalid;
use crate::{Error, Result};

/// One selector result and measurements from exactly its physical execution.
pub(super) struct SelectedRows {
    pub(super) batch: RecordBatch,
    pub(super) work: SelectorWork,
}

/// A one-statement context whose relation providers own its source counter.
/// Constants are not relation reads, and scalar subqueries share these providers.
pub(super) struct MeasuredSelector {
    pub(super) context: SessionContext,
    counter: Arc<SourceCounter>,
}

impl MeasuredSelector {
    pub(super) fn new(snapshot: &DatabaseSnapshot) -> Result<Self> {
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let counter = Arc::new(SourceCounter::default());
        for (name, table) in &snapshot.tables {
            context.register_table(
                name.as_str(),
                Arc::new(CountedTable {
                    input: MemTable::try_new(table.rows.schema(), vec![table.rows.batches()?])?,
                    counter: Arc::clone(&counter),
                }),
            )?;
        }
        for (name, view) in &snapshot.views {
            context.register_table(
                name.as_str(),
                Arc::new(CountedTable {
                    input: MemTable::try_new(
                        view.output.schema(),
                        vec![vec![view.output.clone()]],
                    )?,
                    counter: Arc::clone(&counter),
                }),
            )?;
        }
        Ok(Self { context, counter })
    }

    /// Execute one optimized selector with the same providers, including subqueries.
    pub(super) async fn collect(&self, plan: LogicalPlan) -> Result<SelectedRows> {
        let frame = self.context.execute_logical_plan(plan).await?;
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let physical = frame.create_physical_plan().await?;
        let batches = physical_plan::collect(physical, self.context.task_ctx()).await?;
        let mut work = self.counter.snapshot()?;
        work.output_batches = batches.len();
        work.output_rows = batches.iter().try_fold(0usize, |rows, batch| {
            rows.checked_add(batch.num_rows()).ok_or(Error::Overflow)
        })?;
        let batch = concat_batches(&schema, &batches)?;
        Bag::from_batch(&batch)?;
        Ok(SelectedRows { batch, work })
    }
}

/// Delegate snapshot semantics and place the counter only on actual table scans.
#[derive(Debug)]
struct CountedTable {
    input: MemTable,
    counter: Arc<SourceCounter>,
}

#[async_trait]
impl TableProvider for CountedTable {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn constraints(&self) -> Option<&Constraints> {
        self.input.constraints()
    }
    fn table_type(&self) -> TableType {
        self.input.table_type()
    }
    fn statistics(&self) -> Option<Statistics> {
        self.input.statistics()
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.input.supports_filters_pushdown(filters)
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CountedSource {
            input: self.input.scan(state, projection, filters, limit).await?,
            counter: Arc::clone(&self.counter),
        }))
    }
}

/// Shared only by source streams of one physical selector execution.
#[derive(Debug, Default)]
struct SourceCounter(Mutex<SelectorWork>);

impl SourceCounter {
    /// Record one delivered batch atomically; overflow leaves all fields intact.
    fn record(&self, batch: &RecordBatch) -> Result<()> {
        let mut work = self
            .0
            .lock()
            .map_err(|_| invalid("selector counter poisoned"))?;
        let batches = work.source_batches.checked_add(1).ok_or(Error::Overflow)?;
        let rows = work
            .source_rows
            .checked_add(batch.num_rows())
            .ok_or(Error::Overflow)?;
        let bytes = work
            .source_array_bytes
            .checked_add(batch.get_array_memory_size())
            .ok_or(Error::Overflow)?;
        work.source_batches = batches;
        work.source_rows = rows;
        work.source_array_bytes = bytes;
        Ok(())
    }

    fn snapshot(&self) -> Result<SelectorWork> {
        Ok(self
            .0
            .lock()
            .map_err(|_| invalid("selector counter poisoned"))?
            .clone())
    }
}

/// Pass-through scan node preserving ordering, partitioning, statistics and fetch.
#[derive(Debug)]
struct CountedSource {
    input: Arc<dyn ExecutionPlan>,
    counter: Arc<SourceCounter>,
}

impl DisplayAs for CountedSource {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("DmlSourceWork")
    }
}

impl ExecutionPlan for CountedSource {
    fn name(&self) -> &str {
        "DmlSourceWork"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        inputs: &[Arc<Statistics>],
        _: &StatisticsArgs,
    ) -> DataFusionResult<Arc<Statistics>> {
        inputs.first().cloned().ok_or_else(|| {
            DataFusionError::Internal("DML source wrapper missing child statistics".into())
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.input.with_fetch(limit).map(|input| {
            Arc::new(Self {
                input,
                counter: Arc::clone(&self.counter),
            }) as Arc<dyn ExecutionPlan>
        })
    }

    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        // This wrapper owns no expressions; its child's expressions stay there.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DML source wrapper requires one input".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: Arc::clone(&children[0]),
            counter: Arc::clone(&self.counter),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(CountedStream {
            input: self.input.execute(partition, context)?,
            counter: Arc::clone(&self.counter),
        }))
    }
}

/// Poll the original stream directly; dropping this wrapper drops its input.
struct CountedStream {
    input: SendableRecordBatchStream,
    counter: Arc<SourceCounter>,
}

impl RecordBatchStream for CountedStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl Stream for CountedStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = self.input.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(batch))) = &next {
            if let Err(error) = self.counter.record(batch) {
                return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
            }
        }
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::RecordBatchOptions;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;

    fn zero_columns(rows: usize) -> RecordBatch {
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn source_polling_preserves_pending_errors_empty_batches_and_zero_columns() {
        let counter = Arc::new(SourceCounter::default());
        let mut pending = true;
        let mut items = vec![
            Ok(zero_columns(0)),
            Ok(zero_columns(4)),
            Err(DataFusionError::Execution("source error".into())),
        ]
        .into_iter();
        let source = futures::stream::poll_fn(move |cx| {
            if pending {
                pending = false;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(items.next())
            }
        });
        let mut stream = CountedStream {
            input: Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                source,
            )),
            counter: Arc::clone(&counter),
        };
        assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 0);
        assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 4);
        assert!(
            matches!(stream.next().await,Some(Err(DataFusionError::Execution(message))) if message=="source error")
        );
        assert!(stream.next().await.is_none());
        assert_eq!(
            counter.snapshot().unwrap(),
            SelectorWork {
                source_batches: 2,
                source_rows: 4,
                ..Default::default()
            }
        );
    }

    #[test]
    fn source_counter_overflow_does_not_partially_update_counts() {
        let before = SelectorWork {
            source_rows: usize::MAX,
            ..Default::default()
        };
        let counter = SourceCounter(Mutex::new(before.clone()));
        assert!(matches!(
            counter.record(&zero_columns(1)),
            Err(Error::Overflow)
        ));
        assert_eq!(counter.snapshot().unwrap(), before);
    }
}
