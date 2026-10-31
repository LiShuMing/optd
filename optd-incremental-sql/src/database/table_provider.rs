//! Fetch-aware physical scans of immutable weighted SQL table revisions.

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{
    stats::Precision, ColumnStatistics, DataFusionError, Result as DfResult, Statistics,
};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::{
    execution_plan::{Boundedness, EmissionType},
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, StatisticsArgs,
};
use futures::Stream;

use super::table_rows::TableRows;
use crate::error::invalid;
use crate::row_blocks::RowCursor;
use crate::{Result, RowReadWork};

/// Statement-owned actual row demand, shared by repeated table scans and subqueries.
#[derive(Default, Debug)]
pub(super) struct TableReadCounter(Mutex<RowReadWork>);

impl TableReadCounter {
    pub(super) fn snapshot(&self) -> Result<RowReadWork> {
        Ok(self
            .0
            .lock()
            .map_err(|_| invalid("table read counter poisoned"))?
            .clone())
    }

    /// Publish a successful chunk's accounting together; overflow retains the old counter.
    fn next(&self, cursor: &mut RowCursor, bound: usize) -> Result<Option<RecordBatch>> {
        let mut work = self
            .0
            .lock()
            .map_err(|_| invalid("table read counter poisoned"))?;
        let mut candidate = work.clone();
        let batch = cursor.next(bound, &mut candidate)?;
        *work = candidate;
        Ok(batch)
    }
}

/// Registration retains one root and no leaf vector; logical statistics remain absent.
pub(super) struct RowTable {
    rows: TableRows,
    counter: Arc<TableReadCounter>,
}

impl RowTable {
    pub(super) fn new(rows: TableRows, counter: Arc<TableReadCounter>) -> Self {
        Self { rows, counter }
    }
}

impl fmt::Debug for RowTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowTable")
            .field("rows", &self.rows.num_rows())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for RowTable {
    fn schema(&self) -> SchemaRef {
        self.rows.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !filters.is_empty() {
            return Err(DataFusionError::Plan(
                "weighted row scans do not accept pushed filters".into(),
            ));
        }
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.rows.num_columns()).collect());
        let schema = Arc::new(self.rows.schema().project(&projection)?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(RowScan {
            rows: self.rows.clone(),
            projection,
            limit,
            batch_size: state.config_options().execution.batch_size.into(),
            properties,
            counter: Arc::clone(&self.counter),
        }))
    }
}

/// Physical fetch owns the cursor cap, including fetch installed after scan planning.
#[derive(Clone)]
struct RowScan {
    rows: TableRows,
    projection: Vec<usize>,
    limit: Option<usize>,
    batch_size: usize,
    properties: Arc<PlanProperties>,
    counter: Arc<TableReadCounter>,
}

impl fmt::Debug for RowScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}
impl DisplayAs for RowScan {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WeightedRowScan: projection={:?}, fetch={:?}",
            self.projection, self.limit
        )
    }
}
impl ExecutionPlan for RowScan {
    fn name(&self) -> &str {
        "WeightedRowScan"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "row scan cannot have children".into(),
            ));
        }
        Ok(self)
    }
    fn supports_limit_pushdown(&self) -> bool {
        true
    }
    fn fetch(&self) -> Option<usize> {
        self.limit
    }
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self {
            limit,
            ..self.clone()
        }))
    }
    fn statistics_from_inputs(
        &self,
        _: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> DfResult<Arc<Statistics>> {
        validate_partition(args.partition().unwrap_or(0))?;
        let rows = self
            .limit
            .unwrap_or(self.rows.num_rows())
            .min(self.rows.num_rows());
        let full = rows == self.rows.num_rows();
        let mut stats = Statistics::new_unknown(self.schema().as_ref());
        stats.num_rows = Precision::Exact(rows);
        stats.column_statistics = self
            .projection
            .iter()
            .map(|&index| ColumnStatistics {
                null_count: if full {
                    Precision::Exact(self.rows.null_counts()[index])
                } else if rows == 0 || self.rows.null_counts()[index] == 0 {
                    Precision::Exact(0)
                } else {
                    Precision::Absent
                },
                ..ColumnStatistics::new_unknown()
            })
            .collect();
        Ok(Arc::new(stats))
    }
    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        validate_partition(partition)?;
        if self.batch_size == 0 {
            return Err(DataFusionError::Plan("zero SQL batch size".into()));
        }
        Ok(Box::pin(RowStream {
            schema: self.schema(),
            cursor: RowCursor::new(self.rows.clone(), self.projection.clone()).map_err(external)?,
            remaining: self
                .limit
                .unwrap_or(self.rows.num_rows())
                .min(self.rows.num_rows()),
            batch_size: self.batch_size,
            counter: Arc::clone(&self.counter),
        }))
    }
}

/// Synchronous bounded poll: dropping the stream leaves no producer or borrowed epoch.
struct RowStream {
    schema: SchemaRef,
    cursor: RowCursor,
    remaining: usize,
    batch_size: usize,
    counter: Arc<TableReadCounter>,
}
impl RecordBatchStream for RowStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
impl Stream for RowStream {
    type Item = DfResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        let bound = self.remaining.min(self.batch_size);
        let counter = Arc::clone(&self.counter);
        match counter.next(&mut self.cursor, bound) {
            Ok(Some(batch)) => {
                self.remaining -= batch.num_rows();
                Poll::Ready(Some(Ok(batch)))
            }
            Ok(None) => {
                self.remaining = 0;
                Poll::Ready(None)
            }
            Err(error) => {
                self.remaining = 0;
                Poll::Ready(Some(Err(external(error))))
            }
        }
    }
}

fn validate_partition(partition: usize) -> DfResult<()> {
    if partition != 0 {
        return Err(DataFusionError::Execution(
            "row scan has only partition zero".into(),
        ));
    }
    Ok(())
}
fn external(error: crate::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests;
