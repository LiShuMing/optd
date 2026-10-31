//! Publish one view checkpoint before acknowledging its prepared state.

use crate::planner::IncrementalOptimizer;
use crate::store::{CheckpointStore, PublishPoint};
use crate::{Error, RefreshStats, Result, SqlView, TableChange};
use datafusion::arrow::array::RecordBatch;
use std::collections::BTreeMap;
use std::path::Path;

/// A SQL view whose input vector and output changes commit in one local epoch.
pub struct DurableSqlView {
    view: SqlView,
    store: CheckpointStore,
}
impl DurableSqlView {
    /// Bootstrap a new store or recover its exact committed source vector.
    /// Initial tables are used only when no checkpoint has been published.
    pub async fn open(
        path: impl AsRef<Path>,
        sql: &str,
        initial: BTreeMap<String, RecordBatch>,
    ) -> Result<Self> {
        Self::open_with_optimizer(path, sql, initial, IncrementalOptimizer::default()).await
    }

    /// Bootstrap or rebuild committed sources using the supplied rule registry.
    ///
    /// Rule implementations are not persisted. Supply the desired registry again
    /// on reopen; committed output is still checked against the rebuilt query.
    pub async fn open_with_optimizer(
        path: impl AsRef<Path>,
        sql: &str,
        initial: BTreeMap<String, RecordBatch>,
        optimizer: IncrementalOptimizer,
    ) -> Result<Self> {
        let store = CheckpointStore::open(path)?;
        let view = match store.load()? {
            Some(snapshot) => {
                if snapshot.sql != sql {
                    return Err(Error::Invalid(
                        "SQL definition differs from checkpoint".into(),
                    ));
                }
                SqlView::restore_checkpoint_with_optimizer(&snapshot, optimizer).await?
            }
            None => {
                let view = SqlView::new_with_optimizer(sql, initial, optimizer).await?;
                let bootstrap = view.bootstrap_snapshot()?;
                store.commit(None, &bootstrap)?;
                view
            }
        };
        Ok(Self { view, store })
    }
    /// Current committed SQL view.
    pub fn view(&self) -> &SqlView {
        &self.view
    }
    /// Persist and then acknowledge one atomic multi-table batch.
    pub async fn refresh(&mut self, changes: &[TableChange]) -> Result<RefreshStats> {
        self.refresh_with_observer(changes, |_| Ok(())).await
    }
    /// Refresh with deterministic durability barriers for process-kill acceptance.
    pub async fn refresh_with_observer(
        &mut self,
        changes: &[TableChange],
        observer: impl FnMut(PublishPoint) -> Result<()>,
    ) -> Result<RefreshStats> {
        // Reconcile a prior committed-but-unacknowledged attempt before accepting
        // a new batch. Callers recover rather than replay an uncertain epoch.
        if self
            .store
            .load()?
            .is_none_or(|s| s.epoch != self.view.epoch())
        {
            return Err(Error::Invalid(
                "durable epoch differs; reopen before submitting another batch".into(),
            ));
        }
        let candidate = self.view.prepare(changes).await?;
        let snapshot = self.view.checkpoint_for(&candidate)?;
        self.store
            .commit_with_observer(Some(self.view.epoch()), &snapshot, observer)?;
        let stats = candidate.stats().clone();
        self.view.acknowledge(candidate)?;
        Ok(stats)
    }
}
