//! One owner for typed SQL state, complete Iceberg cursors and local publication.

use std::path::Path;

use crate::iceberg::{same_tables, LocalIcebergSources, SourceWork};
use crate::source_vector::PinnedSourceVector;
use crate::store::{CheckpointStore, PublishPoint, SourceSnapshot};
use crate::{Error, RefreshStats, Result, SqlView};

/// Result of requesting an exact source tuple.
#[derive(Clone, Debug)]
pub enum IcebergRefresh {
    /// Source metadata was revalidated and this tuple is already committed.
    Unchanged {
        /// The already committed local epoch.
        epoch: u64,
    },
    /// One complete source tuple and its SQL output were durably published.
    Committed {
        /// Full endpoint acquisition work, not an O(delta) changelog claim.
        source_work: SourceWork,
        /// Actual local SQL maintenance strategies and work.
        sql_stats: RefreshStats,
    },
}

/// Host-local typed SQL maintenance over explicitly pinned Iceberg tuples.
pub struct IcebergSqlView {
    view: SqlView,
    store: CheckpointStore,
    sources: LocalIcebergSources,
    vector: PinnedSourceVector,
}

impl IcebergSqlView {
    /// Create epoch zero in an empty store from one complete explicit tuple.
    pub async fn bootstrap(
        path: impl AsRef<Path>,
        sql: &str,
        sources: LocalIcebergSources,
        target: PinnedSourceVector,
    ) -> Result<Self> {
        let store = CheckpointStore::open(path)?;
        if store.load_source()?.is_some() {
            return Err(invalid(
                "Iceberg bootstrap requires an empty checkpoint store",
            ));
        }
        let observed = sources.observe(&target, None).await?;
        let (tables, _) = sources.read_all(&observed, &target).await?;
        let view = SqlView::new(sql, tables.clone()).await?;
        let snapshot = view.bootstrap_snapshot()?;
        target.validate_for(&snapshot.tables)?;
        if !same_tables(&tables, &snapshot.tables)? {
            return Err(invalid(
                "SQL bootstrap source caches differ from pinned endpoints",
            ));
        }
        store.commit_source(
            None,
            &SourceSnapshot {
                snapshot,
                vector: target.clone(),
            },
        )?;
        Ok(Self {
            view,
            store,
            sources,
            vector: target,
        })
    }

    /// Strictly restore exact retained endpoints, cached sources and SQL output.
    ///
    /// Current heads are never substituted for committed snapshots. Source
    /// expiry, replacement and schema divergence reject writer continuation.
    pub async fn reopen(
        path: impl AsRef<Path>,
        sql: &str,
        sources: LocalIcebergSources,
    ) -> Result<Self> {
        let store = CheckpointStore::open(path)?;
        let committed = store
            .load_source()?
            .ok_or_else(|| invalid("no Iceberg checkpoint to reopen"))?;
        if committed.snapshot.sql != sql {
            return Err(invalid(
                "SQL definition differs from the Iceberg checkpoint",
            ));
        }
        let observed = sources
            .observe(&committed.vector, Some(&committed.vector))
            .await?;
        let (tables, _) = sources.read_all(&observed, &committed.vector).await?;
        if !same_tables(&tables, &committed.snapshot.tables)? {
            return Err(invalid(
                "retained Iceberg endpoints differ from committed source caches",
            ));
        }
        let view = SqlView::restore_checkpoint(&committed.snapshot).await?;
        Ok(Self {
            view,
            store,
            sources,
            vector: committed.vector,
        })
    }

    /// The committed typed SQL view; callers cannot acknowledge source cursors.
    pub fn view(&self) -> &SqlView {
        &self.view
    }

    /// The complete tuple published with this view's current epoch.
    pub fn source_vector(&self) -> &PinnedSourceVector {
        &self.vector
    }

    /// Read exact endpoints and publish one atomic local SQL/source-vector epoch.
    pub async fn refresh(&mut self, target: PinnedSourceVector) -> Result<IcebergRefresh> {
        self.refresh_with_observer(target, |_| Ok(())).await
    }

    /// Refresh with instrumentation at the actual local durability boundaries.
    ///
    /// A callback error after CURRENT publication leaves the candidate committed.
    /// Reopen before another refresh; replaying an uncertain source step is rejected.
    pub async fn refresh_with_observer(
        &mut self,
        target: PinnedSourceVector,
        observer: impl FnMut(PublishPoint) -> Result<()>,
    ) -> Result<IcebergRefresh> {
        let committed = self
            .store
            .load_source()?
            .ok_or_else(|| invalid("missing committed source epoch"))?;
        if committed.snapshot.epoch != self.view.epoch() || committed.vector != self.vector {
            return Err(invalid(
                "durable source epoch differs; reopen before submitting another tuple",
            ));
        }
        let expected = committed.position()?;
        // Even an identical tuple must reject a replaced UUID or changed schema.
        let observed = self.sources.observe(&target, Some(&self.vector)).await?;
        if target == self.vector {
            return Ok(IcebergRefresh::Unchanged {
                epoch: self.view.epoch(),
            });
        }
        let acquired = self
            .sources
            .changes(&observed, &self.vector, &target, &self.view.tables()?)
            .await?;
        let candidate = self.view.prepare(&acquired.changes).await?;
        let snapshot = self.view.checkpoint_for(&candidate)?;
        target.validate_for(&snapshot.tables)?;
        if !same_tables(&acquired.tables, &snapshot.tables)? {
            return Err(invalid(
                "prepared SQL source caches differ from target Iceberg endpoints",
            ));
        }
        self.store.commit_source_with_observer(
            Some(&expected),
            &SourceSnapshot {
                snapshot,
                vector: target.clone(),
            },
            observer,
        )?;
        let sql_stats = candidate.stats().clone();
        self.view.acknowledge(candidate)?;
        self.vector = target;
        Ok(IcebergRefresh::Committed {
            source_work: acquired.work,
            sql_stats,
        })
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
