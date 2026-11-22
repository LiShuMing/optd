//! Isolated DML state and incremental candidates; no durable publication.

use super::dml_compiler::{DmlCompiler, TransactionMode};
use super::response::{CommitReport, DmlWork};
use super::sql;
use super::store::{DatabaseSnapshot, ViewOutput};
use super::table_rows::TableRows;
use crate::bag::Bag;
use crate::engine::SharedSourceUpdate;
use crate::error::invalid;
use crate::source_state::SourceState;
use crate::{Error, PreparedSqlRefresh, RelationWork, Result, SourceStateWork, SqlView};
use datafusion::sql::sqlparser::ast::Statement;
use std::collections::BTreeMap;

/// Private table snapshot and consolidated changes for one pending transaction.
///
/// Statements read preceding writes in this workspace. View engines remain
/// committed until every affected view has prepared successfully and the session
/// publishes one database root. Dropping the workspace publishes nothing.
pub(super) struct TransactionWorkspace {
    /// Candidate tables and catalog; view outputs remain committed until prepare.
    pub(super) snapshot: DatabaseSnapshot,
    /// Net signed full-row changes per table across all staged statements.
    pub(super) changes: BTreeMap<String, Bag>,
    /// First statement's source root per touched table, for exact net-zero restoration.
    /// Explicit DML transactions preserve schema/table identity throughout the workspace.
    original_rows: BTreeMap<String, TableRows>,
    /// Statement work in execution order, even when later changes cancel earlier ones.
    pub(super) work: Vec<DmlWork>,
    /// A statement failed; the session admits only ROLLBACK for this workspace.
    pub(super) aborted: bool,
}

impl TransactionWorkspace {
    /// Own a candidate snapshot, clearing deltas from the previous epoch.
    pub(super) fn new(mut snapshot: DatabaseSnapshot) -> Self {
        snapshot.deltas.clear();
        snapshot.table_deltas.clear();
        Self {
            snapshot,
            changes: BTreeMap::new(),
            original_rows: BTreeMap::new(),
            work: Vec::new(),
            aborted: false,
        }
    }
}

/// Fully prepared state kept private until one database root is published.
#[must_use = "prepared database changes must be published and installed together"]
pub(super) struct PreparedCommit {
    /// Outcome persisted with the request receipt, when the write has an ID.
    pub(super) report: CommitReport,
    /// View-state candidates to acknowledge only after publishing the root.
    pub(super) candidates: BTreeMap<String, PreparedSqlRefresh>,
    /// Updated session-shared `SourceState` handles (installed only after publish succeeds).
    pub(super) shared_sources: BTreeMap<String, SourceState>,
    /// Complete next-epoch tables, view outputs and output deltas to publish together.
    pub(super) snapshot: DatabaseSnapshot,
}

impl TransactionWorkspace {
    /// Stage a statement and consolidate its changes with earlier table changes.
    ///
    /// All fallible compilation and consolidation precede mutation. On error the
    /// workspace is unchanged; the session decides whether to abort the transaction.
    pub(super) async fn apply_dml(
        &mut self,
        stmt: &Statement,
        mode: TransactionMode,
    ) -> Result<()> {
        let result = DmlCompiler::new(&self.snapshot, mode)?
            .compile(stmt)
            .await?;
        let combined = match self.changes.get(&result.table) {
            Some(old) => old.add(&result.delta)?,
            None => result.delta,
        };
        let table = self
            .snapshot
            .tables
            .get_mut(&result.table)
            .ok_or_else(|| invalid("DML target disappeared"))?;
        let original = self
            .original_rows
            .entry(result.table.clone())
            .or_insert_with(|| table.rows.clone());
        // A canceled transaction must share its original cache/directory root,
        // even if intermediate insert/delete operations changed leaf boundaries.
        table.rows = if combined.is_empty() {
            original.clone()
        } else {
            result.rows
        };
        self.changes.insert(result.table, combined);
        self.work.push(result.work);
        Ok(())
    }

    /// Consume the workspace and prepare affected views at exactly the next epoch.
    ///
    /// Each view sees the net table changes across the whole transaction. Engines
    /// are borrowed: any failure drops all candidates without publishing tables,
    /// outputs or view state. Success still requires publication and installation.
    pub(super) async fn prepare(
        mut self,
        committed_epoch: u64,
        views: &BTreeMap<String, SqlView>,
        shared_sources: &BTreeMap<String, SourceState>,
    ) -> Result<PreparedCommit> {
        self.snapshot.epoch = committed_epoch.checked_add(1).ok_or(Error::Overflow)?;
        let mut next_shared_sources = shared_sources.clone();
        let mut applied_shared: BTreeMap<String, SharedSourceUpdate> = BTreeMap::new();
        for (table, delta) in &self.changes {
            if !delta.is_empty() {
                self.snapshot
                    .table_deltas
                    .insert(table.clone(), delta.weighted_batch()?);
                if let Some(old_source) = shared_sources.get(table) {
                    let mut rel_work = RelationWork::default();
                    let mut src_work = SourceStateWork::default();
                    let updated = old_source.apply(delta, &mut rel_work, &mut src_work)?;
                    next_shared_sources.insert(table.clone(), updated.clone());
                    applied_shared.insert(
                        table.clone(),
                        SharedSourceUpdate {
                            source: updated,
                            work: Some((rel_work, src_work)),
                        },
                    );
                }
            }
        }
        let mut candidates = BTreeMap::new();
        let mut maintenance = BTreeMap::new();
        for (name, view) in views {
            let Some(definition) = self.snapshot.views.get_mut(name) else {
                continue;
            };
            let mut changes = Vec::new();
            let mut view_shared: BTreeMap<String, SharedSourceUpdate> = BTreeMap::new();
            for dep in &definition.dependencies {
                if let Some(delta) = self.changes.get(dep).filter(|d| !d.is_empty()) {
                    changes.extend(sql::changes(dep, delta)?);
                    if let Some(shared) = applied_shared.get_mut(dep) {
                        // Attribute the single shared SourceState::apply work once to the first
                        // consuming view so multi-view sums reflect actual work, not double counting.
                        view_shared.insert(
                            dep.clone(),
                            SharedSourceUpdate {
                                source: shared.source.clone(),
                                work: shared.work.take(),
                            },
                        );
                    }
                }
            }
            if changes.is_empty() {
                continue;
            }
            let candidate = view
                .prepare_with_shared_sources(&changes, Some(&view_shared))
                .await?;
            definition.output = if view.is_ordered() {
                ViewOutput::Ordered(candidate.contents().clone())
            } else {
                ViewOutput::Blocks(candidate.output_blocks()?)
            };
            self.snapshot
                .deltas
                .insert(name.clone(), candidate.delta()?);
            maintenance.insert(name.clone(), serde_json::to_value(candidate.stats())?);
            candidates.insert(name.clone(), candidate);
        }
        let affected_rows = self.work.iter().try_fold(0usize, |n, w| {
            n.checked_add(w.affected_rows).ok_or(Error::Overflow)
        })?;
        let report = CommitReport {
            epoch: self.snapshot.epoch,
            affected_rows,
            dml: self.work,
            maintenance,
        };
        Ok(PreparedCommit {
            report,
            candidates,
            shared_sources: next_shared_sources,
            snapshot: self.snapshot,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::SqlSession;

    #[tokio::test]
    async fn canceled_transaction_restores_the_original_cached_table_revision() {
        let root = tempfile::tempdir().unwrap();
        let mut session = SqlSession::open(root.path()).await.unwrap();
        session
            .execute_script("CREATE TABLE t(v BIGINT NOT NULL); INSERT INTO t VALUES (1),(2),(3);")
            .await
            .unwrap();
        // Reopen loads lazy blocks without eagerly flattening; populate the flat cache once.
        drop(session);
        let store = super::super::store::DatabaseStore::open(root.path()).unwrap();
        let mut workspace = TransactionWorkspace::new(store.snapshot());
        assert_eq!(
            workspace.snapshot.tables["t"]
                .rows
                .materialize()
                .unwrap()
                .work
                .misses,
            1
        );
        assert_eq!(
            workspace.snapshot.tables["t"]
                .rows
                .materialize()
                .unwrap()
                .work
                .misses,
            0
        );
        for sql in ["INSERT INTO t VALUES (0)", "DELETE FROM t WHERE v=0"] {
            let statement = sql::statements(sql).unwrap().remove(0);
            workspace
                .apply_dml(&statement, TransactionMode::Explicit)
                .await
                .unwrap();
        }
        assert!(workspace.changes["t"].is_empty());
        // Equal contents alone can still hide a distinct, unflattened candidate root.
        assert_eq!(
            workspace.snapshot.tables["t"]
                .rows
                .materialize()
                .unwrap()
                .work
                .misses,
            0
        );
        assert_eq!(workspace.work.len(), 2);
    }
}
