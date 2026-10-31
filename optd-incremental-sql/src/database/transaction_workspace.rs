//! Isolated DML state and incremental candidates; no durable publication.

use super::dml_compiler::{DmlCompiler, TransactionMode};
use super::response::{CommitReport, DmlWork};
use super::sql;
use super::store::DatabaseSnapshot;
use crate::bag::Bag;
use crate::error::invalid;
use crate::{Error, PreparedSqlRefresh, Result, SqlView};
use datafusion::sql::sqlparser::ast::Statement;
use std::collections::BTreeMap;

pub(super) struct TransactionWorkspace {
    pub(super) snapshot: DatabaseSnapshot,
    pub(super) changes: BTreeMap<String, Bag>,
    pub(super) work: Vec<DmlWork>,
    pub(super) aborted: bool,
}

impl TransactionWorkspace {
    pub(super) fn new(mut snapshot: DatabaseSnapshot) -> Self {
        snapshot.deltas.clear();
        Self {
            snapshot,
            changes: BTreeMap::new(),
            work: Vec::new(),
            aborted: false,
        }
    }
}

/// Fully prepared state kept private until one database root is published.
#[must_use = "prepared database changes must be published and installed together"]
pub(super) struct PreparedCommit {
    pub(super) report: CommitReport,
    pub(super) candidates: BTreeMap<String, PreparedSqlRefresh>,
    pub(super) snapshot: DatabaseSnapshot,
}

impl TransactionWorkspace {
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
        self.snapshot
            .tables
            .get_mut(&result.table)
            .ok_or_else(|| invalid("DML target disappeared"))?
            .batch = result.batch;
        self.changes.insert(result.table, combined);
        self.work.push(result.work);
        Ok(())
    }

    pub(super) async fn prepare(
        mut self,
        committed_epoch: u64,
        views: &BTreeMap<String, SqlView>,
    ) -> Result<PreparedCommit> {
        self.snapshot.epoch = committed_epoch.checked_add(1).ok_or(Error::Overflow)?;
        let mut candidates = BTreeMap::new();
        let mut maintenance = BTreeMap::new();
        for (name, view) in views {
            let Some(definition) = self.snapshot.views.get_mut(name) else {
                continue;
            };
            let mut changes = Vec::new();
            for dep in &definition.dependencies {
                if let Some(delta) = self.changes.get(dep).filter(|d| !d.is_empty()) {
                    changes.extend(sql::changes(dep, delta)?);
                }
            }
            if changes.is_empty() {
                continue;
            }
            let candidate = view.prepare(&changes).await?;
            definition.output = candidate.contents().clone();
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
            snapshot: self.snapshot,
        })
    }
}
