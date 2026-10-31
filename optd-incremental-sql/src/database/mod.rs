//! SQL writes and synchronous materialized views sharing one durable database root.
//!
//! One session owns the host-local writer lock. SQL writes are compiled into row
//! images and delegated to the existing incremental planner and executor.

mod sql;
mod store;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::common::ScalarValue;
use datafusion::sql::sqlparser::ast::Statement;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::bag::Bag;
use crate::planner::IncrementalOptimizer;
use crate::store::PublishPoint;
use crate::{Error, PreparedSqlRefresh, Result, SqlView};
use store::{DatabaseSnapshot, DatabaseStore, Receipt, TableData, ViewData};

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

/// DML selection and emitted changes, distinct from incremental operator work.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DmlWork {
    /// Expanded target rows inserted/deleted/updated by this statement.
    pub affected_rows: usize,
    /// Rows in referenced source relations; not a physical scan measurement.
    pub source_relation_rows: usize,
    /// Distinct nonzero full-row changes after consolidation.
    pub net_changed_rows: usize,
}

/// Durable outcome of one database transaction; retained for identified retries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitReport {
    /// Database epoch published by this transaction.
    pub epoch: u64,
    /// Sum of affected row counts across DML statements.
    pub affected_rows: usize,
    /// Statement-local source and change accounting.
    pub dml: Vec<DmlWork>,
    /// Existing structured refresh statistics, indexed by view name.
    pub maintenance: BTreeMap<String, Value>,
}

/// Result of a SQL statement or an identified write transaction.
pub struct SqlResponse {
    /// Committed epoch (or the original outcome's epoch for a replayed request).
    pub epoch: u64,
    /// SELECT output, read from a single database/session snapshot.
    pub rows: Option<RecordBatch>,
    /// Durable commit information, absent for reads and uncommitted statements.
    pub commit: Option<CommitReport>,
    /// True only when a persisted request outcome was returned without reexecution.
    pub replayed: bool,
}

impl SqlResponse {
    fn empty(epoch: u64) -> Self {
        Self {
            epoch,
            rows: None,
            commit: None,
            replayed: false,
        }
    }
    fn committed(report: CommitReport, replayed: bool) -> Self {
        Self {
            epoch: report.epoch,
            rows: None,
            commit: Some(report),
            replayed,
        }
    }
}

struct TransactionWorkspace {
    snapshot: DatabaseSnapshot,
    changes: BTreeMap<String, Bag>,
    work: Vec<DmlWork>,
    aborted: bool,
}

impl TransactionWorkspace {
    fn new(mut snapshot: DatabaseSnapshot) -> Self {
        snapshot.deltas.clear();
        Self {
            snapshot,
            changes: BTreeMap::new(),
            work: Vec::new(),
            aborted: false,
        }
    }
}

/// A host-local SQL session with synchronous, strictly incremental materialized views.
///
/// DDL is autocommit-only. Explicit transactions provide base-table read-your-writes;
/// materialized-view reads within those transactions are rejected rather than stale.
pub struct SqlSession {
    store: DatabaseStore,
    committed: DatabaseSnapshot,
    views: BTreeMap<String, SqlView>,
    transaction: Option<TransactionWorkspace>,
    fenced: bool,
}

impl SqlSession {
    /// Open a database, acquire its exclusive writer lock and reconstruct all views.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let store = DatabaseStore::open(path.as_ref())?;
        let committed = store.snapshot();
        let mut views = BTreeMap::new();
        for (name, v) in &committed.views {
            let tables = v
                .dependencies
                .iter()
                .map(|n| {
                    let t = committed
                        .tables
                        .get(n)
                        .ok_or_else(|| invalid("unbound restored view dependency"))?;
                    Ok((n.clone(), t.batch.clone()))
                })
                .collect::<Result<_>>()?;
            let engine = SqlView::new_with_optimizer(
                &v.sql,
                tables,
                IncrementalOptimizer::default().require_incremental(),
            )
            .await?;
            if Bag::from_batch(&engine.contents()?)? != Bag::from_batch(&v.output)? {
                return Err(invalid(
                    "restored materialized view differs from committed result",
                ));
            }
            let plan = sql::plan_query(&sql::context(&committed, false)?, &v.sql).await?;
            if sql::reads(&plan)? != v.dependencies {
                return Err(invalid("restored dependency catalog differs from SQL"));
            }
            views.insert(name.clone(), engine);
        }
        Ok(Self {
            store,
            committed,
            views,
            transaction: None,
            fenced: false,
        })
    }

    /// Current committed database epoch; uncommitted statements do not advance it.
    pub fn epoch(&self) -> u64 {
        self.committed.epoch
    }

    /// Execute exactly one SQL statement.
    pub async fn execute(&mut self, statement: &str) -> Result<SqlResponse> {
        self.execute_observed(statement, |_| Ok(())).await
    }

    /// Execute with deterministic callbacks at actual durable publication barriers.
    pub async fn execute_observed(
        &mut self,
        statement: &str,
        mut observer: impl FnMut(PublishPoint) -> Result<()>,
    ) -> Result<SqlResponse> {
        self.ensure_ready()?;
        let result = self.execute_inner(statement, &mut observer).await;
        if result.is_err() {
            if let Some(tx) = &mut self.transaction {
                tx.aborted = true;
            }
        }
        result
    }

    /// Execute a SQL script, preserving ordinary statement/autocommit boundaries.
    pub async fn execute_script(&mut self, script: &str) -> Result<Vec<SqlResponse>> {
        self.ensure_ready()?;
        let statements = match sql::statements(script) {
            Ok(statements) => statements,
            Err(error) => {
                if let Some(tx) = &mut self.transaction {
                    tx.aborted = true;
                }
                return Err(error);
            }
        };
        let mut responses = Vec::new();
        for statement in statements {
            responses.push(self.execute(&statement.to_string()).await?);
        }
        Ok(responses)
    }

    /// Execute one autocommit write with a persistent caller-supplied request ID.
    ///
    /// An exact retry returns the original commit outcome, even after later commits.
    pub async fn execute_request(&mut self, id: &str, statement: &str) -> Result<SqlResponse> {
        self.execute_request_observed(id, statement, |_| Ok(()))
            .await
    }

    /// Identified autocommit write with publication-barrier instrumentation.
    pub async fn execute_request_observed(
        &mut self,
        id: &str,
        statement: &str,
        mut observer: impl FnMut(PublishPoint) -> Result<()>,
    ) -> Result<SqlResponse> {
        self.ensure_ready()?;
        if self.transaction.is_some() {
            return Err(invalid("identified writes require autocommit mode"));
        }
        let statements = sql::statements(statement)?;
        if statements.len() != 1 {
            return Err(invalid(
                "identified autocommit request requires one statement",
            ));
        }
        let stmt = &statements[0];
        if !matches!(
            stmt,
            Statement::CreateTable(_)
                | Statement::CreateView(_)
                | Statement::Insert(_)
                | Statement::Delete(_)
                | Statement::Update(_)
                | Statement::Drop { .. }
        ) {
            return Err(invalid("request IDs apply to modifying statements"));
        }
        let request = self.request(id, &format!("statement:{}", stmt))?;
        if let Some(report) = self.store.receipt(id, &request.1)? {
            return Ok(SqlResponse::committed(report, true));
        }
        self.autocommit(stmt, Some(request), &mut observer).await
    }

    /// Execute a DML-only transaction identified by the complete SQL script.
    ///
    /// This API makes ambiguous COMMIT retries resolvable without replaying writes.
    pub async fn execute_transaction(&mut self, id: &str, script: &str) -> Result<SqlResponse> {
        self.execute_transaction_observed(id, script, |_| Ok(()))
            .await
    }

    /// Identified DML transaction with durable publication-barrier instrumentation.
    pub async fn execute_transaction_observed(
        &mut self,
        id: &str,
        script: &str,
        mut observer: impl FnMut(PublishPoint) -> Result<()>,
    ) -> Result<SqlResponse> {
        self.ensure_ready()?;
        if self.transaction.is_some() {
            return Err(invalid("transaction already active"));
        }
        let statements = sql::statements(script)?;
        if statements.iter().any(|s| {
            !matches!(
                s,
                Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
            )
        }) {
            return Err(invalid(
                "identified transaction accepts DML statements only",
            ));
        }
        let body = statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(";\n");
        let request = self.request(id, &format!("transaction:{body}"))?;
        if let Some(report) = self.store.receipt(id, &request.1)? {
            return Ok(SqlResponse::committed(report, true));
        }
        let mut tx = TransactionWorkspace::new(self.committed.clone());
        for stmt in &statements {
            Self::apply_dml(&mut tx, stmt, true).await?;
        }
        self.commit_workspace(tx, Some(request), &mut observer)
            .await
    }

    /// Inspect the executable incremental plan of an existing materialized view.
    pub fn explain_view(&self, name: &str) -> Result<String> {
        self.ensure_ready()?;
        self.views
            .get(name)
            .map(|v| v.incremental_plan().explain())
            .ok_or_else(|| invalid("unknown materialized view"))
    }

    /// Replay every committed view delta and compare each prefix to its stored output.
    pub fn verify_output_history(&self) -> Result<()> {
        self.ensure_ready()?;
        let mut states = BTreeMap::<u64, Bag>::new();
        for snapshot in self.store.history()? {
            let live = snapshot
                .views
                .values()
                .map(|v| v.id)
                .collect::<BTreeSet<_>>();
            states.retain(|id, _| live.contains(id));
            for (name, v) in &snapshot.views {
                let mut value = states
                    .remove(&v.id)
                    .unwrap_or_else(|| Bag::empty(v.output.schema()));
                if let Some(delta) = snapshot.deltas.get(name) {
                    value = value.add(&read_delta(delta, v.output.schema())?)?;
                }
                value.validate_relation()?;
                if value != Bag::from_batch(&v.output)? {
                    return Err(invalid(
                        "database output-log replay differs from committed view",
                    ));
                }
                states.insert(v.id, value);
            }
        }
        Ok(())
    }

    fn ensure_ready(&self) -> Result<()> {
        if self.fenced {
            Err(invalid(
                "database publication outcome is uncertain; drop and reopen the session",
            ))
        } else {
            Ok(())
        }
    }

    fn request(&self, id: &str, body: &str) -> Result<(String, String)> {
        if id.is_empty() || id.len() > 256 {
            return Err(invalid("request ID must contain 1..256 bytes"));
        }
        Ok((id.into(), format!("{:x}", Sha256::digest(body.as_bytes()))))
    }

    async fn execute_inner(
        &mut self,
        text: &str,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<SqlResponse> {
        let statements = sql::statements(text)?;
        if statements.len() != 1 {
            return Err(invalid("execute requires one SQL statement"));
        }
        let stmt = &statements[0];
        if matches!(stmt, Statement::Rollback { .. }) {
            if stmt.to_string() != "ROLLBACK" {
                return Err(invalid("ROLLBACK modifiers are not supported"));
            }
            self.transaction
                .take()
                .ok_or_else(|| invalid("no transaction is active"))?;
            return Ok(SqlResponse::empty(self.epoch()));
        }
        if self.transaction.as_ref().is_some_and(|t| t.aborted) {
            return Err(invalid("transaction aborted; ROLLBACK required"));
        }
        match stmt {
            Statement::StartTransaction { .. } => {
                if stmt.to_string() != "BEGIN" && stmt.to_string() != "START TRANSACTION" {
                    return Err(invalid("transaction modes are not supported"));
                }
                if self.transaction.is_some() {
                    return Err(invalid("transaction already active"));
                }
                self.transaction = Some(TransactionWorkspace::new(self.committed.clone()));
                Ok(SqlResponse::empty(self.epoch()))
            }
            Statement::Commit { .. } => {
                if stmt.to_string() != "COMMIT" {
                    return Err(invalid("COMMIT modifiers are not supported"));
                }
                let tx = self
                    .transaction
                    .take()
                    .ok_or_else(|| invalid("no transaction is active"))?;
                let result = self.commit_workspace(tx, None, observer).await;
                if result.is_err() && !self.fenced {
                    // A failed prepare leaves an aborted transaction until ROLLBACK.
                    let mut aborted = TransactionWorkspace::new(self.committed.clone());
                    aborted.aborted = true;
                    self.transaction = Some(aborted);
                }
                result
            }
            Statement::Query(_) => {
                let snapshot = self
                    .transaction
                    .as_ref()
                    .map(|t| &t.snapshot)
                    .unwrap_or(&self.committed);
                let ctx = sql::context(snapshot, true)?;
                let plan = sql::plan_query(&ctx, &stmt.to_string()).await?;
                sql::reject_mv_reads(snapshot, &plan, self.transaction.is_some())?;
                let rows = sql::collect(&ctx, plan).await?;
                Ok(SqlResponse {
                    epoch: self.epoch(),
                    rows: Some(rows),
                    commit: None,
                    replayed: false,
                })
            }
            Statement::Insert(_) | Statement::Delete(_) | Statement::Update(_) => {
                if let Some(tx) = &mut self.transaction {
                    Self::apply_dml(tx, stmt, true).await?;
                    Ok(SqlResponse::empty(self.epoch()))
                } else {
                    self.autocommit(stmt, None, observer).await
                }
            }
            _ => {
                if self.transaction.is_some() {
                    return Err(invalid(
                        "DDL and unsupported statements cannot run in a transaction",
                    ));
                }
                self.autocommit(stmt, None, observer).await
            }
        }
    }

    async fn apply_dml(
        tx: &mut TransactionWorkspace,
        stmt: &Statement,
        explicit: bool,
    ) -> Result<()> {
        let result = sql::dml(&tx.snapshot, stmt, explicit).await?;
        let combined = match tx.changes.get(&result.table) {
            Some(old) => old.add(&result.delta)?,
            None => result.delta,
        };
        tx.snapshot
            .tables
            .get_mut(&result.table)
            .ok_or_else(|| invalid("DML target disappeared"))?
            .batch = result.batch;
        tx.changes.insert(result.table, combined);
        tx.work.push(result.work);
        Ok(())
    }

    async fn autocommit(
        &mut self,
        stmt: &Statement,
        request: Option<(String, String)>,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<SqlResponse> {
        let mut tx = TransactionWorkspace::new(self.committed.clone());
        let mut created = None;
        let mut dropped = None;
        match stmt {
            Statement::Insert(_) | Statement::Delete(_) | Statement::Update(_) => {
                Self::apply_dml(&mut tx, stmt, false).await?
            }
            Statement::CreateTable(_) => {
                let (name, batch) =
                    sql::create_table(&sql::context(&tx.snapshot, true)?, stmt).await?;
                if tx.snapshot.tables.contains_key(&name) || tx.snapshot.views.contains_key(&name) {
                    return Err(invalid("relation already exists"));
                }
                let id = allocate(&mut tx.snapshot)?;
                tx.snapshot.tables.insert(name, TableData { id, batch });
            }
            Statement::CreateView(view) => {
                let name = sql::name(&view.name)?;
                if !view.materialized
                    || stmt.to_string()
                        != format!("CREATE MATERIALIZED VIEW {} AS {}", view.name, view.query)
                {
                    return Err(invalid(
                        "only plain CREATE MATERIALIZED VIEW AS SELECT is admitted",
                    ));
                }
                if tx.snapshot.tables.contains_key(&name) || tx.snapshot.views.contains_key(&name) {
                    return Err(invalid("relation already exists"));
                }
                let definition = view.query.to_string();
                let ctx = sql::context(&tx.snapshot, false)?;
                let plan = sql::plan_query(&ctx, &definition).await?;
                let dependencies = sql::reads(&plan)?;
                let tables = dependencies
                    .iter()
                    .map(|n| {
                        let t = tx.snapshot.tables.get(n).ok_or_else(|| {
                            invalid("MV definitions may reference base tables only")
                        })?;
                        Ok((n.clone(), t.batch.clone()))
                    })
                    .collect::<Result<_>>()?;
                let engine = SqlView::new_with_optimizer(
                    &definition,
                    tables,
                    IncrementalOptimizer::default().require_incremental(),
                )
                .await?;
                let output = engine.contents()?;
                tx.snapshot
                    .deltas
                    .insert(name.clone(), Bag::from_batch(&output)?.weighted_batch()?);
                let id = allocate(&mut tx.snapshot)?;
                tx.snapshot.views.insert(
                    name.clone(),
                    ViewData {
                        id,
                        sql: definition,
                        dependencies,
                        output,
                    },
                );
                created = Some((name, engine));
            }
            Statement::Drop { names, .. } if names.len() == 1 => {
                let name = sql::name(&names[0])?;
                if stmt.to_string() != format!("DROP MATERIALIZED VIEW {}", names[0]) {
                    return Err(invalid("only plain DROP MATERIALIZED VIEW is supported"));
                }
                tx.snapshot
                    .views
                    .remove(&name)
                    .ok_or_else(|| invalid("unknown materialized view"))?;
                dropped = Some(name);
            }
            _ => return Err(invalid("unsupported SQL database statement")),
        }
        let (response, candidates, next) = self.prepare_workspace(tx).await?;
        self.publish(next, request, &response, observer)?;
        self.install(candidates)?;
        if let Some((name, engine)) = created {
            self.views.insert(name, engine);
        }
        if let Some(name) = dropped {
            self.views.remove(&name);
        }
        Ok(SqlResponse::committed(response, false))
    }

    async fn prepare_workspace(
        &self,
        mut tx: TransactionWorkspace,
    ) -> Result<(
        CommitReport,
        BTreeMap<String, PreparedSqlRefresh>,
        DatabaseSnapshot,
    )> {
        tx.snapshot.epoch = self.committed.epoch.checked_add(1).ok_or(Error::Overflow)?;
        let mut candidates = BTreeMap::new();
        let mut maintenance = BTreeMap::new();
        for (name, view) in &self.views {
            let Some(definition) = tx.snapshot.views.get_mut(name) else {
                continue;
            };
            let mut changes = Vec::new();
            for dep in &definition.dependencies {
                if let Some(delta) = tx.changes.get(dep).filter(|d| !d.is_empty()) {
                    changes.extend(sql::changes(dep, delta)?);
                }
            }
            if changes.is_empty() {
                continue;
            }
            let candidate = view.prepare(&changes).await?;
            definition.output = candidate.contents().clone();
            tx.snapshot.deltas.insert(name.clone(), candidate.delta()?);
            maintenance.insert(name.clone(), serde_json::to_value(candidate.stats())?);
            candidates.insert(name.clone(), candidate);
        }
        let affected_rows = tx.work.iter().try_fold(0usize, |n, w| {
            n.checked_add(w.affected_rows).ok_or(Error::Overflow)
        })?;
        let report = CommitReport {
            epoch: tx.snapshot.epoch,
            affected_rows,
            dml: tx.work,
            maintenance,
        };
        Ok((report, candidates, tx.snapshot))
    }

    async fn commit_workspace(
        &mut self,
        tx: TransactionWorkspace,
        request: Option<(String, String)>,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<SqlResponse> {
        let (report, candidates, next) = self.prepare_workspace(tx).await?;
        self.publish(next, request, &report, observer)?;
        self.install(candidates)?;
        Ok(SqlResponse::committed(report, false))
    }

    fn publish(
        &mut self,
        next: DatabaseSnapshot,
        request: Option<(String, String)>,
        report: &CommitReport,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<()> {
        let receipt = request.map(|(id, fingerprint)| Receipt {
            id,
            fingerprint,
            report: report.clone(),
        });
        // Any publication error fences this session, including an ambiguous fsync.
        self.fenced = true;
        self.store.commit(next.clone(), receipt, observer)?;
        self.committed = next;
        Ok(())
    }

    fn install(&mut self, candidates: BTreeMap<String, PreparedSqlRefresh>) -> Result<()> {
        for (name, candidate) in candidates {
            // Session ownership and &mut self exclude another acknowledgement.
            self.views
                .get_mut(&name)
                .ok_or_else(|| invalid("prepared view disappeared"))?
                .acknowledge(candidate)?;
        }
        self.fenced = false;
        Ok(())
    }
}

fn allocate(snapshot: &mut DatabaseSnapshot) -> Result<u64> {
    let id = snapshot.next_id;
    snapshot.next_id = id.checked_add(1).ok_or(Error::Overflow)?;
    Ok(id)
}

fn read_delta(batch: &RecordBatch, schema: datafusion::arrow::datatypes::SchemaRef) -> Result<Bag> {
    let width = schema.fields().len();
    if batch.num_columns() != width + 1 || batch.schema().field(width).name() != "__optd_weight" {
        return Err(invalid("invalid database delta schema"));
    }
    if batch.schema().fields()[..width]
        .iter()
        .zip(schema.fields())
        .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
    {
        return Err(invalid("database delta fields differ from view schema"));
    }
    let weights = batch
        .column(width)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("invalid database delta weights"))?;
    if weights.null_count() != 0 || weights.values().contains(&0) {
        return Err(invalid("zero/NULL database delta weights"));
    }
    let rows = RecordBatch::try_new(schema, batch.columns()[..width].to_vec())?;
    Bag::from_weighted(&rows, weights.values())
}

/// Render typed rows for JSON CLI inspection; durable values remain Arrow IPC.
pub fn display_rows(batch: &RecordBatch) -> Result<Vec<Vec<String>>> {
    (0..batch.num_rows())
        .map(|i| {
            batch
                .columns()
                .iter()
                .map(|a| Ok(ScalarValue::try_from_array(a, i)?.to_string()))
                .collect()
        })
        .collect()
}
