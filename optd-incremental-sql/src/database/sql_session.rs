//! SQL session dispatch, ownership and prepare/publish/install sequencing.

use super::dml_compiler::TransactionMode;
use super::response::{CommitReport, SqlResponse};
use super::sql::{self, RelationScope};
use super::store::{DatabaseSnapshot, DatabaseStore, Receipt, TableData, ViewData};
use super::store_work::{RecoveryWork, StoreWork};
use super::transaction_workspace::{PreparedCommit, TransactionWorkspace};
use crate::bag::Bag;
use crate::error::invalid;
use crate::planner::IncrementalOptimizer;
use crate::store::PublishPoint;
use crate::{Error, PreparedSqlRefresh, Result, SqlView};
use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::sql::sqlparser::ast::Statement;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Caller retry key plus the digest of canonicalized SQL used to detect ID reuse.
struct RequestIdentity {
    id: String,
    fingerprint: String,
}

/// A host-local SQL session with synchronous, strictly incremental materialized views.
///
/// DDL is autocommit-only. Explicit transactions provide base-table read-your-writes;
/// materialized-view reads within those transactions are rejected rather than stale.
/// A publication or installation error fences the session: drop and reopen it to
/// resolve the committed epoch before issuing more SQL. This is a host-local
/// single-writer interface, not a distributed transaction coordinator.
pub struct SqlSession {
    store: DatabaseStore,
    committed: DatabaseSnapshot,
    views: BTreeMap<String, SqlView>,
    transaction: Option<TransactionWorkspace>,
    fenced: bool,
    recovery: RecoveryWork,
}

impl SqlSession {
    /// Open a database, acquire its exclusive writer lock and reconstruct all views.
    ///
    /// Rebuilds strict incremental plans and derived state from committed base
    /// tables, then checks outputs and dependencies against the durable catalog.
    /// Recovery includes full reads and computation; it is not a delta-only path.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let store = DatabaseStore::open(path.as_ref())?;
        let committed = store.snapshot();
        let mut recovery = RecoveryWork {
            store: store.work()?,
            ..Default::default()
        };
        let mut views = BTreeMap::new();
        for (name, v) in &committed.views {
            let tables: BTreeMap<String, RecordBatch> = v
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
            let dependency_rows = tables
                .values()
                .try_fold(0u64, |rows, batch: &RecordBatch| {
                    rows.checked_add(batch.num_rows() as u64)
                        .ok_or(Error::Overflow)
                })?;
            let engine = SqlView::new_with_optimizer(
                &v.sql,
                tables,
                IncrementalOptimizer::default().require_incremental(),
            )
            .await?;
            let output = engine.contents()?;
            if Bag::from_batch(&output)? != Bag::from_batch(&v.output)? {
                return Err(invalid(
                    "restored materialized view differs from committed result",
                ));
            }
            let plan = sql::plan_query(
                &sql::context(&committed, RelationScope::BaseTables)?,
                &v.sql,
            )
            .await?;
            if sql::reads(&plan)? != v.dependencies {
                return Err(invalid("restored dependency catalog differs from SQL"));
            }
            recovery.record_view(dependency_rows, &output, v.output.num_rows())?;
            views.insert(name.clone(), engine);
        }
        Ok(Self {
            store,
            committed,
            views,
            transaction: None,
            fenced: false,
            recovery,
        })
    }

    /// Completed store operations since this session began opening, including retries.
    ///
    /// This read-only diagnostic is available after publication failures. It does not
    /// establish whether a write committed; reopen to resolve a fenced session.
    pub fn store_work(&self) -> Result<StoreWork> {
        self.store.work()
    }

    /// Fixed work of this successful open, including full view-state reconstruction.
    /// Later requests, retries and output-history verification do not change it.
    pub fn recovery_work(&self) -> &RecoveryWork {
        &self.recovery
    }

    /// Current committed database epoch; uncommitted statements do not advance it.
    pub fn epoch(&self) -> u64 {
        self.committed.epoch
    }

    /// Execute exactly one SQL statement.
    ///
    /// A statement error aborts an active explicit transaction until ROLLBACK.
    /// Autocommit/COMMIT may fail after durable publication; a fenced session
    /// must be dropped and reopened rather than treated as a rolled-back write.
    pub async fn execute(&mut self, statement: &str) -> Result<SqlResponse> {
        self.execute_observed(statement, |_| Ok(())).await
    }

    /// Execute with deterministic callbacks at actual durable publication barriers.
    ///
    /// Callbacks run synchronously and may return an error for fault injection.
    /// Even a callback error after pointer publication fences the session; no
    /// compensating rollback is attempted. See [`Self::execute`] for transaction errors.
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
    ///
    /// Parses the whole script first, then stops at the first execution error.
    /// Earlier autocommit statements remain committed; the script is atomic only
    /// when its statements use an explicit transaction that successfully commits.
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
    /// IDs must contain 1..256 bytes and are bound to canonicalized statement text.
    /// Reusing an ID with other SQL is rejected. Only autocommit writes are admitted.
    /// After uncertain publication, reopen and retry with the same ID to resolve
    /// whether the original request committed without applying it twice.
    pub async fn execute_request(&mut self, id: &str, statement: &str) -> Result<SqlResponse> {
        self.execute_request_observed(id, statement, |_| Ok(()))
            .await
    }

    /// Identified autocommit write with publication-barrier instrumentation.
    ///
    /// An already committed retry returns its stored outcome without executing
    /// SQL or invoking callbacks. New writes follow the error and fencing contract
    /// of [`Self::execute_observed`].
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
        if let Some(report) = self.store.receipt(id, &request.fingerprint)? {
            return Ok(SqlResponse::committed(report, true));
        }
        self.autocommit(stmt, Some(request), &mut observer).await
    }

    /// Execute a DML-only transaction identified by the complete SQL script.
    ///
    /// This API makes ambiguous COMMIT retries resolvable without replaying writes.
    /// The script contains only DML, without BEGIN/COMMIT; all statements share
    /// one workspace and publish once. Base tables provide read-your-writes and
    /// materialized-view reads are rejected. IDs follow [`Self::execute_request`],
    /// binding the whole canonicalized script rather than an individual statement.
    /// Preparation errors publish nothing; publication errors require reopening.
    pub async fn execute_transaction(&mut self, id: &str, script: &str) -> Result<SqlResponse> {
        self.execute_transaction_observed(id, script, |_| Ok(()))
            .await
    }

    /// Identified DML transaction with durable publication-barrier instrumentation.
    ///
    /// Stored retries skip execution and callbacks. New writes follow
    /// [`Self::execute_transaction`] and [`Self::execute_observed`].
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
        if let Some(report) = self.store.receipt(id, &request.fingerprint)? {
            return Ok(SqlResponse::committed(report, true));
        }
        let mut tx = TransactionWorkspace::new(self.committed.clone());
        for stmt in &statements {
            tx.apply_dml(stmt, TransactionMode::Explicit).await?;
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
    ///
    /// Reads historical snapshots and payloads, so work grows with retained history.
    /// View identities distinguish a recreated view from a dropped view of the same name.
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

    fn request(&self, id: &str, body: &str) -> Result<RequestIdentity> {
        if id.is_empty() || id.len() > 256 {
            return Err(invalid("request ID must contain 1..256 bytes"));
        }
        Ok(RequestIdentity {
            id: id.into(),
            fingerprint: format!("{:x}", Sha256::digest(body.as_bytes())),
        })
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
                let ctx = sql::context(snapshot, RelationScope::AllRelations)?;
                let plan = sql::plan_query(&ctx, &stmt.to_string()).await?;
                sql::reject_mv_reads(
                    snapshot,
                    &plan,
                    if self.transaction.is_some() {
                        TransactionMode::Explicit
                    } else {
                        TransactionMode::Autocommit
                    },
                )?;
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
                    tx.apply_dml(stmt, TransactionMode::Explicit).await?;
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

    async fn autocommit(
        &mut self,
        stmt: &Statement,
        request: Option<RequestIdentity>,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<SqlResponse> {
        let mut tx = TransactionWorkspace::new(self.committed.clone());
        let mut created = None;
        let mut dropped = None;
        match stmt {
            Statement::Insert(_) | Statement::Delete(_) | Statement::Update(_) => {
                tx.apply_dml(stmt, TransactionMode::Autocommit).await?
            }
            Statement::CreateTable(_) => {
                let (name, batch) = sql::create_table(
                    &sql::context(&tx.snapshot, RelationScope::AllRelations)?,
                    stmt,
                )
                .await?;
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
                let ctx = sql::context(&tx.snapshot, RelationScope::BaseTables)?;
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
        let PreparedCommit {
            report: response,
            candidates,
            snapshot: next,
        } = tx.prepare(self.committed.epoch, &self.views).await?;
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

    /// Prepare every affected view before publishing once, then install candidates.
    /// Preparation failure leaves committed state intact; later errors fence it.
    async fn commit_workspace(
        &mut self,
        tx: TransactionWorkspace,
        request: Option<RequestIdentity>,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<SqlResponse> {
        let PreparedCommit {
            report,
            candidates,
            snapshot: next,
        } = tx.prepare(self.committed.epoch, &self.views).await?;
        self.publish(next, request, &report, observer)?;
        self.install(candidates)?;
        Ok(SqlResponse::committed(report, false))
    }

    /// Publish the shared root while keeping the session fenced until installation.
    /// A failure may already be durable; never clear the fence on the error path.
    fn publish(
        &mut self,
        next: DatabaseSnapshot,
        request: Option<RequestIdentity>,
        report: &CommitReport,
        observer: &mut dyn FnMut(PublishPoint) -> Result<()>,
    ) -> Result<()> {
        let receipt = request.map(|request| Receipt {
            id: request.id,
            fingerprint: request.fingerprint,
            report: report.clone(),
        });
        // Any publication error fences this session, including an ambiguous fsync.
        self.fenced = true;
        self.store.commit(next.clone(), receipt, observer)?;
        self.committed = next;
        Ok(())
    }

    /// Acknowledge candidates only after their outputs have become durable.
    /// Partial installation must keep the fence set so reopening rebuilds all views
    /// from the authoritative root instead of exposing mixed in-memory revisions.
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
