# SQL-driven materialized-view maintenance

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** accepted direction by owner · 2026-10-31; host-local correctness/recovery verified,
performance acceptance pending
**Scope:** host-local SQL tables, synchronous incremental materialized views,
single-writer transactions, durable database publication and SQL-level tests.
The owner accepted this direction before implementation. `SqlSession` and the
`sql_database` example now implement the host-local SQL loop described below;
`sql_loop` remains the separate row-image/IPC interface. Correctness and recovery
qualification are recorded separately from the still-open performance gate in
[SQL database qualification](sql_database_qualification.md).

## 1. Problem

The typed engine accepts full row images and signed multiplicities through
`TableChange`. `SqlView::prepare` builds isolated candidates; acknowledgement
changes visible state. `DurableSqlView` publishes source tables, one view result
and its output delta through a single-view checkpoint. The current CLI supplies
Arrow IPC files and JSON weights, rather than executing SQL writes.

A SQL-facing database needs named tables and views, DML-to-change compilation,
dependency routing and a transaction boundary shared by the base tables and
every affected materialized view. Calling each `DurableSqlView::refresh` in turn
does not provide that boundary.

## 2. Constraints and evidence

Reuse DataFusion SQL parsing, binding, types and expression execution. Reuse Optd
incremental rewriting, physical kernels, `TableChange`, `PreparedSqlRefresh` and
candidate acknowledgement. Keep DML compilation separate from view rewrite rules.
No new dependency is proposed here.

The existing `CheckpointStore` manifest describes one SQL definition and output;
it cannot be reinterpreted as a database commit. Its file hashing, immutable-file
publication, exact-predecessor checks and directory synchronization are reusable
patterns. Existing warehouse formats remain separately readable.

The initial implementation is one host with a serialized writer. Distributed
Iceberg table commits do not acquire atomicity from this local protocol. Source
endpoint differencing remains a separate ingestion mode.

## 3. Options considered

* Execute ordinary DataFusion DML, then diff full table snapshots: easy to connect,
  but duplicates source work and separates the write from view publication.
* Execute DML and refresh each durable view independently: cannot make a multi-view
  transaction atomic.
* Compile DML into typed row changes, prepare all candidates and publish one
  database root: selected proposal.

## 4. Decision

### SQL contract

Implemented first-version statements: CREATE TABLE with supported typed columns,
CREATE MATERIALIZED VIEW AS SELECT, INSERT VALUES/SELECT, predicate DELETE,
single-target UPDATE, SELECT, BEGIN/COMMIT/ROLLBACK and DROP MATERIALIZED VIEW.
Admitted statements have exact semantics; unimplemented clauses fail explicitly.
Materialized views are read-only and maintained synchronously at commit.

An example of the implemented SQL interface is:

```sql
CREATE TABLE sales (id BIGINT, k BIGINT, amount BIGINT);
CREATE MATERIALIZED VIEW sales_by_k AS
SELECT k, COUNT(*) AS n, SUM(amount) AS total FROM sales GROUP BY k;

INSERT INTO sales VALUES (1, 10, 100), (2, 10, 200);
SELECT * FROM sales_by_k; -- (10, 2, 300)
UPDATE sales SET k = 20, amount = 250 WHERE id = 2;
SELECT * FROM sales_by_k; -- (10, 1, 100), (20, 1, 250), unordered
DELETE FROM sales WHERE id = 1;
SELECT * FROM sales_by_k; -- (20, 1, 250)
```

After a process restart, selecting the view must return that same committed
result. SELECT binds a materialized view to its stored committed result, rather
than expanding its definition and accidentally demonstrating full recomputation.

Initial CREATE MATERIALIZED VIEW uses the existing strict incremental policy.
An unsupported plan is rejected before its name is published. Presentation-only
ORDER BY/LIMIT removal is never implicit. Full bootstrap when creating a view is
expected; subsequent maintenance must meet its declared policy.

Outside BEGIN, each modifying statement is its own transaction. Inside BEGIN,
later statements read the transaction's preceding base-table changes. Reads of
materialized views inside an active write transaction are initially rejected;
they must not return stale committed results. An errored explicit transaction is
aborted until ROLLBACK. DDL is initially autocommit-only. Constraints, defaults,
generated columns, UPDATE FROM and similar features are accepted only when
implemented and independently tested, not merely because the parser accepts them.

### Components

| Component | Responsibility |
| --- | --- |
| `SqlSession` | Parse and dispatch statements; manage session transaction state. |
| `DatabaseCatalog` | Stable table/view identifiers, schemas, definitions and dependencies. |
| `DmlCompiler` | Bind typed expressions and produce old/new row images against the statement's input snapshot. |
| `TransactionWorkspace` | Own tentative base-table state and consolidated changes across statements. |
| `MaintenanceCoordinator` | Route changes to affected views and prepare all incremental candidates. |
| `DatabaseStore` | Persist immutable payloads and publish one authoritative database root. |

The exported API is `database::SqlSession`, re-exported by the module facade.
Catalog data lives in `database/store.rs`; SQL binding lives in `database/sql.rs`
and row-image compilation in `database/dml_compiler.rs`. The isolated workspace
and prepared commit live in `database/transaction_workspace.rs`; session dispatch
and prepare/publish/install sequencing live in `database/sql_session.rs`.
These remain private implementations rather than separate public traits.
State algorithms remain in existing maintenance executors;
the SQL frontend does not implement alternative join, grouping or window kernels.

### DML conversion

| Statement | Typed changes emitted |
| --- | --- |
| INSERT | Inserted full row image with weight +1. |
| DELETE | Each matching old row image with weight -1. |
| UPDATE | Matching old row with weight -1 and its newly evaluated row with weight +1. |

DELETE and UPDATE match rows using SQL three-valued predicates: only TRUE matches.
Every assignment in one UPDATE evaluates against the same old row; `SET a=b,b=a`
must swap values. Old/new images are evaluated and materialized once per statement.
An UPDATE moving a join key or group key emits both old and new keys naturally.
Equal old/new rows cancel after consolidation.

Tables preserve duplicate multiplicities; a primary key is not assumed. If a
predicate matches three duplicate rows, all three are updated or deleted. Signed
changes consolidate by complete typed row, not by an assumed key. Initial DML
expressions must be deterministic; volatile functions are rejected explicitly.
INSERT SELECT reads a statement snapshot, including earlier statements' writes,
without seeing its own partially produced output.

The DML selector may scan a complete table when its predicate lacks an index.
Generating a small output delta does not prove cheap row discovery. The first
implementation reports `source_relation_rows` (available rows in referenced
relations), `affected_rows` and `net_changed_rows`. `source_relation_rows` is not a
physical scan counter. Actual selector I/O measurement remains open.

### Transaction and view creation

1. Pin a database predecessor and own the writer lock. Each statement evaluates
   against its current transaction workspace, with statement-level candidate
   isolation.
2. Apply source changes to the workspace. Consolidate net changes by catalog table
   identity (names remain fixed within DML-only explicit transactions), preserving complete images, types and multiplicities.
3. At COMMIT, select affected views using the dependency catalog. Invoke existing
   `SqlView::prepare` for every affected view against its committed state, supplying
   the complete transaction changes for that view. Unaffected views keep old refs.
4. If any candidate fails, publish nothing. Otherwise write and synchronize all
   changed base snapshots, view output/delta payloads, catalog metadata and one
   database manifest containing references to all resulting relations.
5. Publish one `CURRENT` pointer with exact-predecessor validation and directory
   synchronization. That root is the only visibility authority. No child view
   pointer may become visible independently.
6. Install the prepared in-memory states under the database publication lock.
   A SELECT pins one database root, so a join of a table and view cannot mix epochs.
   Acknowledgement after durable publication must be prevalidated and infallible;
   an unexpected failure fences the session and requires recovery.

CREATE MATERIALIZED VIEW holds the initial single-writer barrier, binds the
defining SELECT at one base-table epoch, verifies strict maintenance and builds
initial state/output. Its definition, dependencies and output are published in
one database commit. Concurrent DML cannot slip between bootstrap and registration.
Online background bootstrap/catch-up is deferred until a proper change-log fence
exists. MV-on-MV dependencies are initially rejected; later support requires
cycle checks and topological delta propagation.

### Persistence and observability

Use a new explicitly versioned database format. Persist the catalog, base-table
snapshots, view definitions/policies, maintained outputs, signed output logs and
commit identities. Unchanged payloads may be referenced again by the new root.
Initial recovery rebuilds derived indexes/accumulators from committed bases and
checks every maintained output; this is not a claim of persisted operator state or cheap recovery.

Implemented inspection uses `SqlSession::explain_view`, commit response statistics
and CLI `--explain <view>` / `--replay`. SQL EXPLAIN syntax remains deferred.
Complete recompute blockers come from strict planning; existing operator counters
remain separate from the source relation counts above. Actual source scan,
materialization/copy and write-byte accounting remains incomplete; unmeasured work
must not be reported as zero.

### Arrow allocation accounting correction

IPC arrays can retain slices of one shared allocation. Summing each array's
reported buffer capacity counts that allocation repeatedly and can reject a
small valid checkpoint. The byte budget must count each retained allocation
once, using Arrow's documented base allocation pointer and capacity, while
including array metadata and preserving the separate decoded scalar/row limits.
Distinct allocations that exceed the same budget must still reject. No budget
increase or persisted-format change is authorized by this correction.

## 5. Failure modes and recovery

* Before database CURRENT publication: all files are uncommitted; recovery selects
  the old root. Unreferenced prepared files never become tables or view versions.
* After CURRENT is durable but before a response: the transaction committed.
  Recover from the new root; do not rerun DML blindly.
* Before releasing the publication lock: either all in-memory candidates are
  installed or the session is fenced. Future reads cannot observe partial install.
* Persist a caller-supplied request identifier and command fingerprint for writes;
  the same identifier/body resolves to the committed outcome, while a conflicting
  body is rejected. Reissuing the same SQL text as a new request is a new write.
* During recovery: validate manifests, referenced schemas, hashes and lineage;
  rebuild all views from the same committed table snapshot and check outputs.
  Unknown/corrupt state fails rather than silently exposing mixed versions.
* ROLLBACK discards all source and view candidates. DML without dependent views
  still commits its base data through the same database authority.

## 6. Acceptance criteria

Tests execute SQL scripts through the new session API and real process CLI,
without caller-generated `TableChange`, weights, or Arrow input files.

| Gate | Required evidence |
| --- | --- |
| DDL/DML semantics | CREATE TABLE/MV, inserts, NULL predicates, duplicates, assignment swap, INSERT SELECT snapshot behavior and update key movement match independent typed expectations. |
| Transaction visibility | Autocommit, multiple statements/tables/views, rollback and failure in a second view never expose mixed committed versions; stale MV reads inside write transactions reject. |
| Result/delta correctness | At bootstrap and every commit, each maintained view equals independent full SQL; old result plus committed delta equals the new result. Validate base-table state independently of the DML compiler too. |
| Plan/work scope | Strict creation rejects unsupported plans; inserts/deletes/key-moving updates verify accumulator inputs and affected join/window/group buckets, with DML scans reported separately. |
| Durability | Real process kills before root publication and after root durability recover the proper version; ambiguous-response retries do not apply writes twice. |
| SQL TPC loop | Original/core query text becomes explicit CREATE MV scripts; deterministic INSERT/DELETE/UPDATE workloads verify every supported query, with unsupported CREATEs listed as blockers. No query-number-specific rewriting. |
| Performance | Fixed changes with expanding unchanged tables measure selector scans, maintenance reads, state copying, materialization, checkpoint bytes and elapsed time against a fixed baseline. |

An IVM full-SQL oracle fed only the DML compiler's own possibly wrong output
cannot establish DML correctness. Use explicit old/new expected images or a
separately executed raw-DML reference for that gate. Standard TPC scale and
performance remain separate qualifications from small semantic scripts.

## 7. Rollout / migration

1. Add the typed SQL session, DML conversion and dependency coordinator with
   in-memory transaction tests, preserving the existing engine API.
2. Add the database-wide store, crash/retry tests and a reopenable SQL CLI before
   claiming a durable SQL loop. Keep old single-view stores independently readable.
3. Move benchmark change workloads through SQL scripts, retaining original/core
   identities and result/plan/work/performance separation.

The minimum complete deliverable is CREATE TABLE -> CREATE MATERIALIZED VIEW ->
INSERT/DELETE/UPDATE -> SELECT -> process restart -> SELECT/output-log replay,
including failure and rollback coverage. A frontend-only demo is intermediate.

## 8. Open questions

The exact EXPLAIN/status syntax and eventual optional asynchronous refresh policy
remain undecided. Initial behavior is strict synchronous maintenance with a
single writer, autocommit DDL and base-table-only MV dependencies. Online view
creation, MV dependency chains, indexed DML lookup and segmented state/log
compaction are future extensions with their own acceptance gates.
