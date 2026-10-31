# SQL database loop: usage and qualification

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** implemented; host-local correctness/recovery verified; performance open · 2026-10-31
**Scope:** SQL writes, strict synchronous MVs, atomic local commits, restart and retry.
**Design:** [SQL-driven materialized-view maintenance](sql_database_loop.md).

## Run a SQL database

Write `demo.sql`:

```sql
CREATE TABLE sales(id BIGINT, k BIGINT, amount BIGINT);
CREATE MATERIALIZED VIEW sales_by_k AS
SELECT k, COUNT(*) AS n, SUM(amount) AS total FROM sales GROUP BY k;
INSERT INTO sales VALUES (1,10,100),(2,10,200);
UPDATE sales SET k=20,amount=250 WHERE id=2;
DELETE FROM sales WHERE id=1;
SELECT * FROM sales_by_k;
```

```sh
cargo +1.95.0 run --locked -p optd-incremental-sql --example sql_database -- /tmp/my-database demo.sql
cargo +1.95.0 run --locked -p optd-incremental-sql --example sql_database -- /tmp/my-database read.sql
cargo +1.95.0 run --locked -p optd-incremental-sql --example sql_database -- /tmp/my-database --explain sales_by_k
cargo +1.95.0 run --locked -p optd-incremental-sql --example sql_database -- /tmp/my-database --replay
```

`read.sql` contains `SELECT * FROM sales_by_k;`. Both processes return `(20,1,250)`.
Results are unordered unless the reading SELECT specifies ORDER BY. Reading an MV
uses its stored output. CREATE MV always requests strict incremental planning and
never strips ORDER BY or row bounds from user SQL.

Scripts use ordinary statement boundaries. Wrap DML in BEGIN/COMMIT for an atomic
multi-statement transaction. DDL is autocommit-only. A failed explicit transaction
requires ROLLBACK. Base-table reads inside transactions see preceding writes; MV
reads inside transactions currently reject and abort the transaction.

For an ambiguous response, retain the request ID and exact SQL:

```sh
# write.sql: one modifying statement
sql_database /tmp/my-database --request write-001 write.sql
# batch.sql: DML statements only, without BEGIN/COMMIT
sql_database /tmp/my-database --transaction batch-001 batch.sql
```

An exact retry returns the original commit report with `replayed:true`; conflicting
SQL with the same ID rejects. Canonical parsed SQL is fingerprinted; a different
literal representation is not promised to be the same request. Request identity
is retained in committed metadata, including after later commits and reopen.

## Implementation boundaries

`database/sql.rs` provides SQL binding helpers and `database/dml_compiler.rs`
constructs typed old/new full-row images. UPDATE
assignments all use old values. NULL predicates follow SQL rules, duplicates retain
multiplicity, and INSERT SELECT observes one statement input snapshot.

`database/mod.rs` re-exports the public API. `database/sql_session.rs` coordinates
dispatch, dependency routing and publication; `database/transaction_workspace.rs`
owns isolated DML state and prepared candidates. All affected `SqlView`s prepare before publication. Failure in any one
view leaves base data and every other view unpublished.

`database/store.rs` persists version `optd-sql-database/1`. One durable CURRENT root
references base tables, view definitions, maintained results and signed deltas.
Unchanged payloads can be reused. There are no independently published view roots.
Publication uncertainty fences the session: drop/reopen and resolve the request ID.
The exclusive local writer lock spans the session lifetime. This is a single-host
protocol; it does not make external Iceberg commits atomic.

Recovery validates hashes and metadata lineage, rebuilds operator states from the
same committed bases and compares reconstructed outputs. `--replay` additionally
folds signed output deltas and checks every committed output prefix, including view
DROP/recreate identity. Historic input files remain necessary for this replay.

Initially unsupported: table DROP/ALTER, constraints/defaults, UPDATE FROM,
RETURNING, volatile expressions, MV-on-MV dependencies, online bootstrap,
concurrent reader sessions, distributed transactions and log/state compaction.
Plain NULL/NOT NULL columns are supported. Names are unqualified ASCII identifiers;
quoted mixed-case relation and assignment names reject.

## SQL benchmark path

The `benchmark_sql_database` example runs the SQL database backend (the generic
`benchmark` example also admits this backend through the same environment):

```sh
OPTD_SQL_BENCH_QUERY_MODE=relational_core \
OPTD_SQL_BENCH_DATABASE_ROOT=/tmp/fresh-tpch-sql-run \
OPTD_SQL_BENCH_QUERY_TIMEOUT_SECS=180 \
cargo +1.95.0 run --locked -p optd-incremental-sql --example benchmark_sql_database -- \
  optd-incremental-sql/benchmarks/tpch/manifest.json /tmp/tpch-sql-report.json 6
```

The artifact root must not exist. SQL database and Iceberg backends are mutually
exclusive. `original` preserves SQL; `relational_core` explicitly removes only
outer ordering/row bounds and records original/executed hashes. This changes
Top-K membership and is a separate qualification profile, never production policy.

For every strict-plan-eligible query, the harness generates plain SQL and performs:

1. CREATE TABLE, initial-half INSERT, CREATE MATERIALIZED VIEW.
2. INSERT remaining rows; DELETE every duplicate matching a chosen complete row;
   UPDATE a remaining row's first non-NULL integer field, moving its key when that
   field is a join/group key. Each round is one SQL BEGIN/COMMIT transaction.
3. Compare every base relation to separately constructed typed expected endpoints.
   Compare the MV with fresh full DataFusion SQL over those endpoints. Replay
   output logs; close and reopen the database; repeat those checks.

Initial rows are loaded in one transaction, rather than committing every 64-row
INSERT batch. Current fixture INSERTs contain at most 1024 rows. Rounds stream individual statements inside BEGIN/COMMIT; they do
not require a combined request string smaller than 16MiB. The ordinary session
statement/CLI-script limit remains 16MiB. `input.sql` is an audit artifact; large
files need a client that streams statements instead of the single-script CLI.

Each query keeps `input.sql`, a warehouse and per-round commit statistics. Queries
with complete-node recomputation blockers return `unsupported_strict_plan` with
all blocker nodes. Empty-only or unchanged-output witnesses remain separate from
`pass`. The benchmark exits nonzero for incomplete qualification even when there
is no result mismatch. It is not TPC certification or a performance comparison.

## Verification record

Commands use Rust 1.95.0 on Linux x86_64, offline locked dependencies, four build
jobs, debug information disabled. The local wrapper is
`/tmp/optd-sql-env-20261029.sh`; existing target is
`/tmp/optd-sql-msrv-20261029-target`.

- SQL semantics/recovery: `cargo +1.95.0 test --offline --locked -p optd-incremental-sql --test database_sql --test database_process`.
  Verified 13 SQL integration tests plus the real SIGKILL parent and child entry.
  The child entry is a no-op when not launched by its parent. Both FilesDurable
  and CurrentDurable were killed; two bases and two MVs recovered atomically, and
  identified transaction retry applied once.
- Generated wide SQL: `cargo +1.95.0 test --offline --locked -p optd-incremental-sql --test benchmark_queries generated_wide_sql`.
  Verified a 64-column, 131-row fixture with duplicates, NULL, quoted string, date, decimal,
  DELETE/UPDATE and replaying the entire generated script into another database.
- `streaming_transaction_accepts_large_total_input_with_bounded_statements`:
  verified two INSERT statements totaling over 16MiB commit as one epoch, while
  each statement remains within the parser budget.
- Assignment-cast regression: `nullable_update_target_casts` failed before the fix
  on `UPDATE t SET a=2147483648` for a nullable INT. After strict cast conversion,
  integer overflow, invalid text and decimal overflow reject atomically. Red/green
  logs: `/tmp/optd-database-cast-{red,green}-20261031.log`.
- Seven standalone CLI subprocesses verified the example, reopen, an identified
  insert and its duplicate retry, final `(20,2,300)`, plan inspection and replay.
  Exact commands/results: `/tmp/optd-sql-database-cli-acceptance-20261031/evidence.json`.
- `scripts/agent-check.sh sql`: verified format and warnings-denied Clippy in both
  configurations, 152 default tests passed / 2 ignored, 166 Iceberg tests passed /
  3 ignored. The ignored expensive suites are not counted as executed; SQL TPC
  commands below run separately. Log: `/tmp/optd-database-qualified-gates-20261031.log`.
- Fresh target: `CARGO_TARGET_DIR=/tmp/optd-database-qualified-fresh-target-20261031 cargo +1.95.0 check --offline --locked -p optd-incremental-sql --features iceberg --lib`.
  Verified exit 0 (see log for elapsed time). This target was new, distinct from the cached full test
  target. Log: `/tmp/optd-database-qualified-fresh-check-20261031.log`.
- Documentation build and the complete SQL benchmark matrix are recorded below.

## SQL benchmark qualification

The fixed executable and source manifest are in
`/tmp/optd-sql-database-acceptance-20261031/identity.json`.
The aggregate audit verifies every source hash against local commit `c048cd6`,
the executable hash, report hashes, all three rounds and their restart/log checks.
All passing plans exclude `OperatorRecompute`; snapshot evaluation inside affected
key buckets remains possible. These are repository SQL variants with explicit
outer ordering/row-bound removal, not original-query or TPC performance certification.

| Suite | Query variants | Qualified SQL loop | Strict-plan rejection |
| --- | ---: | ---: | ---: |
| TPC-H | 22 | 21 | 1 |
| TPC-DS | 99 | 74 | 25 |
| Total | 121 | 95 | 26 |

TPC-H Q11 and Q21 have empty-only tiny fixtures, so their qualified profiles use
the separate SF0.01 repository fixture. Q21 includes 60,175 `lineitem` rows,
15,000 `orders`, 100 `supplier` and 25 `nation` rows. Each selected profile must
independently complete all three rounds, observe a nonempty result and a changed
output. The aggregate never combines rounds from different profiles. Q15 remains
rejected because its global MAX requires complete-node recomputation. All 25
TPC-DS blockers, including every blocking node and reason, are retained in the
aggregate report.

The original 123-profile runner encountered shared-disk ENOSPC on TPC-H Q6/Q7;
Q8 failed before writing a report. Its collector then stopped with a missing-report
exception while its executor joined the remaining tasks. Original reports,
failure logs and `progress.json` are preserved. Completed reports not collected
before that exception retain their terminal logs; their process exit codes were
not retained and are explicitly unknown in the audit. The independent three-round
report assertions are still checked. Only Q6/Q7/Q8 are rerun with the same frozen
executable into distinct `tiny-disk-retry` warehouses. Initial failures remain
failures, alongside the successful retries. There are 126 attempts: 95 pass,
26 strict-plan rejections, two empty-only profiles, two ENOSPC failure reports
and one missing-report ENOSPC attempt. A logical query is qualified only by one
complete passing profile.

Exact commands and evidence (local artifacts, not committed):

```sh
# Initial matrix: fixed binary, concurrency 3, per-query deadlines in run.py.
python3 /tmp/optd-sql-database-acceptance-20261031/run.py
# Separate retries; preserves all original outputs and failure evidence.
python3 /tmp/optd-sql-database-acceptance-20261031/retry_disk.py
# Verify commit/source/binary/report/script identities and full per-query rounds.
python3 /tmp/optd-sql-database-acceptance-20261031/audit.py
```

Aggregate: `/tmp/optd-sql-database-acceptance-20261031/qualification.json`.
Per-query reports, `input.sql`, warehouses, failed-attempt logs and retry records
are kept under that same root. All five implementation commits are local only;
no push was performed. Markdown design and qualification records remain local.

Documentation verification command:

```sh
MDBOOK_BUILD__BUILD_DIR=/tmp/optd-database-qualified-book-20261031 mdbook build docs
```

Verified exit 0 using `/tmp/optd-sql-env-20261029.sh`; existing unrelated
documentation was preserved. Log: `/tmp/optd-database-qualified-book-20261031.log`.

## Shared IPC allocation regression

The full-input Q21 checkpoint used about 12MiB for the `lineitem` IPC file but
exceeded the former input guard when array buffer capacities were summed across
shared slices. The failure occurred while loading IPC, before view bootstrap.
Count each base allocation once while including array metadata; retain the
256MiB budget and independent scalar/row limits. The unit test accepts aliased
allocations and rejects a genuine oversized allocation.

The ignored large regression is executed explicitly, rather than counted as a
default-suite pass:

```sh
OPTD_SQL_RECOVERY_WAREHOUSE=/tmp/optd-database-sql-qualification-20261031/tpch-21-sf01/query-21/warehouse \
cargo +1.95.0 test --locked -p optd-incremental-sql --test database_sql \
  database_ipc_reopen -- --ignored
```

The witness asserts repeated IPC capacity accounting, opens the same checkpoint,
compares maintained output with full SQL over committed bases and replays output
history. It failed before correction and passed afterwards. Logs:
`/tmp/optd-database-recovery-red-20261031.log` and
`/tmp/optd-database-qualified-reopen-20261031.log`. The initial diagnostic test
name described a bootstrap hypothesis; that hypothesis was corrected after the
failure was located in IPC loading. No recovery algorithm or resource ceiling
was changed. The earlier timeout and resource-failure reports remain separate
from the final fixed-executable qualification.

## Remaining gates

`source_relation_rows` measures available rows in referenced DML source relations;
it is not actual physical scan I/O. Commit reports separate those counts from
emitted changes and existing operator maintenance statistics. This implementation
still materializes base relations, copies bag/index maps and writes changed full
snapshots. Recovery rebuilds state. No end-to-end O(delta), scalability or speedup
is certified. Indexed DML discovery, segmented state/checkpoints, bounded history
retention and measured copy/I/O/elapsed-time comparisons remain open.
