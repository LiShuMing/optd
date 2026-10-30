# Typed SQL Incremental Execution

**Status:** accepted · 2026-10-29 (independent agent review)
**Scope:** local, typed, multi-table SQL maintenance and reproducible TPC-H/TPC-DS qualification.

## 1. Problem

At `f1e0773`, the single-input Iceberg CDC loop is implemented, but its reference
expression language cannot represent benchmark SQL. It deliberately supports only
Null/Int/Str values, column projections, simple predicates and root accumulators.
Removing its single-input checks would not establish cross-table consistency or
SQL correctness. E7 needs a separate typed execution layer.

## 2. Constraints and evidence

* Preserve the existing CDC v1 identities, format and single-input guarantees.
* Reuse DataFusion 55 logical plans, Arrow 59 types and expression execution.
  Do not convert Decimal128 to floating point or strings for arithmetic.
* The repository has all 22 TPC-H SQL variants in SLT files and small CSV data;
  three are disabled in the old harness. TPC-DS fixtures are absent at baseline.
* Maintenance and the full-query oracle must execute through separate plans.
  Both can share DataFusion scalar semantics; the oracle must not call the
  incremental interpreter or read its operator caches.
* This work changes more than eight files. One writer owns each implementation
  lane; shared manifests and public interfaces have one integration owner.

## 3. Options considered

1. Recompute the entire query and subtract its previous result: minimal, but
   does not establish incremental query execution. Keep it only as the oracle.
2. Extend the reference value/expression language to SQL: reject; duplicating
   decimal, date, NULL, CASE, window and subquery semantics is unnecessary risk.
3. Maintain a typed DataFusion logical-plan graph: selected. Reuse the planner
   and scalar execution, and implement change propagation at operator boundaries.

## 4. Decision

Add `optd-incremental-sql` to the workspace. It uses the existing pinned
DataFusion dependency and does not route through the lossy legacy optd bridge.
The original optimizer and CDC v1 remain independently usable.

```
atomic named-table change batch
  -> typed plan graph + operator relation caches
  -> prepared next state + weighted output delta
  -> durable local checkpoint publication
  -> acknowledge in memory
  -> independent full-query comparison / replay
```

### Types and execution

Rows retain Arrow/DataFusion scalar types, with signed, checked i64
multiplicities. The initial local backend explicitly rejects individual
materializations above 1,000,000 rows or a conservative 256 MiB scalar/buffer
budget before expanding weights. These resource limits are not scale or
performance qualification. Every operator owns its complete current relation. An optimized
DataFusion logical plan supplies stable graph structure, schemas and expressions;
default logical optimizations decorrelate subqueries before graph construction.
The incremental context explicitly disables physical uncorrelated scalar
subqueries, forcing their scalar-to-join lowering into this graph. The independent
oracle keeps the default setting and uses its actual physical output schema.
Equivalent join lowering can conservatively widen output nullability; comparison
requires identical column names, data types and typed values (including NULL),
while each engine's complete schema remains stable across refreshes.
Every expression is recursively checked for ScalarSubquery, Exists, InSubquery
and OuterReference after optimization. Any remaining subquery/reference is
rejected; local execution must never read an original table through a hidden
expression. DDL, DML and non-immutable functions are rejected before changing
state. Missing or unknown table changes are errors.

Graph nodes have stable postorder ids; every node is updated once per epoch.
Before graph bootstrap execution, [registered rewrite rules](./rule_rewrite.md)
compile each node into a verified signed-expression maintenance plan. Rules
select executable expressions, explicit changed-group maintenance or explicit
local recomputation; the refresh driver interprets that compiled plan. Rule
identity and the actual expression tree are exposed through
`incremental_plan().explain()`. See [extension usage](./rule_rewrite_usage.md).
All edges read the same immutable before state and candidate after state.
Repeated/self-join scans share one updated source relation and never apply a
source batch twice. Any shared logical subtree uses the same before/after pair.
TableScan projection/filters are evaluated against the pinned source relation;
fetch pushdown is explicitly classified as nonlinear local recomputation.

Scan updates apply signed rows. Projection, Filter, Alias and Union distribute
changes. An inner join uses `dL join R_before + L_after join dR`, including changes
to both inputs and self-joins without double counting. Arrow/DataFusion executes
each local operation against explicitly substituted child relations, not the
original source tables. Empty deltas do not dirty ancestors.

Nonlinear operators expose the selected maintenance scope. Qualified outer,
semi and anti equijoins now use indexed snapshots over the union of changed
join keys. Qualified windows recompute complete affected PARTITION BY groups;
their Before snapshot reads committed output to preserve exact retractions.
Remaining DISTINCT ALL is normalized to grouped membership when it has output
columns. See [key-scoped snapshots](./scoped_snapshots.md).

Refresh reports `NativeDelta`, `ChangedGroupRecompute`, `KeyedRecompute` or
`OperatorRecompute`. [Changed-group maintenance](./group_maintenance.md) narrows
ordinary, qualified GROUP BY refreshes to indexed groups whose input changed.
It reports changed rows and surviving group rows actually aggregated; native
accumulator updates are still future work. Unqualified keys/functions, global
aggregates, grouping sets, global windows and ordering retain explicit complete
node recomputation in compatibility mode. `IncrementalOptimizer::require_incremental()`
rejects selected `Maintenance::Recompute` before bootstrap. Unsupported plans
fail with an operator-specific reason; there is no whole-query fallback.
Operator-local indexed work excludes complete source/result cache and checkpoint
costs and does not establish end-to-end delta complexity.

ORDER BY determines LIMIT membership. Each node retains its ordered Arrow
output in addition to the bag used for differences. Sort and order-preserving
Projection/Filter/Alias above it are recomputed against that ordered child
output, using one input partition; Limit reads that sequence, not a bag expansion.
The ordered chain remains explicit when physical execution needs its sort
properties. Sequence changes propagate separately from multiset changes, so
dropping a sort key cannot hide a reorder from a later Limit. Regression tests
include Sort -> Projection/Alias -> Limit and bag-preserving reorder.
LIMIT ties without a total order are an
explicit qualification boundary: tests must compare a valid ordering or add an
explicitly labelled deterministic fixture variant, never silently modify SQL.

### Multi-table transaction and recovery

A caller submits all named-table changes for one logical epoch together. Prepare
validates source schemas, positive resulting multiplicities, checked weights and
operator execution before returning an immutable candidate. Failed prepare
leaves source, operator state, output and epoch unchanged. Acknowledge checks
owner and predecessor revision; repeated/stale candidates cannot double-apply.

The local store publishes all typed source snapshots, output and signed output
delta in one immutable epoch directory, then atomically replaces `CURRENT`.
Arrow IPC preserves schemas and values. A manifest binds SQL, epoch, file sizes
and SHA-256 digests. Immutable files and their directory are fsynced, the
temporary CURRENT file is fsynced, then renamed, and the store's parent directory
is fsynced before returning committed or acknowledging memory. Every attempt
has a unique directory; retries never overwrite an orphan attempt. A host-local
OS file lock excludes competing writers. The store checks predecessor epoch;
retrying an already published identical epoch is idempotent, conflicting content
is an error. The current manifest is the only commit authority.
Each manifest also hashes its predecessor pointer; independent consumers follow
only that committed lineage and fold typed signed deltas exactly once per epoch.

On restart, read and validate the manifest/files, rebuild the graph from the
committed source snapshots, and compare rebuilt output with the committed
output before accepting further changes. Rebuilding at recovery is deliberately
allowed; recomputing the full query on every refresh is not. Orphan directories
are retained and ignored. History reclamation is not part of this change.

This is a new typed local checkpoint format, not an unannounced extension of
Iceberg CDC v1. Multi-table Iceberg publication needs its own versioned source
vector protocol; the local epoch provides a concrete, testable consistency
boundary first. No distributed transaction or performance guarantee is claimed.

## 5. Failure modes and recovery

* Invalid schema, absent-row deletion, overflow or operator failure: prepare
  fails and all prior state remains usable.
* Crash after files close but before CURRENT replacement: old epoch remains
  authoritative; retry the unapplied change batch.
* Crash after CURRENT replacement but before in-memory acknowledge: restart
  loads the committed epoch; the caller must not replay it as a new epoch.
* Lost/ambiguous acknowledgement: compare expected epoch and payload identity
  with CURRENT; never append blindly.
* Corrupt manifest, IPC data, SQL mismatch or output mismatch: reject recovery.
* Concurrent writer: reject opening the store; process death releases the lock.
* Unsupported or nondeterministic SQL: reject construction before publication.

## 6. Acceptance criteria

1. Fresh-target build, formatting and warnings-denied lint pass with the
   recorded Rust toolchain; existing incremental suites remain green. The
   verification script and incremental CI include the new SQL crate; larger
   benchmark datasets remain explicit generated qualification fixtures.
2. Hand-calculated cases cover NULL, duplicates, decimal/date expressions,
   two-sided/self join changes, nested aggregate/HAVING, DISTINCT, outer and
   anti joins, pushed-down scan projection/filter/fetch, top-k membership across
   Projection/Alias, expression-subquery rejection/lowering, empty/global
   aggregates and deletes/updates.
3. Each batch is compared with independent full SQL execution over a separate
   source snapshot; invalid changes prove transactional non-mutation.
4. Real subprocess deaths exercise both publication windows, followed by
   source/output/epoch equality and successful next refresh. Corruption,
   conflicting retries and competing writers are separately tested.
5. Benchmark manifests enumerate all TPC-H 22 and TPC-DS 99 logical queries,
   including generated query variants. Every entry records its source, fixture
   identity, execution strategy, tested rounds and failure reason. Missing or
   unsupported entries are not PASS. Repository TPC-H parameter variants and
   synthetic TPC-DS data are labelled explicitly.
6. Public run commands produce a structured coverage artifact. Benchmark
   correctness and scale/performance qualification are separate gates.

## 7. Rollout / migration

First deliver the graph, typed bags and atomic in-memory refresh. Then integrate
the durable store and process-death acceptance. Finally run the complete query
manifests, fix concrete planner/operator gaps and retain query-level evidence.
Each increment is independently usable and does not change existing CDC v1.
The current task is an explicitly continuous implementation request; ordinary
task-related publication follows the repository contract after validation.

## 8. Open questions

No open decision blocks the local typed implementation. Distributed Iceberg
multi-table transactions, native incremental implementations for every nonlinear
operator, source snapshot retention and large-scale performance remain separate
qualification work and must stay visible in the execution report.

## 9. Observed local verification

**Verified, 2026-10-29:** `scripts/agent-check.sh sql` exited zero with Rust
1.95.0: workspace formatting, SQL all-target warnings-denied Clippy, and 61
tests passed. One complete benchmark test is explicitly ignored by the ordinary
test command; query qualification is a separate explicit run. The passing tests
include independent SQL result/delta comparisons, changed-group work accounting,
CLI replay, nine directory synchronization tests, and actual process-death
recovery. The directory synchronization tests observe the real opening path;
they are not hardware power-failure certification.

The run used explicit Rust 1.95.0 compiler and rustdoc, four build jobs,
`CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`,
`CARGO_INCREMENTAL=0`, `CARGO_NET_OFFLINE=true`, and
`CARGO_TARGET_DIR=/tmp/optd-sql-msrv-20261029-target`. This latest gate reused
the integration target and recompiled the modified package; it is not another
fresh-target build claim. Full output is
`/tmp/optd-directory-durability-sql-20261029.log`.

The original default and Iceberg feature gates also passed after the independent
[test process isolation correction](./test_process_isolation.md). Documentation
built with mdBook 0.4.52 using
`MDBOOK_BUILD__BUILD_DIR=/tmp/optd-sql-book-20261029 mdbook build docs`.
Workspace-wide integration and query/data-profile qualification retain their
own records; these local checks do not establish Iceberg vector ingestion,
distributed transactions, native delta algorithms for every operator, or scale
and performance results.

**Verified, 2026-10-29:** the complete `scripts/agent-check.sh full` also exited
zero with the same Rust/compiler/profile settings and mdBook on PATH. It ran
workspace all-target/all-feature warnings-denied Clippy and workspace tests:
327 passed, zero failed, three explicitly ignored tests, and the one named
Postgres-backed integration test `cli_run_cardbench_twice` filtered by that
documented mode. The ignored tests are two existing Parquet metadata tests and
the explicit complete TPC-H qualification above. The default 41-test and
Iceberg 70-test suites passed separately in that command. Full output is
`/tmp/optd-full-gates-20261029.log`; its documentation output is
`/tmp/optd-full-book-20261029`. These counts do not convert ignored/filtered
tests into passes or replace the explicit query qualification reports.
