# DBSP operator prototype and TPC qualification

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** accepted · 2026-10-29
**Scope:** Python in-memory operator semantics and reproducible TPC query experiments in
`dbsp/`; a design for subsequent Rust/SQL integration. No production engine replacement,
cluster run, durable recovery implementation or official TPC performance result.

## 1. Problem

`dbsp/dbsp.py` contains stream/group primitives and a nested-loop batch join. Its existing
tests mostly print examples. A complete incremental query needs stateful operators whose
output deltas compose, including deletions and nonlinear operators. Preserve those existing
untracked files. Add separate Python modules, with no new dependency.

## 2. Constraints and evidence

Current optd source was inspected at HEAD `7b945efef0ed550f6062eec353899f81c476a5f3`.
This is not a greenfield Rust implementation: `optd-incremental-sql/src/engine.rs:42`
already distinguishes `NativeDelta`, `ChangedGroupRecompute`, and `OperatorRecompute`.
Its `join_delta` at line 618 evaluates delta relations against retained complete child
relations; `group_state.rs` already indexes qualified aggregate groups. Other nonlinear
operators and ordered chains select operator recomputation (`engine.rs:773`). The SQL
crate also has its own durable epoch store and Iceberg source-vector coordinator.
This experiment does not rerun or recertify their historical qualification reports.
The next Rust work should improve these existing execution strategies and reuse their
typed planning/durability contracts, rather than create a second SQL frontend or claim
that existing local recovery functionality is absent.

Reference Feldera checkout HEAD `d1dfb6447a30f194c2b59d3c66c3708dda6e5699`:

| Capability | Reference implementation, relative to Feldera root | Transferable rule |
|---|---|---|
| Incrementalization | `sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/visitors/outer/OptimizeIncrementalVisitor.java:83` | Push integration past linear operators; integrate/query/differentiate alone does not save work |
| Join | `crates/dbsp/src/operator/dynamic/join.rs:663` | Indexed traces; include the simultaneous-update cross term |
| Distinct | `crates/dbsp/src/operator/dynamic/distinct.rs:649` | Emit threshold(new count) minus threshold(old count) |
| Outer/semi/anti | `crates/dbsp/src/operator/dynamic/join.rs:777`, `:961` | Match-count transitions retract and restore unmatched output |
| Aggregate | `crates/dbsp/src/operator/dynamic/aggregate.rs:766` | Revisit affected keys; retract old output and insert new output |
| Group/TopK | `crates/dbsp/src/operator/dynamic/group.rs:135` | Recompute touched partitions, subtract previous partition result |
| Window | `crates/dbsp/src/operator/dynamic/time_series/rolling_aggregate.rs:398` | Determine affected time ranges; retain trace until a justified frontier |
| Recursion | `crates/dbsp/src/operator/recursive.rs:100` | Nested fixed point and consolidated output; deletion needs dedicated proofs |
| Persistence | `crates/dbsp/src/circuit/checkpointer.rs:290` | Stable operator IDs and complete state dependencies are a separate contract |

Ordinary SQL equijoins must reject NULL keys; grouping/set equality groups NULL together.
SQL EXISTS does not multiply left rows by the number of matching right rows. SQL LIMIT counts
row multiplicity, whereas Feldera's basic tuple TopK has a different contract.

## 3. Options considered

1. Wrap full-query evaluation with integrate/differentiate: correct differences but no proof
   of incremental work reduction.
2. Immediately build a new Rust SQL runtime: too broad before operator semantics are qualified.
3. A small stateful Python DAG with explicit SQL-equivalent plans and independent SQLite
   oracle: selected. Do not copy Feldera implementation source into this repository.

## 4. Decision

### Prototype contract

Rows are hashable tuples and a bag maps rows to signed integer weights. Consolidate zeros;
never use Counter operations that discard negative deltas. Materialized source/output state
has nonnegative weights. Updates are a before-image withdrawal plus an after-image insertion.

Each epoch supplies all input-table deltas together. Evaluate a DAG in topological order,
once per node, using both child deltas and retained keyed traces. Prevalidate all source
retractions before mutation. Runtime expression failures are outside transaction rollback
guarantees: discard/rebuild the prototype after an evaluation exception.

Implement linear map/filter/union, indexed inner/left/full/semi/anti equijoin, distinct,
INTERSECT/EXCEPT ALL and DISTINCT, affected-group aggregation (COUNT, SUM, AVG, MIN, MAX,
DISTINCT), and affected-partition transforms for TopK/window experiments. Group evaluation
may scan an entire affected group; global TopK may scan its whole partition. Report these
costs rather than calling every update proportional to input delta size. NULL/empty global
aggregation and duplicate-row LIMIT are explicit cases. Arbitrary Python callbacks are not
a SQL parser or a SQL expression/type system.

Record delta weights, affected keys, output delta tuple count/absolute weight, candidate pair
work and visited trace/group members. Compare maintained
output and emitted deltas against an independent SQLite full-query execution after every
valid epoch. Ordered queries additionally require explicit ORDER BY/LIMIT in both paths and
tie-aware comparison. Never count the oracle as incremental execution.

### TPC experiment

Read SQL from the owner-named `tpc-benchmark/benchmark_case/tpch/query` and
`benchmark_case/tpcds_1g/query` directories. First qualify representative complete queries
(TPCH Q1/Q3/Q4/Q13/Q22, TPCDS Q3/Q12/Q97), including their order/limit and subqueries/window
where present. If any query is not completed, report it as unqualified. Use controlled
synthetic witness data with duplicates, NULLs, skew, empty groups and matched predicates.
These are not dbgen/dsdgen data or an official TPC benchmark. Store source SQL hashes and
explicit SQLite dialect translations. Scale experiments vary baseline and delta sizes and
separate initialization, maintenance, full evaluation and comparison time.

### Complete implementation path

1. Extend the existing typed DataFusion plan graph in `optd-incremental-sql`, inventorying
   strategy coverage for all 22 TPCH and 99 TPCDS query files from this benchmark checkout.
   Preserve schemas, DECIMAL/date/NULL semantics, stable node identities and capability
   rejection. Existing qualification of other SQL/data profiles is not qualification of
   these exact files or evidence of native incremental work for all their operators.
2. Replace generic affected-group work selectively with reversible SUM/COUNT accumulators,
   counted ordered MIN/MAX and DISTINCT structures; share keyed arrangements across joins.
   Qualify non-equality joins, correlated subqueries/decorrelation, HAVING, set operations,
   ranking, frame boundaries and tie rules. No silent full-query fallback.
3. Reuse existing local atomic source epochs and checkpoints; extend persistence/recovery
   to the new keyed traces and input/sink identities. Test retry, crash, replay and migration.
   Bound memory only with explicit watermarks/retention semantics; late data is a policy.
4. Add recursion/time-window qualification separately: TPC suites do not prove those features.
5. Run deterministic independent state trajectories at delta ratios 0, 0.01%, 0.1%, 1%, 10%,
   append/delete/update/mixed, fact-only/dimension/multi-table, uniform/skew and large-fanout.
   Keep the full query result oracle each epoch before claiming correctness or performance.
6. Load real generator output locally after first qualifying SF1 resources. Available TPCDS
   `benchmark/dsdgen` is 2.10.0 and SCALE is integer: do not invent fractional scale. Preserve
   dimension closure when selecting fact partitions. Follow maintenance templates in
   `benchmark/mtsqls` and `benchmark/run_refresh.sh` for actual refresh qualification; random
   insertions are not equivalent to their deletion/returns/sales workflow. TPCH dbgen/data
   were not located in the inspected asset subtrees and need an explicit location.

## 5. Failure modes and recovery

Reject negative source states and noninteger weights before evaluating an epoch. No durable
checkpoint, concurrent writer, external sink transaction or crash recovery is claimed here.
SQLite REAL comparisons need bounded numerical tolerance; financial DECIMAL equivalence is
a later typed-runtime gate. Stable sorting used for reproducibility must not be mistaken for
an SQL tie-breaking requirement. Unsupported SQL must remain visibly unsupported.

## 6. Acceptance criteria

* Preserve original `dbsp.py` and `test_dbsp.py` byte-for-byte.
* Meaningful assertions and seeded differential tests for all implemented operators, including
  simultaneous join updates, self-join, duplicate thresholds, NULLs, empty state and deletion.
* Representative query plans compare full bags and per-epoch deltas against SQLite using
  independently evaluated SQL, never the same callback interpreter for both paths.
  Every query needs a nonempty result and a later result-changing epoch; NULL-extension,
  duplicate thresholds, empty global initialization and window peer changes need witnesses.
* Reproducible command, Python/SQLite versions, source identities, seeds, counts, work counters,
  timing boundaries and explicit uncovered gates in a local generated report.
* Peer review the design before nontrivial prototype implementation; review the resulting
  change and run focused Python checks. No Rust code changes imply no new Rust validation claim.

## 7. Rollout / migration

Keep this as a local research artifact under the existing task branch. No push, deployment,
new dependencies or existing Rust-runtime changes are authorized by this experiment. Promote
operator algorithms into the typed runtime only with a separate implementation design.

## 8. Open questions

SQL expression compatibility, efficient range joins, complete window support, recursion under
deletions, durable epoch/sink coordination and a realistic state memory budget remain gates.
Record observed Python results separately from this proposed implementation path.

Design review: a separate read-only reviewer accepted this prototype scope on 2026-10-29
with no blocking findings. Its suggestions on output work, nonempty witnesses, ordering ties
and empty global aggregation are incorporated above. This is design approval only.
