# SQL DML execution work accounting

**Status:** accepted · 2026-10-31; implementation pending
**Scope:** ROADMAP R01b. Diagnostic accounting only; transaction representation,
source IO, durable store work and recovery computation remain separate increments.

## 1. Problem

`source_relation_rows` sums the available rows of distinct logical dependencies.
It does not measure optimized-away scans, repeated scan ports, LIMIT consumption
or target rebuilds. SQL DML must expose actual source consumption separately.

## 2. Constraints and evidence

Preserve statement snapshots, old/new UPDATE images, duplicates, NULLs, strict
view planning and transaction atomicity. Existing receipt JSON remains readable.
No process/thread-global mutable counters and no source reexecution to obtain
statistics. Preserve unrelated comment edits in `dml_compiler.rs`.

## 3. Options considered

Logical cardinality estimates cannot establish actual work. DataFusion's memory
sources do not retain suitable output metrics by default. Replacing providers or
adding predicates to count rows would change optimization or add row work.
Wrap physical source streams after planning, with statement-local counters.

## 4. Decision

Introduce `SelectorWork`, `DmlTargetWork` and `DmlExecutionWork` diagnostics.
Add `DmlWork.execution: Option<DmlExecutionWork>` with a serde default: historical
receipts keep `None` (unknown); new successful DML has measured values. Identified
retries retain their original measurements and keep `replayed=true`.

Create the normal optimized physical selector plan once. Add transparent wrappers
at `DataSourceExec` nodes after physical optimization. Count every successful batch
delivered through these stream boundaries, its rows and Arrow array buffer bytes.
Count repeated scan executions separately. Optimized-away sources count zero.
LIMIT counts only delivered batches/rows, possibly more than final selected rows.
The count is neither disk IO nor CPU row visits inside a DataFusion operator.
No asynchronous side task, global counter or retained batch copy is introduced.
Checked statement-local counters use a short mutex only when a batch is yielded.
Planning and physical expression evaluation remain DataFusion responsibilities.

Record selector output batches/rows, old/new image rows, target input rows and
distinct entries collected into a Bag, then actual full-map copy and final Arrow
materialization at their existing boundaries. Reuse `RelationWork` with additive
Deserialize support. Counter failures reject preparation before publication.

The existing `futures` dependency becomes unconditional to implement Stream and
poll the existing DataFusion stream. It is already resolved transitively in both
configurations; retain version/features and the lockfile. Record this manifest
adjustment separately with its reason; no new package is introduced.

## 5. Failure modes and recovery

Each selector collection owns its counter; concurrent sessions cannot mix work.
Failed statements never publish their table replacement or diagnostic receipt.
Pending batches and errors pass through with their normal polling behavior.
Dropping a stream drops its input without introducing a producer task.
Legacy receipt measurements stay absent, including after retry and reopen.

## 6. Acceptance criteria

* VALUES reads no relation; a selective predicate distinguishes source rows from
  output rows; constant false and COUNT statistics optimization can remove reads.
* LIMIT and repeated scan ports report delivered batches/rows; unit tests cover
  zero-column batches, Pending/error/end and arithmetic overflow.
* At fixed affected rows and growing untouched input, expose current target work.
* Swap assignments, duplicates, NULLs, explicit transaction read-your-writes,
  failed preparation, retry/reopen and output-history verification still work.
* Default/Iceberg format, lint and SQL/recovery tests plus private rustdoc pass.

## 7. Rollout / migration

No durable format version or dependency version changes. New optional diagnostic
fields are backward readable. Source row count semantics are documented on the
public types; do not interpret Arrow buffer bytes as actual allocated bytes.

## 8. Open questions

Per-table attribution after scan fusion, full DataFusion internal allocations,
selector planning costs and end-to-end performance remain separate tasks.

## Implementation review

The source API review confirmed that memory-source metrics cannot supply these
counts and that wrapping after optimization preserves the chosen physical plan.
Do not add a consumer task or materialize a source to count it. Checked counters
must fail preparation, and old receipt JSON must deserialize without fabrication.
The only manifest change exposes the already-resolved Stream API directly.

## Dependency verification

Rust 1.95.0, offline locked checks for `optd-incremental-sql` default and Iceberg
both exited 0 using the ROADMAP environment and project target directory. Logs:
`target/roadmap-r01b-dependency-20261031.log` and
`target/roadmap-r01b-dependency-iceberg-20261031.log`. Only `futures` changes from
an optional direct dependency to an unconditional one; its version/features and
Cargo.lock remain unchanged. This is build evidence, not implementation acceptance.
