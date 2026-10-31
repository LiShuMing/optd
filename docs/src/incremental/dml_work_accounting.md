# SQL DML execution work accounting

**Status:** accepted · 2026-10-31; implementation verified (ROADMAP R01b)
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
sources do not retain suitable output metrics by default. Adding predicates to
count rows would add row work. Wrapping all physical `DataSourceExec` nodes also
counts VALUES constants as relation reads and misses physical expression subqueries.
Delegate snapshot MemTable providers and wrap only their actual scans, sharing a
counter owned by the single statement. Preserve source properties, statistics,
filter/projection requests and fetch behavior explicitly.

## 4. Decision

Introduce `SelectorWork`, `DmlTargetWork` and `DmlExecutionWork` diagnostics.
Add `DmlWork.execution: Option<DmlExecutionWork>` with a serde default: historical
receipts keep `None` (unknown); new successful DML has measured values. Identified
retries retain their original measurements and keep `replayed=true`.

Create the optimized physical selector plan once using delegated snapshot providers.
Add a pass-through wrapper at each provider's scan result. Count every successful batch
delivered through these stream boundaries, its rows and Arrow array buffer bytes.
Count repeated scan executions separately. Optimized-away sources count zero.
LIMIT counts only delivered batches/rows, possibly more than final selected rows.
The count is neither disk IO nor CPU row visits inside a DataFusion operator.
No asynchronous side task, global counter or retained batch copy is introduced.
Checked statement-local counters use a short mutex only when a batch is yielded.
Planning and physical expression evaluation remain DataFusion responsibilities.
Physical expression subqueries use the same registered providers and statement
counter; retain DataFusion's existing subquery optimization setting. Tests check
both selected rows and consumption, including branches mixing constants and scans.

Record selector output batches/rows, old/new image rows, target input rows and
distinct entries collected into a Bag, then actual full-map copy and final Arrow
materialization at their existing boundaries. Reuse `RelationWork` with additive
Deserialize support. Counter failures reject preparation before publication.

The existing `futures` dependency becomes unconditional to implement Stream and
poll the existing DataFusion stream. It is already resolved transitively in both
configurations; retain its version/features. Add the already-resolved `async-trait`
0.1.92 as a direct dependency to implement DataFusion's object-safe table-provider
API. Cargo.lock adds only this dependency edge to this crate; no resolved package,
version, feature or checksum changes. Record manifest adjustments separately with
their reasons. No new resolved package is introduced.

## 5. Failure modes and recovery

Each consumed DmlCompiler owns its selector/counter; concurrent statements cannot
mix work. A compiler cannot be reused for a second statement.
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
counts. Stream regression found that generic physical-source classification also
counts VALUES: move instrumentation to provider-owned scans. Delegate statistics
and fetch to preserve COUNT scan elimination and LIMIT pushdown. Do not add a
consumer task or materialize a source to count it. Checked counters
must fail preparation, and old receipt JSON must deserialize without fabrication.
The manifest changes expose the already-resolved Stream and async provider APIs.

## Dependency verification

Rust 1.95.0, offline locked checks for `optd-incremental-sql` default and Iceberg
both exited 0 using the ROADMAP environment and project target directory. Logs:
`target/roadmap-r01b-dependency-20261031.log` and
`target/roadmap-r01b-dependency-iceberg-20261031.log`. The first checks validate
unconditional `futures`; subsequent locked checks validate the direct async-trait
edge. No resolved package identity changes. This is build evidence, not
implementation acceptance.

Provider dependency checks: `cargo +1.95.0 check --offline --locked -p
optd-incremental-sql` and the same command with `--features iceberg` both exited 0
in the ROADMAP environment. Logs: `target/roadmap-r01b-provider-dependency-20261031.log`
and `target/roadmap-r01b-provider-dependency-iceberg-20261031.log`. Lockfile review
shows exactly one added direct `async-trait` reference for this crate.
