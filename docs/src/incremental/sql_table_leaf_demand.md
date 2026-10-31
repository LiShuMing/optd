# Lazy SQL table scans and weighted table comparison

**Status:** accepted · 2026-10-31; weighted comparison/root reuse and lazy table/provider increment verified locally
**Scope:** Final R02d SQL table/provider stage after graph-leaf commit `d31e71a`.
Checkpoint payload formats, native source ingestion and durable operator states
remain R07/R08. No rule-admission, numeric-contract or dependency changes.

## 1. Problem

SQL table leaves still expand during every write. SQL context registration also
walks all leaf batches even when the optimizer eliminates a read or the query only
requires a small prefix. Comparing candidate/committed tables currently consumes
eager Arrow batches. Graph-only laziness therefore cannot complete R02d.

## 2. Constraints and evidence

Preserve statement snapshots, old-row UPDATE, duplicate/NULL identity, exact
schemas, read-your-writes, prefix/final overflow and endpoint budgets. Never remove
semantic ordering or row bounds. Preserve epoch-local retry/publication/recovery.
Metadata-only provider registration must not allocate full table batches or walk
all stored leaves. Work that moves to a demand boundary must become measurable.

Existing DataFusion 55 interfaces have been inspected locally: TableProvider,
ExecutionPlan/StatisticsArgs, RecordBatchStream and limit/projection protocols.
MemTable's provider statistics are absent; its physical source reports exact row
and logical-null counts. Preserve the provider/logical-planning contract so CREATE
MV dependency discovery is not folded away by newly exposed logical statistics.
Do not require a new package. Offline metadata discovery uses the actual host
filter (`--filter-platform x86_64-unknown-linux-gnu`) because all-platform metadata
requests an uncached Android-only package; this does not change build dependencies.

## 3. Options considered

Creating a MemTable in scan() avoids unused-table work but still expands every
leaf before LIMIT can stop. A vector of all leaf handles also walks the directory
at registration. Select a cursor over immutable ordered leaves and a source whose
poll path materializes only consumed row ranges. Keep DataFusion responsible for
relational planning/execution outside this source boundary.

## 4. Decision

### Weighted table revisions

Switch production SQL table construction to the existing lazy row-leaf policy.
Retain final global budgets and distinct-row identity checks at candidate creation.
Account SQL leaf-boundary validation through a new optional diagnostic, preserving
unknown absence in old receipts. Keep an eager reference policy in tests if useful.

Cache exact expanded row count and per-column logical NULL counts in each revision;
update them by subtracting/replacing touched leaf metadata. Do not scan untouched
leaves for metadata. Derive weighted NULL semantics from the same admitted scalar
identity and compare independently with Arrow logical_nulls() for qualified types.
No estimate of scalar bytes may be labeled exact physical Arrow bytes.

Provide a canonical weighted row-range conversion below SQL. Select only the
requested positive multiplicity range and projected columns before expanded Arrow
allocation. Preserve exact complete-row order and zero-column row counts. Return
named actual work, including stored entries visited and emitted buffers/rows.
Validate offsets, projections, checked arithmetic and resource budgets explicitly.

### Provider and source

Add a focused SQL table-provider leaf module. It owns a RowBlocks revision and an
actual read-work counter; construction/schema/logical planning read metadata only.
A physical source has one bounded partition, an explicit projection and fetch,
exact row/NULL statistics when known, and unknown unmaterialized physical bytes.
Limit-prefix NULL statistics are unknown unless provable; never reuse full-table
NULL counts for a strict prefix. Keep provider-level statistics absent, as before.
The deliberate change from exact cached-memory byte sizes to unknown lazy byte
sizes is documented; it must not change results or fabricate bytes.

Implement an owned leaf cursor using the persistent directory's strict successor,
retaining the immutable root and only its current key/offset. Each stream poll
reads at most the remaining fetch and configured batch size. A large duplicate
weight must not force a complete leaf expansion for LIMIT 1. Projection, fetch,
partition validation, EOF/error and cancellation/drop behavior are explicit.
Physical with_fetch creates a source with the new cap, so a later optimizer fetch
cannot leave a stale uncapped underlying cursor. Do not spawn producer/background
work. Repeated executions own independent cursors and bind the same immutable epoch.

Reuse a leaf's existing complete Arrow cache where available. A range miss creates
only its requested batch, reports actual conversion and may fill the complete
leaf cache only when it actually materialized that complete leaf. Do not claim
that partial chunks populate a complete cache or that a cache hit allocates again.
New counters distinguish chunk visits, stored entries visited, cache hits and
actual row/byte materializations. Partial repeated reads may repeat partial work;
measure that honestly rather than adding an unbounded chunk-cache framework.

### Ownership and accounting

DML keeps its existing actual delivered-source counter after source projection/
fetch. Extend SelectorWork with optional table-read diagnostics. Extend target
work with optional distinct leaf-validation diagnostics. New current reports carry
measured zero; old absent domains remain unknown. No mandatory historical work
fields or payload format changes.

Plain SELECT returns optional base-table read diagnostics on SqlResponse; these
measure provider leaf/range work, not view IO or all SQL CPU. Context construction
for metadata-only DDL/recovery performs no table materialization. A named context
binding can expose its read counter to execution while retaining a simple context
adapter for planning-only callers. Document any excluded planning/bootstrap work.

StoreWork gets optional actual checkpoint leaf-demand and bootstrap-validation
counters. Merge RowMaterialization's leaf work at the store export boundary as well
as its existing complete concatenation work. Extend checked interval subtraction
and old-report tests for every new domain. Native source/checkpoint payload IO
remains separate; moving CPU work must not relabel full file writes as incremental.

### Equality and cancellation

Compare table contents through exact weighted rows, never expanded duplicates.
Skip identical immutable roots/subtrees/leaf payloads where possible; retain an
exact fallback across different split boundaries. No probabilistic hash match alone
proves content equality. Propagate all fallible identity/cache operations.

Retain the initial revision only for tables first changed in a transaction. When
its consolidated signed changes cancel to empty, restore that original row root;
this avoids scanning the entire table merely to discover a canceled transaction.
DML-only explicit transactions cannot change schema/table identity. Statement
snapshots and recorded statement work remain intact even when the final net change
cancels. No-op writes continue to reuse checkpoint descriptors exactly.

## 5. Failure modes and recovery

All budget/identity validation belongs to private candidates. Stream failures return
typed errors and cannot publish a transaction; failed explicit statements retain
the existing abort-until-rollback behavior. Counter overflow is checked atomically.
No mutex crosses await and no stream outlives its immutable snapshot ownership.
Retry/reopen/crash protocols and receipt identity remain unchanged. Old diagnostic
absence is not synthesized into measured zero during deserialization or replay.

## 6. Acceptance criteria

* Fixed small writes at growing untouched table sizes and duplicate weights:
  zero write-time expansion; bounded touched-row validation/copies; required full
  checkpoint concatenation/leaf expansion/writes measured separately.
* Unused registered tables and eliminated reads have zero leaf demand. LIMIT 1
  over many blocks or a high duplicate weight emits/materializes only its requested
  prefix; larger fetch/chunk sizes, projections and zero columns remain exact.
* Full and limited statistics, nullable/dictionary/nested logical-null behavior,
  repeated scans, scalar subqueries and filter+LIMIT combinations match independent
  DataFusion/Arrow oracles. Do not remove existing COUNT metadata optimizations.
* Canceled transaction roots reuse original payloads without full comparison or
  expansion; alternate leaf splits still compare exactly. NULL, key moves, update
  swaps, failed/stale/concurrent snapshots and old receipt absence are covered.
* Every demanded scan/export/bootstrap conversion is attributed to its actual
  domain. Work audit accepts all new optional fields and retains unknown history.
* Default/Iceberg fmt/Clippy/tests, private rustdoc, mdBook/report tools and fresh
  frozen SQL TPC witnesses pass. Full TPC/performance qualification stays separate.
* R02d may become verified only after its complete graph/source/SQL cache contract
  and these gates pass; partial provider construction is insufficient.

## 7. Rollout / migration

Commit this design before implementation. Implement weighted comparison/metadata/
range conversion, then lazy provider and complete demand accounting. Add RED
regressions at the current eager baseline before changing behavior. Preserve public
canonical diagnostic import paths and additive optional JSON compatibility.

## 8. Local design review

Reviewed SQL context/selector registration, physical source statistics and fetch,
RowBlocks' lazy graph work, transaction consolidation, store export/reuse and checked
work intervals. Scan-time eager batching would not satisfy prefix/unused-input
acceptance. An owned successor cursor and fetch-aware physical source avoid that
boundary error. Returning optional named diagnostics protects historical absence;
physical byte statistics must stay unknown until an actual batch exists. This is
local source/design review, not independent implementation or performance evidence.

## 9. Weighted comparison and cancellation increment

Table equality now skips shared aligned directory subtrees and compares exact Bag
payloads. Different tree/split shapes use an ordered distinct-entry iterator holding
at most two bounded leaf reference vectors; it never fills an Arrow leaf/full cache
or expands duplicate weights. Shape mismatch is inconclusive, not inequality. The
exact fallback may still visit an unchanged distinct-row prefix; no universal
logarithmic/changed-key comparison bound or comparison-CPU measurement is claimed.

TransactionWorkspace retains the original row root only for tables it touches and
restores it when consolidated signed changes become empty. This preserves table
identity, statement snapshots and statement work while making canceled final tables
share the committed revision. Two RED regressions established the previous eager
comparison's cache fills and canceled candidate's unnecessary flattening miss.
Both pass after implementation. A 240-step independent weighted-map oracle with
retained/discarded revisions and a dual-table COMMIT/reopen/history case also pass.

The local default/Iceberg gate passes 215/229 tests (2/3 ignored), formatting and
both Clippy configurations. Private rustdoc and mdBook pass. Fresh frozen-binary
TPC-H Q1/TPC-DS Q1 strict three-round SQL/reopen/output-log witnesses pass; exact
commands and identities are in ROADMAP and
`target/roadmap-r02d-table-reuse-live-20261031`. This increment adds no diagnostic
schema, so report-tool tests were not rerun; the live numeric work audit passed.

At the comparison/root-reuse milestone, SQL tables/providers remained eager.
The following increment implements the remaining table/provider stages; complete
R02d acceptance uses the verification gates recorded below and in ROADMAP.


## 10. Lazy table/provider implementation

Production tables now retain weighted lazy leaves, as graph caches already do.
Only replaced leaves contribute to maintained row and logical-NULL metadata.
NULL counts use Arrow logical validity on the already encoded distinct boundary
batch, weighted by the same canonical entries; no scalar NULL heuristic or
unchanged-leaf traversal is used. An eager policy remains test-only.

`Bag::range_batch` skips positive weights as scalars and projects selected rows
before allocating expanded output columns. `RowCursor` owns an immutable revision
and a strict-successor key/offset, without collecting leaf handles. A source poll
converts at most its remaining fetch/configured batch bound. Physical `with_fetch`
changes that source's cap; unpolled/dropped streams create no producer work.
Complete canonical leaf reads may populate the existing full cache. Partial or
projected misses do not impersonate full-cache fills. Partial misses may re-encode
at most one bounded leaf's distinct order each time; `ordering_rows` reports this,
while temporary sort/scalar buffer bytes and positive/count preflight visits
remain outside these counters. Preflight also touches only the same bounded
distinct leaf; range probes are not a total SQL CPU or memory-access measurement.

Provider/logical statistics remain absent, preserving CREATE MV dependency binding.
Physical statistics use maintained exact row/NULL totals and unknown physical
bytes. Strict-prefix NULL counts are unknown unless zero is provable. COUNT
metadata optimization is retained. SQL registration and planning retain metadata
and roots without traversing/expanding stored leaves.

Actual demand is reported through `RowReadWork`: leaf chunks, complete-cache hits,
weighted entries inspected, distinct ordering rows and newly materialized projected
batches/rows/Arrow bytes. SELECT exposes optional `SqlResponse.table_reads`; the
CLI labels it `base_table_reads`. DML's optional selector `table_reads` stays
separate from delivered source batches. Targets expose optional `leaf_validation`.
StoreWork separates optional `table_leaf_demand` during checkpoint export from
`table_leaf_validation` during restore, alongside full concatenation and file IO.
Current executions report measured zero; missing historical fields remain unknown.
Store interval arithmetic and complete counter events remain checked/atomic.

A RED test on the eager SQL factory observed 100,000 expanded bootstrap rows for
100,000 identical values. Lazy construction and the following one-row insertion
now each validate one distinct value and expand zero rows. Independent Arrow
NULL tests cover dictionary value/key NULLs, nested struct parent/child validity
and list NULL/empty values. Random weighted updates/ranges use an integer-vector
oracle; SQL cases use a separate DataFusion MemTable reference, and snapshot/retry/
reopen checks verify immutable epochs and diagnostic identities.

Verification commands, actual gate results, fixed-work measurements, fresh frozen
TPC witnesses and exclusions are recorded in ROADMAP. R02d status is gated there;
this implementation text does not itself certify full TPC coverage or performance.
