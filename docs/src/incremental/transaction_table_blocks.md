# Immutable transaction table blocks

**Status:** accepted · 2026-10-31
**Scope:** ROADMAP R02b; immutable block snapshots and statement-local changed-block
updates. Persistent state directories, lazy engine caches and block checkpoint IO
remain R02c/R02d/R07a.

## 1. Problem

Every DML statement collects the complete target into a Bag, copies that full map,
sorts and materializes the complete target before the next statement. R01b exposes
this cost even for VALUES and net-zero updates. Transaction batching does not remove it.

## 2. Constraints and evidence

Preserve typed full-row multiplicities, NULL, exact schema metadata, old-row swaps,
statement snapshots, read-your-writes, atomic failure and all existing resource limits.
Do not add row identity/primary-key assumptions. Keep persisted manifest and single
batch IPC formats for now. Retain deterministic relation ordering where callers
previously observed Bag order; ORDER BY remains a separate semantic operator.

## 3. Options considered

Caching only one full Bag removes conversion but retains full map copying and Arrow
materialization on each statement. A fixed hash shard directory bounds directory
size but permits touched shards to grow with the whole table. Select sorted leaves
with a maximum number of distinct full-row entries and shared immutable payloads;
unchanged leaves retain both Bag and Arrow cache. Splitting an overfull touched leaf
bounds historical entry copying. Many duplicates of one row are a legitimate large
bucket; lazy expansion remains a separate optimization.

## 4. Decision

Introduce an internal TableRows snapshot with schema, checked total rows/bytes,
a sorted directory of shared immutable leaves and a per-snapshot flattened cache.
Leaves hold a Bag and its ordered Arrow batch; the directory key is the leaf's
lowest encoded complete row. Use existing Arrow row ordering and reject encoding
ties between unequal scalar rows as Bag currently does. Empty tables retain schema.
All leaf boundary and delta encodings share the snapshot's same `RowConverter`
instance, including dictionary encodings, as Arrow's comparison contract requires.
A candidate owns its new directory and shares all unchanged leaves. The initial
leaf bound is 256 distinct full-row entries, a bounded-work choice rather than a
claim of an optimal performance threshold. Expanded duplicate counts remain subject
to the existing global materialization limits.

Apply a consolidated statement delta by locating changed rows in the committed
leaf directory, grouping changes per touched leaf, updating each leaf once and
splitting changed leaves as needed. Validate exact schema, checked multiplicities,
final nonnegative rows and global row/byte budgets before publishing the candidate.
Deleted empty leaves disappear; no automatic full-table compaction occurs.
First compute all touched leaves' final Bags and checked total volume, then check
the final global budget before expanding replacement leaves. A cross-leaf key move
must not be rejected because its transient preparation order exceeds that budget.
Encode signed delta entries in batches bounded by 1024 entries and the scalar-byte
budget; a two-image delta can exceed either valid endpoint's volume. Final leaves
retain endpoint limits and scalar-identity checks.
An empty delta shares the entire table snapshot and performs no target conversion.
Schemas for which Arrow cannot construct a row converter retain a single-leaf mode:
the existing Bag materializer determines whether their final relation is admitted.
Do not introduce earlier rejection of an otherwise valid one-value relation, or
reject a two-image UPDATE whose final relation still has only one distinct value.
For ordered schemas, compare scalar identity when sort encodings tie, including
when a new key equals an existing leaf boundary.

Bind base-table MemTable providers to the ordered leaf batches, including measured
DML providers. Every selector sees one immutable snapshot. UPDATE old/new images
remain a single selector projection; all assignments read old values. Views inside
explicit transactions retain the existing refusal policy.

Read-time/import bootstrap may collect the full table once and construct leaves.
Store and view bootstrap boundaries that still need one RecordBatch explicitly
flatten the leaves; share a successful flattened cache within the same snapshot.
Use a synchronous mutex around each snapshot's optional flattened Arrow cache;
never hold it across await. Return a named materialization result carrying the
actual cache-miss work, so shared hits do not report another materialization.
Count actual store-side table materialization separately from statement updates.
R07a will remove complete changed-relation checkpoint writes; R02b must not claim it.
The initial R02b directory clone is proportional to leaf count and measured.
[Shared state directories](shared_state_directories.md) replaces it with actual
path copying in R02c; full checkpoint and bootstrap work remains.

Extend DML target diagnostics with touched/split/reused leaf and directory-copy
counts, and count historical entries actually copied/materialized at leaf boundaries.
The existing collection counters describe real Arrow-to-Bag collection (zero on
maintained statement targets); add separately named snapshot size diagnostics if
needed. Do not relabel total target size as actual input work. Recovery distinguishes
bootstrap collection from later statement work. Preserve optional old receipt fields.
New block, bootstrap and flatten diagnostics are optional on deserialization: absent
historical fields remain unknown. Live counters start at measured zero; interval
subtraction preserves unknown domains rather than fabricating zero measurements.

## 5. Failure modes and recovery

All fallible row conversion, Bag arithmetic, resource checks and new leaf creation
precede workspace replacement. Failed/discarded candidates cannot mutate shared
leaves or old flattened caches. A successful statement replaces only its workspace
snapshot; publication and acknowledgement protocols remain unchanged. Reopen reads
the existing full IPC payload and rebuilds blocks explicitly. No format migration.

## 6. Acceptance criteria

* Fixed one-row changes and fixed touched-leaf size with growing untouched leaves:
  no full target collection, no unchanged leaf payload copy/materialization.
* Measure remaining directory copying and full checkpoint materialization separately.
* Compare multiple statement transactions and every committed source/MV endpoint
  against independently constructed SQL results, including swaps, two-row key moves,
  duplicate old rows, NULL, inserted-then-deleted rows and rollback after failure.
* Exercise leaf first/last-key deletion, insert before first key, splits, empty and
  net-zero updates, exact scalar/schema ordering and all existing resource guards.
* Hold old snapshots across successful/failed updates and verify their contents.
* Retry/reopen, checksums, both publication barriers and process-death recovery pass.
* Default/Iceberg format/lint/tests, private rustdoc and mdBook pass. Rerun selected
  TPC SQL loops with source/work identities; this is not controlled performance proof.

## 7. Rollout / migration

Introduce and test the representation before switching all transaction consumers.
Keep R02b in progress until SQL integration and operation-boundary scaling tests
pass. Maintain public SQL/receipt imports and the current persisted format.

## 8. Review and remaining gates

Existing Bag code and scalar regressions establish the current ordering guard:
multiple unequal rows with equal Arrow sort encodings fail instead of exposing
HashMap order. Reuse that guard and preserve the no-converter singleton exception.
The 256-entry leaf bound limits copied historical distinct entries; high duplicate
multiplicity still expands on touched-leaf Arrow materialization. Directory cloning
and full serialized table writes remain explicitly outside that bound.

Review before implementation identified final-state vs delta-image admission as
critical: the delta may have several distinct unsupported values even when the
final target is a valid singleton. Update that single leaf using Bag arithmetic
before applying its existing materialization checks. Retain schema, row/byte limits
and old-snapshot isolation. Test this branch rather than infer it from common
integer/string workloads. No dependency or numerical-contract changes are needed.

Implementation gates remain the full acceptance list above. The design is not
implementation or performance evidence. Directory overlays and lazy duplicate
expansion remain separate increments.

Remaining work includes complete directory copies, MemTable batch-header clones,
full changed-table checkpoint concatenation/writes, full bootstrap/history conversion
and retained flattened caches. This representation does not establish end-to-end
delta complexity, bounded retained memory or a controlled performance improvement.
