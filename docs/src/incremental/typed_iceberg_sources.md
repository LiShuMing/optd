# Typed Iceberg Source Vectors for Local SQL Epochs

**Status:** accepted after independent agent and integration review · 2026-10-29
**Scope:** an optional typed SQL source adapter over explicitly selected, retained
Iceberg v2/Parquet snapshots and the qualified local catalog; atomic publication
of a complete source vector with the local SQL checkpoint. No implementation,
CDC v1 extension, native changelog, remote output transaction or performance claim.

## 1. Problem

The typed SQL graph accepts atomic named-table changes and durably publishes
complete typed sources, output and signed output deltas. It does not yet bind
those tables to Iceberg identities or snapshot cursors. A caller can supply
correct changes, but a restart cannot currently prove which Iceberg source
snapshots those source caches represent.

The next increment should read exact typed before/after source endpoints, submit
one change batch for all query tables, and publish the selected source vector in
the same committed epoch as the result. A cursor written separately from CURRENT
would leave a crash window in which the cursor and SQL state disagree.

## 2. Constraints and current evidence

**Verified from the current checkout and exact locked dependency source:**

* Cargo.lock already locks DataFusion 55.0.0, Arrow 59.2.0 and Iceberg 0.10.1 at
  `5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3`. This proposal requires no version,
  source or MSRV migration. Optional package edges/features are a separately
  reviewable dependency increment before implementation.
* `optd-incremental/src/iceberg_source.rs::IcebergSource::new` rejects a table
  list whose length is not one. `observe` checks UUID, mainline, retained
  checkpoint and schema, then materializes exact endpoints. Its `batch_into`
  converts Decimal/Float values to strings and dates/timestamps to integers in
  the old Value representation. That conversion cannot serve typed SQL input.
* `LocalCatalog::load_table` reads an immutable metadata file named by a durable
  local pointer. Its ordinary `update_table` rejects removing retained snapshots
  and moving backward in sequence number. It does not offer a multi-table read
  transaction, a transaction-wide source-vector API, or distributed fencing.
* `LocalCatalog::reader_lock` is a cooperating local scan/maintenance pin, not
  an automatic retention lease on arbitrary source tables or external vacuums.
  Its path/UUID writer locks also do not establish cross-table consistency.
* In the locked Iceberg source, `TableScanBuilder::snapshot_id(id).build()`
  resolves that exact retained snapshot and calls `snapshot.schema(metadata)`.
  Omitting snapshot_id selects the current head. A snapshot lacking schema_id
  falls back to the current schema in `Snapshot::schema`; this ambiguity needs
  explicit admission below. `plan_files()` exposes complete scan tasks and
  attached delete descriptors; `to_arrow().await` produces typed Arrow batches.
  Upstream positional-delete tests exist, but were not run by this design task
  and are not this adapter's own acceptance evidence.
* `optd-incremental-sql/src/store.rs` currently recognizes only checkpoint
  format 1. Its Snapshot/Manifest contain SQL, epoch, source IPC batches, output,
  delta, file digests and the predecessor pointer; they contain no Iceberg
  cursor. Retry equality currently compares only those existing fields.
* `SqlView::prepare` already applies every table's change once against one
  immutable before vector and one candidate after vector. Group indexes are
  derived state rebuilt on restart. The adapter should reuse this boundary.

**Not verified:** a typed Iceberg adapter, its format-2 checkpoint, a multi-table
source fixture or its recovery. Existing typed SQL and single-source CDC passes
must not be presented as those future gates passing.

## 3. Options considered

1. Remove the old IcebergSource/CDC single-input guards. Reject: neither their
   value representation nor their committed protocol provides this typed vector.
2. Record external source cursors beside the existing checkpoint or in an
   independent cursor file. Reject: cursor/output atomicity would be unproven.
3. Read typed snapshot endpoints into the existing SQL graph, with a new,
   explicitly versioned vector-bearing checkpoint envelope. Select. Reuse
   Iceberg IO and the existing local publication boundary; retain old CDC v1.
4. Infer row changes from newly added data files alone. Reject for this first
   increment: overwrites, removed files, duplicate rows and delete files require
   exact complete endpoint semantics. Native changelog work is separate.

## 4. Decision

### 4.1 Separate adapter and optional dependency boundary

Introduce a new optional Iceberg input module in the typed SQL package. Reuse
`iceberg::Catalog`, `Table`, the pinned scan/Arrow API, and the existing
`LocalCatalog` as the first qualified catalog. Do not call the old
IcebergSource's Value conversion or route through the legacy optimizer bridge.
No old CDC source-generation identifier, envelope, table schema or single-input
admission rule changes.

This endpoint-vector protocol is separate from CDC v1. The
`cross_table_position = true` requirement in `change_capture.md` §4.1 remains
unchanged for multi-table CDC v1 views; an explicit tuple here is not evidence
that independently committed Iceberg snapshots satisfy that requirement.

An implementation dependency increment may add optional edges to the already
locked Iceberg/futures packages and to `optd-incremental` with its iceberg feature
for LocalCatalog reuse. It must document that this is IO-only reuse, keep the SQL
package's default graph independent, and prove that the Iceberg and DataFusion
RecordBatch types unify through the same Arrow 59 packages. Extracting the
catalog into another shared crate is not required by this proposal.

The initial source profile is local catalog, main reference, Iceberg v2 and
Parquet. Remote catalogs, tags/branches, schema evolution and external vacuum
coordination remain unsupported until separately qualified. All actual IO runs
inside the caller's live async runtime; no nested runtime or hidden polling loop.

### 4.2 Exact vector identity and consistency meaning

A caller supplies a complete, explicit target selection for every registered SQL
source name. Missing, extra, duplicate or substituted names are errors. A self
join has one source binding, even if the graph scans it more than once.

A versioned PinnedSourceVector contains a sorted map from SQL source name to:

| Field | Meaning |
| --- | --- |
| catalog binding | Qualified local catalog identity and canonical root; no credentials or signed URLs |
| table identifier | Namespace/name plus stable table UUID |
| reference | Exactly main for this increment |
| snapshot selection | Exact snapshot ID, or the qualified initial Empty state |
| snapshot sequence | Exact sequence number, paired with a nonempty snapshot ID |
| snapshot descriptor digest | Domain-separated hash of that immutable snapshot record, including parent and manifest-list identity |
| schema identity | Iceberg schema ID and canonical complete schema digest, including field IDs, order, types, requiredness and defaults |
| Arrow layout identity | Complete canonical Arrow source schema and a normalizer version |

Canonical domain-separated serialization, sorted keys and an explicit vector
version define the vector digest. A hash of currently observed table metadata is
not the cursor: ordinary head advancement changes metadata even when the
selected snapshot is unchanged. Optional observation locations belong in audit
information rather than logical cursor equality.

Empty is not snapshot zero, an arbitrary missing ID, or a nonempty snapshot with
zero rows. New Empty selections are admitted only when the table has no snapshot.
A committed initial Empty cursor remains a valid before endpoint after the first
append, using its bound initial schema. Once a source has a nonempty snapshot
cursor, resetting it to Empty is a rollback error. A zero-row overwrite retains
its real, nonempty snapshot ID.

The vector means "evaluate exactly this tuple of source states." Capturing or
accepting the tuple does not prove the states were committed in one upstream
transaction or existed simultaneously at wall-clock time. For example,
`{orders: s10, lineitem: s17}` remains a valid explicit selection even if those
snapshots were published at different times. A future convenience capture-heads
API must label its independent observations and freeze their IDs before IO;
it cannot advertise a transactionally consistent database snapshot.

No refresh may resolve a table's moving head during graph execution. New commits
arriving after selection do not enter this candidate. Concurrent source replacement
or schema mutation cannot be atomically excluded by a local output commit; the
adapter validates at admission and again on restart/next refresh. It must not
claim distributed source/output fencing across that remaining race.

### 4.3 Admission, lineage and typed scans

Resolve one immutable Table metadata observation per source. Validate the bound
catalog/table identity, UUID, source names and fixed schema before any candidate
publication. For an existing cursor:

1. Reconstruct and exactly compare its retained snapshot/sequence/descriptor and
   schema identity. A reused number on a different UUID is not the same state.
2. Require the requested target to equal that cursor or be its descendant on the
   observed mainline. Walk parent links, never HashMap iteration or numeric
   snapshot-ID ordering. Missing intermediate history means ancestry is unknown
   and is an error, not permission to assume a descendant.
3. Reject rollback, off-main snapshots, changed/reused snapshot descriptors,
   changed field IDs/order/names/types/requiredness/defaults, schema ID changes,
   and a changed current schema even when only unused query columns changed.
   This conservative first profile has no implicit schema migration.
4. For every nonempty admitted endpoint require an explicit snapshot schema ID.
   Do not rely on Snapshot::schema's current-schema fallback. An implementation
   may later qualify missing schema IDs under a separate explicit rule.

Read each endpoint with `scan().snapshot_id(id).select_all().build()` and typed
Arrow IO. Empty endpoints construct a correctly typed zero-row batch from their
bound schema; they never scan the new current head. Initially admit the actual
Arrow mappings needed by TPC inputs: Boolean, Int32/64, Utf8, Decimal128 with its
exact precision/scale, Date32, and microsecond time/timestamp representations
whose timezone exactly matches the pinned Iceberg type. Further types require
round-trip and difference tests before admission; reject unsupported fields even
if an SQL projection would ignore them.

Iceberg field IDs bind column correspondence. Establish a canonical Arrow layout
from the pinned Iceberg schema and compare every read batch with that layout.
Any explicitly permitted lossless physical representation normalization, such
as a string representation conversion, belongs to the versioned normalizer and
has direct tests. Never cast a decimal through float/string, truncate precision,
retag incompatible buffers or quietly fill an unknown required column. Required
fields must actually contain no NULL values.

Consume streams incrementally into the existing typed Bag and check cumulative
rows/bytes before concatenating or expanding a complete endpoint. The existing
1,000,000-row and 256 MiB safety budgets apply to the complete local source, not
separately to each streamed batch. Bound scan concurrency and report decoded
rows/bytes. An endpoint larger than the local budget is an explicit rejected
refresh, not a scale qualification result.

The first adapter qualifies append and complete-file overwrite snapshots with no
live position/equality delete files. Inspect the complete planned task set and
reject unqualified delete content before using its result. The dependency's
ability to read some delete files is not enough to mark this adapter qualified.
A later accepted profile may enable position/equality deletes after independent
whole-row oracle tests; it must use complete Iceberg scan semantics, never scan
only newly added data files or ignore attached deletes.

### 4.4 Full endpoint difference and one graph prepare

For each changed source cursor, read the complete before and target endpoint and
compute exact typed multiset subtraction:

```text
source_delta[name] = Bag(target_snapshot[name]) - Bag(before_snapshot[name])
```

The before bag must equal the committed source cache, including its full schema,
values, NULLs and duplicate multiplicities. A mismatch aborts. Convert only the
resulting signed, consolidated rows into TableChange and submit every named-table
change together through one `SqlView::prepare`. No graph node reads the catalog
or a moving provider behind this batch.

Unchanged cursors may reuse their verified committed source cache and report
zero endpoint scans. This exception does not make changed-source acquisition a
native changelog: changed tables still read two full endpoints. Intermediate
insert/delete events can cancel between endpoints; this is final-state snapshot
difference, not event-preserving CDC. Report changed source count, actual endpoint
reads/rows/bytes and resulting signed change rows independently of SQL strategy
counters and checkpoint bytes. No O(delta) or speedup claim follows.

Before accepting a candidate, compare its named source bags with every target
endpoint. A table whose snapshot changes but whose relation does not still
advances its cursor and commits an epoch, even if the SQL delta is empty. A
request for the identical already-committed vector returns Unchanged without
inventing a new source transition.

### 4.5 Cursor and output publication in CheckpointStore

Introduce an explicit checkpoint format 2 for Iceberg-backed typed epochs. The
new envelope contains the existing Snapshot plus the mandatory PinnedSourceVector.
Its immutable manifest binds a `source-vector.json` file by size and SHA-256,
just as it binds all named source IPC files, output, delta and predecessor.
The vector map and IPC map must have identical names and compatible schemas.
The first vector becomes immutable source-binding identity for that store.

There is exactly one publication sequence:

```text
read and validate complete pinned before/target vector
  -> full endpoint differences
  -> one typed graph prepare and source-cache verification
  -> write all source IPC / output / delta / source-vector / manifest
  -> fsync immutable files and epoch directory
  -> fsync temporary CURRENT, rename it, fsync store directory
  -> acknowledge graph and install exactly that committed vector
```

Do not update a cursor sidecar before or after this sequence. Source configuration
outside the store is only a requested binding, never commit authority. Expected
predecessor epoch and expected vector digest are checked under the existing
exclusive writer ownership. Retry equality includes the complete vector and
payload, not merely output rows; two different source vectors yielding the same
result are distinct commits. History follows only hash-bound committed lineage
and validates source-binding/schema stability and per-source monotone ancestry
where the retained source records are available.

Retain format-1 in-memory input checkpoints unchanged. Existing format-1-only
load/history APIs must reject format 2 rather than silently dropping its vector;
new source-aware APIs return the complete envelope. Existing ordinary commit may
not write an Iceberg-backed format-2 store, and source-aware commit may not adopt
a nonempty format-1 store. No mixed-mode or mixed-format lineage is automatic.
A source-aware output-log reader may intentionally inspect committed local
output without source IO; that read-only operation cannot resume the writer.

The public coordinator should own SqlView, CheckpointStore and the committed
source vector together. Prepared source refresh objects bind their exact before
vector, frozen target vector, scanned tables, change batch and work counters;
callers cannot independently acknowledge cursors or splice a fresh target into
an old prepared SQL candidate. Exact Rust API names and optional feature wiring
are implementation-review details, not permission to weaken these invariants.

### 4.6 Restart and retained source validation

Strict writer restart acquires the existing local store writer lock, validates
the format-2 envelope/checksums, and checks requested SQL and complete source
bindings against the committed vector. It reloads source metadata by identity
and rejects UUID/reference/schema/lineage divergence or missing required history.
It re-reads each exact committed endpoint and compares it with committed source
IPC; it never rebuilds using current heads. Then rebuild the SQL graph/group
indexes from committed sources and compare complete output, including ordered
presentation, with committed output before accepting another refresh.

This first restart mode requires retained, accessible sources. Cached output can
still be read independently, but source unavailability or expiry does not permit
a writer to invent a replacement cursor or silently rebootstrap from NOW.
Explicitly rebootstrap into a new store/generation after diagnosis if necessary.
No source vacuum, metadata expiration, orphan deletion or new retention worker
is introduced here. Pinning a snapshot ID is not itself a physical file-retention
lease; missing files fail the read without publication.

## 5. Failure modes and recovery

* Source IO/type/schema/identity/lineage error, stale before vector, resource
  limit or SQL prepare error: no cursor, SQL epoch or CURRENT advancement.
* Some tables finish reading and another fails: discard the entire prepared
  source vector. Never publish a partial named-table subset.
* Later head commits arrive during reads: this candidate remains at its frozen
  selections. They are eligible only for a later explicit vector.
* Crash before CURRENT publication: the old vector and output are authoritative;
  staged files are ignored. Re-observe the same requested target and retry from
  the old committed vector; no guessed partially advanced cursor.
* Crash/lost response after CURRENT publication but before in-memory acknowledge:
  reopen the committed target vector and output. A same-vector retry resolves
  that commit, and must not apply its endpoint difference as another source step.
* Cursor/file or cursor/schema substitution, malformed vector or changed hashes:
  fail closed. Rebuilding the SQL output alone cannot validate source identity.
* Replaced source with the same name, schema mutation, branch rollback or expiry:
  reject strict writer restart even if cached local IPC/output remains readable.
* Concurrency with uncoordinated external mutation: a frozen admitted vector can
  remain valid historical input, but the local commit does not fence an external
  writer. Verify identity again at subsequent admission/restart; report this
  guarantee separately from atomic local cursor/output publication.

## 6. Acceptance criteria

1. Optional dependency scaffold builds in its exact staged tree with locked
   Iceberg 0.10.1/Arrow 59.2/DataFusion 55/Rust 1.95; default typed SQL dependencies
   remain independent. Format-1 and old single-input CDC tests remain green.
2. Real local Iceberg v2 Parquet fixtures preserve typed Decimal/date/NULL/string
   values and duplicate counts across multiple record batches, empty snapshots,
   append, overwrite, deletion by whole-file replacement and identity-preserving
   physical rewrites. Unsupported types and delete-file profiles fail explicitly.
3. A two-table join receives changes on both sides in one vector, including a
   self join sharing one source. Compare source deltas, candidate source caches,
   maintained output and folded output deltas against hand-specified rows and an
   independent full SQL plan over separately scanned exact target snapshots.
4. Deterministic barriers advance one/both current heads after selection or
   between source scans. The candidate uses only the selected tuple; another
   selected mixed-time tuple is labelled as such, never a distributed snapshot.
5. Test missing/extra bindings, replaced UUID at the same identifier, reused
   snapshot IDs, schema/field-ID/order/requiredness/default changes, changed
   descriptor, rollback, off-main target, missing intermediate ancestry, expired
   snapshots and missing files. Every failure proves prior vector/output/CURRENT
   and in-memory state remain unchanged; no auto-migration or head fallback.
6. Real subprocess death before and after format-2 CURRENT publication recovers
   the exact committed vector, sources, ordered output and delta history, then
   completes another two-sided refresh. Include zero data delta with a changed
   cursor, lost acknowledgement, conflicting same-output/different-vector retry,
   actual held-writer rejection and source-cache mismatch on strict restart.
7. Independently fold the full committed format-2 output log; verify monotonically
   advancing per-source cursors and stable bindings. Corrupt cursor JSON/digests,
   omit its manifest entry, splice another epoch's cursor, and attempt both
   format/mode substitutions; all must be rejected before writer continuation.
8. Qualify TPC-H/TPC-DS on generated typed Iceberg tables using unchanged query
   SQL and the same explicit source-vector rules. Record exact query/data/input
   vector identities, nonempty/change evidence and SQL strategies. Tiny fixture
   passes are not scale/runtime performance or distributed transaction evidence.
9. Formatting, scoped warnings-denied lint and relevant default/feature tests
   pass with actual commands/results. Process tests retain the reviewed test-only
   fork/file-lock isolation; no production ownership retries are added to make
   the tests pass.

## 7. Rollout / migration

This document is a proposal only. First review source bindings, the strict type
profile and format-2 compatibility decisions. Then implement/verify in separate
lanes: dependency scaffold, typed scan/endpoint adapter, source-aware store
format, coordinator/restart, and independent process/query acceptance. Integrate
only after each stage's observed results exist. Root owns shared manifests,
engine/store changes and the design index; this design task edits none of them.

Existing stores remain format 1 and existing CDC outputs remain CDC v1. A new
Iceberg-backed store starts at epoch zero with one explicit complete vector.
Any requested migration of an existing memory-input store needs its own explicit
verification and review; this proposal does not infer external cursor identities
from local Arrow files.

## 8. Open questions for review

* Confirm the initial strict profile: local/main/v2/Parquet, explicit snapshot
  schema IDs, fixed schema, no live delete files, and the listed TPC scalar types.
* Confirm strict online writer restart, including full exact-endpoint re-read,
  rather than a cache-only restart followed by best-effort source validation.
* Confirm separate format-2 source-aware APIs and fail-closed format/mode
  admission, while retaining existing format-1 memory APIs unchanged.
* Decide optional dependency wiring for LocalCatalog reuse before implementation;
  no dependency or format change is authorized by this proposed document alone.
