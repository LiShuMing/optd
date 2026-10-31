# Shared weighted raw-source state

**Status:** accepted · 2026-10-31; implementation pending
**Scope:** R02d source-sharing stage, after maintained-node cache commit `db822fa`.
Deferred duplicate-leaf expansion and complete R02d acceptance remain separate.

## 1. Problem

Raw sources still copy a complete Bag once per changed source. Materialized SQL
row blocks cannot replace them directly: an unread source can retain `i64::MAX`
positive multiplicity and later retract it without ever expanding an Arrow batch.
The stage-one regression protects this existing admission and overflow contract.

## 2. Constraints and evidence

Preserve exact ScalarValue full-row equality/hash, signed-prefix overflow checks,
final nonnegative counts, schema identity, source naming, canonical demanded output,
atomic candidates and the existing materialization budgets at actual demand.
Do not impose endpoint expansion limits merely to store positive raw weights.
No persisted format, dependency, rule eligibility or numeric-contract change.

## 3. Options considered

A positive ordered Arrow-leaf index imposes unnecessary encoding/expansion rules on
unread sources. A fixed hash shard still copies a growing unrelated shard. Select
a persistent point directory keyed by the complete row's 64-bit hash, with an exact
Bag collision bucket. The existing AVL directory shares untouched paths; the Bag
retains exact equality within hash collisions. Hash order is never output order.

## 4. Decision

Introduce an internal immutable `SourceState` below engine orchestration. Each
revision has schema, distinct cardinality, a shared directory of Arc<Bag> collision
buckets and an optional shared complete-Bag cache. Bootstrap collects once, builds
the index and may retain that original complete Bag for initial scans.

Use standard Rust hashing of the existing ScalarValue row identity; do not persist
hash keys or make them protocol identity. Equal rows must select the same bucket.
Keep collision equality exact. A test-only hash function forces collisions, proving
that a hash match is never treated as row equality. State is reconstructed from
existing source payloads on reopen, so hasher implementation changes require no
format migration.

For each incoming source change, validate exact schema and probe only incoming
rows against committed and staged weights before checked addition. Consolidate
all changes exactly as R02a, then update each touched hash bucket once. Reuse
untouched buckets and directory branches. Only final bucket relations must be
nonnegative. Empty net changes share the whole revision and skip source updates.
No Arrow encoding or expansion occurs during weighted state updates.

On an explicit source snapshot/export demand, construct and cache the complete
Bag from borrowed bucket entries; materialize only when the caller requires an
Arrow batch. Preserve canonical Bag ordering, materialization guards and errors.
Do not eagerly expand a SourceRead merely for accounting. Snapshot cache misses
return named actual collection work; graph execution records those misses, while
external checkpoint/export conversion remains a separately identified boundary.

Extend diagnostics with actual source updates, validation probes, touched buckets,
directory node copies and snapshot-Bag collections. Existing `sources.map_rebuilds`
now counts actual rebuilt bucket maps, and copied row entries count historical
entries in those buckets. A separate source-update count proves one consolidated
update per changed source. Preserve original receipt fields and unknown missing
historical measurements; do not relabel total state size as actual copied work.

## 5. Failure modes and recovery

All fallible Bag construction and checked arithmetic belongs to private candidates.
Old roots, full-Bag caches and independent candidates remain valid. Cache locks
never cross await. A failed/stale candidate is discarded by the existing protocol.
Snapshots rebuild from existing source payloads; native ingestion and durable
operator state are still R08/R07b. Hash collisions can enlarge necessary local
bucket work and are measured explicitly, not hidden under an O(delta) claim.

## 6. Acceptance criteria

* Fixed changes and fixed collision-bucket size with increasing unrelated rows:
  copied historical source entries remain bounded; directory copies follow AVL
  paths, and source-update count remains one per changed source.
* Compare deterministic mixed changes and retained/discarded roots with independent
  exact weighted maps. Force hash collisions, duplicate/NULL/key moves, absent
  deletes and prefix/final overflow. Never identify rows by hash alone.
* Preserve unused `i64::MAX` state and later retraction; demanded expansion still
  fails at the same resource boundary. Snapshot Bag cache hits collect zero again.
* Existing SQL, custom rules, staged-prefix, concurrency, retry/reopen and crash
  regressions pass in default/Iceberg configurations. fmt/Clippy/private rustdoc,
  mdBook, report tools and selected frozen-binary SQL TPC witnesses pass.
* Report remaining eager duplicate-leaf expansion, snapshot/export and checkpoint
  costs. This stage alone cannot mark R02d or performance qualification complete.

## 7. Rollout / migration

Commit this reviewed design first. Integrate through the existing source snapshot
bindings without changing selected incremental expressions. Update work-scope tests
from inferred complete-map costs to measured bucket work and source-update counts.
Keep R02d in progress through this stage and the later leaf-laziness stage.

## 8. Local review

Reviewed staged source validation, SourceRead's lazy Bag binding, bootstrap scans,
checkpoint exports and full-row HashMap equality. Using the same row Hash contract
with exact collision Bags preserves previously admitted scalar identities without
requiring Arrow row encodings. A full source snapshot must not masquerade as a
changed-row lookup; its actual collection is separately attributed. The only
claimed bound holds the affected collision bucket fixed, as required by R10a.
This records local source/design review, not independent review or implementation.
