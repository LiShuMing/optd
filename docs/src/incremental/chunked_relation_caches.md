# Chunked maintained relations and lazy complete caches

**Status:** accepted · 2026-10-31; implementation pending
**Scope:** ROADMAP R02d. Reuse immutable row blocks for positive raw sources and
operator relations; keep signed transient changes as Bags. No persisted format,
rule admission, ordering, numeric-contract or dependency change.

## 1. Problem

R02c removes complete state-directory copies, but each nonempty source/cache change
still copies a complete Bag. Every dirty delta node sorts/expands a complete Arrow
cache, even when its consumer only needs signed changes or affected-key indexes.
R01 accounting and fixed-delta tests make these remaining costs explicit.

## 2. Constraints and evidence

Preserve exact row multiplicities, NULL, schema/scalar identity, final resource
guards, staged source-overflow checks, cached window Before outputs, physical
sequence for ordered nodes, strict custom-rule execution, candidate isolation and
public complete-result APIs. Root output and complete snapshots may legitimately
require complete materialization. Instrument those demands separately from leaf
updates; deferred errors may not escape successful prepare/publication.

## 3. Options considered

A lazy Arrow batch alone retains complete Bag copies. A hash-sharded Bag has
unbounded touched shards and introduces separate scalar-identity questions.
Reuse the ordered bounded row leaves already qualified for SQL transaction tables,
sharing the representation below the SQL layer. Do not copy the implementation
or import SQL/DML planning responsibilities into the physical relation module.

## 4. Decision

Extract an internal `RowBlocks` representation from transaction `TableRows`.
The SQL adapter retains its public diagnostics and behavior. Move reusable block,
bootstrap and materialization work types to the state/work layer and preserve old
public import paths using explicit re-exports/adapters. No whole-map conversion is
needed merely to pass changes between source, graph and SQL ownership layers.

Use RowBlocks for positive raw sources and maintained relations. Signed incoming,
intermediate and outgoing deltas remain Bags. Probe staged source additions only
for incoming rows through row-block lookup, retaining ordered checked arithmetic
before net consolidation. Apply a nonempty net delta once per source/node and
measure actual touched-leaf copies, splits, encoding and directory work. Retain all
R02b final-global-budget and shared-converter safeguards.

A relation's complete Arrow cache is optional and shared within that immutable
revision. Dirty delta nodes update leaves and defer complete concatenation until
an explicit snapshot, ordered consumer, checkpoint or root-result demand. Preserve
an original physical batch for replacement/bootstrap nodes so ORDER BY and peers
retain actual sequence. Compare ordered Before/After sequences when propagation
requires it. Finalize the root before returning a prepared refresh; its public
borrowed contents API remains infallible. Snapshot operations explicitly request
materialization and record misses. Repeated demands share the cache.

Deliver in two reviewable stages within R02d: first shared positive blocks and lazy
complete concatenation, then deferred/chunked leaf caches and duplicate expansion.
The first stage retains eager touched-leaf batches, their measured cost and the
existing SQL provider behavior; it cannot mark R02d verified. The second stage
must preserve provider laziness and account actual leaf cache misses instead of
materializing unused registered tables. No invented zero or hidden complete Bag
rebuild is permitted to make either stage's counters look bounded.

## 5. Failure modes and recovery

All candidate leaves and final root materialization validate before prepare returns.
Old snapshots remain unchanged. A private cache may memoize the same immutable
revision, never another epoch; no lock crosses await. Shared cache hits do not
repeat actual work. Existing checkpoints still write complete source/result batches
and rebuild derived state on restore. A cache does not certify durable publication.

## 6. Acceptance criteria

* Fixed changes and bounded touched buckets with growing untouched rows: source and
  intermediate caches copy bounded historical leaf entries, not whole Bags. Report
  lookup/directory/split/leaf/flatten costs and required root output independently.
* Linear pipelines and qualified group/join/window plans must not concatenate
  unrequested complete intermediate relations. Repeated snapshots flatten once.
* Duplicates, NULL, moves across leaves, unsupported singleton/tied encodings,
  final/global volume budgets and signed-prefix overflow retain exact behavior.
* Preserve original SQL ORDER BY/LIMIT membership, aliases, hidden sort-key changes,
  window peers, custom rules and complete Before/After snapshots across epochs.
* Independent complete SQL and delta-prefix oracles, failed/concurrent/stale
  candidates, retry/reopen and process-death recovery pass in default/Iceberg modes.
* fmt/Clippy/private rustdoc, mdBook and report-tool tests pass; selected current
  SQL TPC loops retain result/strict-plan/work identities. Full TPC and controlled
  performance qualification remain separate roadmap requirements.
* R02d stays in progress while eager duplicate-leaf expansion or unaccounted
  provider materialization prevents the complete stated acceptance contract.

## 7. Rollout / migration

Commit the design before extracting and integrating shared row storage. Preserve
SQL APIs, persisted formats and canonical diagnostic import paths. Record partial
stage evidence without relabeling it as completion of R02d or the whole roadmap.

## 8. Local design review

Reviewed engine preparation, physical SnapshotRead/SourceRead, RelationValue and
checkpoint export. Root contents currently returns a borrowed batch, requiring
explicit finalization before returning a candidate. Ordered caches carry physical
sequence and cannot be replaced solely by a sorted multiset. Staged source
arithmetic must probe historical exact row weights before consolidation, including
negative intermediate prefixes and representable extreme cancellation. Existing
RowBlocks' shared converter and two-phase final-budget checks remain mandatory.
This is local source/design review, not independent review or implementation proof.
