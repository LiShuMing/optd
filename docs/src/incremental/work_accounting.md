# Incremental work accounting and source-change consolidation

**Status:** accepted · 2026-10-31; R01a implemented and verified, R02a pending
**Scope:** ROADMAP R01a, R02a and their work-scope regressions. Other end-to-end
measurements remain explicit follow-ups R01b–R01d.

## 1. Problem

Bucket and accumulator counters describe local evaluation but omit copies of
state directories, complete source/relation rebuilds and Arrow materialization.
`SqlView::prepare` also rebuilds a source for each TableChange entry rather than
once for its consolidated atomic batch.

## 2. Constraints and evidence

Use explicit per-refresh accounting, never process/thread-global mutable counters.
Preserve atomic candidate isolation and invalid-input rejection. Do not change
CommitReport or checkpoint schemas. RefreshStats already serializes as an opaque
maintenance JSON value; additive accounting is diagnostic, not persisted state.
No dependency, lockfile or toolchain change.

## 3. Options considered

Infer costs from total state size: insufficient, since copies can happen multiple
times. Global allocation counters: misattribute concurrent asynchronous refreshes.
Instrument owned operation boundaries and pass a local accounting object: selected.

## 4. Decision

Add documented structured RefreshWork statistics to RefreshStats. Count directory
entries when directories are actually cloned, full-row entries when complete Bag
maps are rebuilt, and expanded rows plus Arrow array bytes when a complete changed
relation is materialized. Distinguish source relation work and intermediate/output
cache work. Zero means the instrumented operation did not occur, not that there
was no allocation or IO. Describe exclusions in the public type and reports:
DataFusion planning/internal buffers, affected-bucket payload updates, complete
replacement batches from physical expressions (including their cache collection),
SQL selector work, external source IO, checkpoint IO and recovery are separate
domains. Cache counters here measure full-map copies and Arrow materialization
caused by signed delta application; a zero does not qualify a replacement plan.

R02a consolidates all changes for each source before updating its maintained table
once. Validate table names, schemas and weighted input before skipping canceled
changes. Validate the final candidate relation after all changes: atomic batches
already permit canceling insert/retract pairs and do not require every intermediate
row count to be nonnegative. No-op batches still advance the epoch as before.
Preserve checked intermediate arithmetic as well: for each incoming nonzero row,
check `committed_weight + staged_weight + incoming_weight` in the existing batch
order before combining deltas. A later cancellation must not hide an overflow
that the old path rejected. This validation touches only incoming changed rows.

## 5. Failure modes and recovery

Checked counters must not wrap; instrumentation failure occurs during prepare,
before publication. A failed prepare never mutates committed tables, directories
or watermarks. Metrics belong only to their candidate; concurrent candidates do
not share counters. No stored-state migration is needed. Old opaque maintenance
JSON remains readable and must not be interpreted as having measured zero costs.

## 6. Acceptance criteria

* Compare actual source/cache rebuild counters for one changed row with growing
  untouched relations; confirm the existing whole-state costs are visible.
* Verify unchanged and canceled batches do not report source/cache rebuilding.
* Verify state-directory cloning separately from bucket payload sharing, including
  aggregate and keyed state and abandoned/error candidates.
* R02a groups split changes per source, preserves final results/signed deltas and
  rejection of wrong schemas, unknown tables, overflow and absent-row deletion.
* Keep semantic, work-bound and performance claims separate. Run default/Iceberg
  format, lint, focused tests and existing SQL/recovery suites appropriate to risk.

## 7. Rollout / migration

Expose additive diagnostics in the existing serialized refresh stats, keeping
public import paths through explicit re-exports. New APIs have doc comments.
Record verification and commits in ROADMAP. No push is part of this increment.

## 8. Open questions

Full selector/physical read measurement, byte-level map-copy allocation tracking,
checkpoint and recovery metrics, persistent directory overlays and lazy/chunked
caches remain separate tasks. These counters are not end-to-end complexity proof.

## Review checklist before implementation

The owner accepted the roadmap and sequential implementation. Source review
confirmed three distinct copy sites: eager candidate directories, per-operator
state preparation, and nonempty complete relation-map updates. Account for each
at its own boundary; do not estimate copies from touched-key counts. Bootstrap
and physical subexpression batches remain excluded explicitly. The consolidation
review identified schema validation and intermediate overflow as invariants even
for batches whose net change cancels; both require regression coverage.
