# Demand-only maintained relation leaves

**Status:** accepted · 2026-10-31; graph-leaf stage implemented and locally verified
**Scope:** R02d graph-leaf stage after source-sharing commit `1f4e775`.
SQL table/provider laziness and complete R02d acceptance remain later stages.

## 1. Problem

Graph leaves bound copied distinct entries, but eagerly expand all duplicates on
each update. COUNT's constant input projection turns thousands of historical rows
into one high-weight entry; delta-only consumers do not need its full Arrow image.

## 2. Constraints and evidence

Preserve row/scalar identity, NULL, final global budgets, exact encoding ties,
original ordered bootstrap/replacement batches, cached window Before results,
isolated revisions and root contents finalized before prepare returns. SQL table
leaves and their current provider/work contracts stay eager in this stage.
No rule, dependency, persisted format or numerical-contract change.

## 3. Options considered

Splitting expanded duplicates still constructs unnecessary historical Arrow rows.
Select optional immutable leaf caches, with explicit graph/table cache policy.
Keep shared representation and candidate update logic; do not duplicate storage.

## 4. Decision

Graph bootstrap/replacement uses demand-only leaf policy; SQL table bootstrap keeps
its existing eager policy. A leaf retains its exact Bag, row/scalar volume and a
shared mutex-protected optional Arrow batch. Directory boundaries for lazy leaves
encode distinct rows with the revision's shared converter. Preserve Bag's exact
sort-encoding tie validation before any successful candidate; encoding at most the
bounded distinct leaf must not expand its historical duplicate multiplicity.

A graph update validates final budgets and leaf conversion identity but stores no
expanded Arrow leaf. Original bootstrap/replacement batches still populate complete
revision caches, preserving physical order. Root/snapshot/ordering consumers request
leaf batches only on a complete-cache miss; each leaf cache fills once and unchanged
leaves share their prior cache. No lock crosses await. Root remains finalized in
prepare. SQL table batches remain eager, so their existing selector counters do not
move to an unmeasured demand boundary in this stage.

Return named materialization work containing complete concatenation work and actual
leaf miss work. Add separate graph diagnostics for demanded leaf batches and lazy
leaf boundary encoding (distinct rows and Arrow buffer sizes). Existing caches
counters continue to mean update-time map copies/materialization. Do not add new
mandatory fields to deserialized historic work structures. Source/export and
bootstrap accounting retain their explicit exclusions; counters do not estimate
DataFusion internals or temporary scalar/sort allocation traffic.

Internal batch and same-contents APIs become fallible instead of suppressing cache
errors. SQL caller propagation preserves existing failures and publication atomicity.
SQL no-op comparison still uses its eager batches; weighted comparison and provider
laziness remain outstanding and must not be described as finished.

## 5. Failure modes and recovery

Every fallible identity/volume preflight occurs in a private candidate. A demanded
Arrow failure discards the candidate before publication. Old revisions stay valid.
Caches belong to exact immutable leaves/revisions; failed or stale candidates cannot
install another epoch. Existing checkpoint payloads/rebuild-on-reopen stay unchanged.

## 6. Acceptance criteria

* Change one row with fixed COUNT/SUM output, increasing unchanged rows and duplicate
  weight: zero update-time graph leaf expansion, bounded distinct validation and
  only necessary root leaf demand/concatenation.
* Explicit complete snapshots and ordering still read required full inputs; their
  actual leaf misses and cache hits are counted, not hidden behind delta metrics.
* SQL target block diagnostics stay unchanged; dictionary/float ties, unsupported
  singleton types, zero columns, NULL, cross-leaf moves and resource guards retain
  their behavior. Retained/discarded roots and independent multiset oracles pass.
* Default/Iceberg gates, private rustdoc, mdBook/report tools and fresh frozen SQL
  TPC witnesses pass. R02d remains in progress until SQL provider laziness and all
  other stated acceptance criteria are complete.

## 7. Rollout / migration

Commit the locally reviewed design before implementation. Preserve canonical
public diagnostic paths and old JSON absence semantics. Record this milestone
separately from full R02d, full-TPC and controlled performance qualification.

## 8. Local review

Reviewed graph Relation construction, update budgeting, shared row converters,
SnapshotRead/root demand and SQL batches()/same_contents() callers. Keeping eager
SQL policy prevents shifting selector/export conversion out of existing counters.
Fallible propagation avoids suppressing future cache errors. Distinct-row encoding
preserves pre-publication admission while removing duplicate-weight expansion.
This is local design/source review, not independent implementation verification.

## 9. Observed implementation evidence

`RowBlocks::from_relation_batch` now selects demand-only graph leaf caches; SQL
`from_batch` retains eager table leaves. Updated graph leaves store weighted Bags,
volume and distinct-row boundary validation. `RowMaterialization` returns named
complete-cache and leaf-miss work. Refresh diagnostics distinguish update-time
`caches`, `leaf_validation`, `demanded_leaves` and `complete_caches`. Old mandatory
serialized work shapes are unchanged. SQL batch/contents comparison errors now
propagate through typed Results instead of being suppressed.

At fixed SUM output and one inserted duplicate, 1024/8192 inputs produce 0/0
update-time expanded rows, 514/514 distinct leaf-validation rows and 1/1 demanded
leaf output rows plus one root concatenation. The COUNT constant-projection
regression failed before implementation (1541 update-time expanded rows at the
first size) and now asserts zero update-time expansion at both sizes, one demanded
output row, bounded validation and independent full SQL equality.

Nine row-block unit cases pass, including dual-policy randomized/identity/budget/
zero-column cases and two dedicated lazy-cache/demand-sharing cases. Full local
gates pass: 210 default tests (2 ignored), 224 Iceberg tests (3 ignored), fmt and
both Clippy configurations. Private rustdoc, mdBook and 38 report-tool tests pass.
Frozen default-binary TPC-H Q1/TPC-DS Q1 each pass strict planning, three SQL DML
rounds, independent SQL, reopen and delta replay; source/binary/report identities
are in `target/roadmap-r02d-leaf-live-20261031`. ROADMAP records exact commands,
logs and exclusions. This milestone does not complete SQL table/provider laziness,
R02d, full TPC coverage or controlled performance qualification.
