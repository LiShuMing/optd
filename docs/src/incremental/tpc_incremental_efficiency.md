# TPC relational-core incremental maintenance

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** accepted direction · 2026-10-30 (owner requested known-issue fixes,
explicit removal of excluded presentation operators and greater incremental coverage).
Implementation and observed acceptance are recorded separately in the
[qualification report](tpc_incremental_efficiency_qualification.md).
**Scope:** auditable benchmark variants, structured plan/work qualification,
indexed inner-join deltas and safe aggregate-state improvements. No standard TPC
performance claim, source-CDC claim or arbitrary removal of internal ordering.

## 1. Problem

The latest compatible-mode witnesses execute complete-node recomputation in all
121 selected queries. Inner joins read full peer snapshots; aggregate eligibility
rejects qualified key expressions wrapped in aliases; report work is debug text.
Presentation operators obscure relational maintenance coverage.

## 2. Constraints and evidence

Preserve original SQL/manifests and existing recovery/output semantics. A separate
benchmark mode may remove only the outer query's ORDER BY and row bounds together;
its output is the relational core, not the original Top-K result. Do not remove
window ordering, subquery row bounds or internal ranking. Both incremental and
independent full-SQL executions use the same explicitly recorded variant.

## 3. Options considered

Silently editing reference SQL or dropping ordering inside the production planner
would change its contract: rejected. Keep full snapshots as the only inner-join
implementation: rejected. Reuse the existing explicit index and signed-join
operations, with independently verified dependency scopes: selected.

## 4. Decision

Add a benchmark-only parsed-SQL relational-core mode with original/executed hashes
and removed-clause metadata. Retain debug statistics for compatibility and add
structured statistics and every planned complete-recompute blocker, independently
of which nodes happen to run. Report strict-plan eligibility separately from exact
snapshot reopening and semantic pass.

Qualified inner joins use delta-left joined with indexed Before-right plus indexed
After-left joined with delta-right. Prepare a shared union-of-changed-keys index
once; expose actual reads at IndexLookup, not snapshot evaluation. Preserve the
unindexed algebraic rule for joins lacking qualified equality keys.

Aggregate qualification peels semantically transparent aliases. Qualified builtin
COUNT, exact integer/decimal SUM and decimal AVG use retractable DataFusion
accumulator states, retaining group cardinality and serialized accumulator cells,
not historical group input rows. Global aggregates retain one group even at zero
input rows. Floating arithmetic, DISTINCT/FILTER/ordered aggregates and mixed
unsupported functions retain explicit existing paths. Expressions and builtin
implementation identity are qualified before planning.

Bag clones share immutable row storage. Source changes are consolidated once
and bound directly to SourceRead; unchanged sources are not diffed or validated
again. Empty output deltas preserve existing materializations. Updated full
relation maps/materializations and derived index-directory clones remain costs.

## 5. Failure modes and recovery

Malformed or unqualified index reads reject planning. SQL variant generation uses
the parser, preserves nested query semantics and rejects non-query statements.
Reopen reconstructs qualified derived indexes using existing committed sources;
checkpoint formats and publication authority remain unchanged.

## 6. Acceptance criteria

* Focused differential tests verify simultaneous inserts/deletes, duplicates,
  NULL and residual join predicates, cross terms, candidate isolation and reopen.
* Fixed changes with many unchanged keys read only affected peer buckets.
* Query-mode tests prove internal ORDER BY/LIMIT and window ordering are retained;
  output metadata distinguishes original SQL and altered relational-core semantics.
* Structured reports expose all planned recompute blockers and actual per-node work.
* Default/Iceberg tests, formatting and warnings-denied lint pass; local TPC loops
  compare both result bags and signed changes against independent full SQL.
* Report all remaining complete recomputation and whole-cache/source/checkpoint
  costs; do not infer end-to-end delta complexity from bucket-read counters.

## 7. Rollout / migration

Default production SQL behavior stays exact. Relational-core is an explicit
benchmark selection and cannot be aggregated as unchanged original-SQL coverage.

## 8. Open questions

Persistent state, eliminating full-map copies, incremental source acquisition,
range/global dependency algorithms and cost-based alternatives remain independently
qualified work; this increment must not describe them as completed.
