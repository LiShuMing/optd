# Optd incremental plans and execution

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** accepted architecture · 2026-10-30 (owner requested the separated
architecture and reuse of Optd's expression framework)
**Scope:** replace implicit per-operator maintenance execution with explicit
Optd logical/physical plans; retain typed SQL, existing rule extensions and recovery.

## 1. Problem

`DeltaExpr` is bound to an original operator by its graph id. Scoped snapshots
hide key projection, index preparation, bucket lookup and relation evaluation in
the refresh driver. Adding an algorithm requires editing that driver.

## 2. Constraints and evidence

Reuse `optd-core::{PlanNode, NodeType, Rule, HeuristicsOptimizer}`. The existing
DataFusion representation does not cover this backend's complete typed SQL and
window contract; keep DataFusion expressions and schemas losslessly in operator
templates, with schema-only input placeholders. Add only the workspace-internal
`optd-core` dependency, without changing versions or the toolchain. Preserve
source bindings, ordering, strict mode, custom rules, candidate isolation,
checkpoint formats and independent full-SQL differential tests.

## 3. Options considered

1. Rename the existing interpreter or wrap opaque maintenance expressions in
   `PlanNode`: rejected; this leaves the hidden execution algorithm intact.
2. Convert SQL to the smaller legacy SQL/value representation: rejected; loses
   admitted expressions and types.
3. Extend the generic Optd node family with typed incremental operations,
   expand scoped snapshots through Optd rules, then lower to explicit physical
   operations: selected.

## 4. Decision

The existing rule registry remains a compatibility frontend. Its verified
maintenance expressions compile into `ArcPlanNode<IncrementalNode>`. These nodes
carry operator templates, typed versioned relation reads, key projections,
candidate index preparation, bucket lookups and state completion explicitly.
Optd rules expand scoped snapshot requests and implement logical operations.
Callers can register additional Optd logical transformation rules. Verify both
the expanded logical graph and physical graph before bootstrap. Explain both.

The physical executor receives only compiled operations, versioned bindings and
state slots. It cannot consult an original SQL graph node or select a rule.
Refresh orchestration handles source changes, dirty propagation, candidate
installation and statistics, without matching `Maintenance` or SQL operators
to choose an incremental implementation. DataFusion remains the relation kernel.

Outer/semi/anti joins explicitly apply their typed original operator to complete
Before/After affected buckets and subtract the outputs. This does not yet expand
outer join to inner/anti or introduce match counts. Window Before explicitly
reads cached output. Index preparation is shared across snapshot branches and
executes once. Preparation updates candidate indexes; actual bucket and cached
output materialization happens only in the corresponding physical lookup.
Committed indexes are borrowed immutably until completion. Group maintenance
exposes projection, preparation, lookup,
aggregate evaluation and completion separately. Ordered recomputation retains
the candidate sequence through an explicit replacement operation.

## 5. Failure modes and recovery

Reject malformed arity, unresolved groups, unsupported nodes, wrong schemas,
invalid state wiring, reference/version mismatches and incomplete lowering.
Logical extension rules are trusted algebraic transformations, not a proof of
SQL equivalence. No runtime fallback substitutes an implementation. Prepared
state remains private until acknowledge; reopen rebuilds derived indexes from
the compiled state specifications and committed inputs.

## 6. Acceptance criteria

* Actual plans use core Optd nodes and run core Optd transformation rules.
* Scoped plans expose key projection, index preparation, individual bucket
  lookups and typed relation evaluation; executable nodes carry their semantics.
* A custom Optd rule changes execution without modifying the refresh driver.
* The executor has no original-node lookup or `Maintenance`/`DeltaExpr` switch.
* Existing custom-rule, NULL/duplicate/residual outer/semi/anti, partitioned
  window/tie, grouped DISTINCT, strict, durability and Iceberg tests pass.
* New structural/rejection tests establish logical/physical separation and
  shared state preparation; differential tests retain work-scope assertions.
* Formatting, warnings-denied lint, default/feature tests and a fresh-target
  feature-library build are recorded separately. No performance claim.

## 7. Rollout / migration

Retain public maintenance descriptions as compatibility/diagnostic metadata;
only compiled Optd physical plans execute. Existing checkpoint formats remain.
This design supersedes the interpreter and no-new-dependency decisions in
`rule_rewrite.md`, and the opaque execution mechanism in `scoped_snapshots.md`.
Their SQL semantics and qualification constraints continue to apply.

## 8. Open questions

Cost-based choice, persistent indexes, outer-join match-count maintenance and
whole-refresh cache/source complexity are subsequent work, not acceptance claims.
