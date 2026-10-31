# Optd incremental plans — observed qualification

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** verified local implementation, tests and small-data query suites · 2026-10-30
**Scope:** explicit Optd logical/physical maintenance plans, typed SQL execution,
state-access separation and local reconstruction. No performance certification.

## Implementation

The [accepted architecture](optd_plan_execution.md) is implemented using
`optd-core::PlanNode`, `NodeType`, `Rule` and `HeuristicsOptimizer`. The only new
dependency edge is the existing workspace `optd-core` package; no pinned version
or toolchain change. Local commits are `988fcac` (dependency/lock manifest) and
`e27cb01` (implementation/tests). Documents remain local for review.

`planner/plan.rs` defines typed incremental nodes, operator templates, explicit
state declarations and physical instructions. `planner/lower.rs` compiles the
compatibility rule frontend. `planner/optd_rules.rs` contains real Optd snapshot
expansion and implementation rules. `planner/verify_plan.rs` checks phase, types,
ports, versioned bindings and shared state wiring before bootstrap. The public
`with_plan_rule` extension accepts ordinary Optd logical transformation rules.

`executor.rs` receives physical instructions and immutable versioned bindings.
It has no original SQL-node lookup, maintenance-expression interpretation or rule
selection. `engine.rs` orchestrates source changes, dirty propagation, candidates
and acknowledgement. Relation kernels retain SQL expressions and qualified
schemas in templates whose inputs are schema-only placeholders. Actual affected
bucket/output reads occur at physical lookup operations, not index preparation.
Group preparation retains early row/byte budget checks without Arrow expansion.

Outer/semi/anti joins retain affected-key Before/After differences; window Before
reads actual committed output. This is not inner/anti expansion or match-count
maintenance. The existing grouped-recompute contract is retained. Full relation
caches, endpoint scans, copying/materialization and checkpoint writes remain real
costs. Planning is currently deterministic heuristic rewriting, not cost-based
algorithm selection.

## Tests

Seven new `tests/optd_plans.rs` cases verify all seven outer/semi/anti variants,
explicit lookup/kernel nodes, one shared index preparation, typed closed kernels,
logical/physical separation, window cached-Before, group state operations, ordered
replacement, six malformed-rule profiles, custom rule execution and durable
recompilation. A standard Optd rule expands the inner-join formula to three terms;
simultaneous insertions/deletions and cross terms match independently planned SQL
and folded signed deltas. An intentionally non-equivalent empty-delta transform
proves the physical runtime follows the transformed program. That case is an
execution-control test, not a claim of arbitrary extension correctness.

The retained scoped-snapshot tests check duplicate/NULL/residual join conditions,
first/last matches, two-sided changes, partition movement, window peers, candidate
isolation, actual bucket work and strict refusal. Durable and real local Iceberg
endpoint tests continue to pass, including refresh after reopen.

## Final observed gates

Environment: Linux x86_64, Rust/Cargo 1.95.0, pinned DataFusion 55/Arrow 59.2,
offline dependency resolution, four build jobs, debug info and incremental
compilation disabled. Commands use `bash /tmp/optd-sql-env-20261029.sh`.

| Actual command / artifact | Observed result |
| --- | --- |
| `scripts/agent-check.sh sql` | Exit 0; default 127 passed / 1 ignored; Iceberg 141 passed / 2 ignored. Workspace formatting and both all-target warnings-denied Clippy profiles passed. Log: `/tmp/optd-explicit-plan-qualified-gates-20261030.log`. |
| `env CARGO_TARGET_DIR=/home/admin/xwork/optd/target/ivm-optd-plan-final-fresh-20261030-85384dc cargo +1.95.0 check --offline --locked -p optd-incremental-sql --features iceberg --lib` | Exit 0 in an initially absent target, 2m38s. The final package source was saved before its compilation. This is a fresh feature-library check, not a fresh all-target test build. Log: `/tmp/optd-explicit-plan-final-fresh-check-20261030.log`. |
| `mdbook build /tmp/optd-explicit-plan-book-source-20261030` with isolated output | Exit 0; tracked/owned documents only, preserving and excluding the unrelated Python/DBSP links in the isolated copy. Log: `/tmp/optd-explicit-plan-book-20261030.log`. |
| `cargo +1.95.0 build --offline --locked -p optd-incremental-sql --features iceberg --example benchmark --message-format=json` | Exit 0; compiler-artifact reports `features=["iceberg"]`. Artifact log: `/tmp/optd-explicit-plan-benchmark-artifacts-20261030.jsonl`. |

`/tmp/optd-explicit-plan-source-digests-20261030.json` records 37 source/test and
manifest hashes from base `85384dc` plus the qualified changes. All hashes were
rechecked after the local commits; commits do not change the executed source.
The read-only frozen executable is `/tmp/optd-explicit-plan-benchmark-20261030`,
SHA-256 `8df8b961af14f213a0e35435bb0a1927c77dcfe73b7a8532de93d0c86f81816d`.
Identity/features are recorded in `/tmp/optd-explicit-plan-binary-20261030.json`.

## Retained failures and corrections

The first extended test gate failed because a new execution-control test compared
unordered physical row sequences. The test now compares row multiplicities,
consistent with SQL without ORDER BY; ordered-result assertions remain exact.
Failure log: `/tmp/optd-explicit-plan-sql-final-20261030.log`.

Deferring group materialization initially deferred the existing resource-budget
rejection too. The unchanged `changed_group_materialization_enforces_bag_row_and_byte_budgets`
test caught this. `Bag::check_materialization` now preflights positive row/byte
expansion during preparation without allocating Arrow output; physical lookup
retains actual materialization. Failure log:
`/tmp/optd-explicit-plan-final-gates-20261030.log`. The final passing gate includes
the unchanged regression and all new tests. No expected output was regenerated.

## Query suites

The frozen executable completed the unchanged repository SQL on the previously
qualified small-data profiles. Every selected passing query completed bootstrap
and three incremental rounds with independent full-SQL comparisons. Backend:
`typed_iceberg_endpoint_vector`, using fresh independent source-state roots.

* TPC-DS: 99/99 passing query-directed witnesses, zero remaining or failed profiles.
  Progress/commands/report hashes:
  `/tmp/optd-explicit-plan-tpcds-witness-20261030/progress.json`.
  Aggregate: `/tmp/optd-explicit-plan-tpcds-witness-20261030/aggregate.json`,
  SHA-256 `64940cefb8ad177757f677ec8e3b67681bef3d981046e74cdbde2410d6b573a1`.
* TPC-H: 22/22 with a complete passing profile per query. The tiny profile has
  20 nonempty passes and two correctly classified empty-only cases (Q11/Q21),
  so that profile returns exit 1. Independent unchanged-SQL Q11/Q21 profiles
  each return exit 0 with three rounds. No partial rounds are combined.
  Commands, input-manifest hashes, deadlines and report identities:
  `/tmp/optd-explicit-plan-tpch-evidence-20261030.json`.
  Aggregate: `/tmp/optd-explicit-plan-tpch-aggregate-20261030.json`, SHA-256
  `65fe5b9382d0b1a3fa1dcd21a93a01895894784900f2817ee5d2f5b293cccee1`.

Selected evidence covers 121 initializations and 363 incremental rounds. Including
both empty-only diagnostic profiles and their replacements, actual execution is
123 initializations and 369 rounds. All owned runners/children completed and were
reaped, with no query timeout. The reused TPC-H orchestration's inherited
supersession-only metadata was corrected after completion; raw runner evidence is
retained alongside it, and commands/reports/results were not changed.

These are local compatible-mode semantics witnesses. They do not prove every TPC
query has a strict incremental implementation, standard-scale performance,
end-to-end delta-sized work, or arbitrary SQL output ordering. The scoped runtime
tests separately establish affected-bucket work for qualified join/window plans.

Actual suite commands:

```text
python3 optd-incremental-sql/benchmarks/tools/run_query_witness_suite.py /tmp/optd-explicit-plan-tpcds-witness-20261030 --binary /tmp/optd-explicit-plan-benchmark-20261030 --expected-sha256 8df8b961af14f213a0e35435bb0a1927c77dcfe73b7a8532de93d0c86f81816d --expected-backend typed_iceberg_endpoint_vector --iceberg-root /tmp/optd-explicit-plan-tpcds-sources-20261030 --timeout-seconds 180
python3 /tmp/optd-explicit-plan-tpch-orchestration-20261030.py
```

## Delivery boundary

All changes are local to `optd-incremental-ivm`; no push, merge or deployment.
Unrelated Python/DBSP documents and their two summary links are preserved.
Acceptance of arbitrary custom algebra, persistent indexes, accumulator/match-
count algorithms, cross-operator optimization and scale performance remain
separate work.
