# Strict incremental gaps: scalar-product repair qualification

**Status:** S1 scoped implementation and query gates verified; S2–S6 remain open · 2026-11-06 UTC.
**Scope:** [execution specification](strict_gap_execution_spec.md), batch S1 only.

This is the historical S1 snapshot. Subsequent work is recorded in
[the S2–S5 qualification](strict_batches_progress_20261106.md); its results do
not replace or reclassify any numbers below.

## Implementation

`builtin.scalar_product` proves that a condition-free Left Join's right input
always has exactly one row: a global aggregate, optionally behind projection or
alias. It generates `delta_left × scalar_before + left_after × delta_scalar` in
the existing Optd representation. Signed fragments use a dedicated product
kernel, preserving the qualified output schema without outer NULL extension.
Filters/HAVING, grouping, row bounds, predicates and NULL-aware semantics do not
inherit scalar qualification. Existing unsupported-plan guards remain intact.

`SnapshotReadWhenChanged` binds a complete Before/After input to an upstream
delta trigger. Empty triggers avoid accessing that snapshot. The verifier checks
the trigger's graph identity, not just its Arrow schema. Final compiled snapshot
dependencies are exposed in preflight and both benchmark backends.
`guarded_snapshots` measures reads, skipped reads and expanded rows; cache hits
still count consumed rows. Older absent counters remain unknown.

This is a scalar/global dependency subset of R06. General range/key/NULL impact
contracts, float grouping, floating aggregates, STDDEV and global ranking remain
unfinished. No dependency or checkpoint format changed.

## Observed verification

Artifacts: `target/strict-gap-20261106/`; previous frozen reports remain under
`target/benchmark-20261106/`. Nothing in the 494-configuration baseline is
reclassified. The candidate includes the previously uncommitted width-bounded
test importer; this is not an unchanged-importer paired performance comparison.

* RED: `scalar_aggregate_subquery_has_a_strict_incremental_plan` failed on the
  previous implementation with `builtin.non_inner_join_recompute`, exit 101.
* Focused tests: 112 passed, including four new scalar SQL/differential/scope
  tests and two structural/verifier tests. Counts overlap the full tests below.
* Full typed crate: default 331 passed / 2 ignored; Iceberg 345 passed / 3 ignored.
  These are actual executions; ignored existing gates are not passes.
* Both feature configurations passed all-target Clippy with `-D warnings`.
  Formatting, mdBook and Python benchmark checks (47 tests) passed separately.
* Fixed one-row left insertion, left cardinality 32 → 4,096: exactly one scalar
  snapshot row read and one left snapshot read skipped at both sizes. Scalar
  changes require 33 / 4,097 left snapshot rows and 66 / 8,194 signed output rows.
  These tested operator boundaries exclude source, output and durable IO costs.
* Thirty-six two-sided atomic rounds compare typed rows and signed deltas with
  independent integer formulas and full SQL. Other tests cover nullable SUM/COUNT,
  duplicates, empty inputs, UPDATE, discarded candidates, invalid deletes,
  retries, reopen and output history.

Toolchain: Rust/Cargo 1.95.0, offline/locked resolution, two Tokio workers.
Dependency cache was reused at `target/roadmap-20261031`; this is not a clean
dependency-build qualification. Source hashes were recorded before validation
and are checked again before final report generation.

## Query evidence

The focused matrix uses unchanged query/schema/data manifests, a frozen candidate
executable and new database state per configuration. Each configuration has a
180s asynchronous budget and 210s outer process deadline. It covers thirteen
previously refused queries on directed and public-generator profiles, plus Q9
original SQL on both profiles. It is not a fresh full-99 coverage report.

Q9 directed relational-core and original-SQL profiles both completed all three
SQL DML/MV/reopen/history rounds with nonempty output and observed changes.
All fifteen original Left Join blockers select the new rule; zero full-node
recompute blockers remain. Generated-data relational-core SQL exceeded its 180s
asynchronous budget; its completed rounds remain unknown.

The 28-configuration matrix finished with **2 pass, 2 async-timeout failures,
24 strict refusals**. The twelve other query IDs retain exactly their original
blocker counts on both data profiles. The two failures are generated-data Q9
SQL runs in relational-core and original modes; neither is reclassified.

The independent **600s generated-data relational-core SQL** run completed in
**243.830s**, exit 0, with all three INSERT/DELETE/UPDATE, MV comparison,
base-table comparison, reopen and output-history rounds verified. It observed
nonempty results and output changes, with zero complete-node blockers. Q9's
transformation records no removed clauses. Actual guarded snapshot counters:

| Round | Reads | Skipped reads | Expanded snapshot rows |
| --- | ---: | ---: | ---: |
| insert_remaining_half | 30 | 0 | 30 |
| delete_matching_rows | 18 | 12 | 15 |
| update_keys | 0 | 0 | 0 |

An independent generated-data **original SQL / typed Arrow** run completed three
exact typed-bag and signed-delta comparisons (18.410s observed wall time). Its
final plan contains only `AccumulatorDelta` and `NativeDelta`. This is in-memory
execution evidence, not SQL DML, checkpoint or recovery certification. A separate
600s SQL follow-up used the same frozen executable and new state. Different
backend, budget and possible concurrency overlap prevent speedup attribution.

The two independent follow-ups add two completed configurations to the separate
28-configuration matrix; they do not change any of its statuses. Generated
`report.html` / `benchmark.json` preserve per-profile identity and missing values.
HTML numbers, hashes, anchors and self-contained resources are statically checked;
browser layout and interaction are unverified. No commit or push is part of this
execution record.

## Reproduction and outstanding gates

Use `cargo test -p optd-incremental-sql --test scalar_product` for focused tests.
On this host, actual commands are prefixed with
`bash /tmp/optd-sql-env-20261029.sh env`, `CARGO_TARGET_DIR`, `TMPDIR`,
`TOKIO_WORKER_THREADS=2`, then `cargo +1.95.0 ... --offline --locked`.
`gates.json`, `environment.json`, per-run `audit.json` and local `run.py` retain
exact commands, deadlines, manifests and executable identities.

Continue S2 after its float equality/representative design; S3/S4 require an
explicit numerical contract. S5 requires ordered peer counts and affected ranges.
Source acquisition, SQL import/selection, checkpoint and recovery costs remain
separate S6 gates. No full-99, native CDC, controlled performance or browser
interaction acceptance is claimed.
