# Scoped snapshots — observed qualification

**Status:** verified local evidence · 2026-10-30
**Scope:** typed SQL join/window scoped snapshots, DISTINCT normalization and
strict planning. This record supersedes no earlier executable's evidence.

## Implementation

The [accepted design](scoped_snapshots.md) is implemented through two additional
named rules (`builtin.keyed_join`, `builtin.partitioned_window`) and the existing
signed-expression interpreter. Nineteen rules now exist. A selected
`ScopedSnapshot` declares `JoinKeys` or `WindowPartitions` derived state. Default
outer/semi/anti equijoins execute After minus Before on complete affected key
buckets; windows use committed cached output for Before. Only versions referenced
by the executed tree are materialized, at most once each. No executor fallback
replaces an index or expression failure.

`KeyedState` indexes typed key bytes to immutable input buckets; candidates share
unchanged bucket contents and install updated state only on acknowledgement.
Window output buckets follow the actual signed program, including custom trees.
Derived indexes rebuild on reopen without a checkpoint-format change. DISTINCT
ALL with columns normalizes to grouping, preserving fields, qualifiers, metadata
and original functional dependencies (the builder's stronger uniqueness metadata
is deliberately not substituted). Zero-column DISTINCT is excluded.
`require_incremental()` rejects selected full-node recomputation before bootstrap.

## Focused evidence

`tests/scoped_snapshots.rs` compares multi-epoch results and independently folded
signed deltas against separately planned full DataFusion SQL. It checks all seven
outer/semi/anti variants with ordinary, NULL-safe and residual-condition keys;
composite keys, duplicates, first/last matches, two-sided changes, key movement,
self joins, empty deltas and discarded/failed candidates. Each join fixture has
200 unrelated keys, with exact input-row/evaluation counters and no full-child
scan to locate affected buckets.

Eleven focused tests run in the snapshot target. Window coverage includes ROW_NUMBER, RANK, LAG, LEAD, SUM, ROWS/RANGE frames,
multiple expressions, NULL partitions and partition movement with 200 unrelated
partitions. A separate peer-sensitive test folds retractions against the actual
committed result and checks zero before-window executions; it does not assert a
unique SQL peer order. Durable join/custom and window reopen tests refresh again.
The real-source test refreshes full outer join and decimal partitioned SUM across
local Parquet/Iceberg endpoint changes, reopens their stores under strict mode,
refreshes again, and folds the committed output histories. DISTINCT covers
duplicate/NULL membership and final deletion. A direct logical
normalization regression covers qualifiers and the zero-column exclusion.

A custom After-only join intentionally differs from full SQL and proves that
the interpreter runs the supplied tree without secretly substituting the default
formula; it checks zero before calls and exact after input rows. Mixed scopes
fail verification. Strict mode rejects global aggregate/sort/window, non-equijoin
and floating-key join/window profiles before bootstrap. Correctness of arbitrary
user extensions remains their author's responsibility.

## Verification record

Environment: Linux x86_64, Rust/Cargo 1.95.0, pinned lockfile, DataFusion 55,
Arrow 59.2, optional pinned Iceberg, offline resolution, four build jobs, debug
information and incremental compilation disabled. Commands run through
`bash /tmp/optd-sql-env-20261029.sh`; the ordinary build target is reused.
Fresh-target qualification is recorded separately.

| Actual command through the environment wrapper | Observed result |
| --- | --- |
| `scripts/agent-check.sh sql` | Exit 0; final default suite 106 passed / 1 ignored; feature suite 120 passed / 2 ignored. Both all-target warnings-denied lint configurations and workspace formatting passed. Log: `/tmp/optd-scoped-sql-complete-20261030.log`. |
| `env CARGO_TARGET_DIR=/tmp/optd-scoped-final-fresh-check-20261030-target cargo +1.95.0 check --offline --locked -p optd-incremental-sql --features iceberg --lib` | Exit 0 in an initially empty target, 2m43s. This is a fresh feature-library check, not a fresh all-target build. |
| `scripts/agent-check.sh full` with restored linker PATH and isolated mdBook input | Exit 0; legacy default 41 and feature 70 passed; workspace all-feature warnings-denied lint and 386 tests passed / 4 ignored / 1 Postgres test explicitly filtered. Log: `/tmp/optd-scoped-full-complete-20261030.log`. |
| `scripts/agent-check.sh sql-tools` | Exit 0; pinned-source verification and 28 Python tests passed. Log: `/tmp/optd-scoped-tools-20261030.log`. |
| `cargo +1.95.0 check --manifest-path /tmp/optd-scoped-doc-consumer-20261030/Cargo.toml --offline --locked` | Exit 0; all three complete usage examples compile in an external consumer. |

The initial normalization regression failed because aggregate construction adds
uniqueness dependencies to its schema. The implementation now checks fields and
qualifiers and retains the original schema. Failure log:
`/tmp/optd-scoped-sql-final-20261030.log`; the final passing gate includes this
regression and the constant-partition strict-refusal case.

The first full gate accidentally omitted `/usr/local/bin` from PATH and failed
to locate `cc`. This orchestration error is retained in
`/tmp/optd-scoped-full-20261030.log`; subsequent gates restore the linker path.
An initial final-binary freeze raced a default-feature test build at Cargo's
shared named example path, producing a non-Iceberg executable. All 99 attempted
TPC-DS invocations and the TPC-H profiles failed at backend admission; those
results are retained in `/tmp/optd-scoped-final-tpcds-witness-20261030` and
`/tmp/optd-scoped-final-tpch-evidence-20261030.json`. They are not correctness
failures or passes. The replacement freeze checks Cargo's compiler-artifact
features and then executes one real Iceberg query before full suites.

## Final query-suite executable

The replacement executable is `/tmp/optd-scoped-qualified-benchmark-20261030`,
SHA-256 `3b461a5981db5ffe9dbe3fe79932f4a41b72fe5b4ff4a17de579e246eff0ccb1`.
The artifact record `/tmp/optd-scoped-binary-feature-artifacts-20261030.jsonl`
reports `features=["iceberg"]`; preflight output is
`/tmp/optd-scoped-feature-preflight-20261030.json` (exit 0, one query).
This is observed build/artifact/executable evidence, not embedded compile-time
source attestation. Source and test/fixture SQL are not altered during these runs.

The final TPC-DS run completed all 99 unchanged repository query IDs on
query-directed witnesses, each with three verified rounds. The source backend is
`typed_iceberg_endpoint_vector`, root
`/tmp/optd-scoped-qualified-tpcds-sources-20261030`. Progress/commands/per-query
identities: `/tmp/optd-scoped-qualified-tpcds-witness-20261030/progress.json`.
Aggregate: `/tmp/optd-scoped-qualified-tpcds-witness-20261030/aggregate.json`,
SHA-256 `a4de3e297e1cf7f837583c1b6a9628ae661494cfce8de9d824c85fc634165480`.
Observed: 99 with pass, zero remaining, zero profiles with failures. These are
nonempty-change semantics witnesses, not standard-scale/performance results.
The final TPC-H tiny run completed all 22 queries and all three rounds: 20
passed with nonempty changed results; Q11/Q21 were empty-only and the per-profile
runner therefore correctly returned exit 1. Separate Q11/Q21 runs use the unchanged
repository SQL on `/tmp/optd-tpch-repository-sql-dbgen122-sf01/manifest.json`,
with new independent Iceberg roots; both returned exit 0 with three rounds.
The strict cross-profile aggregate has 22 with pass, zero remaining, zero profiles
with failures. It chooses one complete passing profile per query; it does not
combine partial rounds. Orchestration evidence:
`/tmp/optd-scoped-qualified-tpch-evidence-20261030.json`, SHA-256
`7624d6bd1f5d0319ce357d5b50b4acd4c475711e4e9fcbe76986e49aeb1e1390`.
Aggregate: `/tmp/optd-scoped-qualified-tpch-aggregate-20261030.json`, SHA-256
`638d455ab870eadb425000ff1c606165ea2c6bf6ef0a6a029e12885135e19ee3`.
The tiny profile took 302.030 seconds, Q11 17.704 seconds and Q21 217.278 seconds;
these observations are execution evidence, not controlled performance comparisons.

The exact final commands are stored in the TPC-H orchestration evidence and
TPC-DS progress record, including unchanged SQL/manifest hashes, frozen executable
hash/identity, backend, independent source roots and deadlines. TPC-DS invocation:

```text
python3 optd-incremental-sql/benchmarks/tools/run_query_witness_suite.py /tmp/optd-scoped-qualified-tpcds-witness-20261030 --binary /tmp/optd-scoped-qualified-benchmark-20261030 --expected-sha256 3b461a5981db5ffe9dbe3fe79932f4a41b72fe5b4ff4a17de579e246eff0ccb1 --expected-backend typed_iceberg_endpoint_vector --iceberg-root /tmp/optd-scoped-qualified-tpcds-sources-20261030 --timeout-seconds 180
python3 /tmp/optd-scoped-qualified-tpch-orchestration-20261030.py
```

Final source digests are recorded in
`/tmp/optd-scoped-source-digests-20261030.json`. The final isolated book build uses
tracked book inputs plus only this change's documents/links at
`/tmp/optd-scoped-book-source-20261030`; unrelated Python/DBSP files and links
are excluded. The three complete usage examples compile independently. All owned
build/test/query processes completed and were reaped.

## Delivery

Changes are prepared on `optd-incremental-ivm` on top of `8544666`. Local design,
implementation, tests and evidence are delivered together; generated data/logs
remain outside the repository. Unrelated Python/DBSP documents and their index
links are preserved. Remote publication remains pending: a prior automatic
review rejected the `origin/main` push and no new explicit authorization of that
destination has been received. No push is retried in this increment.
Prior `/tmp/optd-scoped-tpcds-witness-20261030` full99 results use the binary from
before the DISTINCT metadata repair and remain preliminary evidence only.

## Limits

Keyed counters measure the original operator's affected input/output work,
including cached Before rows. `full_input_scans=0` means no complete child scan
was used to find affected buckets, not that source/result I/O disappeared or that
all buckets cannot be affected. Index maps and complete relation caches are still
copied, result caches materialized, Iceberg endpoints scanned and checkpoints
fully written. Default inner joins still use full-side snapshots. Grouped
aggregates still recompute touched groups rather than update native accumulators.
Global operations, unsupported key equality and unqualified windows reject in
strict mode and remain explicitly classified in compatibility mode. TPC query
success in compatibility mode is not full strict-incremental TPC coverage or a
performance result.
