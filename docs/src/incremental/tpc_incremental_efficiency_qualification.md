# TPC incremental efficiency — observed qualification

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** verified local code, targeted gates and both TPC query modes · 2026-10-30
**Scope:** operator maintenance, explicit benchmark variants and local correctness.
No standard TPC performance or end-to-end delta-complexity qualification.

## Implementation and evidence boundary

The [design](tpc_incremental_efficiency.md) adds qualified retractable builtin
COUNT, integer/decimal SUM and decimal AVG accumulator state. Its explicit Optd
physical operation consumes signed input changes, including for global aggregates;
it stores group cardinality, accumulator cells and output, not historical input.
Grouped MIN/MAX, DISTINCT and other qualified aggregates retain affected-group
maintenance. Unsupported aggregates remain explicit complete-recompute blockers.

Qualified equijoins use `delta-left JOIN indexed-before-right` plus
`indexed-after-left JOIN delta-right`. Shared index preparation derives the union
of changed keys. Existing physical lookup and signed-join operations perform the
work; the runtime does not interpret optimizer rules. Outer/semi/anti joins retain
affected-key snapshot differences, and qualified windows retain affected-partition
maintenance. This change does not introduce match-count maintenance for outer joins.

Immutable Bag clones share storage. Source changes are consolidated once and used
directly by SourceRead; unchanged sources skip repeated validation. Empty output
deltas preserve previous materializations. Changed relation-map rebuilds, index
directory clones (including the accumulator group directory), source endpoint
scans and full checkpoint writes remain costs.
These costs are not included in the operator bucket-read counters.

## Commands and observed gates

Environment: Linux x86_64, Rust/Cargo 1.95.0, pinned DataFusion 55, offline
dependencies, four build jobs, debug information and incremental compilation off.
The environment wrapper is `/tmp/optd-sql-env-20261029.sh`.

| Command | Observed result |
| --- | --- |
| `bash /tmp/optd-sql-env-20261029.sh bash scripts/agent-check.sh sql` | Exit 0. Default: 134 passed / 1 ignored. Iceberg: 148 passed / 2 ignored. Workspace formatting and both all-target warnings-denied Clippy configurations passed. `/tmp/optd-efficiency-gates-final.log`. |
| `python3 -m unittest discover -s optd-incremental-sql/benchmarks/tools -p 'test_*.py'` | Exit 0, 32 passed. `/tmp/optd-efficiency-python-audit.log`. |
| `bash /tmp/optd-sql-env-20261029.sh env CARGO_TARGET_DIR=/home/admin/xwork/optd/target/ivm-efficiency-fresh-20261030 CARGO_BUILD_JOBS=2 cargo +1.95.0 check --offline --locked -p optd-incremental-sql --features iceberg --lib` | Exit 0, 3m15s, initially absent target. Feature-library build proof; not a fresh all-target test build. `/tmp/optd-efficiency-fresh-check.log`. |
| `bash /tmp/optd-sql-env-20261029.sh cargo +1.95.0 build --offline --locked -p optd-incremental-sql --features iceberg --example benchmark --message-format=json` | Exit 0. Compiler artifacts: `/tmp/optd-efficiency-benchmark-build.jsonl`. |
| `bash /tmp/optd-sql-env-20261029.sh env MDBOOK_BUILD__BUILD_DIR=/tmp/optd-efficiency-book-html-20261030 mdbook build /tmp/optd-efficiency-book-source-20261030` | Exit 0. Isolated copy of tracked/owned documents; unrelated Python/DBSP links excluded only in that copy. `/tmp/optd-efficiency-book-20261030.log`. |

Frozen executable: `/tmp/optd-efficiency-benchmark-20261030`, SHA-256
`64b0dba69dce55c3781774b98863f503e9e9a0448e85fe3831eb0c399a9331c5`.
`/tmp/optd-efficiency-binary-evidence.json` records its features and 51 source,
test, manifest and tool hashes, rechecked unchanged after the runs. The subsequent
work-summary tool and its tests are evidence readers, outside that build identity.

Differential tests cover simultaneous two-sided join changes, cross terms,
duplicate/NULL/residual predicates, aggregate group deletion and empty global
state, discarded/failed candidates and recovery. Fixed changes against many
unchanged keys assert affected bucket reads. A 2,000-row group receives only two
changed rows at its accumulator, with zero historical aggregate-input reads.
This proves an operator work bound, not a bound on full refresh memory or I/O.

Earlier failing lint/test logs are retained under `/tmp/optd-efficiency-*`.
Corrections include qualifying aggregate aliases and updating tests that assumed
all global SUMs recompute or all inner joins use full snapshots. Oracle result
and signed-delta comparisons remain in place; no fixture or expected output was
regenerated to conceal a failure.

## Explicit SQL variants

`OPTD_SQL_BENCH_QUERY_MODE=original` is the default and preserves original SQL
bytes. `relational_core` parses the SQL and removes only the outer ORDER BY and
LIMIT/OFFSET/FETCH. Window ordering and nested row bounds remain. Removing row
bounds changes query membership: this variant is not an equivalent Top-K query.
Both incremental and independent full-SQL executions use the recorded variant.
Production planning never silently drops these clauses.

Reports retain original/executed SQL SHA-256 values, executed SQL and removed
clauses. Coverage aggregation requires an explicit expected mode and refuses to
mix the two modes. `plan_qualification` enumerates every planned node and all
complete-recompute blockers, even if a blocker does not execute in a given round.
`strict_plan_eligible` means the actual plan has no complete-node recompute;
these benchmark runs are not separately executed strict-policy runs.

## TPC-DS observed results

Both profiles completed 99/99 queries, each with bootstrap, three incremental
rounds, reopen between rounds, independent full-SQL bag comparison and signed
output-delta replay. Backend: `typed_iceberg_endpoint_vector`, independent fresh
source-state roots. Ordered presentation is not checked by these bag comparisons.

| Query mode | Semantic witnesses | Plans without complete-node recompute | Plans with complete-node recompute |
| --- | ---: | ---: | ---: |
| original | 99/99 | 3/99 | 96/99 |
| relational_core | 99/99 | 74/99 | 25/99 |

Core blocker memberships overlap: aggregate recompute 20 queries, non-inner join
recompute 4, window recompute 2. Original also has outer/internal ordering and
limit blockers. Memberships are per query, not counts of physical operators.
Core plans use accumulators in 83 queries and indexed inner joins in 95 queries.
Across selected three-round core witnesses: accumulator delta inputs 830 rows,
historical inputs 0; affected-key snapshot inputs 3,293 rows, full input scans
reported by those keyed operators 0. Source acquisition is separate: 2,928
endpoint scans and 2,574 decoded rows. The witnesses contain only 1–42 input rows
per query (median 6), so these counters are not a scale/performance experiment.

Progress, exact commands and per-query report identities:

* `/tmp/optd-efficiency-tpcds-original-20261030/progress.json`
* `/tmp/optd-efficiency-tpcds-core-20261030/progress.json`

Aggregate SHA-256 identities:

* Original: `/tmp/optd-efficiency-tpcds-original-20261030/aggregate.json`,
  `bb6c5149f9cdd6871a4f75383231259b30dd5dedbd5c328b713ef4b8a722bf34`.
* Core: `/tmp/optd-efficiency-tpcds-core-20261030/aggregate.json`,
  `37a53925d3211b28be39c4d42e0dac7c0868d686e1df80cf6c14decdb105cb82`.

## TPC-H observed results and retained timeouts

The first driver incorrectly applied a uniform 180-second query deadline to the
Q21 SF0.01 profile whose prior qualification used 600 seconds. The retained
original Q21 report records that timeout, not a passing result. Follow-up uses
the unchanged executable and data, a 600-second async deadline, a 900-second hard
deadline and new independent source-state roots. Both follow-ups exited 0:
original 212.544 seconds, relational core 210.419 seconds. These are uncontrolled
correctness-run durations, not a performance comparison. Both original and core
180-second timeout reports remain in the aggregates as failed profiles.

Both modes now have 22/22 nonempty changing witnesses with complete bootstrap
and three-round SQL/result-delta comparisons, including reopen. Tiny-profile
empty-only Q11 and Q21 do not qualify as nonempty changing witnesses; independently
complete SF0.01 profiles provide those two queries. No rounds are combined across
profiles. The aggregate therefore has `with_pass=22`, `remaining=0` and
`profiles_with_failures=1` per mode: the retained timeout remains visible.

| Query mode | Semantic witnesses | Plans without complete-node recompute | Plans with complete-node recompute |
| --- | ---: | ---: | ---: |
| original | 22/22 | 4/22 | 18/22 |
| relational_core | 22/22 | 21/22 | 1/22 |

The remaining core blocker is Q15's global `MAX(revenue0.total_revenue)`.

Commands/profile identities: `/tmp/optd-efficiency-tpch-20261030/progress.json`
and `/tmp/optd-efficiency-tpch-q21-retry-20261030/progress.json`.
Final qualified aggregate SHA-256 values:

* Original: `/tmp/optd-efficiency-tpch-20261030/original-qualified-aggregate.json`,
  `a2a3f743970c406ca0b96766334dd42dae3029f1a5b3c65603e94a90445a85ab`.
* Core: `/tmp/optd-efficiency-tpch-20261030/relational_core-qualified-aggregate.json`,
  `721fdf93bea542d1eb709762ffa2c5bc741d7828b9f074a7e7440fbf0fd7a4b8`.

## Combined qualification

Both modes have 121/121 local semantic witnesses. The original plans have 7/121
without complete-node recomputation; relational-core plans have 95/121. Local
affected-group/key/partition recomputation is allowed in that count. It does not
certify source reads, state-copy complexity, all data distributions or performance.

`/tmp/optd-efficiency-final-work-20261030.json` enumerates each query's selected
complete profile, every planned blocker/reason and actual operator/source work.
`/tmp/optd-efficiency-final-evidence-20261030.json` records input hashes, executable
identity, follow-up commands and validation logs. The work-summary tool checks
aggregate hashes, actual planned blockers and three-round profile completeness;
unknown legacy plan qualifications cannot count as strict-plan eligible.

## Remaining work

Grouping sets/ROLLUP, unqualified or global aggregates, non-equality outer joins,
unpartitioned or unqualified windows and internal ordered/limited subqueries can
still need complete-node recompute. Their reasons remain visible; strict policy
refuses them. Future range/global dependency and Top-K algorithms must preserve
their SQL semantics. Persistent change-friendly state, source CDC, checkpoint
I/O reduction and cost-based maintenance selection remain unqualified.

Local code commit: `2deb89b` (25 code/test files). Documents and generated
evidence remain local and uncommitted; this iteration has not been pushed.
The pre-existing Python/DBSP document files and index links were preserved.
