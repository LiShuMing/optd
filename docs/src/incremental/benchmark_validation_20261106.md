# Incremental benchmark validation — 2026-11-06

**Status:** verified execution record · 2026-11-06; failed/unknown gates remain open.
**Scope:** current-source tests and measured work for the typed SQL incremental engine.
**Source:** `55847897937c9695befe2a6b174c3315453d94a0`, with the pre-existing three
publication-policy document edits preserved. No engine or SQL semantics change.

## Test contract

The subsequent [scalar-product repair qualification](strict_gap_progress_20261106.md)
and [numeric/rank repair qualification](strict_batches_progress_20261106.md)
record different candidates and focused reruns. This document's frozen baseline
counts and failures remain unchanged.

This run applies the existing accepted contracts in `tpc_incremental_efficiency.md`,
`sql_database_loop.md`, `query_witness_fixtures.md`, and `unified_work_reports.md`.
It does not introduce a new maintenance algorithm, numerical tolerance, or SQL rewrite.
Failures, empty results, unchanged output, strict-plan refusal, and timeouts remain
separate outcomes. Existing fixtures, source SQL, expected values, and guards are not
modified to improve coverage.

| Axis | Selection |
| --- | --- |
| SQL identities | All 22 repository TPC-H queries and all 99 pinned TPC-DS numbered queries |
| Original SQL | Typed Arrow execution; complete-recompute eligibility is reported separately |
| Strict SQL loop | `relational_core`, explicitly removing only outer ordering/row bounds; CREATE TABLE/MV, INSERT/DELETE/UPDATE, reopen and output-history verification |
| Public-generator inputs | DuckDB 1.2.2 dbgen/dsdgen SF0.01; original repository/pinned query and schema profiles |
| Small inputs | Existing repository TPC-H data and freshly generated, explicitly nonstandard, per-query TPC-DS witnesses |
| Iceberg sample | TPC-H 1/6/12/15 and TPC-DS 1/10/23/28/45/67; endpoint-difference and recovery checks, not native CDC qualification |
| Work scaling | 1,024 / 8,192 / 65,536 rows, one-row INSERT/DELETE/UPDATE, combined COUNT/SUM/MIN/MAX/COUNT DISTINCT MV; independent arithmetic and full SQL checks, retry and reopen |
| Mixed benchmark loop | All 60,175 SF0.01 lineitems, projected to six required columns; original Q6, 32 mixed SQL write steps, independent Python Decimal results, rollbacks and reopen |
| Engineering and recovery | `scripts/agent-check.sh sql`, benchmark Python tools, targeted work-scope tests and actual process-death tests |

The main matrix requests **494 independent query profiles**: 484 from the two
datasets × two execution contracts × 121 queries, plus 10 Iceberg profiles.
One profile must independently finish bootstrap and all three update rounds to
qualify. A nonempty result and observed output change are additional requirements
for `pass`; profiles never contribute partial rounds to another profile.

## Environment and reproducibility

Commands execute from `/home/admin/xwork/optd`. Rust 1.95.0, offline locked
dependencies, debug build, four compiler jobs, and compiler incremental mode off.
Dependencies reuse `target/roadmap-20261031`; this is not a fresh-target build claim.
The actual benchmark examples are built in this run, copied to frozen executable
paths, and hashed along with source, lockfile, manifests, SQL, schema and data.

The matrix initially used three concurrent correctness processes. After 253
completed profiles, the remaining profiles moved to eight concurrent processes
to improve correctness-test throughput; this is not a performance experiment.
Three owned in-flight attempts were explicitly interrupted and retained in
`orchestration-restart.json`; they restart with new isolated state under
`continuation/`, without overwriting their original directories. Completed
profiles are not rerun or discarded. Each process uses two Tokio workers.
Each profile has its own fresh database/Iceberg root. The asynchronous
deadline is 180 seconds, except TPC-H Q21 uses its established 600 seconds; the
outer process deadline is 60 seconds longer. A timeout is retained as a failure.
Concurrent correctness durations must not be interpreted as speedups.

Local evidence root: `target/benchmark-20261106/`. It contains `protocol.json`,
`binaries.json`, `datasets.json`, `jobs.json`, per-profile commands and logs,
`progress.json`, and frozen executables. Temporary witness inputs are under
`/tmp/optd-benchmark-20261106-witnesses/`; generated SQL/data are not published.

```sh
bash /tmp/optd-sql-env-20261029.sh env \
  CARGO_TARGET_DIR=/home/admin/xwork/optd/target/roadmap-20261031 \
  TMPDIR=/home/admin/xwork/optd/target/benchmark-20261106/tmp \
  cargo +1.95.0 build --offline --locked -p optd-incremental-sql \
  --features iceberg --example benchmark --example benchmark_sql_database \
  --example sql_database
python3 target/benchmark-20261106/run_matrix.py
# Actual run continued the uncompleted profiles, retaining the interruption audit:
python3 target/benchmark-20261106/continue_matrix.py
python3 target/benchmark-20261106/run_scale.py
python3 target/benchmark-20261106/run_q6_loop.py
bash /tmp/optd-sql-env-20261029.sh env \
  CARGO_TARGET_DIR=/home/admin/xwork/optd/target/roadmap-20261031 \
  TMPDIR=/home/admin/xwork/optd/target/benchmark-20261106/tmp \
  TOKIO_WORKER_THREADS=2 bash scripts/agent-check.sh sql
python3 -m unittest discover -s optd-incremental-sql/benchmarks/tools -p 'test_*.py'
```

The local matrix/scale orchestration scripts refuse to reuse existing run roots;
a reproduction must select new evidence and witness paths in a copy of those
scripts. Underlying repository benchmark entry points are unchanged. The completed
report will retain script identities and the exact actual commands for each run.

## Interpretation boundaries

This is a current-source local qualification run, not an official TPC benchmark,
standard-scale production test, distributed-transaction test, or controlled
full-versus-incremental performance comparison. Original SQL and relational-core
membership differ and cannot be used as paired performance arms. Both execution
sides use DataFusion scalar implementations; mathematical and handwritten semantic
tests provide additional independent checks but not a second complete SQL engine.

Work totals retain separate domains and unknown counters. Recovery, full-oracle
verification, and history replay are not attributed to refresh. Fixed-delta
measurements can expose unavoidable output changes and remaining full-table
checkpoint work; low bucket-read counts alone cannot certify end-to-end work.

Baseline results below come from completed execution and identity checks; follow-up results remain separate.

## Test-tool repair discovered during execution

The unchanged baseline SQL importer uses 1,024 rows per generated INSERT regardless
of column count. A 64-column, 1,024-row reproduction exceeds the existing 65,536
expression-node validation guard before query execution. The same rows and types,
imported in the same transaction in 128-row statements, succeed with independent
COUNT/SUM and full-SQL checks (`wide-import-reproduction/summary.json`).

After the frozen baseline matrix completes and its source hashes are verified,
limit generated statements by both rows (1,024) and cells (8,192). Keep the engine
budget, typed values, all input rows, transaction boundary, queries and comparison
assertions unchanged. Add an executable 64-column/1,025-row regression checking
one commit, exact source/MV results and reopen; demonstrate failure before the
importer change and success after it. Run focused default/Iceberg tests, formatting
and both lint configurations. Keep the original matrix and failures intact; any
real-data follow-up uses a separately frozen binary, isolated state and an explicit
deadline, and never changes the baseline's classification.

## Additional engineering scope

The same environment additionally executed `cargo +1.95.0 test --offline --locked
-p optd-core` and `bash scripts/agent-check.sh iceberg`. Both commands exited 0:
16 core tests, 41 legacy-incremental default tests and 70 legacy-incremental
Iceberg tests; legacy format and both Clippy configurations passed. These are
separate from typed-SQL default (324 passed, 2 ignored), typed-SQL Iceberg
(338 passed, 3 ignored), and Python benchmark-tool tests (46 passed). Counts across
feature configurations include repeated tests, not unique scenarios. The whole
workspace, DataFusion bridge and PostgreSQL integration were not qualified.
Logs and actual commands are in `extended-gates.json` and their indexed log files.

## Iceberg orchestration correction

The baseline's ten Iceberg sample invocations used artifact roots under repository
`target/`, violating the existing fixture guard that only permits children of
`/tmp`. All ten exited before query execution, produced no coverage report, and
remain `missing_report` in the 494-profile baseline. This is an orchestration
error, not an observed SQL result difference.

`rerun_iceberg.py` used the same frozen executable, manifests, query mode and
180-second query budget, with new actual directories under
`/tmp/optd-benchmark-20261106-iceberg-followup/` and concurrency three. All **10/10**
independent reruns passed three rounds. Commands, reports, hashes and recovery
checks are retained in `iceberg-followup-summary.json` and `iceberg-followup/`.
The original guard is unchanged; these remain endpoint-difference checks and do
not qualify native CDC ingestion.

## Verified baseline results

The frozen matrix completed **494/494** distinct configurations, exited 0, and
verified all recorded source and executable hashes after completion. The driver
exiting 0 means collection/audit completed, not that every query passed.

| Outcome | Configurations |
| --- | ---: |
| Nonempty result and observed output change, all three rounds compared | 314 |
| All three rounds compared, empty results only | 32 |
| All three rounds compared, nonempty but no observed output change | 11 |
| Strict-plan refusal | 26 |
| Reported failure | 85 |
| Hard process timeout | 16 |
| No report because Iceberg artifact-root contract was violated | 10 |
| Total | 494 |

**357** configurations completed all three comparisons (**1,071** complete rounds).
Of these, **227** original-SQL configurations included explicit complete-node
recomputation; they are semantic comparisons, not strict incremental certification.
Across the 111 execution/launch failures, 47 were fixture-import expression-budget
failures, 54 were query/process deadlines, and 10 were the artifact-root error.
No completed comparison reported a typed result-bag or signed-output-delta mismatch;
failed or timed-out configurations remain unverified, not correct by inference.

| Data | Suite | Contract | Complete comparisons | Nonempty + changing | Strict refusal | Other failure |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| directed_or_repository | tpcds | sql_database / relational_core | 86/99 | 86 | 13 | 0 |
| directed_or_repository | tpcds | typed_arrow / original | 99/99 | 99 | 0 | 0 |
| directed_or_repository | tpch | sql_database / relational_core | 22/22 | 20 | 0 | 0 |
| directed_or_repository | tpch | typed_arrow / original | 22/22 | 20 | 0 | 0 |
| public_generator_sf001 | tpcds | sql_database / relational_core | 2/99 | 1 | 13 | 84 |
| public_generator_sf001 | tpcds | typed_arrow / original | 98/99 | 61 | 0 | 1 |
| public_generator_sf001 | tpch | sql_database / relational_core | 6/22 | 6 | 0 | 16 |
| public_generator_sf001 | tpch | typed_arrow / original | 22/22 | 21 | 0 | 0 |

A query may choose one complete configuration across the two datasets; rounds
are never spliced. Under that definition, strict SQL loops have a nonempty,
changing complete witness for **22/22 TPC-H** and **86/99 TPC-DS** queries. This does
not mean they all finish on the larger generated dataset: real-data strict SQL
loops completed only **6/22 TPC-H** and **2/99 TPC-DS** (one TPC-DS result empty).
The exact original SQL on real data completed **22/22 TPC-H** and **98/99 TPC-DS**;
TPC-DS Q72 exceeded the deadline. Explicit recomputation in original SQL is
reported separately and is common with its ordering/row-bound operators.

The 13 strictly rejected TPC-DS query IDs are
`7,9,13,17,22,26,27,35,36,39,44,49,85`. Per-node operator and reason evidence is
retained; limitations include floating aggregate contracts, STDDEV, joins lacking
usable equality keys, floating grouping keys and unpartitioned ranking. No guard
or assertion was relaxed to improve these counts.

The generated datasets contain 86,805 TPC-H rows / 10,673,344 CSV bytes and
277,976 TPC-DS rows / 26,554,945 CSV bytes, for **364,781 rows** across 32 tables.
Every input table was actually counted and SHA-256 checked. These are SF0.01
public-generator datasets, not an official TPC certification or production scale.

## Verified work and mixed-loop results

| Unchanged-state scale | Single INSERT: copied target entries | Aggregate delta rows | Aggregate historical rows | Checkpoint table rows | Checkpoint table bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 256 | 1 | 0 | 1,025 | 17,578 |
| 8,192 | 256 | 1 | 0 | 8,193 | 134,058 |
| 65,536 | 256 | 1 | 0 | 65,537 | 1,065,898 |

All nine scale/operation configurations passed integer-formula, full-SQL, MV,
retry and reopen checks. DELETE selectors read 1,025 / 8,193 / 65,537 rows; UPDATE
selectors read 1,024 / 8,192 / 65,536 rows. UPDATE feeds two signed rows to the
aggregate. The original one-statement 65,536-row seed budget failure is preserved;
that scale was separately completed with bounded statements in one transaction.
This proves bounded work for the measured local state updates and demonstrates
remaining full-table selector/checkpoint costs; it does not qualify end-to-end
delta complexity.

The Q6 loop retained all 60,175 lineitems, projected to the six required columns,
and passed 32 mixed writes, four rollbacks, four reopens, 41 full-SQL and 41 MV
comparisons against a separately maintained Python Decimal multiset oracle.
Complete output-history verification passed. It is deterministic, not long-running
randomized/concurrent stress. Three additional scope tests passed with growing
untouched state: correlation peer visits stayed 4; copied cache entries stayed
514; balanced directory paths grew from 8 to 11 for 256 to 2,048 keys.

## Remaining project gates

Keep R07a/R07b, R08, R10a/R10b/R10c and the numerical/window/range limitations
open. Prioritize bounded checkpoint/recovery work, source/DML selection, stage
instrumentation to locate real-data SQL-loop timeouts, and controlled full-vs-delta
experiments with identical query contracts and isolated state. Concurrent debug
correctness durations include import, full-oracle checks and recovery; they do
not establish incremental latency or a speedup.

Evidence is local under `target/benchmark-20261106/`: `report.html` is the immutable
baseline, `queries.csv` indexes every configuration, and `summary.json` contains
machine-readable numbers. The final report will append independently identified
repairs/reruns. No commit, push, deployment or remote benchmark publication occurred.

## Importer repair verification

Only `benchmarks/database_harness.rs` changed in executable/test source. Generated
INSERT statements are now bounded by 1,024 rows and 8,192 cells; the engine's
expression budget, all row values/types, transaction boundary, queries, expected
results and strict-plan guards are unchanged.

The 64-column / 1,025-row regression actually failed on the old importer with
`quantified expression validation exceeds node budget` (test exit 101). After the
change, the benchmark integration target passed **7 default tests** and **8 Iceberg
tests**, with one ignored full-suite test in each configuration; that full matrix
was run separately as recorded above. Source rows, a single import commit,
independent COUNT/SUM, MV, reopen and output history are checked. Formatting and
both warnings-denied all-target Clippy configurations passed; the new benchmark
SQL executable built and was frozen separately.

`followup-gates.json` records every command and observed status.
`followup-source.json` and `harness-repair.patch` identify the exact change and
new executable; all original engine source hashes match the frozen baseline.
The original 47 import-budget failures are retained. This importer repair is not
a qualification of all those original failed profiles.

## Independent real-data follow-up and final delivery

These three configurations use the repaired importer, new isolated state,
concurrency three and a **600-second** asynchronous deadline (hard deadline 660s).
Baseline timeout was 180 seconds; this is a different validation contract, not a
paired performance experiment and not a replacement for any baseline failure.

| Query | Baseline | Follow-up | Complete compared rounds | Whole-profile seconds |
| --- | --- | --- | ---: | ---: |
| tpcds Q20 | fail | hard_timeout | unknown | 660.097 |
| tpcds Q40 | fail | fail: query exceeded 600s async deadline | None | 611.49 |
| tpch Q6 | fail | pass | 3 | 390.068 |

TPC-H Q6 passed with **all 16 original columns and all 60,175 rows**, distinct
from the six-column mixed-loop projection. Both TPC-DS queries reached initial
MV publication but did not finish the three-round contract within 600 seconds;
partial state is retained and is not counted as a passing profile. These results
leave the end-to-end SQL-loop efficiency gate open.

Final local artifacts:

* `target/benchmark-20261106/final-report.html`: self-contained report, searchable
  per-query index, frozen baseline plus separately identified corrections/follow-ups.
* `target/benchmark-20261106/final-summary.json`: machine-readable baseline,
  work-scaling results, engineering gates and independent follow-ups.
* `target/benchmark-20261106/queries.csv`: all 494 baseline configurations.
* `target/benchmark-20261106/artifact-index.json`: static validation and indexed
  raw reports/logs/scripts/checksums, including scale and mixed-loop responses.

HTML validation covers counts, CSV agreement, unique anchors, evidence hashes and
absence of external assets. Browser layout/interactive validation was not run.
No performance acceptance, native-CDC certification, full-workspace qualification,
commit, push or deployment is claimed.
