# Strict incremental gaps: numeric and rank repair qualification

**Status:** S2–S5 qualified subsets verified; public-data SQL deadlines and broader gates remain open · 2026-11-06 UTC.
**Scope:** the thirteen known TPC-DS relational-core refusals, not full TPC certification.

## Implemented behavior

[Execution spec](strict_gap_execution_spec.md) and
[numerical/rank design](strict_numeric_and_rank_design.md) define the contract.
S1's historical evidence stays in its separate qualification document.

* S2: Float32/64 group keys normalize signed zero, preserve NaN payload identity,
  and maintain DISTINCT through aggregate group reference counts. Source-row
  identity remains exact; join/extremum argument domains were not broadly opened.
* S3: canonical integer-to-Float64 AVG retains retractable count/sum and checked
  absolute integral mass <= 2^53. Candidates outside that exact arithmetic domain
  fail atomically; general floating SUM/AVG remains unqualified.
* S4: STDDEV sample/population and same-argument paired floating AVG use centered
  Welford moments with counted argument values. Nonfinite/unrepresentable input
  domains fail atomically. The owner's numerical contract is
  `abs(error) <= 1e-10 + 1e-9 * abs(reference)`, applied only to identified result
  columns. Keys, NULL, multiplicity, source rows, unrelated integer averages and
  exact maintained-endpoint delta/history replay retain exact comparison.
* S5: a separate Optd Rank state/update operation maintains one global canonical
  RANK per Window node. Ordered peer counts provide predecessor prefix counts;
  unchanged prefixes and gaps are skipped. A necessary rank shift may change the
  whole suffix. ASC/DESC, NULL ordering, ties and compound keys are qualified.
  This does not implement Top-K or replace existing partition-window algorithms.

Each changed numerical cell is canonically rebased over that affected group's
counted distinct argument values before publication. This permits exact recovery
from the current source/output checkpoints. Its complexity is delta plus affected
value cardinality, **not** a pure delta-only numerical accumulator. `numeric_work`
records those visits separately from complete historical input rows.
`rank_work` records peers visited, historical rows consumed and emitted changes.
Source selection, complete output caches and checkpoint I/O remain separate costs.

## Evidence identities

Local artifacts: `target/strict-batches-20261106/`. `source.json` fixes the main
candidate's source/file hashes, binary hashes and original baseline hashes.
`run.py` preserves the 45-configuration protocol. Every configuration gets fresh
SQL state; maximum two jobs run within that runner. Main runs overlap tests, so
observed wall times are not performance comparisons.

The final numerical-lineage refinement excludes unrelated exact integer averages
from tolerance. `source-final-diff.json` records that refinement and test/doc/comment
differences from the main frozen candidate. `lineage-final/` independently freezes
the final executable and reruns Q17/Q39 directed SQL and generated Arrow. These
results are separate; they do not overwrite the main matrix or its binary.

SQL: DuckDB v1.4.3. Public data generator: DuckDB v1.2.2, **SF0.01**;
`public_generator_sf001` is a legacy profile label, not SF0.001. Manifest hashes
retain exact data/SQL provenance. Relational-core removes only recorded outer
ORDER BY/LIMIT/OFFSET/FETCH; original SQL remains a separate configuration.
The old 494-configuration reports and S1 results remain immutable.

## Observed query results

The frozen main driver exited 0 after collecting **45/45 configurations** and
verifying frozen executable/baseline identities. Collection success is not an
all-query pass. There were **27 complete configurations / 81 compared rounds**:
22 nonempty changing passes, four empty-only and one unchanged-output witness.
Six original-SQL strict refusals and twelve generated-data SQL deadlines remain
separate outcomes.

| Data / backend / contract | Changing pass | Empty-only | Unchanged | Strict refusal | Deadline |
| --- | ---: | ---: | ---: | ---: | ---: |
| Directed / SQL database / relational_core | 13 | 0 | 0 | 0 | 0 |
| Directed / SQL database / original | 0 | 0 | 0 | 6 | 0 |
| Public SF0.01 / Arrow / relational_core | 8 | 4 | 1 | 0 | 0 |
| Public SF0.01 / SQL database / relational_core | 1 | 0 | 0 | 0 | 12 |

Known IDs: `7,9,13,17,22,26,27,35,36,39,44,49,85`.
Public Arrow Q13's output remained unchanged; Q39/Q44/Q49/Q85 remained empty.
The directed witnesses for all thirteen were nonempty and changing. Original SQL
Q7/Q17/Q36/Q39/Q44/Q49 retained sorting/row-bound refusal, each with one complete-node
blocker and zero executed rounds.

Public SQL Q9 completed three rounds in 238.077s under its explicit 600s asynchronous
budget. Other public SQL configurations used 180s asynchronous / 210s process
budgets: Q7/Q13/Q17/Q26/Q27/Q35 hit process deadlines; Q22/Q36/Q39/Q44/Q49/Q85
returned explicit asynchronous deadline failures. No completed numerical, bag or
signed-delta comparison reported a mismatch; incomplete runs are unverified.
There is no claim that those twelve failures are only import cost or that a
larger budget would necessarily pass. Durations include overlapping correctness
work and do not establish speedups.

The final-lineage candidate separately completed **4/4 configurations / 12 rounds**:
Q17/Q39 directed SQL passed, public Arrow Q17 passed and public Arrow Q39 remained
empty-only. Main and follow-up binary/source manifests remain distinct.

## Effective UPDATE follow-up

A round-by-round review found that the main insert/delete/update ordering left
zero UPDATE affected rows for Q7/Q13/Q17/Q22/Q26/Q27/Q35/Q36/Q49/Q85. Main results
remain valid insert/delete and no-op-round comparisons; those ten rounds are
**not** evidence of effective UPDATE maintenance. Even the other three main
UPDATE rounds did not change the query output after preceding deletions.

An independent `effective-update/` protocol freezes the final SQL CLI, uses each
same complete directed fixture in fresh state, then updates before deleting.
All **13/13** queries passed with **62 affected source rows / net_changed_rows=124**, and all thirteen observed output changes. Each query compares the MV
to independent full SQL under its declared result-column numerical contract,
checks exact CLI-rendered source images computed independently from the SQL
predicate and old row, reopens the database, compares its exact rendered endpoint,
and verifies the stored Arrow output history exactly. These are thirteen separate
one-update witnesses, not extra three-round main profiles or typed source-bit
comparisons through JSON. The main typed source-bag checks and dedicated Arrow
numeric/rank tests retain their separate stronger type/bit coverage.

Protocol, frozen CLI hash, generated SQL, raw CLI responses, exact commands and
per-query audit results live under `target/strict-batches-20261106/effective-update/`.
No additional engine or query text changes were needed for this follow-up. The original empty/no-op/update
outcomes and the twelve public-data SQL deadlines remain unchanged.

## Final engineering verification

Final delivery checks exited 0 on the final frozen crate source: **344 default
passed / 2 ignored**, **358 Iceberg passed / 3 ignored**, and **48 Python tests
passed**. Both all-target Clippy configurations used `-D warnings`; fmt, private
rustdoc in both configurations and mdBook passed. Existing ignored cases remain
unexecuted gates, not passes. Earlier red/compile/old-negative-test failures are
retained separately and do not replace the final executions.

Tests include independent integer and mathematical moment oracles, exact typed
source/delta bags, arbitrary deletions, duplicates/NULL, signed zero/NaN identity,
discard/retry/reopen, process death and fixed-delta growing-state regressions.
Stable moment tests use 54 refresh/reopen rounds at origins 0, 1e12 and 1e16;
rank tests cover both ordering directions, compound keys, NULLs and ties.

Rust 1.95.0, offline/locked dependencies, four build jobs, debug=0, compiler
incremental mode off, two Tokio workers and two Rust test threads. Dependency
artifacts reuse `target/roadmap-20261031`; **not** a clean dependency build.
`delivery-source.json` hashes the actual final crate files. Exact commands and
statuses are in `gates.json`; `checks.py` is the local orchestration record.

```sh
bash /tmp/optd-sql-env-20261029.sh env \
  CARGO_TARGET_DIR=/home/admin/xwork/optd/target/roadmap-20261031 \
  TMPDIR=/home/admin/xwork/optd/target/benchmark-20261106/tmp \
  TOKIO_WORKER_THREADS=2 RUST_TEST_THREADS=2 \
  cargo +1.95.0 test --offline --locked -p optd-incremental-sql --no-fail-fast
# Repeat with --features iceberg; focused tests:
# --test scalar_product --test numeric_groups --test moments --test rank_state
python3 target/strict-batches-20261106/checks.py
python3 target/strict-batches-20261106/report.py
```

The run scripts use exclusive fresh roots/frozen outputs; select new artifact
paths for a repeat. `report.html` is a self-contained local report, `summary.json`
and `work.json` retain counts and actual operation counters. Browser layout and
interaction were not verified. No remote runtime or controlled performance
qualification follows from these local checks.

## Verification and remaining gates

Final counts, timeouts, commands and hashes are collected in the independent
HTML/JSON report beside this document's local artifact directory. No full-99 or
performance claim follows from this focused thirteen-query qualification.

R03d remains open for VAR_SAMP/VAR_POP; R03e for general floating SUM/AVG;
R05a for specialized partition/frame algorithms; R05b for other global windows
and original sorting/Top-K. R06's general range/NULL/global dependency contract,
R07 checkpoint/operator persistence, R08 native source ingestion and R10 controlled
performance remain independent unfinished work. DELETE/UPDATE selection may scan
sources, and changed relations still write complete checkpoint payloads. A follow-up
should persist per-phase progress before completion (import, MV bootstrap, refresh,
full-SQL oracle, checkpoint and recovery), so a process deadline retains its exact
stage and partial counters rather than only an unknown-round outcome.
