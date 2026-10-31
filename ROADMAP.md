# Incremental SQL roadmap

**Status:** accepted direction by owner · 2026-10-31
**Scope:** `optd-incremental-sql` and its SQL, Optd, state, storage and source interfaces.
This is the work ledger. Acceptance criteria are targets, not completed evidence.

## Baseline and invariants

Current starting commit: `44d2ed2`. The SQL qualification at `c048cd6` recorded
21/22 TPC-H and 74/99 TPC-DS strict SQL loops in `relational_core` mode; 26 queries
were rejected. These are historical witnesses, not current-binary TPC or performance
certification. Original SQL and benchmark variants must remain separately identified.

Preserve signed full-row multiplicities, duplicates, NULL and old-row UPDATE
semantics, strict-plan refusal, atomic multi-view publication, candidate isolation,
request retry and recovery. Rules declare expressions and state; physical operators
execute them. Do not silently remove internal ordering or row bounds. No new
dependency, lockfile, format or numerical-contract change without its own design.

An affected range may legitimately grow: a global threshold, rank shift or first
outer-join match can change many outputs. Measure necessary output work separately
from avoidable scans. Never claim end-to-end delta complexity from bucket counters.

## Execution order and ledger

Use `pending`, `in_progress`, `verified` and `blocked` with evidence per increment.
Every implementation increment records its design, tests, limitations and commit.
Priority orders work; dependencies can move a prerequisite ahead of its consumer.

| ID | Priority | Work | Acceptance criterion | Status |
| --- | --- | --- | --- | --- |
| R01a | P0 | Refresh work accounting: state-directory copies, relation rebuilds and materialization | Actual operation-boundary counters distinguish rows, entries and Arrow bytes; fixed-delta tests expose current scale costs; explicit scope exclusions | verified |
| R01b | P0 | SQL selector and DML work accounting | Measure consumed source batches/rows separately from available relation rows; report target rebuild and delta generation | verified |
| R01c | P0 | Store and recovery work accounting | Record reused/written payloads and actual file bytes, metadata/history reads and bootstrap/recovery work; retries do not impersonate fresh execution | verified |
| R01d | P0 | Unified SQL/benchmark work reports | Separate semantics, strict planning, affected scope, source/state/materialization/storage work and controlled performance; unknown measurements remain unknown | verified |
| R02a | P1 | Consolidate table changes before rebuilding source state | One net update per changed source; cancellation skips source rebuild; preserve absent-row and atomic-batch validation | verified |
| R02b | P1 | Transaction table blocks and change overlays | Small writes avoid full target conversion; preserve duplicate rows, statement snapshots, swaps and read-your-writes | verified |
| R02c | P1 | Shared state directories and candidate overlays | Copy only changed directory blocks/entries while discarded/failed candidates leave committed state intact | verified |
| R02d | P1 | Lazy/chunked relation caches | Avoid complete Bag copies, repeated sorting and Arrow materialization for small changes; retain required output order | pending |
| R03a | P1 | Global and grouped MIN/MAX value indexes | Reference-counted ordered values survive duplicate extrema, final deletion, NULL, empty input and restart; qualify TPC-H Q15 and TPC-DS Q23 | pending |
| R03b | P1 | DISTINCT aggregate state | Per-value reference counts update on zero crossings; support global COUNT DISTINCT and mixed aggregates; qualify TPC-DS Q28 | pending |
| R03c | P1 | GROUPING SETS / ROLLUP | Maintain each grouping set with a grouping identity; distinguish synthetic and real NULL; verify the nine historical ROLLUP blockers independently | pending |
| R03d | P2 | Variance/STDDEV retractable aggregates | Define stable numerical state and deletion behavior; test TPC-DS Q17/Q39 against an independent oracle | pending |
| R03e | P2 | Floating aggregate qualification | Explicit numerical/rounding contract, special-value and multibatch tests; no removal of existing guards merely to enlarge eligibility | pending |
| R04a | P1 | Outer/semi/anti match-count algorithms | Handle first/last matches, two-sided changes, residuals, duplicates and NULL; avoid unchanged peer-pair recomputation beyond necessary output | pending |
| R04b | P1 | Mark joins | Maintain TRUE/FALSE/UNKNOWN with correlated/null semantics; qualify TPC-DS Q10/Q35/Q45 | pending |
| R04c | P2 | Non-equality joins and range indexes | Rules prove affected ranges; exact deletes and boundary movement tests; explicit refusal when no sound scope is available | pending |
| R05a | P2 | Window-specific physical maintenance | Partition aggregates use accumulators; finite frames use ordered ranges; ranking tracks affected positions; preserve cached Before output and ties across restart | pending |
| R05b | P2 | Unpartitioned windows, ordering and Top-K | Ordered state, ties, NULL order and delete refill preserve original SQL membership; internal ORDER BY/LIMIT remain semantic operators | pending |
| R06 | P1 | Extensible dependency/impact contracts | Represent group keys, join keys, partitions, ranges and global dependencies; propagate threshold changes to unchanged source keys without missing outputs | pending |
| R07a | P1 | Delta logs and block checkpoints | Changed blocks/deltas replace full changed-relation payload writes; publication and SIGKILL recovery remain atomic | pending |
| R07b | P1 | Persist operator state and restore incrementally | Version states and rule identity; recover with bounded replay; incompatible states fail or rebuild explicitly | pending |
| R07c | P1 | Request indexes, history retention and compaction | Retry lookup avoids history traversal; compaction retains retry/replay/recovery reachability and never deletes live referenced files | pending |
| R08 | P1 | Incremental source ingestion | Read changed files/native changes rather than both full endpoints; qualify delete-file semantics and source provenance separately from host-local atomicity | pending |
| R09a | P2 | Physical algorithm alternatives and cost selection | Compare qualified delta/indexed/scoped algorithms using measured state and change sizes; strict mode never silently selects full recomputation | pending |
| R09b | P2 | Reusable execution templates | Avoid repeatedly planning/materializing local DataFusion expressions; inputs remain epoch-correct and task-isolated | pending |
| R09c | P1 | Plan preflight and complete blockers | Enumerate every blocking node with precise function/type/scope reason before CREATE MV; unsupported plans publish nothing | pending |
| R10a | P0 | Scaling and work-scope regressions | Hold changes, bucket size and required output fixed; grow untouched data; assert source reads, state copies, materialization and writes separately | pending |
| R10b | P1 | Long-running randomized SQL/recovery loops | Mix transactions, duplicate/NULL/key moves, hot keys, window peers, discarded candidates and deterministic crash points; independent SQL and delta-prefix oracles | pending |
| R10c | P1 | Controlled performance qualification | Record machine, source/data/binary identities and isolated state; compare full vs incremental at multiple change ratios; no performance claim from correctness durations | pending |
| R11 | P2 | Engineering quality and internal contract protection | Preserve Google Rust guidance, review critical internal docs, default/Iceberg format/lint/rustdoc/tests, and local source ownership; public missing_docs alone is insufficient | pending |
| R12 | P3 | SQL product and concurrency extensions | Separately design MV-on-MV dependency DAGs, read-only reader snapshots, online bootstrap, DDL evolution and constraints; keep distributed transactions outside the local protocol | pending |

## First increment

Start with [refresh work accounting](docs/src/incremental/work_accounting.md),
then R02a. R01b/R01c/R01d extend the same accounting contract before claiming a
complete end-to-end baseline. R10a adds assertions alongside each implementation.

## Evidence ledger

No new TPC runtime or performance qualification is recorded yet. Historical evidence:
`/tmp/optd-sql-database-acceptance-20261031/qualification.json`.
Keep generated reports outside Git. Commit this owner-requested roadmap; preserve
pre-existing unrelated Markdown and source changes.

R01a implementation commit: `64e0db7` (local; no push in this increment).

### R01a — refresh accounting

Implemented candidate-local `RefreshWork`, split into source/cache map rebuilds,
cache materialization and derived state-directory copies. Copy counts refer to
distinct full-row entries; materialized row counts expand duplicates. Arrow array
bytes are reported buffer sizes, not allocation traffic or disk IO. Multiple
directory copies count separately, including eager clones for empty refreshes.

**Verified:** seven focused tests compare independent full-query endpoints, grow
untouched input at fixed changes, check duplicate-count units, expose directory
copies for aggregates/joins/windows, isolate concurrent/failed/stale candidates,
and retain maintenance JSON through SQL retry/restart/output-history verification.

Environment: Rust 1.95.0, locked offline dependencies, four build jobs, debug=0,
incremental compiler disabled. A fresh target on the project filesystem is used:
`target/roadmap-20261031`; `TMPDIR` points to its `tmp` subdirectory. Commands:

```sh
bash /tmp/optd-sql-env-20261029.sh env \
  CARGO_TARGET_DIR=/home/admin/xwork/optd/target/roadmap-20261031 \
  TMPDIR=/home/admin/xwork/optd/target/roadmap-20261031/tmp \
  cargo +1.95.0 test --offline --locked -p optd-incremental-sql --test refresh_work
```

Result: exit 0, 7 passed, 0 failed. Log:
`target/roadmap-r01a-tests-20261031.log`. The first link attempt on the full
temporary filesystem failed with SIGBUS (exit 101); it is not a test failure or
a successful verification. Only its exact unused temporary linker output was
removed after checking ownership, link count and open handles.

**Verified gates:** the same wrapper/target/TMPDIR with
`bash scripts/agent-check.sh sql` exited 0: formatting, Clippy with warnings denied,
160 default tests (2 ignored) and 174 Iceberg tests (3 ignored). Log:
`target/roadmap-r01a-gates-20261031.log`. Ignored tests are the explicit full TPC
qualification, large Q21 recovery witness and the feature's subprocess-only driver;
this run does not replace those qualification runs.

The same environment with `RUSTDOCFLAGS='-D warnings'` and
`cargo +1.95.0 doc --offline --locked -p optd-incremental-sql --no-deps --document-private-items`
exited 0; log `target/roadmap-r01a-rustdoc-20261031.log`.
`MDBOOK_BUILD__BUILD_DIR=/home/admin/xwork/optd/target/roadmap-book-20261031 mdbook build docs`
also exited 0; log `target/roadmap-docs-20261031.log`.
SQL selector/DML, external source reads, DataFusion internals, local bucket payloads,
physical expression materializations and complete replacement-cache collection,
checkpoints and recovery work are excluded;
R01b–R01d remain pending. This is diagnostic and semantic evidence, not a measured
performance improvement or end-to-end delta-complexity qualification.

### R02a — consolidate atomic source changes

Implemented consolidation before source replacement. Every incoming change still
checks its table name, exact schema and weights. Probing only its incoming rows
preserves ordered intermediate source overflow checks; consolidated deltas retain
their own checked arithmetic. Only the final relation must be nonnegative. Apply
each nonempty net delta once; canceled sources share the committed Bag. No-op
epochs, candidate isolation and publication formats are unchanged.

**Verified regression:** before the implementation, the R01a wrapper/target/TMPDIR
and `cargo +1.95.0 test --offline --locked -p optd-incremental-sql --test refresh_work`
exited 101 (9 passed, 4 failed). The failures expose three source rebuilds instead
of one, four instead of two for interleaved sources, four instead of one for a
valid signed-prefix batch, and three instead of zero for extreme cancellation.
Log: `target/roadmap-r02a-red-20261031.log`.

After implementation the same command exited 0 (13 passed, 0 failed), log
`target/roadmap-r02a-green-20261031.log`. The added cases use independent complete
SQL endpoints with growing untouched input, split updates, interleaved outer-join
inputs, negative intermediate counts, schema/name/weight rejection, final absent
deletions, source/delta overflow before later cancellation and representable
`i64::MIN` prefixes that cancel without materialization.

**Verified gates:** same wrapper/target/TMPDIR and `bash scripts/agent-check.sh sql`
exited 0: formatting, both Clippy configurations, 166 default tests (2 ignored)
and 180 Iceberg tests (3 ignored), including independent SQL semantics, local
durability, output replay and process-death recovery. Log:
`target/roadmap-r02a-gates-20261031.log`. The ignored qualification cases have the
same explicit exclusions as R01a; no full TPC matrix was rerun.

The R01a private-rustdoc and mdBook commands also exited 0 on this increment;
logs: `target/roadmap-r02a-rustdoc-20261031.log` and
`target/roadmap-r02a-docs-20261031.log`. Implementation is the local commit titled
`Consolidate atomic source changes before rebuilding tables`; no push is included.

Source-map copies still grow with
the complete source once per nonempty source change; cache rebuilds and state
directory copies remain. Consolidating staged deltas also has its own work.
This increment removes repeated complete-source rebuilds; it does not establish
end-to-end delta complexity or a controlled latency/memory improvement.

### R01b — actual SQL selector and target work

Design: [DML work accounting](docs/src/incremental/dml_work_accounting.md).
The physical scan providers count delivered batches, expanded rows and Arrow buffer
bytes without retaining batches or introducing background work. Preserve provider
statistics, fetch, projection and scalar subquery behavior. VALUES counts zero
relation reads. `source_relation_rows` retains its logical meaning. Optional
`DmlWork.execution` separates actual selector output, target collection/full-map
copy/materialization and old/new row images. Old receipts retain unknown fields;
identified retries retain their original diagnostic and `replayed=true`.

**Verified regression:** the six initial focused cases failed against the previous
implementation (exit 101, missing execution fields), log
`target/roadmap-r01b-red-20261031.log`. Subsequent seven focused cases passed (exit 0),
log `target/roadmap-r01b-green-20261031.log`. The complete gates additionally tested
a mixed table/constant UNION branch. Cases cover fixed changes with growing
untouched input, eliminated/limited/repeated reads, scalar subqueries, duplicate
outputs, NULL rejection, transaction snapshots, retries/reopen and concurrent roots.
Two unit tests check Pending/error/empty/zero-column stream behavior and atomic
counter-overflow rejection.

Commands use the R01a wrapper, Rust 1.95.0, locked offline dependencies, four jobs,
debug=0, compiler incremental disabled, project target/TMPDIR. Focused command:
`cargo +1.95.0 test --offline --locked -p optd-incremental-sql --test dml_work`.
`bash scripts/agent-check.sh sql` exited 0: format, both Clippy configurations,
175 default tests (2 ignored), 189 Iceberg tests (3 ignored). Log:
`target/roadmap-r01b-gates-20261031.log`. Ignored qualification cases remain excluded.
The private-rustdoc command from R01a exited 0; log
`target/roadmap-r01b-rustdoc-20261031.log`. mdBook via the same wrapper and
`MDBOOK_BUILD__BUILD_DIR=/home/admin/xwork/optd/target/roadmap-book-20261031`
exited 0; log `target/roadmap-r01b-docs-20261031.log`. An initial unwrapped invocation
could not find mdBook (exit 127); it is not reported as a successful build.

The existing resolved futures and async-trait APIs were exposed in separate local
commits `d1d1f74` and `c85014b`; versions and resolved package identities are unchanged.
Implementation is local-only. Counting delivered Arrow buffers does not measure
allocator/device IO. Full target conversion and replacement remain visible costs;
no efficiency or full TPC/performance qualification is claimed.

### R01c — SQL database storage and recovery work

Design: [store and recovery accounting](docs/src/incremental/store_work_accounting.md).
Expose cumulative `SqlSession::store_work()` and immutable `recovery_work()`.
Record successful file-helper boundaries using actual serialized lengths and rows:
CURRENT/manifest reads/writes, checksum passes, table/view/delta reads/writes/reuse,
lineage traversals and visits. Reopen separately records per-view dependency inputs,
reconstructed output materialization and durable output comparison. Diagnostics are
not persisted and never change original receipt identity. Completed operations
remain inspectable in fenced sessions after a later barrier fails.

**Verified feature baseline:** before implementation, the focused `database_work`
target failed compilation (exit 101) because the new APIs did not exist; this is
not a pre-existing semantic failure. Log `target/roadmap-r01c-red-20261031.log`.
Four initial integration cases then passed (exit 0), log
`target/roadmap-r01c-green-20261031.log`. Complete gates include a fifth case for
shared dependencies across two restored views, plus one atomic-counter overflow
unit test. Cases compare written byte counts with actual payload/manifest/CURRENT
files, expose full writes at fixed changes with growing data, check no-op reuse,
retry vs history vs recovery boundaries, NULL/duplicate-sensitive endpoints,
isolated roots and failures at both actual publication barriers.

Commands use the R01a wrapper/environment and project target/TMPDIR. Focused:
`cargo +1.95.0 test --offline --locked -p optd-incremental-sql --test database_work`.
`bash scripts/agent-check.sh sql` exited 0: format, both Clippy configurations,
181 default tests (2 ignored), 195 Iceberg tests (3 ignored), including subprocess
recovery. Log `target/roadmap-r01c-gates-20261031.log`. Private rustdoc with warnings
denied and mdBook in the R01a environment also exited 0; logs
`target/roadmap-r01c-rustdoc-20261031.log`, `target/roadmap-r01c-docs-20261031.log`.
No full TPC matrix or controlled performance comparison was executed.

This verification covers the host-local SQL database, not standalone checkpoint
or native-source IO. Partial failed helpers, directory/fsync work, allocator/device
traffic, comparison CPU and internal bootstrap operator visits remain unmeasured.
Read volumes describe decoded file lengths; hash passes are separately counted.
Current whole-payload writes and history scans remain visible costs. Local commit
only; no publication is included.

### R01d — unified work reporting

Design: [unified SQL and benchmark reports](docs/src/incremental/unified_work_reports.md).
The SQL CLI separates current execution diagnostics from original retry receipts,
command-local storage from successful open recovery, and unavailable SELECT/CREATE
bootstrap internals from measured zero. SQL benchmark rounds capture transaction
storage, pre-reopen verification, recovery and post-reopen verification separately.
Typed/Iceberg benchmark rounds retain explicit unknown storage/recovery fields.
Checked `StoreWork::since` produces same-session intervals and rejects underflow.

The work audit accepts typed refresh stats and SQL commit maintenance, includes
source/cache rebuild/materialization, state copies, selector/target work, storage
and recovery. Preserve per-domain measured/unknown round counts and observed partial
sums. Original replay receipts are never summed as fresh execution. Plan blockers,
semantic status and scope/performance qualification remain independent; a missing
plan does not discard otherwise measured work. Reject negative, boolean and
noninteger measurements, contradictory blockers and incomplete round lists.

**Verified regression:** initial work-audit tests against the old implementation
exited 1 (10 executed, 1 failure, 3 errors; includes duplicated inherited checks
subsequently removed), log `target/roadmap-r01d-red-python-20261031.log`.
The final complete Python tool suite exited 0 (38 tests), log
`target/roadmap-r01d-tools-20261031.log`:
`python3 -m unittest discover -s optd-incremental-sql/benchmarks/tools -p 'test_*.py'`.
Rust focused CLI and benchmark tests exited 0 (7 passed, 1 full-matrix test ignored),
log `target/roadmap-r01d-green-rust-20261031.log`. They check real CLI request/retry,
reopen/history attribution and an independent wide SQL three-mutation script.
A unit test checks every serialized StoreWork counter, zero intervals and underflow.

**Verified gates:** R01a environment/wrapper and project target/TMPDIR with
`bash scripts/agent-check.sh sql` exited 0: formatting, both Clippy configurations,
183 default tests (2 ignored), 197 Iceberg tests (3 ignored). Log
`target/roadmap-r01d-gates-20261031.log`. Private rustdoc with warnings denied and
mdBook also exited 0; logs `target/roadmap-r01d-rustdoc-20261031.log` and
`target/roadmap-r01d-docs-20261031.log`. Existing ignored cases remain unexecuted.

**Verified actual reports:** built `--example benchmark_sql_database` with the
R01a locked/offline environment (exit 0), log
`target/roadmap-r01d-benchmark-build-20261031.log`. Binary SHA-256:
`6dd9878634ad1d78ea6e13ed6c085a76cfd1f4bc5514c8210b8c552fb851ea5d`.
It was built from working-tree R01d changes over `51cfb0f`, not an unchanged older
committed binary. Execute each with `OPTD_SQL_BENCH_QUERY_MODE=relational_core`,
`OPTD_SQL_BENCH_QUERY_TIMEOUT_SECS=180`, a distinct fresh
`OPTD_SQL_BENCH_DATABASE_ROOT=target/roadmap-r01d-live-20261031/<suite>-q1`, and
`timeout --signal=TERM --kill-after=5s 240s target/roadmap-20261031/debug/examples/benchmark_sql_database <manifest> <report> 1`:

* TPC-H manifest `optd-incremental-sql/benchmarks/tpch/manifest.json`, report
  `target/roadmap-r01d-live-20261031/tpch-q1.json`, SHA-256
  `a72736b3150931717730000f61dd243c38bf5a2eea6cefd1cea6a2558360c4d7`.
* TPC-DS witness manifest `/tmp/optd-efficiency-tpcds-core-20261030/q01/fixture/manifest.json`,
  report `target/roadmap-r01d-live-20261031/tpcds-q1.json`, SHA-256
  `64c13b98bfad8a9b9f751af745ff9b2832f95b2655d8736c10b9ad9c3411c118`.

Both processes exited 0, strict plans passed, all three INSERT/DELETE/UPDATE rounds
compared independent base-table/MV endpoints, replayed output history and reopened
successfully. Actual report audit via `summarize_incremental_work.py` exited 0;
exact argument array (including generated aggregate SHA-256 identities) is
`target/roadmap-r01d-live-20261031/audit-command.json`, output `unified-work.json`,
log `audit.log` in the same directory. `work-audit.json` records binary/report identities.
The three-round TPC-H case records 18013 transaction table rows written, 12009
selector source rows and 18013 reconstruction dependency rows; corresponding
TPC-DS witness counts are 10, 8 and 10. These count distinct operation boundaries,
not one additive IO or complexity total.

Only these two current-binary queries were rerun, with different fixture scales.
`relational_core` remains distinct from original SQL, unavailable source acquisition
remains unknown, and `performance_qualified=false`/scope unknown are preserved.
No full TPC matrix, allocator/device-IO measurement or controlled performance claim.
Implementation is local-only.

### Next increment

R02b: review the immutable transaction-table block design, implement bounded
changed-leaf updates and snapshot binding, and prove statement work separately
from remaining directory copying and full checkpoint materialization.


R02b design review is complete:
[immutable transaction table blocks](docs/src/incremental/transaction_table_blocks.md).
Use shared bounded sorted leaves, preserve no-converter singleton admission and
final-state UPDATE checks, measure directory clones and checkpoint flattening
separately. Implementation and regression gates remain outstanding; no source
representation change or efficiency improvement is claimed yet.


### R02b — immutable transaction table blocks

Design: [transaction table blocks](docs/src/incremental/transaction_table_blocks.md).
Table snapshots share bounded ordered leaves with cached Arrow batches. DML updates
only touched leaves; selectors bind their batches without a full-table collection.
Snapshots share the same Arrow row converter. Final global resource checks precede
leaf expansion; bounded signed-delta encoding preserves valid two-image updates.
Store flatten misses and recovery/bootstrap conversion are separately measured;
old serialized diagnostics retain unknown new domains.

**Verified regression:** old code failed all three new table-block tests (exit 101),
`target/roadmap-r02b-red-20261031.log`. After implementation they passed (exit 0),
`target/roadmap-r02b-green-20261031.log`. Fixed VALUES insertion at 1024/8192 input
rows reads zero selector rows, collects zero target rows, copies 256 historical
leaf entries and materializes 257 leaf rows. Directory copies remain 4/32; full
checkpoint concatenation and writes remain 1025/8193 rows. These are measured
boundaries, not an end-to-end performance claim. Independent multiset/SQL cases
exercise key moves, duplicates/NULL, swaps, splits, discarded/failed candidates,
old-root retention, schema/scalar/encoding/volume guards, zero-column rows and
multi-chunk signed encoding. Existing DML counter assertions now expect actual
leaf work rather than the removed complete-target conversion.

Commands use R01a's Rust 1.95.0 offline/locked wrapper, four jobs, debug=0, compiler
incremental disabled, project target/TMPDIR. Focused command:
`cargo +1.95.0 test --offline --locked -p optd-incremental-sql --lib --test cli --test database_work --test dml_work --test table_blocks`.
Log `target/roadmap-r02b-focused-20261031.log` exited 0; later complete gates include
three additional unit cases. Initial full gates failed both Clippy modes on a new
test modulo spelling, retained in `target/roadmap-r02b-gates-20261031.log`. After
correction, `bash scripts/agent-check.sh sql` exited 0: format, both Clippy modes,
194 default tests (2 ignored), 208 Iceberg tests (3 ignored), including process-death
recovery. Log `target/roadmap-r02b-gates-fixed-20261031.log`. Required private rustdoc,
mdBook and 38 Python tool tests exited 0; logs `target/roadmap-r02b-rustdoc-20261031.log`,
`target/roadmap-r02b-docs-20261031.log`, `target/roadmap-r02b-tools-20261031.log`.

Selected current working-tree binary SQL witnesses passed TPC-H Q1 and TPC-DS Q1,
three INSERT/DELETE/UPDATE rounds, independent SQL, reopen and delta-history checks.
Artifacts and binary/report hashes: `target/roadmap-r02b-live-20261031/`.
Commands use `OPTD_SQL_BENCH_QUERY_MODE=relational_core`, query timeout 180s,
fresh `OPTD_SQL_BENCH_DATABASE_ROOT` per suite and
`timeout --signal=TERM --kill-after=5s 240s target/roadmap-20261031/debug/examples/benchmark_sql_database <manifest> <report> 1`.
Manifests: `optd-incremental-sql/benchmarks/tpch/manifest.json` and
`/tmp/optd-efficiency-tpcds-core-20261030/q01/fixture/manifest.json`.
`audit-command.json` records the work audit; both selected strict plans and semantic
witnesses passed. No full TPC matrix or controlled performance rerun occurred;
`performance_qualified=false`. Directory copies, batch-header clones, retained flat
caches, full history/bootstrap and full checkpoint writes remain. Implementation is
local-only; next work is R02c, followed by the remaining ledger.


### R02c — shared ordered state directories

Design: [shared state directories](docs/src/incremental/shared_state_directories.md).
Implemented an internal persistent AVL directory for table leaves, aggregate groups,
accumulators and join/window input/output indexes. Root clones share branches;
updates copy only shared nodes on changed paths and balancing siblings. Exclusive
nodes move their entries. Count copies immediately around explicit clones following
`Arc::try_unwrap`, not a racy ownership-size estimate. Record completed physical
updates, including window output mutation; empty/canceled candidates copy zero.

**Verified regression:** the new `state_directory_work` test failed on the old code:
512 accumulator entries copied for one changed key among 256 (bound 36), exit 101,
`target/roadmap-r02c-red-20261031.log`. An initial test borrow typo failed compilation,
retained separately as `target/roadmap-r02c-test-compile-20261031.log`. After the
implementation, focused tests passed (exit 0): 55 unit cases including four directory
cases in `target/roadmap-r02c-green-fixed-20261031.log` plus the later full gate;
13 refresh, one multi-family scaling and three SQL table-block cases in
`target/roadmap-r02c-focused-final-20261031.log`. The earlier unit log contains 54
cases before the additional clone-boundary test; final complete gates include 55.

Scaling holds one affected key/bucket fixed: 256/2048 unrelated-key inputs copy
8/11 directory entries respectively for grouped MIN, SUM accumulator and outer
join; window input/output each copies 8/11. Independent SQL endpoints match.
A separate BTreeMap oracle checks 4000 mixed operations, exact balance/height/length,
predecessors, retained roots and complete deletion. Shared payload/copy bounds are
checked up to 65536 keys. A counted-value oracle independently verifies every actual
entry clone across 2048 monotone insertions/interior deletions and rotations.

The old full-directory cost assertions were intentionally replaced by path bounds;
concurrent updates to different keys may now have different path lengths. Their
source/cache equality and per-candidate work stability remain asserted, including
failed/stale candidates. Initial adaptation failures are retained in
`target/roadmap-r02c-existing-costs-20261031.log` and
`target/roadmap-r02c-focused-20261031.log`; no semantic expectations were regenerated.

Commands use R01a's wrapper/toolchain/target/TMPDIR. Focused:
`cargo +1.95.0 test --offline --locked -p optd-incremental-sql --lib --test state_directory_work --test refresh_work --test table_blocks -- --nocapture`.
`bash scripts/agent-check.sh sql` exited 0: format, both Clippy modes, 199 default
tests (2 ignored), 213 Iceberg tests (3 ignored), including process-death recovery.
Log `target/roadmap-r02c-gates-20261031.log`. Private rustdoc, mdBook and 38 Python
tool tests exited 0; logs `target/roadmap-r02c-rustdoc-20261031.log`,
`target/roadmap-r02c-docs-final-20261031.log`, `target/roadmap-r02c-tools-20261031.log`.
Selected strict SQL TPC-H Q1/TPC-DS Q1 passed three DML rounds, independent SQL,
reopen and delta history using the R02b commands with fresh R02c artifact roots.
Reports, binary/report hashes and work audit: `target/roadmap-r02c-live-20261031/`.

This removes full directory cloning, not source/cache Bag rebuilding, selector
scans, per-bucket work, Arrow flattening, bootstrap/history or checkpoint costs.
Directory allocations and tree comparisons are not allocator-byte or CPU counters.
No full TPC matrix or controlled performance qualification was run. Local-only
implementation; next work is R02d and all remaining ledger entries.
