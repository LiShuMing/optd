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
| R02d | P1 | Lazy/chunked relation caches | Avoid complete Bag copies, repeated sorting and Arrow materialization for small changes; retain required output order | verified |
| R03a | P1 | Global and grouped MIN/MAX value indexes | Reference-counted ordered values survive duplicate extrema, final deletion, NULL, empty input and restart; qualify TPC-H Q15 and TPC-DS Q23 | verified |
| R03b | P1 | DISTINCT aggregate state | Per-value reference counts update on zero crossings; support global COUNT DISTINCT and mixed aggregates; qualify TPC-DS Q28 | verified |
| R03c | P1 | GROUPING SETS / ROLLUP | Maintain each grouping set with a grouping identity; distinguish synthetic and real NULL; verify the nine historical ROLLUP blockers independently | in_progress |
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
implementation, focused tests passed (exit 0): unit directory cases in `target/roadmap-r02c-green-fixed-20261031.log`
plus the later full gate;
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


### R02d — stage one: shared maintained blocks and lazy complete caches

**Status: in_progress.** Design:
[chunked relation caches](docs/src/incremental/chunked_relation_caches.md).
Extracted the SQL row-block implementation into the neutral `row_blocks` module;
SQL keeps its existing TableRows import and diagnostic aliases. Maintained graph
relations now share bounded leaves, update touched leaves and defer complete Arrow
concatenation until a snapshot, ordering or root-result demand. Root contents is
finalized before prepare returns. Bootstrap/replacement physical sequence remains
available. Complete replacements record collection and leaf bootstrap separately.
Extended diagnostics use one boxed value with flattened JSON, preserving existing
Iceberg result field types while avoiding a large enum allocation footprint.

**Verified partial scope:** with fixed one-row changes and fixed one-row SUM root
output, growing input from 1024 to 8192 leaves cache copying at 514 historical
entries, leaf materialization at 516 rows, and complete concatenation at one root
row/miss. Directory node copies grow from 8 to 12. Independent full SQL agrees.
Three `relation_block_work` cases additionally retain an unread raw source at
`i64::MAX` multiplicity and delete it back to its starting state; eager constant
projection/COUNT duplicate expansion remains explicitly measured, not fixed.
Raw-source complete-map copies remain 1024/8192 and are not relabeled as leaf work.

An initial whole-R02d target failed against the old source cost (exit 101),
`target/roadmap-r02d-red-20261031.log`; it included COUNT's constant projection and
is not a claim that the final SUM stage-one case was run unchanged on the baseline.
`target/roadmap-r02d-cache-focused-fixed-20261031.log` passed 55 unit, one initial
scaling and 12 SQL semantic cases. Later gates include all three new integration
cases. The prior complete-cache materialization assertion was changed to include
leaf splitting and a separately asserted complete-root miss. Intermediate wrong
test-target/compile/lint attempts are retained under the R02d logs.

Commands use R01a's wrapper/toolchain/target/TMPDIR. Initial complete gates retained
both Clippy failures (new test comparison style and expanded Iceberg enum layout)
in `target/roadmap-r02d-stage1-gates-20261031.log`. After fixes,
`bash scripts/agent-check.sh sql` exited 0: fmt, both Clippy modes, 202 default
tests (2 ignored) and 216 Iceberg tests (3 ignored). Log:
`target/roadmap-r02d-stage1-gates-fixed-20261031.log`. Private rustdoc, 38 Python
report-tool tests and mdBook exited 0; logs
`target/roadmap-r02d-stage1-rustdoc-final-20261031.log`,
`target/roadmap-r02d-stage1-tools-20261031.log`,
`target/roadmap-r02d-stage1-docs-verified-20261031.log`.

**Verified frozen-binary SQL witnesses:** TPC-H Q1 and TPC-DS Q1 each pass strict
planning, three DML rounds, independent SQL, reopen and delta history. Rebuild the
default example after feature tests, copy it into a fresh artifact root as `runner`,
hash before execution and verify the same hash after both runs. Use R02b's query
mode/timeouts/manifests and independent database roots; coverage reports record the
actual frozen runner command. Artifacts, source/binary/report hashes and exact work
audit: `target/roadmap-r02d-stage1-verified-live-20261031/`. An earlier successful run
at `target/roadmap-r02d-stage1-live-20261031/` had a post-run binary hash from a
shared path subsequently replaced by Cargo feature tests; that identity limitation
is preserved, and the frozen rerun supplies the bound binary evidence. Prior
unfrozen hash records likewise remain observations, not proof of loaded-file identity.

R02d remains incomplete: raw-source state still copies full Bags, touched duplicate
leaves still expand eagerly, bootstrap/replacement leaf caches may retain complete
row volume, and full checkpoints/recovery still rebuild/write complete state.
No full TPC or controlled performance qualification was run. Implementation is a
local, reviewable first stage; continue source-state sharing and leaf laziness before
marking R02d verified, then proceed through the remaining roadmap.


### R02d — shared weighted raw-source stage

Design: [shared weighted raw-source state](docs/src/incremental/raw_source_state.md).
Source revisions now share exact full-row hash-collision buckets and AVL directory
paths, without Arrow encoding or expansion during raw-weight updates. Prefix checks
probe only incoming rows, then each nonempty source net delta updates its touched
buckets once. Complete SourceRead/export demand collects and caches a Bag; demanded
Arrow output retains its existing canonical ordering and safety budgets. Huge unread
positive multiplicities remain admitted and retractable. No format/dependency or
rule-eligibility change is included.

`RefreshWork.sources` now measures actual bucket-map rebuilds/history copies;
`source_state` separately records consolidated source updates, incoming-row probes,
touched buckets, actual directory copies and graph-demanded full Bag collections.
It shares the existing boxed `cache_work` allocation to retain public Iceberg enum
field types/sizes. External snapshot/export/checkpoint Bag collection and source
physical-expression expansion remain explicit scope exclusions; their work is not
reported as measured zero. Hash collisions can enlarge a touched bucket: bounds
hold the affected collision-bucket size fixed.

**Verified regression:** the same fixed-delta/fixed-SUM-root integration test failed
before the source change (exit 101, copied 1024 historical source entries) and passes
afterward. Logs: `target/roadmap-r02d-source-red-20261031.log` and
`target/roadmap-r02d-source-measurements-20261031.log`. At 1024/8192 source rows,
actual historical source entries copied are **1/1**, source-directory node copies
**9/13**, source updates/probes/touched buckets **1/1/1**, complete source Bag
collections **0/0**. Maintained cache history copies/materialized rows remain
514/516; required root concatenation remains one row/one miss. Necessary full
SourceRead has a separate integration assertion for actual collection.

Five unit cases cover constant-hash collision equality, duplicate/NULL/key moves,
final/absent deletes, signed-zero/NaN payload identity, prefix overflow, unread
`i64::MAX` storage/retraction, and independent exact-map oracles for 1500 changes
each with normal/constant hashing, discarded candidates and retained roots.
Four integration cases cover scaling, independent SQL, huge unread weights,
remaining eager duplicate expansion and fetched-source collection accounting.
Existing custom-rule, SQL atomicity/retry/reopen and process-death cases also pass.

Commands use the R01a wrapper/target/TMPDIR and Rust 1.95.0 locked offline setup.
`bash scripts/agent-check.sh sql` exited 0: fmt, both Clippy configurations,
**208 default tests passed / 2 ignored**, **222 Iceberg tests passed / 3 ignored**.
Log: `target/roadmap-r02d-source-gates-final-20261031.log`. The initial gate's
Clippy rejection of a test's manual modulo predicate is retained in
`target/roadmap-r02d-source-gates-20261031.log`; it was fixed before the final full
run. The private-rustdoc command from R01a exited 0, log
`target/roadmap-r02d-source-rustdoc-20261031.log`. mdBook and 38 report-tool Python
tests exited 0, logs `target/roadmap-r02d-source-docs-20261031.log` and
`target/roadmap-r02d-source-tools-20261031.log`.

After all feature jobs completed, explicitly built the default
`benchmark_sql_database` example and copied it into a fresh immutable execution
artifact root. TPC-H Q1 / TPC-DS Q1 each pass strict planning, three SQL DML rounds,
independent full SQL, reopen and output-delta replay. Artifacts and exact per-query
commands: `target/roadmap-r02d-source-live-20261031/`; the runner hash agrees before
and after execution. Source HEAD/diff/new-module/Cargo.lock identities, report
hashes, aggregate wrappers and the numeric work audit are preserved there. Build
and run logs: `target/roadmap-r02d-source-bench-build-20261031.log` and
`target/roadmap-r02d-source-witness-20261031.log`. These are two small current-binary
witnesses, not full TPC or performance qualification. Both qualification flags stay
false.

This is a local-only implementation increment following design commit `25f6afe`.
**R02d remains in progress**: duplicate leaf weights still expand eagerly, and SQL
provider demand/leaf attribution must be completed. R03 onward, full qualification,
checkpoint/durable operator-state work and the rest of the ledger remain pending.


### R02d — demand-only maintained graph leaves

Design commit: `d35fb0b`,
[demand-only graph leaves](docs/src/incremental/lazy_relation_leaves.md).
Graph relation bootstrap/replacement selects lazy leaf caches; SQL table bootstrap
retains its existing eager policy. Each graph leaf retains exact weighted rows and
precomputed volume, validates its distinct-row boundary using the shared revision
converter, and expands only at an actual snapshot/root demand. Full original
physical batches still preserve bootstrap/replacement ordering. Required root
contents are finalized during prepare. SQL batch/no-op comparison failures now
propagate through typed Results; SQL read/write behavior and persisted formats
are unchanged.

Separate current diagnostics record update-time graph map work (`caches`),
distinct-row identity/boundary batches (`leaf_validation`), actual expanded leaf
misses (`demanded_leaves`) and full concatenations (`complete_caches`). The boxed
extension keeps existing public Iceberg enum field types/sizes unchanged. No new
mandatory fields are added to deserialized historical work records. Temporary
scalar/sort allocations and source expression expansion retain their exclusions.

**Verified regression:** before implementation, COUNT's constant-projection
regression failed its zero-update-expansion assertion with 1541 rows at the first
1024-row size; log `target/roadmap-r02d-leaf-red-20261031.log`. Afterward the test
passes for 1024/8192 rows, asserting zero update-time expansion, one required root
leaf row, bounded distinct validation and equality with independent full SQL.
Fixed SUM output at those sizes measures **0/0 update-time expanded rows**,
**514/514 distinct leaf-validation rows**, **1/1 demanded leaf rows** and one root
concatenation. Source history copying remains 1/1 and directory copies 9/13.
Log: `target/roadmap-r02d-leaf-measurements-20261031.log`. Work is bounded here by
fixed leaf entries/required output, not a general end-to-end performance claim.

Nine row-block unit cases pass, including two new cache-sharing/demand cases.
Existing randomized multiset, dictionary/float identity, budget/fallback, bulk
key-move and zero-column cases now exercise both eager/lazy policies. SQL target
block-work expectations remain unchanged. Complete default/Iceberg tests cover
ordered/limited plans, hidden-key sequence changes, window peers, custom rules,
concurrent/stale/failed candidates, retry/reopen and process-death recovery.

Using the R01a Rust 1.95.0 locked offline wrapper/target/TMPDIR,
`bash scripts/agent-check.sh sql` exited 0: format and both Clippy configurations,
**210 default passed / 2 ignored**, **224 Iceberg passed / 3 ignored**. Log:
`target/roadmap-r02d-leaf-gates-20261031.log`. The private-rustdoc command from R01a,
mdBook and 38 report-tool tests exited 0; logs:
`target/roadmap-r02d-leaf-rustdoc-20261031.log`,
`target/roadmap-r02d-leaf-docs-20261031.log`,
`target/roadmap-r02d-leaf-tools-20261031.log`.

After feature jobs ended, explicitly built the default benchmark example, froze its
binary in fresh `target/roadmap-r02d-leaf-live-20261031/`, and ran TPC-H Q1/TPC-DS Q1
in isolated SQL database roots. Both pass strict planning, three DML rounds,
independent SQL, reopen and output history. Binary hashes agree before/after each
execution; source HEAD/diff/row-block/Cargo.lock and report/audit identities are
preserved with exact commands. Logs:
`target/roadmap-r02d-leaf-bench-build-20261031.log` and
`target/roadmap-r02d-leaf-witness-20261031.log`. These remain two small witnesses;
full TPC and both performance/end-to-end delta-work qualification flags stay false.

Implementation is local-only. **R02d remains in progress**: SQL table leaves and
providers are eager and no-op comparison still consumes eager batches. The next
stage must make table/provider demand explicit and measured before R02d can be
verified. All other pending roadmap acceptance criteria remain required.


Next R02d design: [lazy SQL table scans](docs/src/incremental/sql_table_leaf_demand.md),
accepted locally before implementation. It specifies weighted comparison and net-zero
root restoration, maintained NULL metadata, fetch-aware range cursors, optional
SELECT/DML/store demand diagnostics, and complete regression/gate requirements.
Implementation and R02d completion remain pending; this design is not a new PASS.


### R02d — exact weighted comparison and canceled-root reuse

First implementation increment of design commit `4001396`,
[lazy SQL table scans](docs/src/incremental/sql_table_leaf_demand.md).
`RowBlocks::same_contents` compares exact weighted rows without requesting Arrow
batches. Aligned persistent directories skip shared subtrees; an inconclusive shape
mismatch uses bounded-leaf canonical row iteration across different split boundaries.
The fallback can visit an unchanged distinct-row prefix; comparison CPU work remains
outside current counters. No general changed-key/logarithmic bound is claimed.

`TransactionWorkspace` lazily retains the first row root per touched table. A
consolidated net-zero delta restores that original revision, including its directory
and complete cache, while retaining all actual statement work and preserving other
tables' writes. This avoids a complete comparison/flatten merely to recognize a
canceled transaction. No persisted/diagnostic format, dependency or rule change.

**Verified RED/green regressions:** before the implementation, equal weighted tables
with different splits filled expanded Arrow leaf caches, and a canceled INSERT /
DELETE table root required a new complete flatten (miss=1 rather than 0). Both
focused commands exited 101; logs:
`target/roadmap-r02d-table-reuse-compare-red-20261031.log`,
`target/roadmap-r02d-table-reuse-root-red-20261031.log`. Both now pass. New coverage
includes 240 deterministic updates against an independent integer weighted map,
retained/discarded roots, repeated canceled splits and a directory payload-call
oracle proving shared paths skip payload comparisons. A dual-table SQL transaction
cancels one table and changes the other: the original 512-row payload is reused,
only the other table's two-row payload is written, and reopen/full SQL/output
history remain correct. Existing retry/concurrency/failure/crash cases also pass.

Using the R01a Rust 1.95.0 locked offline wrapper/target/TMPDIR,
`bash scripts/agent-check.sh sql` exited 0: fmt, both Clippy configurations,
**215 default tests passed / 2 ignored**, **229 Iceberg passed / 3 ignored**.
Log: `target/roadmap-r02d-table-reuse-gates-20261031.log`. The R01a private-rustdoc
command exited 0, log `target/roadmap-r02d-table-reuse-rustdoc-20261031.log`.
mdBook exited 0, log `target/roadmap-r02d-table-reuse-docs-20261031.log`.
No report-tool schema changed, so its 38 unit tests were not repeated in this
increment; their last fresh result is the preceding graph-leaf milestone.

After all feature jobs ended, explicitly rebuilt the default benchmark example and
froze a runner in fresh `target/roadmap-r02d-table-reuse-live-20261031/`.
TPC-H Q1 / TPC-DS Q1 each pass strict planning, three SQL DML rounds, independent
full SQL, reopen and output-log replay. Numeric work audit passes; exact commands,
source/diff/workspace/directory/row-block/Cargo.lock and binary/report identities are
preserved. Runner hashes agree before/after each execution. Build/run logs:
`target/roadmap-r02d-table-reuse-bench-build-20261031.log`,
`target/roadmap-r02d-table-reuse-witness-20261031.log`.
Only two small witnesses ran; full-TPC and performance/end-to-end qualification
remain incomplete and their flags stay false.

Implementation is local-only. **R02d remains in progress**. Next implement maintained
NULL metadata, projected weighted range conversion, fetch-aware SQL table streams
and optional selector/SELECT/store leaf-demand work as specified by the accepted
design. SQL production tables and providers are still eager at this milestone.


### R02d — lazy SQL table leaves and fetch-aware scans

Final implementation stage of design `4001396`,
[lazy SQL table scans](docs/src/incremental/sql_table_leaf_demand.md).
Production SQL tables now retain weighted lazy leaves. Maintained exact row and
Arrow logical-NULL metadata change only with replaced leaves. `Bag::range_batch`
skips weights and projects before expanded output allocation. Owned `RowCursor`
uses strict directory successors and preserves immutable statement epochs.
`RowTable` registration/planning never collects or expands leaf vectors. Physical
fetch and later `with_fetch` cap actual synchronous poll-time conversion, including
high duplicate weights. Zero-column projections keep explicit row counts.
Complete leaf caches are reused; partial/projected misses do not fill them.
Provider statistics stay absent, preserving CREATE MV dependency discovery;
physical row/NULL statistics retain COUNT optimizations and lazy physical bytes
remain unknown. A strict prefix never inherits full-table NULL counts.

Actual successful table demand is an optional `RowReadWork` on SELECT and DML
selectors; targets expose optional distinct `leaf_validation`. The CLI reports
`base_table_reads` separately from unmeasured selector CPU/view reads. StoreWork's
optional `table_leaf_demand` and `table_leaf_validation` distinguish checkpoint
leaf expansion from restore encoding. Checked intervals cover all new counters;
old absent diagnostics remain unknown and original retry receipts retain their
work identity. Range positional probes and ordering rows are separate counts;
positive/count preflight visits and temporary sort/scalar bytes remain excluded.
Partial misses can repeat bounded-leaf sorting; no complete CPU or allocation
measurement is claimed.

**Verified RED/green:** the eager SQL factory expanded 100,000 identical bootstrap
rows (expected zero); the regression failed before switching production policy.
Log: `target/roadmap-r02d-row-provider-red-20261031.log`. Lazy bootstrap and a
following one-row insertion each validate one distinct value and expand zero rows.
New tests cover 80 deterministic weighted revisions against an independent integer
vector; dictionary value/key NULLs, struct parent/child validity and NULL/empty
lists against independent Arrow arrays; projected/chunked/filter/scalar-subquery
SQL against a separate MemTable; metadata-only COUNT/eliminated reads; late fetch;
zero columns; independent old/new cursors; cache partial/full boundaries; counter
overflow/fused errors; retained snapshots, retry/reopen and historical absence.
The existing persistent-directory BTreeMap oracle now also checks strict successors.

**Verified fixed-work SQL regression**, table sizes 1,024 / 8,192, one inserted row:

| Boundary | 1,024 rows | 8,192 rows |
| --- | ---: | ---: |
| Target historical distinct entries copied | 256 | 256 |
| Target distinct rows validated | 257 | 257 |
| Target write-time expanded rows | 0 | 0 |
| Transaction LIMIT 1 expanded rows / range probes | 1 / 1 | 1 / 1 |
| Transaction LIMIT 1 distinct ordering rows | 256 | 256 |
| Checkpoint leaf-demand expanded rows | 257 | 257 |
| Complete checkpoint concatenated/written rows | 1,025 | 8,193 |

Two SQL tests also verify a 32,768-duplicate relation's repeated LIMIT 1 requests
each expand one row without populating its complete cache, plus rollback,
persisted request identity, restart and output-history correctness. Targeted
commands use the R01a wrapper/target/TMPDIR with
`cargo +1.95.0 test --offline --locked -p optd-incremental-sql --test sql_table_scans --test table_blocks --test dml_work`:
exit 0, 13 passed. Log:
`target/roadmap-r02d-row-provider-targeted-20261031.log`.
Old eager target-materialization assertions were updated to the intended zero and
paired with distinct-validation/checkpoint-demand assertions; no SQL result,
strict-plan or failure guard was relaxed.

**Verified final gates:** using the R01a Rust 1.95.0 locked offline wrapper,
target and TMPDIR, `bash scripts/agent-check.sh sql` exited 0: fmt, both Clippy
configurations, **226 default passed / 2 ignored**, **240 Iceberg passed / 3
ignored**. Log: `target/roadmap-r02d-row-provider-final-gates-20261031.log`.
The final run follows the last explicit-import/comment cleanup; no source changes
follow it. Private rustdoc with the R01a warnings-denied command exited 0, log
`target/roadmap-r02d-row-provider-rustdoc-20261031.log`. mdBook exited 0, log
`target/roadmap-r02d-row-provider-docs-20261031.log`. The same wrapper with
`bash scripts/agent-check.sh sql-tools` exited 0: fixture-hash verification and
39 report-tool tests, log `target/roadmap-r02d-row-provider-tools-20261031.log`.

After every feature job ended, explicitly built the default example with
`cargo +1.95.0 build --offline --locked -p optd-incremental-sql --example benchmark_sql_database`
and froze a runner in fresh `target/roadmap-r02d-row-provider-live-20261031/`.
The wrapper plus `python3 target/roadmap-r02d-row-provider-witness-20261031.py`
exited 0: TPC-H Q1/TPC-DS Q1 each pass strict planning, three SQL DML rounds,
independent full SQL, reopen and output-log replay. The unified numeric-work audit
also passes. Exact commands, separate database roots, source HEAD/diff and new
production-file hashes, Cargo.lock, runner/report hashes and the before/after
binary identity are preserved. Logs:
`target/roadmap-r02d-row-provider-bench-build-20261031.log`,
`target/roadmap-r02d-row-provider-witness-20261031.log`.
These are two small `relational_core` witnesses; full-TPC and performance/
end-to-end-delta-work qualification remain incomplete and their flags stay false.

**R02d is verified locally:** source/graph/table revisions share weighted state,
small writes rebuild bounded leaves, demand boundaries expand only requested
chunks or explicitly required complete snapshots, comparison/canceled transactions
avoid expansion, and source/graph/SQL/store work remain separately attributable.
Full checkpoint concatenation/file writes remain visible and belong to R07;
predicate filters may still require complete scans. No universal changed-key
complexity, allocation-traffic or performance claim is made. Implementation is
local-only. Next owner: this task, R03a ordered reference-counted MIN/MAX state;
R03 and later items retain their own acceptance gates.


### R03a — design and implementation lane

Claimed by this task. [Retractable MIN/MAX value indexes](docs/src/incremental/extrema_value_indexes.md)
defines counted shared value directories under the existing accumulator rewrite,
exact nonfloating type/NULL contracts, optional actual work, independent oracles
and fresh TPC-H Q15/TPC-DS Q23 acceptance. Local source/design review is complete;
The accepted design was committed as `28f4da8` before implementation. The existing
accumulator rule and Optd operation now select counted ordered MIN/MAX cells for
qualified exact types; native COUNT/SUM/AVG cells retain their prior arithmetic.
Persistent value roots isolate candidates and copy changed paths. Per-value net
changes skip cancellations; NULLs do not enter the value directory, while group
cardinality independently preserves NULL-only groups and empty global output.
Canonical builtin MIN/MAX identity additionally rejects same-name registered user
functions. No new package, rule name, Optd operation or persisted format is added.

**Verified RED/green:** ordinary global/grouped MIN/MAX selected recomputation on
the baseline and now selects `AccumulatorDelta`.
Logs: `target/roadmap-r03a-red-20261031.log` and
`target/roadmap-r03a-green-20261031.log`. Independent counted BTreeMap, integer-group
and full SQL oracles cover arbitrary deletion, duplicate extrema, NULL/empty input,
all admitted value types, mixed native/indexed aggregates, group removal/recreation,
key moves, projected cancellation, failed/discarded/stale candidates and signed
output replay. The value-index oracle executes 240 updates; the SQL oracle executes
80 mixed epochs. SQL CREATE MV and INSERT/DELETE/UPDATE tests reopen after every
round, retry the original request and verify every output-history prefix.

At 1,024 and 8,192 unchanged groups or distinct values in a hot group, one changed
key consumes four MIN/MAX argument rows, updates four net values, looks up two
extrema and reads zero historical aggregate inputs. Actual value-path copy counts
satisfy logarithmic bounds; projected equal-value replacement copies zero value
nodes. Group removal skips unnecessary value-index work. `value_index_work` is
optional, absent for native-only/old reports; a report-tool regression independently
checks nested sums and absence without fabricating zeros.

**Verified final gates:** the R01a Rust 1.95.0 locked offline wrapper, project target
and TMPDIR with `bash scripts/agent-check.sh sql` exited 0: format, both warnings-denied
Clippy configurations, **238 default passed / 2 ignored**, **252 Iceberg passed /
3 ignored**. Log: `target/roadmap-r03a-final-gates-20261031.log`. The private-rustdoc
command from R01a exited 0, log `target/roadmap-r03a-rustdoc-20261031.log`.
The same wrapper with `bash scripts/agent-check.sh sql-tools` exited 0: fixture-hash
verification and **40 tool tests**, log `target/roadmap-r03a-tools-20261031.log`.
An earlier full check failed because an old strict-policy test still expected
integer MIN to be rejected; its refusal case now uses unqualified floating MIN.
The original failed log remains `target/roadmap-r03a-gates-20261031.log`.
No correctness assertion was relaxed: the ordinary MIN path has its own RED/green
plan and independent result/work tests. Raw DISTINCT/modifier eligibility is tested
before SQL optimization, which can legitimately split a DISTINCT aggregate.

After all feature jobs ended, explicitly built the default `benchmark_sql_database`
example and froze its runner. The wrapper with
`python3 target/roadmap-r03a-extrema-witness-20261031.py` exited 0: **TPC-H Q15 and
TPC-DS Q23 each pass three SQL DML rounds, strict planning, independent full SQL,
reopen and output-history replay**, plus unified numeric-work audit. Both witnesses
observe nonempty output and an output change. Q15 performs 22 value updates and
two extremum lookups; Q23 performs three value updates and three lookups. These
actual counts include no-op rounds rather than implying every statement changes
the result. Initial/final result rows: Q15 0/1, Q23 0/0.

Fresh artifacts: `target/roadmap-r03a-extrema-live-20261031/`. Commands, independent
database roots, source HEAD/diff/new production-file hashes, lockfile, runner,
report hashes and unchanged before/after binary identity are retained. Logs:
`target/roadmap-r03a-bench-build-20261031.log`,
`target/roadmap-r03a-witness-20261031.log`. Additional per-query extrema-work receipts
record the executed index counters. These are two small `relational_core` SQL
witnesses, not full-TPC or controlled performance qualification; both performance
and end-to-end delta-work flags stay false. Complete checkpoint writes and derived
state reconstruction on restart remain explicit R07 work.

mdBook with the R01a wrapper and project book output exited 0, log
`target/roadmap-r03a-docs-20261031.log`. **R03a is verified locally.** Implementation
is the local commit titled `Maintain retractable MIN/MAX through counted value indexes`;
no push. Next owner: this task, R03b DISTINCT membership state.


### R03b — design and implementation lane

Claimed by this task after R03a `0c30be5`.
[Reference-counted DISTINCT aggregates](docs/src/incremental/distinct_aggregate_state.md)
defines membership zero crossings under existing accumulator plans, normalized
DISTINCT grouping with refcounts, mixed exact native aggregates, optional measured
work and a fresh TPC-DS Q28 strict SQL witness. Local source/design review is
complete. The accepted design was committed as `c51edf7` before implementation.
The existing accumulator rule/Optd operation now maintains qualified single-argument
COUNT/SUM/AVG DISTINCT with counted persistent value directories and native state
updated only for membership zero crossings. MIN/MAX DISTINCT retains counted extrema.
Normalized DISTINCT/group-only nodes use the group entry's row count and emit keys
only on membership changes, without retaining/recomputing duplicate buckets.
Canonical builtin identity is checked for all five aggregate functions; original
numerical, key-type and modifier boundaries remain. No dependency, rule-name, Optd
operation or persisted format change.

**Verified RED/green:** mixed global COUNT DISTINCT with COUNT/SUM/Decimal AVG
selected `OperatorRecompute` at R03a and now selects only accumulator aggregates.
Logs: `target/roadmap-r03b-red-20261031.log`,
`target/roadmap-r03b-green-20261031.log`. The eight DISTINCT integration tests and
an independent 180-step membership-map oracle cover duplicate and final deletions,
NULL/all-NULL/empty input, all admitted count argument types, exact Decimal256
COUNT/SUM/AVG, independent DISTINCT arguments, mixed ordinary/extrema state,
projection-level cancellation, group removal/recreation and signed output replay.
A 120-epoch independent integer-set oracle checks refreshed COUNT/SUM DISTINCT;
fresh complete SQL endpoints verify schemas and output. SQL INSERT/DELETE/UPDATE
tests reopen after every round, retry the original request and check every durable
output-history prefix. Failed, discarded and stale candidates preserve installed
counts and outputs.

**Verified work scope:** at 1,024 / 8,192 untouched groups or hot-group values,
nonfinal duplicate deletion updates three value counts and sends zero entering or
leaving values to the three DISTINCT native functions. Final deletion/replacement
sends exactly three values per sign, with logarithmically bounded actual path copies
and zero historical aggregate input reads. Projected cancellations copy zero value
nodes and perform zero native input updates. A normalized DISTINCT bucket containing
1,024 / 8,192 duplicates consumes one changed row, copies one group directory node,
reads zero historical rows and emits no output change until the final occurrence.
The optional `distinct_work` domain records exact value/count/copy and native input
units separately from extrema and source/cache/SQL/store work. A report-tool test
checks exact nested sums and preserves missing old diagnostics as unknown.

**Verified final gates:** the R01a Rust 1.95.0 locked offline wrapper, project target
and TMPDIR with `bash scripts/agent-check.sh sql` exited 0: fmt, both warnings-denied
Clippy configurations, **247 default passed / 2 ignored**, **261 Iceberg passed /
3 ignored**. Log: `target/roadmap-r03b-gates-20261031.log`. The R01a private-rustdoc
command exited 0, log `target/roadmap-r03b-rustdoc-20261031.log`. The same wrapper
with `bash scripts/agent-check.sh sql-tools` exited 0: fixture-hash verification and
**41 tool tests**, log `target/roadmap-r03b-tools-20261031.log`. An initial focused
plan test still expected DISTINCT's old Group state; it now asserts the actual
Accumulator state and adds a FILTER aggregate to retain explicit Group contract
coverage. Its original failure remains in
`target/roadmap-r03b-targeted-20261031.log`; final full checks pass both paths.

After all feature jobs ended, explicitly built the default `benchmark_sql_database`
example and froze a runner. The wrapper with
`python3 target/roadmap-r03b-distinct-witness-20261031.py` exited 0: **TPC-DS Q28
passes three strict SQL DML rounds, independent full SQL, reopen and output-history
replay**, plus unified numeric-work audit. The six original SQL quantity/price
branches retain AVG/COUNT/COUNT DISTINCT; the directed fixture has six store_sales
rows. Initial/final output rows are 1/1 and output changes are observed. Actual
DISTINCT work: four consumed argument rows/value updates, one copied value-directory
node, three entering values and one leaving value. The script requires actual
membership work as well as semantic/nonempty/change evidence.

Fresh artifacts: `target/roadmap-r03b-distinct-live-20261031/`. Commands, independent
warehouse root, source/lockfile/runner/report identities and unchanged before/after
binary hash are retained. Logs: `target/roadmap-r03b-bench-build-20261031.log`,
`target/roadmap-r03b-witness-20261031.log`. This small `relational_core` witness is
not full-TPC, standard-scale or controlled performance qualification; performance
and end-to-end delta-work flags stay false. Full checkpoint writes and derived-state
bootstrap on restart remain R07; unqualified types and modifiers keep strict guards.

mdBook with the R01a wrapper and project book output exited 0, log
`target/roadmap-r03b-docs-20261031.log`. **R03b is verified locally.** Implementation
is the local commit titled `Maintain DISTINCT aggregates from value membership changes`;
no push. Next owner: this task, R03c GROUPING SETS / ROLLUP.


### R03c — design and implementation lane

Claimed by this task after R03b `88b36bd`.
[Incremental GROUPING SETS normalization](docs/src/incremental/grouping_set_normalization.md)
defines schema-exact ordinary aggregate branches, typed internal grouping identity
and UNION ALL under existing Optd rules/physical states. Real/synthetic NULL,
duplicate sets and empty grand totals retain separate state. The nine actual
TPC-DS ROLLUP queries are Q5/Q14/Q18/Q22/Q67/Q70/Q77/Q80/Q86, each with an existing
directed fixture manifest. Source/design review is complete; implementation and
all acceptance remain pending. No dependency, format or numerical guard change.
