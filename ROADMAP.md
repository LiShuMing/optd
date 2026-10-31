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
| R01b | P0 | SQL selector and DML work accounting | Measure consumed source batches/rows separately from available relation rows; report target rebuild and delta generation | pending |
| R01c | P0 | Store and recovery work accounting | Record reused/written payloads and actual file bytes, metadata/history reads and bootstrap/recovery work; retries do not impersonate fresh execution | pending |
| R01d | P0 | Unified SQL/benchmark work reports | Separate semantics, strict planning, affected scope, source/state/materialization/storage work and controlled performance; unknown measurements remain unknown | pending |
| R02a | P1 | Consolidate table changes before rebuilding source state | One net update per changed source; cancellation skips source rebuild; preserve absent-row and atomic-batch validation | pending |
| R02b | P1 | Transaction table blocks and change overlays | Small writes avoid full target conversion; preserve duplicate rows, statement snapshots, swaps and read-your-writes | pending |
| R02c | P1 | Shared state directories and candidate overlays | Copy only changed directory blocks/entries while discarded/failed candidates leave committed state intact | pending |
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
