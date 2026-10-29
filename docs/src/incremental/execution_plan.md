# Incremental Engine — Iteration and Execution Plan

**Status:** proposed · 2026-10-28
**Scope:** track delivery of the accepted CDC v1 contract through the existing M1–M4 milestones.
Section 10 records M1, section 11 the local consumer/keyed/maintenance loop, and
section 12 typed SQL/source-vector query qualification. The broader performance
and distributed schedule remains proposed; each acceptance record is independent.

## 1. Problem

At the inspection baseline, the algebraic core and Iceberg reader existed while the writer and
recovery APIs did not. The first delivery target was a restartable, replayable single-view CDC
loop. Section 10 records that implementation; expanding operators and storage modes still requires
independent recovery evidence.

The [accepted output design](./iceberg_cdc_output.md) now fixes the CDC v1 schema, batch identity,
writer exclusion boundary, empty-delta behavior and consumption semantics, referencing Snowflake
and MaxCompute Streams. Implementation must qualify the pinned storage path and obey that contract.
The [overview](./overview.md) also still lists implemented features as missing. Both design drift
and missing code must be tracked explicitly.

## 2. Constraints and evidence

The inspection baseline is commit `9652795`. **Verified** below means observed in source or by
the commands in §9; it does not mean a runtime guarantee has been tested.

| Area | Verified in the repository | Remaining work |
| --- | --- | --- |
| Delta representation | `change.rs`: `BTreeMap<Row, i64>`; `Value` is Null/Int/Str | Overflow policy, durable encoding and typed schema |
| Incremental evaluation | `plan.rs`, `rules.rs`, `engine.rs`: projection, filter, union, inner join and root aggregate state | Persistent output; richer operator-local state later |
| Algebra tests | `tests/differential.rs` and `tests/regression_verify.rs` exist | Independent semantic cases, recovery and real storage coverage |
| Refresh | `View::refresh` computes and advances memory together | Prepare without mutation; acknowledge only after durable success |
| Source API | Synchronous, infallible `snapshot` and `changes`; no historical-load API | Fallible pinned reads, durable snapshot identities and refresh observation |
| Iceberg reader | Timeline captured once; private snapshot scan; errors can panic | Reload observations explicitly; expose historical reads; runtime ownership |
| Multi-table safety | Default Iceberg capability is false; an assumption flag can enable it | Keep disabled for M1; an index alignment is not a consistency proof |
| Output | `DeltaSink`, `IcebergCdcSink`, `CommitMeta` occur in design, not implementation | Deliver M1 |
| Verification | CI already runs workspace fmt, clippy and all-feature nextest | One local entry point, explicit 1.95 check and recovery fixtures; docs PR gate |
| Toolchain | Crate declares Rust 1.95; local default is 1.94.1; 1.95.0 is installed | Use explicit 1.95 commands; default `stable` alone does not establish MSRV |

**Inferred:** storage failures and retries will be difficult to handle correctly through an
infallible source trait and a mutating refresh API. The same interpreter is used for incremental
evaluation and recompute, so differential agreement alone cannot establish SQL semantics.

**Verified by pinned-source inspection on 2026-10-28:** a properties-only `fast_append` is
supported, and the transaction layer has automatic retry/rebase behavior. The accepted v1 design
requires properties-only empty batches and `commit.retry.num-retries=0`.

**Unknown:** the selected catalog's durable/definitive commit behavior and end-to-end recovery.
Source inspection does not replace the E0 storage experiments or E5 process-kill tests.

## 3. Options considered

| Option | Benefit | Decision |
| --- | --- | --- |
| Add outer joins, MIN/MAX and general SQL integration first | More visible query coverage | Defer; persistent state and recovery remain unresolved |
| Implement the sink around the current mutating `refresh` | Small initial diff | Reject as the default driver; failed writes leave memory ahead of durable output |
| Add a separate database outbox | General coordination mechanism | Defer; duplicates the intended Iceberg checkpoint authority |
| Close recovery contracts, then implement single-view output | Smallest testable durable loop | Recommended; preserve the existing M1–M4 roadmap |

## 4. Accepted CDC contract and implementation boundaries

The [CDC Output Protocol v1](./iceberg_cdc_output.md) is authoritative for the wire schema,
canonical encoding, fingerprints, UPDATE annotation, batch envelope and bookmark semantics.
The owner requested finalization on 2026-10-28 after comparison with the official Snowflake and
MaxCompute Stream designs. This execution plan does not redefine those fields.

* Payload is a complete row image followed by operation/update flags, signed 64-bit weight,
  optional logical row id, batch id, batch sequence, ordinal and preparation timestamp.
* M1 is unkeyed, so row ids are NULL and update flags false. M3 introduces a new keyed generation
  with paired BEFORE/AFTER rows. No one-row UPDATE fusion is planned.
* Batch identity binds the definition, generation, predecessor and durable source range. A payload
  digest validates it; two distinct ranges with the same payload remain distinct batches.
* C1 plus SHA-256 defines durable encoding. Pure formatting or timestamps do not change identity.
* A zero-row batch, including empty bootstrap, commits its envelope through a properties-only
  snapshot. E0 demonstrates this pinned path instead of reopening the format decision.
* One generation has one output table. M1 uses single-host OS-backed writer exclusion and a local
  catalog with definitive completion; distributed fencing and unresolved remote commits are deferred.
* Disable hidden transaction retries. Unknown outcomes must be resolved before another append;
  one negative read is insufficient when a prior request can still complete.
* Stream reads pin their upper boundary without advancing a cursor. Consumer effects and cursor
  move together at the destination, with transaction or batch-id idempotency support.

E2/E3 require fallible pinned/historical reads, refreshed observations, explicit async
runtime ownership and prepare/acknowledge APIs; the M1 implementation is recorded in §10. A prepared candidate contains immutable delta,
candidate state, endpoints and envelope; only a validated durable success changes memory.
Repeated/stale acknowledge must not apply the delta again. Loading current state and substituting
an old watermark is prohibited. E2 specifies caller/test migration without an unrelated executor
rewrite; algebra functions can remain synchronous.

M1 preserves required input snapshots and output history, and disables batching, compaction and
expiration. It keeps multi-table consistency guards and fails before publish on overflow, invalid
columns, unsupported operators/types or incompatible definitions. Automatic rebuild policy and
finalized SQL aggregate results remain separate work; accumulator output must be declared.

## 5. Execution order and ownership

Owners below are roles to assign when work starts, not claims that people or agents have been
scheduled. Each task has one writer; review is read-only. Shared source/API changes are sequential.
No runtime dependency or lockfile changes are included in this planning change.

| Task | Priority / dependency | Owner role | Concrete deliverable and acceptance |
| --- | --- | --- | --- |
| E0: protocol qualification | P0, first | Protocol owner | Follow accepted CDC v1; publish independent schema/codec/hash vectors; select a durable local catalog; demonstrate properties-only bootstrap/deltas, retry disabling and definitive commit outcome resolution |
| E1: reproducible gates | P0, before implementation claims | Build owner | Verification script with scoped fast/full modes; explicit Rust 1.95 configuration; default and Iceberg feature checks; docs on PRs; discoverable `AGENTS.md` entry pointing to the canonical contract; record pre-existing failures separately |
| E2: source foundation | P0, after E0 | Engine owner | Fallible pinned/historical reads, refreshed observations, chosen async migration and runtime lifetime tests; expired and mismatched identities are errors |
| E3: prepared refresh | P0, after E2 | Engine owner | Prepare/acknowledge state machine; invalid candidates leave contents and checkpoint unchanged; repeated/stale acknowledgements cannot double-apply; overflow cases covered |
| E4: Iceberg CDC writer | P0, after E3 and E1 | Storage owner | Implement v1 system fields/envelope, `DeltaSink`, exclusive ownership, weighted full bootstrap and one batch per snapshot; read-back verifies flags, weights, identities and properties-only empty batches |
| E5: recovery acceptance | P0, after E4 | Recovery test owner | Real subprocess kill/restart harness, lost-ack injection, duplicate/content-conflict checks, empty-delta checkpoint, changing-source test; closes CDC M1 only when all pass |
| E6: downstream consumption | P1, after M1 | Source owner | Independent stream cursors, pinned non-consuming peek, transactional/idempotent consume, `CdcLogSource` and state checkpoints; weighted and row-expanded projections match a base-table oracle; closes M2 |
| E7: query integration | P1, after M1 | Planner owner | Design and implement a narrow DataFusion/SQL bridge with a supported-type/operator matrix; compare with independent SQL results before widening coverage |
| E8: keyed updates | P2, after M2 and a consumer requirement | Schema owner | New keyed generation with endpoint uniqueness checks, generation-scoped logical row ids and adjacent BEFORE/AFTER pairs; key changes remain unpaired DELETE/INSERT; closes M3 |
| E9: layout and retention | P2, after M2 | Storage owner | Stable row identities through batching/compaction, safe cursor-aware retention and source checkpoint pins; replay equivalence and lagging-consumer tests; closes M4 |
| E10: performance and more operators | P2, after M1; operator-specific designs | Engine owner | Measure endpoint scans, memory and join cost first; select manifest diff, indexes, persistent aggregate state or operator expansion from measured bottlenecks |

The critical path is **E0 → E2 → E3 → E4 → E5**, with E1 required before E4 acceptance. E6 and
E7 may be scheduled independently after M1 if their file ownership is distinct. This plan does not
start concurrent writers or create tasks in external systems.

Recommended review batches:

1. Accepted-v1 codec/schema vectors and E0 storage qualification; E1 as a separate infrastructure change.
2. Source API and pinned historical reads, including caller migration and regression tests.
3. Prepare/acknowledge lifecycle and arithmetic validation.
4. Bootstrap/output encoding, then commit/read-back driver and writer exclusion.
5. Process recovery harness and M1 evidence record.
6. M2 consumer/checkpoints and the narrow SQL bridge, each with its own design.

Budget approximately **10–17 engineer-days for E0–E5**, excluding review wait and unavailable
infrastructure: E0 1–2, E1 1–2, E2 2–3, E3 1–2, E4 3–5, E5 2–3. These task ranges sum to
10–17 days and are planning estimates, not measured throughput or delivery dates. Re-estimate
after the E0 qualification probes. Do not promise M2–M4 dates before the M1 evidence identifies remaining risk.

## 6. Failure modes and recovery acceptance

| Scenario | Required result |
| --- | --- |
| Kill during bootstrap before output commit | Restart publishes one complete bootstrap with original multiplicities |
| Kill after data file close, before commit | Files remain invisible; restart recomputes from durable predecessor; no lost or duplicated delta |
| Kill immediately after catalog commit, before acknowledge | Restart recognizes the committed batch and loads its exact input snapshots |
| Catalog accepts commit but response is lost | Read-back recognizes success; no second append and no deletion of referenced files |
| A definite commit failure followed by retry | Retry uses the same prepared delta and identity; memory remains at predecessor until success |
| Source range advances but view delta cancels to zero | Durable checkpoint advances through a properties-only snapshot; restart does not loop over that range |
| Separate source ranges yield identical deltas | Both batches commit once; digest equality does not suppress legitimate output |
| Two writer processes start | Second process cannot enter the M1 writer loop while the first holds ownership; killed owner can be restarted |
| Input snapshot expires or source branch changes | Typed failure before publish; explicit rebuild/new generation, no epoch reinterpretation |
| Output schema/view definition changes | Refuse continuation of the old generation even when column count is unchanged |
| New input commit arrives while a candidate is prepared | Prepared snapshot remains stable; next observation sees the later commit |
| Overflow, invalid columns, unsupported source/type | No output commit and no state advancement; diagnostic identifies the rejected input |

The subprocess harness must coordinate deterministic barriers around the real file write and
catalog commit, kill the child without cleanup, then launch a new process against the same
warehouse. A mock error or an in-process panic does not satisfy the two process-kill gates.
Use a bounded timeout so runtime lifetime defects fail instead of hanging CI indefinitely.

After **every** recovered step, compare the weighted fold of the entire output generation with
the view evaluated over the checkpoint's input snapshots. Independently verify snapshot metadata,
row multiplicities and exactly one committed batch identity. Add hand-calculated fixtures for
NULLs, duplicate rows, key changes, empty inputs and aggregate accumulator layout; a shared
interpreter can otherwise reproduce the same bug on both sides.

## 7. Rollout and verification gates

* **Implementation foundation qualified:** accepted CDC v1 referenced; historical-load and properties-only paths
  demonstrated; retry suppression and definitive catalog completion qualified.
* **Core ready:** E2/E3 pass in-memory differential/regression cases, independent semantic
  fixtures and state-transition tests.
* **CDC M1 ready:** E4/E5 run against the selected real catalog/warehouse with all §6 cases,
  including actual process death. Output read-back alone is insufficient.
* **Consumer ready:** M2 tests recover consumer state and cursor together; replay from bootstrap
  or a validated state checkpoint has the same result as the base-table oracle.
* **Maintenance ready:** batching/compaction/expiration are enabled only after M4 replay tests
  cover rewritten files, unread history and missing source snapshots.

E1 should provide these commands without rewriting unrelated workspace configuration:

```bash
cargo +1.95.0 fmt --all -- --check
cargo +1.95.0 clippy -p optd-incremental --all-targets --all-features --locked -- -D warnings
cargo +1.95.0 test -p optd-incremental --no-default-features --locked
cargo +1.95.0 test -p optd-incremental --features iceberg --locked
mdbook build docs
```

Add a documented opt-in recovery command once E5 supplies its fixture and executable; do not list
a nonexistent test binary as a runnable gate. Full workspace CI remains required for integration
changes. For a build/MSRV claim, use a fresh `CARGO_TARGET_DIR`, record toolchain and exit status,
and distinguish dependency-fetch failures from compilation failures. Fixture generators and
catalog startup/cleanup instructions belong in the repository; generated tables do not.

Performance work records baseline commit, hardware, command, data size, change fraction, elapsed
time, bytes read/written, peak memory and file count. The current Iceberg source scans endpoints;
incremental algebra alone does not establish incremental I/O or a speedup.

## 8. Open questions and next action

The CDC v1 format is final in the accepted output design: system fields, C1/SHA-256, UPDATE
profiles, properties-only empty batches and stream bookmarks. E0–E5 local implementation and
qualification are recorded in §10; the consumer/keyed/maintenance continuation is in §11.
E6 now supplies local consumer bookmarks and destination effects;
distributed fencing remains a separate design.

Product decisions that can follow M1: whether consumers need current SQL results or accumulator
state, the supported SQL/type subset, latency/file-size targets and when to implement the
already specified keyed profile. Do not block recovery work on partition counts or broad operator coverage.

E7's typed SQL and explicit Iceberg-source integration are now recorded in §12.
The next optional workload-driven increment is E10: measure endpoint scans,
operator recomputation and memory before selecting native changelogs, persistent
accumulators or indexes. Logical TTL, orphan deletion, distributed fencing and
production/scale qualification remain separate work.

## 9. Initial inspection and validation record

* **Verified by source inspection:** files named in §2, `Cargo.toml`,
  `.github/workflows/CI.yaml`, `.github/workflows/docs.yaml`, `AGENT.md` and the existing
  incremental design pages. No output sink or historical-load public API was found.
* **Verified environment:** `rustc --version` / `cargo --version` report 1.94.1;
  `rustup toolchain list` includes 1.95.0. `command -v mdbook` returned no executable.
* **Attempted baseline:**
  `cargo +1.95.0 test -p optd-incremental --no-default-features --locked --offline --target-dir /tmp/optd-iteration-plan-baseline-20261028`
  exited 101 before compilation: the pinned `iceberg-rust` Git checkout was unavailable offline.
  Tests did not run; this is not evidence of an algebra failure or a passing baseline.
* **Not covered at the initial planning stage:** compilation, runtime/storage correctness, real
  process-kill recovery, performance and a rendered mdBook build. This change adds a plan and book
  index entry only; implementation claims require the gates above. The subsequent CDC v1
  finalization updates this plan to reference the accepted contract without changing runtime code.

## 10. Implementation loop 1 — 2026-10-28

**Outcome:** a runnable single-host unkeyed CDC loop now connects source observation, exact LOAD,
prepared refresh, real Parquet output, atomic Iceberg checkpoint publication and restart recovery.
The broader schedule remains proposed; this record reports the delivered M1 lane and leaves
consumer/keyed/layout work separate.

| Task | Delivered in this loop | Boundary |
| --- | --- | --- |
| E0 | Strict C1/envelope encoding, SHA-256 vectors from an independent Python generator; durable local catalog and properties-only commits qualified | Remote catalogs and unresolved remote late commits remain unsupported |
| E1 | `scripts/agent-check.sh`, discoverable `AGENTS.md`, Rust 1.95 incremental CI and docs PR build configuration | Local gates executed; new GitHub workflow execution requires a push/PR |
| E2 | Fallible snapshot/change APIs; async checkpoint/head observation; exact historical reads and schema-layout checks | Single Iceberg input; retained materialized endpoints |
| E3 | Immutable prepare, commit-then-acknowledge, idempotent acknowledgement, stale/foreign-candidate guards, checked evaluator arithmetic | Reference evaluator, not a vectorized production executor |
| E4 | v1 schema/envelopes, full weighted bootstrap, zero-row snapshots, writer exclusion, held-candidate retry, independent read-back and bounded row expansion | One generation/table; no compaction or distributed writers |
| E5 | Real process-death tests for bootstrap and incremental refresh in both commit windows; advance source before recovery | This qualifies the local M1 deployment; M2/M3-specific acceptance remains future work |

The implementation's source entry points are `cdc.rs`, `iceberg_source.rs`, `iceberg_sink.rs`,
`local_catalog.rs` and `engine.rs` in `optd-incremental/src/`. Reproduction commands and API migration
notes are in [Run the Local CDC Loop](./cdc_loop.md).

Verification commands and observations:

* Initial baseline, Rust/Cargo 1.95.0:
  `cargo +1.95.0 test -p optd-incremental --no-default-features --locked --target-dir /tmp/optd-cdc-v1-target`
  passed 26 tests; the corresponding `--features iceberg` baseline passed 29.
* `CARGO_TARGET_DIR=/tmp/optd-cdc-v1-acceptance scripts/agent-check.sh iceberg`
  passed workspace formatting, scoped default/feature clippy with warnings denied and both test
  configurations. The target directory was fresh for the first run; review hardening was compiled
  and retested in that same directory. Final results: **35 default-feature tests and 49 Iceberg-feature tests passed**, with zero
  failures or ignored tests. The latter includes the subprocess worker entry and its parent
  harness, which executes all four real kill/restart windows.
* `python3 scripts/cdc-golden-vectors.py` reproduced the checked-in C1 byte/hash expectations.
* The `cdc_loop` example was built with `cargo +1.95.0 build -p optd-incremental --features iceberg
  --example cdc_loop --locked --target-dir /tmp/optd-cdc-v1-target`. Separate CLI processes passed
  init → append → bootstrap → empty-delta refresh → restarted no-op → dump, preserving weights
  `[2, 1]` and a zero-record second checkpoint.
* `/tmp/optd-cdc-tools/bin/mdbook --version` reported 0.4.48;
  `/tmp/optd-cdc-tools/bin/mdbook build docs` passed. This build tool was installed under `/tmp`,
  outside the repository and workspace dependency graph.
* `git diff -- Cargo.lock` changes only this crate's direct dependency edges. Comparing locked
  `(name, version, source)` sets with HEAD found no package/version changes. Reused dependencies:
  serde/serde_json for strict envelopes, sha2 0.10.8, async-trait 0.1.92 and tempfile 3.14.0;
  Tokio gains explicitly declared runtime/macros/time features already present in the lock graph.

A non-scoped clippy attempt found three pre-existing Rust 1.95 warnings in untouched `optd-core`:
`derivable_impls` in `cascades/memo.rs`, `manual_is_multiple_of` in `cascades/tasks2.rs` and
`useless_conversion` in `heuristics/optimizer.rs`. Scoped incremental lint uses `--no-deps` and
passes; this is not a claim that full workspace lint/tests pass. The full workspace suite and
GitHub Actions execution were not run. There is no performance, remote-catalog or multi-table
exactly-once claim.

Changes remain local on `codex/incremental-execution-plan` for review; no commit/push was requested.
Next owner/action: start E6's independent consumer cursor/destination transaction contract, then
E7's restricted SQL bridge. M3 keyed identity and M4 maintenance stay behind their own acceptance
criteria; do not infer them from a working append-only change table.

## 11. Implementation loop 2 — 2026-10-28

**Outcome:** M2 local consumer transactions and checkpoint-backed `CdcLogSource`, M3 keyed
annotations, and the conservative local M4 batching/compaction/snapshot-retention path are
implemented under [Streams and Maintenance](./streams_and_maintenance.md). This record supersedes
§10's outstanding consumer/keyed/maintenance and workspace-lint status, not its historical results.

| Lane | Delivered | Boundary |
| --- | --- | --- |
| M2 / E6 | Read-only producer-coexisting log access; independently named OS-locked consumers; immutable peeks; one checksummed atomic destination-state/cursor file; suffix-only folding from a trusted state checkpoint; downstream DeltaSource | Local weighted destination only; physical reads still scan the CDC table; external side effects need their own transaction/ledger |
| M3 | Required ordered KeySpec in schema identity; C1/base64url row identities; complete endpoint uniqueness/unit-weight checks; adjacent BEFORE/AFTER pairs; key changes as unpaired DELETE/INSERT | Explicit keys and fresh generation; no inferred lineage or broad SQL type support |
| M4 | Contiguous `emit_many`, checksummed multi-envelope index, replace-snapshot compaction with complete envelope archive, metadata-pointer CAS, reader scan lock, explicit old-output-snapshot expiration | All logical CDC rows/files remain retained; no key-bucket tuning, logical TTL, orphan deletion, distributed ownership or bounded-storage claim |
| Workspace fixes | Three original `optd-core` lints plus warnings exposed in gungnir, representation, bridge and benchmark crates; fork-compatible benchmark root detection | No lint suppression or remote URL changes; external benchmark fixtures/services remain separate gates |

**Verified:** Rust/Cargo 1.95.0, local filesystem and the pinned Iceberg revision. These commands
reuse the previous loop's target directories; this is a change/regression validation record, not a
claim of a second completely fresh configuration build.

* `CARGO_TARGET_DIR=/tmp/optd-cdc-v1-target scripts/agent-check.sh iceberg` passed formatting,
  default/feature lint and tests: **37 default tests and 59 Iceberg tests**. After adding the
  all-empty maintenance regression and shared reader pins, the final
  `cargo +1.95.0 test -p optd-incremental --features iceberg --locked --target-dir /tmp/optd-cdc-v1-target`
  passed **60 tests**, zero failed/ignored. Shared pins allow concurrent producer/consumer audits
  while excluding maintenance; the storage regression exercises both cases.
* `cargo +1.95.0 clippy --workspace --all-targets --all-features --locked --target-dir /tmp/optd-cdc-v1-target -- -D warnings`
  passed after fixing all discovered warnings. The original three core warnings are resolved.
* Storage acceptance includes ten actual OS-kill windows: four producer bootstrap/incremental,
  two destination replacement, two compaction and two multi-batch publication windows. Parent
  processes reopen the same warehouse and compare durable cursor/state, identities and replay
  with independent expected relations. Workers are test entry points, not extra kill scenarios.
* Keyed tests compare C1/base64url with an independent Python vector and cover full-endpoint
  duplicate/null keys, malformed pairs, aggregate replacement, restart and key changes. Storage
  tests also reject corrupted state/index/archive checksums and concurrent scan maintenance.
* The CLI was exercised as separate processes through init, duplicate append, bootstrap, empty
  refresh, peek, consume, repeated consume, compact, expire, dump, append and restart refresh.
  The first two checkpoints retained weights `[2,1]`; expiration removed two old snapshots;
  replay output was byte-for-byte unchanged. A subsequent refresh and consume reached sequence 3.
* `python3 scripts/cdc-golden-vectors.py` reproduces the additional keyed id fixture.
  `/tmp/optd-cdc-tools/bin/mdbook build docs` and `git diff --check` passed.
* The lockfile adds the incremental crate's edge to already locked `base64 0.22.1` for reversible
  row ids. No package/version/source changes accompany this loop.

**Full workspace integration audit:** the first
`cargo +1.95.0 test --workspace --all-features --locked --target-dir /tmp/optd-cdc-v1-acceptance`
compiled and ran through CLI, core, representation, cost, gungnir and CDC tests, then failed in the
benchmark integration fixture because its root finder asserted that the origin URL belonged to
upstream. That bug is now fixed by Git top-level/workspace-manifest validation, with temporary Git
fixtures covering nested directories, missing origins, arbitrary fork origins and unrelated repos.
The final rerun,
`cargo +1.95.0 test --workspace --all-features --locked --no-fail-fast --target-dir /tmp/optd-cdc-v1-acceptance`,
exited **101: 256 passed, 1 failed, 2 already-ignored**. The fork/no-origin root-finder regression,
all CLI/core/cost/representation/gungnir/CDC tests, 31 SQL logic cases and 36 planner cases passed.
The sole failure is `optd-perfbench::cardtest_integration::cli_run_cardbench_twice`: PostgreSQL on
localhost is absent (`Connection refused`, OS error 111). PostgreSQL binaries/container runtime
were not available, and unattended sudo requires a password. No credentials or privileged install
were requested and the test was not suppressed. Its CI job already supplies PostgreSQL 15; rerun
that integration target with the documented test database service before claiming full workspace
test success. The generated benchmark workspace was removed after this failed fixture.

The owner has now requested commits and a fast-forward push to `origin/main` after each verified
repair increment. Publish this increment from `codex/incremental-execution-plan`, preserving the
PostgreSQL integration-gate limitation above. Next owner/action: E7 restricted SQL integration and accumulator/final-result semantics;
measure workloads before choosing key buckets, log TTL or orphan-file deletion.

## 12. Implementation loop 3 — 2026-10-29

**Outcome:** typed SQL maintenance now closes the local source → calculation →
output delta → durable checkpoint → restart/replay loop, including explicit
multi-table Iceberg endpoint vectors. TPC-H 22/22 and TPC-DS 99/99 obtain complete
nonempty/changing query evidence through both direct Arrow inputs and real
Iceberg v2/Parquet fixtures. The complete records are
[Arrow qualification](./sql_qualification.md) and
[Iceberg qualification](./typed_iceberg_qualification.md); their backends and
dataset limitations are kept separate.

| Lane | Delivered and observed | Boundary |
| --- | --- | --- |
| Typed execution | Separate DataFusion 55 / Arrow 59 typed graph, exact signed bags, immutable multi-table prepare/acknowledge, native linear/inner-join deltas, changed-group indexes and explicit nonlinear operator recomputation | No old lossy Value bridge, hidden whole-query fallback or all-native/performance claim |
| Local persistence | Immutable hashed IPC and predecessor manifests, fsync/atomic CURRENT, complete output and signed log, ordered rebuild validation, retry-directory-sync fix | Host-local ownership; no external sink transaction or hardware power-loss certification |
| Source vectors | Optional local/main/v2/Parquet adapter; UUID/schema/descriptor/retained-ancestry checks, exact endpoint differences, vector/output format-2 atomicity and strict source re-read on reopen | Explicit tuple rather than upstream cross-table transaction; fixed schema/no delete manifests; full endpoint scans and local budgets |
| Query acceptance | Original pinned SQL/schema, independent full-query plans over exact input endpoints, typed output and signed-delta comparisons, restart and per-prefix replay | Small independent profiles; directed DS witnesses; repository-specific H parameters; no TPC certification/scale/performance |
| Reproduction | Source preparation, fixed manifests, 99 explicit typed recipes, executable-pinned hard-timeout suite, backend-aware strict aggregation, usable API documentation and CI gates | Public upstream inputs are explicit ignored local downloads; generated data stays outside the repository; remote CI is not proven by local tests |

**Verified:** fresh-target current-source feature check; default SQL 72 passed /
1 ignored; feature SQL 84 passed / 2 ignored; both scoped all-target strict lint
configurations; full workspace strict lint and 350 passed / 4 ignored / 1 filtered;
28 Python tools tests; mdBook build. The full gate's Postgres fixture exclusion
and ignored driver/fixture meanings are recorded in the qualification page.
The complete user API example also compiles in a separate temporary consumer.

TPC-H source qualification retains its first Q21 120-second timeout in the
aggregate; a new 600-second-budget run completes all checks. TPC-DS completes
99 process exits of 0, 99 bootstrap comparisons and 297 mutation rounds. Every
chosen complete profile has nonempty output and a real signed delta. Empty-only,
unchanged and incomplete evidence cannot be selected as a passing profile.

The standing publication scope for this continuous task is the established
`optd-incremental-ivm` task branch and normal fast-forward updates to the verified
`origin/main`, following fetch/integration and validation. It does not authorize
force pushes, ref deletion, another destination, production deployment or remote
runtime claims. Dependency edges were isolated in separately verified commits;
no existing package version/source/checksum or MSRV changed. Root owns final
integration and publication; subsequent performance/deployment work needs its
own acceptance evidence.
