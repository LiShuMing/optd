# Incremental Rust organization and ownership refactor

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** accepted direction by owner · 2026-10-31; local checks recorded below
**Scope:** both incremental Rust crates; internal organization and readability.

## 1. Problem

The owner requested a refactor based on Google's Rust development guidance and
an enduring rule in `AGENT.md`. The SQL crate currently implements errors in its
root, graph construction and durable publication in its engine, and session,
response, transaction workspace and DML compilation in large combined modules.
The algebra crate combines canonical encoding, output schemas, generation
identity, envelopes, batches and trusted replay in one CDC module.

## 2. Constraints and evidence

Reference: [Google Rust Style Guide](https://google.github.io/styleguide/rustguide.html),
retrieved 2026-10-31. Relevant sections: code organization, named fields, enum
parameters, ownership/borrowing, error reporting and must-use prepared work.
The guide explicitly distinguishes general advice from codebase-specific rules.
Retain Cargo layout, async execution, Arrow sharing, Rust 1.95, dependencies and
public import paths. Baseline is local commit `c048cd674c7c5419d8ad3f3fd48b66c302ad7748`.

## 3. Options considered

* Reformat alone: does not address responsibilities or ambiguous internal state.
* Introduce new crates/traits or change public APIs: adds compatibility risk with
  no immediate semantic benefit.
* Extract cohesive private leaf modules, retaining public re-exports; selected.

## 4. Decision

* Record context-appropriate Google guidance in `AGENT.md`.
* Separate SQL errors, graph construction and durable-view orchestration.
  Keep `planner/mod.rs` as a facade over rule-registry/plan orchestration in
  `planner/incremental_optimizer.rs`; keep rewrite and physical execution separate.
* Make the database module an explicit facade over response, session, workspace,
  SQL-binding and DML-compiler implementations. Replace transaction/request result
  tuples and unclear mode booleans with named internal types.
* Give INSERT, DELETE and UPDATE separate row-image compilation helpers. Preserve
  their single statement snapshot, old-row assignment and strict cast behavior.
* Separate algebra CDC encoding, errors, schemas, generation identity, envelopes,
  prepared batches and trusted replay while preserving `cdc::*` public paths.
  Keep canonical bytes, digest domains, field IDs and validation order intact.
* Represent mutually exclusive trusted replay profiles with an enum and mark
  prepared incremental work as must-use. Keep required candidate isolation copies.

## 5. Failure modes and recovery

No new format, algorithm or recovery protocol is introduced. Existing checks for
failed transactions, stale candidates, uncertain publication, SQL immutability,
source identity and exact retry stay in the same execution order. Extraction
must not widen private state beyond its owning module subtree. Re-export old
public paths and use accessors across sibling leaf modules.

## 6. Acceptance criteria

Run formatting, warnings-denied Clippy and tests for both crates in default and
Iceberg configurations, including differential rules, SQL DML, strict planning,
canonical CDC vectors and real process-death recovery. Run fresh-target library
checks and build the book. Reuse existing semantic tests instead of adding tests
that merely mirror file movement. Keep existing TPC evidence tied to its original
binary/commit; do not relabel it as a new benchmark run or performance proof.

## 7. Rollout / migration

No migration. Code and the explicitly requested `AGENT.md` rule are local changes;
other Markdown and existing unrelated work remain unstaged. No push is authorized.
Local commits: `8560ef2` (working contract), `b252e5d` (CDC organization),
`2ad8d7c` (SQL organization). The original baseline and its TPC artifacts are retained.

## 8. Open questions

End-to-end incremental efficiency, unsupported query plans, compaction and
distributed execution remain separate work. This refactor does not close them.

## 9. Observed verification

Environment: Rust 1.95.0, Linux x86_64, offline locked dependencies, four build
jobs and debug information disabled. Local wrapper:
`/tmp/optd-sql-env-20261029.sh`.

| Command via wrapper | Observed result |
| --- | --- |
| `scripts/agent-check.sh sql` | Exit 0; 152 default tests passed / 2 ignored; 166 Iceberg tests passed / 3 ignored; format and both warnings-denied Clippy checks pass. |
| `scripts/agent-check.sh iceberg` | Exit 0; 41 default tests and 70 Iceberg tests passed; no ignored tests; format and both warnings-denied Clippy checks pass. |
| `env OPTD_SQL_RECOVERY_WAREHOUSE=/tmp/optd-database-sql-qualification-20261031/tpch-21-sf01/query-21/warehouse cargo +1.95.0 test --offline --locked -p optd-incremental-sql --test database_sql database_ipc_reopen -- --ignored` | Exit 0; one explicit compatibility regression passed in 17.78s. Opens the pre-refactor persisted checkpoint, checks against full SQL, and replays committed output history. |
| `python3 /tmp/optd-google-refactor-source-audit-20261031.py` | Exit 0; canonical encoding, output-schema and prepared-batch code match the baseline after accounting for qualification, visibility, accessors and formatting. All 19 CDC public names are retained; no dependency/lockfile changes. |

The SQL suite includes real process-death tests, multi-view atomic recovery,
strict-plan and custom-rule tests, old-row UPDATE assignments, NULL/duplicate
semantics and generated wide SQL. The algebra suite includes canonical protocol
vectors, keyed/unkeyed replay and actual Iceberg persistence/restart checks.
The source audit supplements execution evidence; it does not substitute for it.

Logs:

* `/tmp/optd-google-refactor-sql-gates-20261031.log`
* `/tmp/optd-google-refactor-core-gates-20261031.log`
* `/tmp/optd-google-refactor-compatibility-20261031.log`
* `/tmp/optd-google-refactor-source-audit-20261031.json`

Fresh-directory validation used the initially absent target
`/tmp/optd-google-refactor-fresh-20261031`, with this command via the wrapper:

```sh
env CARGO_TARGET_DIR=/tmp/optd-google-refactor-fresh-20261031 bash -c \
  'cargo +1.95.0 check --offline --locked -p optd-incremental -p optd-incremental-sql --lib && cargo +1.95.0 check --offline --locked -p optd-incremental -p optd-incremental-sql --features optd-incremental/iceberg,optd-incremental-sql/iceberg --lib'
```

Observed exit 0: default check 2m35s; feature check 2m15s. Log:
`/tmp/optd-google-refactor-fresh-20261031.log`.
The documentation command
`env MDBOOK_BUILD__BUILD_DIR=/tmp/optd-google-refactor-book-20261031 mdbook build docs`
also exited 0; log `/tmp/optd-google-refactor-book-20261031.log`.

Existing TPC result counts remain evidence of their original executable only;
this refactor does not rerun the full TPC matrix or certify a speedup.


## 10. Documentation follow-up (2026-10-31)

Scope: comments, public-field formatting and compile-time documentation policy;
no execution algorithm, public signature or persistence-format changes. This
uses the working contract's comment-fix design exemption. The follow-up covers
DML old/new images and old-row UPDATE evaluation, transaction candidate ownership,
prepare/publish/install ordering, ambiguous commit fencing, identified retries,
retractable aggregate preconditions, affected-key bucket access and remaining
index-directory copying. Internal types explain failure and ownership contracts;
public plan fields distinguish state slots, global relation IDs and local ports.

The initial Rustdoc audit reported 43 missing public-doc diagnostics in the
algebra crate and 27 in the SQL crate. Both crates now deny `missing_docs`.
Final checks use Rust 1.95.0 with offline locked dependencies, four jobs and the
existing target directory. For each crate, in both default and Iceberg configurations:

```sh
cargo +1.95.0 rustdoc --offline --locked -p <crate> --lib [--features iceberg] -- -D warnings
cargo +1.95.0 clippy --offline --locked -p <crate> --no-deps --all-targets [--features iceberg] -- -D warnings
```

All eight checks exited 0, including documentation links. Workspace format and
`git diff --check` also passed. `cargo test --doc` exited 0 in both configurations,
but neither crate contains executable doctests: this is not a semantic test run.
A supplemental comparison of all 17 changed Rust files found the same source
tokens after excluding comments, whitespace, trailing field commas and the new
lint attributes. This text check supplements compiler checks; it is not runtime proof.

No fresh-target build, semantic regression suite, TPC matrix or performance
benchmark was run for this comment-only follow-up. Earlier verification in this
file remains tied to its original changes. Evidence and exact log paths:
`/tmp/optd-comments-evidence-20261031.json` and
`/tmp/optd-comments-source-audit-20261031.json`.

This report and pre-existing Markdown changes remain local and unstaged. Source
changes are committed locally; no publication has been authorized for this follow-up.
