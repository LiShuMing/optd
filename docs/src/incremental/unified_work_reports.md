# Unified SQL and benchmark work reports

**Status:** accepted · 2026-10-31; implementation verified (ROADMAP R01d)
**Scope:** ROADMAP R01d, existing SQL database CLI, typed benchmark harnesses and
structured Python work audit. No new incremental algorithms or performance claims.

## 1. Problem

Operator counters, DML receipts and cumulative store counters use different scopes.
The current work audit omits whole-state copies/materialization and cannot process
SQL database benchmark rounds. Missing legacy measurements must remain unknown.

## 2. Constraints and evidence

Keep semantic comparison, complete planned blockers, actual execution work and
controlled performance separate. Historical receipt work is original execution
work; an identified retry must not report that work as fresh. The existing fixture
identity, SQL variant, strict-plan and comparison contracts remain unchanged.

## 3. Options considered

A single total row/byte count mixes different units and double-counts repeated
boundary work. Persisting IO totals inside the manifest is self-referential.
Use explicit domains at report boundaries and preserve original structured data.

## 4. Decision

Add checked StoreWork::since(before) for cumulative snapshots from one session;
reject counter underflow. It cannot prove that two equal-shaped snapshots belong
to the same session, so document that caller precondition. Keep recovery immutable.

SQL CLI emits command-local storage work separately from open-time recovery.
Original CommitReport remains unchanged. Explicitly label replayed receipts as
original diagnostics, with fresh DML/refresh work absent for retries. SELECT
consumption and CREATE MV bootstrap operator work remain unknown. Whole scripts
report aggregate command storage, with per-commit receipt execution diagnostics.

SQL database benchmark rounds capture store snapshots before writes, before/after
verification, and after reopen. Report transaction storage separately from full
oracle/history verification storage and open-time recovery. Attach executed DML
and view maintenance from the actual report. Empty transactions have known empty
work, not a fictional commit. Keep independent source/MV/delta-prefix checks.
Typed benchmark rounds expose refresh work and null for unavailable SQL/storage
measurements. Source acquisition remains its existing distinct domain.

Extend the structured work audit to accept both actual typed sql_stats and SQL
commit maintenance. Flatten only documented numeric work leaves (including state
copy, source/cache rebuild/materialization, DML delivery/target, store and recovery).
Retain per-domain measured/unknown round counts; aggregate values are observed
partial sums, never imputed complete totals. Reject bool/negative/noninteger work
and malformed supported structures. Retain raw planned blockers and semantic
status independently; neither measured zeros nor small passing witnesses imply
scope or performance qualification. Existing legacy reports remain readable.

## 5. Failure modes and recovery

Counter subtraction is checked. An execution failure never emits a successful
work report. Reopen and verification work cannot be attributed to transaction
maintenance. Missing original execution fields remain unknown after retry/reopen.
No persisted schema or receipt migration; only diagnostic output is additive.

## 6. Acceptance criteria

* Audit actual SQL database rounds and typed refresh rounds with copies and IO
  represented in their correct domains; legacy missing fields remain unknown.
* Test negative/bool work, partial rounds and contradictory plan blockers.
* Test CLI retry/reopen storage and recovery boundaries using actual database files.
* A small independent benchmark SQL loop includes all three mutations, verified
  source endpoints, MV contents and delta-prefix history after each reopen.
* Default/Iceberg format/lint/tests, Python tool tests, private rustdoc and mdBook pass.
* No TPC-wide, scope-scaling or controlled performance claim from these checks.

## 7. Rollout / migration

Additive JSON diagnostics; no dependency or data format changes. Existing work
fields retain their meaning. Record exact tests and limitations in ROADMAP.

## 8. Open questions

Complete native-source IO, CREATE MV bootstrap internals, allocator accounting and
controlled full-vs-incremental performance remain separate increments.

## Design review

The database harness currently invokes full source/oracle/history verification
inside each round and reopens the database. Measure those intervals separately
before exposing storage totals. Work audit must not sum original replayed receipts
or fill absent counters with zero. Operator work and refresh orchestration fields
are distinct boundaries and remain separately named instead of combined totals.


## Implementation verification

The SQL CLI keeps original CommitReport data and adds `execution_work` that is
absent on replay. Command `work.storage` excludes open-time `work.recovery`.
Benchmark rounds use explicit storage/recovery fields; typed and Iceberg rounds
retain null for unavailable storage/recovery. Work auditing accepts these fields
and historical sql_stats independently of plan metadata. `work_domains` describes
which rounds supply each domain; observed sums remain partial. Same-session
subtraction is a documented caller precondition, not a runtime identity check.
Default/Iceberg gates, private rustdoc, mdBook, 38 Python tests and current-binary
TPC-H Q1/TPC-DS Q1 three-round SQL loops passed; see ROADMAP for exact identities,
commands and limits. Full TPC and controlled performance qualification remain open.
