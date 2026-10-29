# Host-local SQL Incremental Loop

**Status:** accepted after independent agent review · 2026-10-29
**Scope:** a runnable example for typed local bootstrap, atomic multi-table changes,
checkpoint inspection, and independent signed-log consumption. This does not add
an Iceberg source/sink, a service, or a new durable format.

## 1. Problem

The typed SQL API and checkpoint store need a concrete external entry point so a
caller can initialize named Arrow tables, publish one change epoch, restart, and
consume the committed output log without a bespoke Rust application.

## 2. Constraints and evidence

Reuse `DurableSqlView`, `CheckpointStore::history`, Arrow IPC, and the existing
serde/tokio dependencies. The canonical execution and persistence design is
[Typed SQL Incremental Execution](./sql_execution.md). No new dependency or
library API is needed. Changes retain complete typed Arrow row images and signed
64-bit weights; the CLI does not parse CSV or convert decimals into floats.

## 3. Options considered

1. Add a production binary/service and new public configuration format: defer;
   the current backend is host-local and bounded.
2. Ship only an API example: insufficient for repeatable restart and log
   consumption across real process boundaries.
3. Add one standalone Cargo example with strict JSON input: selected.

## 4. Decision

Provide `cargo run -p optd-incremental-sql --example sql_loop -- <command>`:

* `init <warehouse> <sql-file> <tables.json>` bootstraps an empty warehouse or
  recovers an identical epoch-zero bootstrap idempotently.
  The tables file is a JSON object mapping registered table names to Arrow IPC
  **file** paths. Relative paths resolve against the JSON file's directory.
  Existing warehouses must have epoch zero, identical SQL and identical typed
  source multisets; otherwise the call is rejected. The response explicitly says
  `initialized_or_identical_bootstrap`, never claiming this call created an
  existing warehouse. The returned JSON names epoch,
  output row count, schema, and current operator strategies.
* `apply <warehouse> <changes.json>` recovers the committed SQL and source state,
  then atomically publishes the supplied changes. The strict object contains
  `expected_epoch` and `changes`, where every change contains `table`, `ipc`,
  and `weights` (one signed i64 per row). It compares `expected_epoch` with the
  recovered view while that view owns the store's writer lock, before prepare.
  The returned JSON names committed epoch, output row count, and refresh stats.
* `show <warehouse>` validates and reports the current committed snapshot and
  complete output rows as typed scalar display values. The display is for human
  inspection; durable values remain in Arrow IPC.
* `replay <warehouse>` follows only the hash-bound committed lineage, folds each
  signed output delta from an empty typed relation, and checks the resulting
  multiset against that epoch's stored output before advancing. It returns a
  JSON list of validated epoch and row counts. Replay compares bags, not ORDER
  BY presentation, because the change log encodes multiplicities rather than
  sequence positions.

Every command emits one JSON success object to stdout, or one error object to
stderr with a nonzero exit status. Arguments and JSON are validated strictly.
IPC readers preserve schema metadata, support multiple record batches in one
file, and concatenate them with the file schema, including empty relations.
JSON and SQL files are limited to 16 MiB; IPC files and decoded batches are
limited to 256 MiB. Input materializations respect the execution backend's row
bound before use, including zero-column batches with nonzero row counts.
`init` checks epoch zero and requested source contents after opening; a concurrent
bootstrap cannot be reported as initialization with different sources. `apply` discovers SQL
through a temporary store handle, drops that handle, then opens the durable view;
the mandatory epoch comparison after reopening closes the mutation race.

The log consumer uses its own `HashMap<Vec<ScalarValue>, i64>` and checked
arithmetic. It never calls the engine's private bag implementation. It verifies
payload schema, the terminal `__optd_weight: Int64` column, non-null weights,
nonnegative resulting relation, continuity, and per-epoch output equality.
Unknown or corrupt history fails rather than skipping an epoch.

## 5. Failure modes and recovery

* A stale or repeated `expected_epoch` fails before prepare and publication;
  callers inspect `show` or reopen after an ambiguous response instead of
  submitting the same rows against a newer predecessor.
* Unknown tables, IPC schema mismatch, incorrect weights, invalid deletion, or
  SQL failure use the API's transactional rejection and retain the prior epoch.
* Concurrent writers use the existing exclusive host-local store lock.
* `show` and `replay` validate committed bytes but do not rerun SQL. `apply`
  additionally uses SQL recovery validation before accepting new changes.
* A failed CLI command cannot claim its requested epoch committed. Publication
  may already have happened after a lost response; inspection is authoritative.

## 6. Acceptance criteria

An integration test runs the example's command handler with real temporary IPC
files and JSON: initialize a two-table join/aggregate, insert and delete across
both tables, reopen between calls, inspect output, and consume all committed
epochs. It verifies stale/repeated predecessor and invalid deletion retain
epoch/output. A replay-specific test creates a validly checksummed snapshot with
an inconsistent delta and requires consumption to reject it. Tests invoke the
same CLI handler, avoiding a Cargo subprocess or parallel nested builds.

Formatting, warnings-denied lint, and relevant tests must pass; exact commands
and observed results will be recorded after execution.

## 7. Rollout / migration

Add `examples/sql_loop.rs`, a private shared example module, and integration
tests that include that module. The example is opt-in and shares the existing
checkpoint format. Generated IPC/JSON and warehouse contents remain temporary,
not committed fixtures. Link this design from the documentation index after
review acceptance.

## 8. Open questions

No implementation decision is blocked. Read-only concurrent readers, automatic
source change extraction, output exports, distributed transactions, and durable
external consumer offsets remain separate work.

## Command example

Given `view.sql` and Arrow IPC files beside these JSON files, `tables.json` is:

```json
{"orders": "orders.arrow", "lineitem": "lineitem.arrow"}
```

A `changes.json` for the first atomic batch is:

```json
{
  "expected_epoch": 0,
  "changes": [
    {"table": "orders", "ipc": "orders-delete.arrow", "weights": [-1]},
    {"table": "lineitem", "ipc": "lineitem-insert.arrow", "weights": [1, 1]}
  ]
}
```

The following are usage examples, not recorded verification results:

```sh
cargo run -p optd-incremental-sql --example sql_loop -- init ./warehouse ./view.sql ./tables.json
cargo run -p optd-incremental-sql --example sql_loop -- apply ./warehouse ./changes.json
cargo run -p optd-incremental-sql --example sql_loop -- show ./warehouse
cargo run -p optd-incremental-sql --example sql_loop -- replay ./warehouse
```

After the first successful apply, change `expected_epoch` to the epoch returned
by that call for the next batch. If the response is lost, inspect `show` and
reconcile the source batch before choosing the next predecessor.
