# Run Typed SQL over Pinned Iceberg Sources

This guide connects existing local Iceberg tables to `IcebergSqlView`: select
exact snapshots, bootstrap typed SQL, publish a complete source/result epoch,
reopen it, and maintain the next explicitly selected tuple. The API is behind
`optd-incremental-sql`'s `iceberg` feature. Its protocol and admission rules are
defined in [Typed Iceberg Source Vectors](typed_iceberg_sources.md).

This is a library entry point. The Arrow-input `sql_loop` example and the
single-source legacy `cdc_loop` example do not accept this source-vector format.

## Source and ownership requirements

Use the supplied `optd_incremental::local_catalog::LocalCatalog` on a durable
local filesystem, Iceberg format v2, the `main` reference, and Parquet data.
Existing tables must retain every selected snapshot and the parent chain
needed to prove advancement from the committed endpoint. Snapshot IDs are
identities; their numeric ordering does not establish ancestry.

Every SQL source name has one fixed table binding. A self join still has one
binding. Supply every binding in each selection, including unchanged tables.
The table UUID, canonical catalog root, complete Iceberg schema, field IDs,
order, requiredness, defaults and Arrow layout remain fixed for the store's
lifetime, including columns unused by the query. Each snapshot must explicitly
identify its retained schema. Replacing a table under the same name or changing
its schema requires a separate, explicitly bootstrapped store.

The initial scalar profile admits Boolean, Int32, Int64, Utf8,
Decimal128 with valid nonnegative scale and precision at most 38, Date32,
microsecond Time64, and microsecond Timestamp with or without its preserved
timezone. Nested, floating-point, binary and other source types are rejected;
the adapter does not cast them to strings or floats. Any delete manifest is
rejected, including an empty delete-only manifest. Scan tasks with delete files
or non-Parquet data are also rejected. A full-snapshot overwrite that removes
old data files can represent deletions within this profile.

`SnapshotSelection::Empty` means a newly selected table has never had a
snapshot. It does not mean an arbitrary empty current result. A committed Empty
endpoint can subsequently advance to its first snapshot. There is no implicit
latest/head selection or automatic polling; the caller supplies actual IDs.

An explicit multi-table tuple can intentionally combine different source times.
Its atomic local publication does not prove that the upstream tables were
committed in one distributed transaction. `pin` captures metadata identities
without reading rows; successful pinning alone is not successful qualification
of an endpoint scan or SQL refresh.

## Complete API example

The following complete Rust binary reads existing `sales.orders` and
`sales.lineitems`. Both have `id: Int64`; `lineitems.amount` is
`Decimal128(18, 2)`. Additional columns must satisfy the fixed source profile.
The initial and next snapshot IDs are supplied as command-line arguments and
must already exist on retained mainline ancestry. The checkpoint directory must
be new for this first-run demonstration. No source table is written by it.

In a separate application, depend on the checked-out package with
`features = ["iceberg"]`, the same pinned `iceberg` package used by this
workspace, and Tokio with `macros` and `rt-multi-thread`. Do not introduce a
different Iceberg/Arrow version merely to compile the example. The workspace
currently pins Iceberg revision
`5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3` and Rust 1.95.0.

```rust
use std::collections::BTreeMap;
use std::error::Error;
use std::path::{Path, PathBuf};

use ::iceberg::{NamespaceIdent, TableIdent};
use optd_incremental_sql::coordinator::{IcebergRefresh, IcebergSqlView};
use optd_incremental_sql::iceberg::{LocalIcebergSources, SnapshotSelection};
use optd_incremental_sql::store::CheckpointStore;

const SQL: &str = "SELECT o.id, SUM(l.amount) AS total \
                  FROM orders o JOIN lineitems l ON o.id = l.id \
                  GROUP BY o.id";

fn open_sources(root: &Path) -> optd_incremental_sql::Result<LocalIcebergSources> {
    let namespace = NamespaceIdent::new("sales".to_owned());
    LocalIcebergSources::open(
        root,
        BTreeMap::from([
            (
                "orders".to_owned(),
                TableIdent::new(namespace.clone(), "orders".to_owned()),
            ),
            (
                "lineitems".to_owned(),
                TableIdent::new(namespace, "lineitems".to_owned()),
            ),
        ]),
    )
}

fn selections(orders: i64, lineitems: i64) -> BTreeMap<String, SnapshotSelection> {
    BTreeMap::from([
        ("orders".to_owned(), SnapshotSelection::Snapshot(orders)),
        (
            "lineitems".to_owned(),
            SnapshotSelection::Snapshot(lineitems),
        ),
    ])
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 6 {
        return Err(
            "usage: app CATALOG_ROOT NEW_STORE ORDERS_BEFORE LINEITEMS_BEFORE \
             ORDERS_AFTER LINEITEMS_AFTER"
                .into(),
        );
    }
    let catalog_root = PathBuf::from(&args[0]);
    let store = PathBuf::from(&args[1]);
    let before = selections(args[2].parse()?, args[3].parse()?);
    let after = selections(args[4].parse()?, args[5].parse()?);

    let sources = open_sources(&catalog_root)?;
    let initial = sources.pin(&before).await?;
    let view = IcebergSqlView::bootstrap(&store, SQL, sources, initial).await?;
    println!("bootstrap epoch={}", view.view().epoch());

    // Release the checkpoint writer lock before opening another owner.
    drop(view);
    let sources = open_sources(&catalog_root)?;
    let target = sources.pin(&after).await?;
    let mut view = IcebergSqlView::reopen(&store, SQL, sources).await?;

    match view.refresh(target).await? {
        IcebergRefresh::Unchanged { epoch } => println!("unchanged epoch={epoch}"),
        IcebergRefresh::Committed {
            source_work,
            sql_stats,
        } => println!("source_work={source_work:?}; sql_stats={sql_stats:?}"),
    }
    println!(
        "committed epoch={} rows={} vector={}",
        view.view().epoch(),
        view.view().contents()?.num_rows(),
        view.source_vector().digest()?,
    );

    // Offline inspection also takes the store's exclusive writer lock.
    drop(view);
    let checkpoints = CheckpointStore::open(&store)?;
    for committed in checkpoints.history_source()? {
        println!(
            "epoch={} delta_rows={} vector={}",
            committed.snapshot.epoch,
            committed.snapshot.delta.num_rows(),
            committed.vector.digest()?,
        );
    }
    Ok(())
}
```

For a real process restart, start at `IcebergSqlView::reopen`, retaining the same
SQL text, catalog root, complete bindings and store path. Do not run bootstrap
against the existing store. After reopening, pin the caller's next complete
selection and pass it to `refresh`. An unchanged table can retain its committed
ID while another advances. An identical tuple returns `Unchanged` only after
actual catalog identity/schema/descriptor validation; it creates no new epoch.
A physical rewrite with identical rows still advances the source tuple and
commits an epoch with an empty output delta.

## Publication, recovery and output

Bootstrap reads the exact selected endpoints and creates epoch zero. Refresh
reads both complete before/after endpoints for each changed source, checks the
before rows against the committed source cache, and computes their typed,
duplicate-preserving difference. All table changes enter one SQL preparation.
Source acquisition therefore performs full endpoint scans; it is not a native
changelog or work proportional only to the changed rows. Nonlinear SQL operators
can explicitly report affected-group or operator recomputation, as described in
[Typed SQL Execution](sql_execution.md).

The candidate's source IPC files, complete output, signed output delta and full
source vector share one format-2 checkpoint and one durable `CURRENT`
publication. Output delta rows have the output payload plus the terminal
`__optd_weight: Int64` column. `history_source` follows only the hash-bound
committed lineage, so an independent consumer can fold those signed bags;
unpublished attempts are not output history. This is a local checkpoint/output
log, not an Iceberg CDC sink or an external destination transaction.

The formats are deliberately separate:

| API/path | Stored protocol |
| --- | --- |
| Arrow-only `DurableSqlView` and `CheckpointStore::load/history` | SQL checkpoint format 1 |
| `IcebergSqlView` and `CheckpointStore::load_source/history_source` | SQL checkpoint format 2, containing source-vector version 1 |
| Legacy `optd-incremental` CDC loop | Its existing separate CDC v1 protocol |

Do not mix formats in one store or try to adopt old checkpoints implicitly.
The public source-aware store API validates committed bytes and lineage without
external IO. `IcebergSqlView::reopen` additionally revalidates retained source
identities, rescans the exact committed snapshots, checks complete source
caches, rebuilds SQL state/indexes, and verifies the stored output. It does not
substitute current heads when committed history or files have expired.

Each open store owns a nonblocking, host-local OS writer lock until dropped;
calls through one handle are also serialized. Opening a second writer or
inspection handle fails while that owner is live; drop it first. Cooperating
local source read pins protect scans against this catalog's physical
maintenance. They are not remote
fencing or retention leases against arbitrary external vacuum operations.

On a refresh error, stop the current loop. A failure before publication leaves
the prior committed epoch authoritative; a response lost after `CURRENT`
publication may mean the new epoch committed. Drop the owner and use strict
`reopen` before submitting further work, then inspect `source_vector` and epoch.
Do not replay uncertain row changes or advance a separate cursor file. The
observer API exposes `FilesDurable` and `CurrentDurable` for fault injection;
an error after the latter requires reopen rather than acknowledging old memory.
Missing history/files, changed UUID/schema, off-main snapshots, backward moves,
invalid bindings and corrupt checkpoints reject continuation instead of falling
back to a new source state.

The adapter scans sequentially with batches of 8192 and bounded concurrency.
Each endpoint is limited to 1,000,000 decoded rows and 256 MiB of decoded or
estimated scalar materialization storage; duplicate rows count before
consolidation. A manifest list is limited to 16 MiB and an endpoint to 100,000
scan tasks. These are admission bounds, not verified performance targets.
`SourceWork` reports changed sources, endpoint scans, decoded batches/rows/Arrow
bytes and consolidated signed change rows separately from SQL work.

## Reproduce query qualification

Run from the repository root with Rust 1.95.0. Prepare the pinned SQL/schema
identities first; the TPC-DS fetch is explicit network preparation, and its
`--verify-only` mode is offline. See the
[benchmark README](../../../optd-incremental-sql/benchmarks/README.md) for source,
license, data-generator and query-directed fixture boundaries.

```bash
python3 optd-incremental-sql/benchmarks/tools/extract_repository_tpch.py
python3 optd-incremental-sql/benchmarks/tools/fetch_duckdb_sources.py
python3 optd-incremental-sql/benchmarks/tools/fetch_duckdb_sources.py --verify-only

OPTD_BENCH_TARGET=$(mktemp -d /tmp/optd-typed-iceberg-target.XXXXXX)
CARGO_TARGET_DIR="$OPTD_BENCH_TARGET" cargo +1.95.0 build --locked \
  -p optd-incremental-sql --features iceberg --example benchmark
OPTD_BENCH_BINARY="$OPTD_BENCH_TARGET/debug/examples/benchmark"
OPTD_BENCH_SHA256=$(sha256sum "$OPTD_BENCH_BINARY" | cut -d ' ' -f 1)
OPTD_QUALIFICATION=$(mktemp -d /tmp/optd-typed-iceberg-qualification.XXXXXX)

OPTD_SQL_BENCH_ICEBERG_ROOT="$OPTD_QUALIFICATION/tpch-snapshots" \
  "$OPTD_BENCH_BINARY" optd-incremental-sql/benchmarks/tpch/manifest.json \
  "$OPTD_QUALIFICATION/tpch-coverage.json"

python3 optd-incremental-sql/benchmarks/tools/run_query_witness_suite.py \
  "$OPTD_QUALIFICATION/tpcds-suite" \
  --binary "$OPTD_BENCH_BINARY" --expected-sha256 "$OPTD_BENCH_SHA256" \
  --expected-backend typed_iceberg_endpoint_vector \
  --iceberg-root "$OPTD_QUALIFICATION/tpcds-snapshots"
```

Use new, separate child directories under `/tmp` for each qualification. The
direct benchmark chooses the backend through `OPTD_SQL_BENCH_ICEBERG_ROOT`;
the suite additionally requires explicit `--expected-backend` and
`--iceberg-root`, checking executable and report identities. Do not inherit a
conflicting backend-selection environment. An optional final logical query ID
limits the direct executable; `--query ID` limits the witness suite and leaves
full-suite coverage explicitly incomplete.

The TPC-H repository fixture can leave fixed-parameter queries empty and
correctly return nonzero. Use separately prepared generator-data profiles with
the original SQL/schema for additional evidence, as described in the README;
retain the incomplete report rather than rewriting parameters or calling empty
results complete. When combining profiles with `summarize_coverage.py`, pass
`--expected-backend typed_iceberg_endpoint_vector` and each report's actual
SHA-256. Arrow-only results cannot fill an Iceberg qualification gap.

For each selected query, the Iceberg backend creates real local v2/Parquet
snapshots, bootstraps from half the rows, then publishes three complete
overwrite rounds: insert the remaining half, remove the first row, restore it.
It independently scans the exact pinned endpoints, executes full DataFusion
SQL over those scans, and compares typed result bags and signed output deltas.
It also checks stored source caches, strict reopen after bootstrap/every round,
and final committed-output-log replay. The explicit tuple and complete endpoint
reads are recorded in the report.

A complete query profile requires bootstrap, all three successful rounds,
nonempty results and an observed output change. Empty-only, unchanged, failed
and timed-out profiles remain incomplete. The oracle shares DataFusion scalar
semantics, and independent presentation-order, standard-scale performance and
official TPC benchmark certification are outside these checks. These commands
describe the process; an actual pass requires retained execution evidence.

Run the source/store/SQL gates and offline benchmark-tool tests separately:

```bash
scripts/agent-check.sh sql
scripts/agent-check.sh sql-tools
mdbook build docs
```

`sql` runs formatting plus default/typed-Iceberg warnings-denied lint and tests;
`sql-tools` verifies prepared source identities and runs Python tool tests. The
typed-source tests cover exact snapshots, typed duplicates/NULLs, overwritten
and empty endpoints, rejected identities/history, resource bounds, strict
restart, and real child-process death at local publication boundaries. A failed
or skipped gate remains separate from successful query reports.
