# Run the Local CDC Loop

The local implementation of [CDC Protocol v1](./iceberg_cdc_output.md) is available behind
`optd-incremental`'s `iceberg` feature. It writes real Parquet files and Iceberg v2 snapshots,
including properties-only snapshots for empty changes. Its supported deployment is one host,
one cooperating writer and the supplied durable local filesystem catalog.

## Run a reproducible example

Use Rust 1.95.0. Run these commands from the repository root in the same shell:

```bash
OPTD_CDC_DEMO=$(mktemp -d /tmp/optd-cdc-demo.XXXXXX)
cdc_demo() {
  cargo +1.95.0 run --locked -p optd-incremental --features iceberg \
    --example cdc_loop -- "$OPTD_CDC_DEMO" "$@"
}
cdc_demo init
cdc_demo append 1 1 2 -1
cdc_demo refresh
cdc_demo append -3
cdc_demo refresh
cdc_demo refresh
cdc_demo dump
```

The demo view keeps `id > 0`. The first refresh bootstraps two physical records: `id=1` with
weight `+2`, and `id=2` with weight `+1`. The second refresh advances the checkpoint with zero
records, since the new negative input does not pass the filter. The third refresh reports no new
source snapshot. `dump` independently scans actual Parquet and prints validated envelopes and rows.
Its JSON diagnostic renders `__meta_timestamp` as UTC epoch microseconds; the stored column is
Iceberg `timestamptz`.

`cdc_demo watch 1000` polls for new snapshots every second and prints only committed batches.
Use the same warehouse path from another shell to append source data. `dump`, `peek` and
`consume` use a read-only log entry point and can run while the watcher owns the writer lock.
Stop the watcher before opening another refresher or running maintenance.
Restarting refresh/watch recovers from the output checkpoint. An error stops the loop rather than
silently skipping a batch. Input and output live under the chosen temporary warehouse; no generated
table data belongs in Git. The append command is a fixture generator, not a general SQL write API.

## Library path

* `cdc::Generation` binds a view plan, declared payload schema and stable source UUIDs. It produces
  the canonical plan/schema descriptors and fingerprints. `PreparedBatch` checks complete images,
  orders changes and creates the durable envelope; `expanded(limit)` emits bounded unit-weight copies.
* `IcebergSource::observe(...).await` materializes pinned checkpoint/head snapshots. It is refreshed
  explicitly from catalog metadata and never creates a nested runtime. Source positions are
  snapshot sequence numbers, not history-array offsets.
* `View::load_at` loads the exact retained source state. `prepare_refresh` does not mutate memory;
  `acknowledge` applies a committed candidate once and rejects foreign/stale candidates.
* `CdcLoop::recover` and `step` connect observation, bootstrap, prepared refresh, commit, read-back
  and acknowledgement. Failed attempts retain their candidate; restarts reload durable authority.
* `IcebergCdcSink` validates schema/property bindings, enforces output writer ownership and scans
  committed output to validate identities, hashes, row counts and the resulting relation.
* `LocalCatalog` publishes immutable metadata with an fsynced atomic pointer under OS-held locks.
  The ordinary commit path refuses history expiration and backward moves. Qualified output
  maintenance uses an internal metadata-pointer CAS; remote catalogs and distributed fencing
  are outside this adapter. Attempt directories are reserved atomically to prevent overwrite.

The source trait's snapshot/change methods now return `Result`, and provide `snapshot_at`.
`IcebergSource::new` is async; callers must await it inside the same live runtime used to construct
tables. Pure algebra evaluation remains synchronous over materialized input. This is an API change
within the research crate; downstream callers need to propagate source errors rather than assume
all reads succeed.

## Verification

```bash
scripts/agent-check.sh fast
scripts/agent-check.sh iceberg
python3 scripts/cdc-golden-vectors.py
mdbook build docs
```

The `fast` gate checks workspace formatting and the incremental crate's default-feature lint/tests.
The `iceberg` gate adds feature lint/tests, including the real subprocess-kill harness. Both use
Rust 1.95.0 by default; set `OPTD_RUST_TOOLCHAIN` only when deliberately testing another compatible
toolchain. Scoped lint uses `--no-deps`; full workspace lint is a separate gate.

For a fresh configuration build, set `CARGO_TARGET_DIR` to an unused directory before running the
gate. `scripts/agent-check.sh full` additionally requires full workspace test services and mdBook;
it fails when a required tool/check is missing instead of claiming it passed.

The recovery test launches a real child process, waits at deterministic file/commit barriers,
kills it without cleanup, advances the input and launches recovery against the same warehouse.
It covers bootstrap and incremental refresh before and after commit. Lost-ack, held-candidate,
duplicate-payload, empty-delta, full-image retraction and expired-checkpoint cases have separate
storage tests. Consumer replacement, compaction and multi-batch publication have an additional six real
kill/restart windows.
Storage tests also cover two independent consumers, pinned reads, downstream oracle comparison,
keyed updates, batch indexes, corrupted checksums, compaction and slow-consumer recovery after
expiration. Gold encoding vectors come from the independent Python generator.

## Streams and maintenance

After the example above, run:

```bash
cdc_demo peek reporting
cdc_demo consume reporting
cdc_demo consume reporting
cdc_demo compact
cdc_demo expire
cdc_demo dump
```

`peek` prints the pinned range and durable bookmark without advancing it. `consume` publishes a
local weighted destination together with the bookmark in one checksummed, fsynced atomic file.
The second consume is a no-op. A different consumer name has an independent bookmark. State lives
under `consumers/` in the warehouse; keep it with the table. A held consumer name excludes other
consumers of that name. These guarantees apply to this local destination, not arbitrary external
side effects.

`compact` rewrites all CDC rows into a new layout with a complete checksummed envelope archive.
`expire` requires the current head to be a verified compaction; it expires earlier **output snapshot
metadata**, preserving all CDC rows, files and source snapshots. After another refresh, compact
again before expiring. Neither operation changes batch identities, ordinals, timestamps or consumer
bookmarks. No automatic deletion or orphan cleanup runs.

Library additions:

* `CdcLogReader::read_batches` performs a full audit; `read_since(cursor, state)` validates a suffix
  from a trusted local destination checkpoint. Both pin one immutable metadata observation.
* `LocalConsumer::peek` returns immutable `PinnedChanges`; `consume` commits complete batches with
  effects and cursor as a unit. Stale overlapping ranges fail; repeated committed ranges are no-ops.
  The observer APIs are durability instrumentation, not an external destination connector.
* `LocalConsumer::source(table_id)` creates a pinned `CdcLogSource` implementing `DeltaSource` for
  downstream views. It starts from the state checkpoint, folds only its suffix and rejects positions
  before that checkpoint. Logical batch sequences are its source positions.
* `OutputSchema::keyed(KeySpec(vec![1000]))` enables keys by required payload field ids. Use
  `PreparedBatch::from_endpoints` to validate both complete relations, including unchanged keys.
  Stable-key replacements produce adjacent BEFORE/AFTER records, unit weights and the same C1
  base64url identity; changed keys produce unpaired DELETE/INSERT. Duplicate/null keys fail.
* `IcebergCdcSink::emit_many` writes a contiguous logical prefix in one snapshot, including empty
  batches. `compact` and `expire_snapshots` preserve the archive and replay coordinates.

## Remaining boundaries

The reference implementation materializes input endpoints and still physically scans the CDC
table. Consumer checkpoints avoid folding the old prefix, but do not establish incremental IO,
low latency or a speedup. Compaction currently materializes the whole log in memory. Key-bucket
partition tuning, logical CDC TTL and orphan cleanup need workload/consumer-low-watermark policies.
SQL integration, finalized SQL aggregates, distributed ownership and external destination connectors
remain separate work. See the [execution plan](./execution_plan.md) for evidence and next actions,
and the [streams design](./streams_and_maintenance.md) for recovery constraints.
