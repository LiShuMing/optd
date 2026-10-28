# CDC Output to Iceberg

How an incremental view's delta leaves the engine, and why the Iceberg table itself is the
outbox. This is the write-side counterpart to `iceberg_source.rs` (the read side, which captures
changes by snapshot diff).

Everything below was checked against **iceberg 0.10.1** as pinned in this workspace
(`apache/iceberg-rust` rev `5b82ab9`), not against the spec alone. The API facts decide the
design here: two of the shapes one would naturally pick are not implementable today.

## 1. The delta we are writing

`View::refresh()` returns `Option<Multiset>` — a weighted multiset of rows (`engine.rs`). A
`+n` weight means *n copies inserted*, `-n` means *n copies deleted*. That is already the CDC
payload; the only question is how to represent it in Iceberg.

Requirements:

* Every delta is written **exactly once**, even across crashes.
* A consumer can reconstruct the view by replaying deltas in order.
* No information is lost when the process dies between "computed the delta" and "wrote it".

## 2. Why the output is an append-only changelog

Three candidate shapes, and what the crate actually supports:

| Shape | Needs | Status in iceberg 0.10.1 |
|---|---|---|
| Append-only changelog (each row carries its weight/op) | `Transaction::fast_append` | **Supported** |
| Merge-on-read table with position/equality deletes (upsert) | A row-delta commit action that adds `DeleteFile`s | **Not supported** — the delete *writers* exist (`writer/base_writer/position_delete_writer.rs`, `equality_delete_writer.rs`) but `Transaction` exposes no action to commit their output. Writes would be orphaned. |
| v3 changelog with `_change_type` / `_change_ordinal` / `_commit_snapshot_id` metadata columns | Writer support for reserved field ids | **Not supported** — the reserved ids are defined (`metadata_columns.rs`), but no writer emits them. |

So the design is forced, not chosen: **append one row per change, with an explicit weight
column.** It is v2-compatible, readable by every client, and degrades to the v3 metadata-column
form later if the crate grows that support.

A nice consequence: the changelog table is itself a valid Iceberg table, so the existing
snapshot-diff source can read it — see §7.

## 3. Table schema

```
weight     int32   not null    -- +n insert, -n delete; sign is the op, magnitude the multiplicity
ordinal    int32   not null    -- position within this delta; fixes replay order
<view columns ...>  nullable    -- the row image; NULL for columns the delta does not carry
```

Notes:

* **Weight, not an `op` string.** `Multiset` is weighted; an aggregate delta can insert a row
  with weight > 1, and a fused update is a `-1`/`+1` pair. Deriving an op string loses the
  magnitude.
* **`ordinal` matters** because a delta is a multiset, not a sequence, but consumers that apply
  row-by-row need a deterministic order within a batch. It is the index into the delta as
  produced by `View::refresh`, so replay is reproducible.
* **No key column in M1.** Keyed views (see §5) add the key columns when the view declares them.
* **No partition in M1.** Partitioning is a file-layout decision, not a semantic one; add
  `bucket(N, key)` only when a consumer joins on the key. Partitioning by commit position would
  create unbounded partitions.
* **Schema evolution of the output** is out of scope for M1: a change in the view's schema
  (column added/removed/reordered) aborts the sink with an error rather than emit rows a
  consumer would misread. The sink records the view schema (field ids) in the snapshot summary.

## 4. Commit protocol — the Iceberg table is the outbox

One refresh = **one Iceberg snapshot**. The protocol:

1. Compute the delta for the source range `(from, to]`.
2. Write the rows to Parquet data files under the table's location, via
   `DataFileWriterBuilder` + `ParquetWriterBuilder` (`writer/base_writer/data_file_writer.rs`).
   Files are written *before* any commit is attempted.
3. `Transaction::new(&table).fast_append().add_data_files(files).set_snapshot_properties(...)`
   then `commit(&catalog)`.
4. The snapshot summary carries the resume point:

   ```
   veladb.view         = <view name / id>
   veladb.delta.from   = <source position before>
   veladb.delta.to     = <source position after>
   veladb.delta.rows   = <number of changelog rows>
   veladb.delta.hash   = <hash over the delta>      -- dedupe / integrity
   veladb.base.snapshot = <source snapshot id(s)>    -- for Iceberg sources
   ```

   `set_snapshot_properties` exists on `FastAppendAction`, and the properties come back from
   `snapshot.summary().additional_properties`.

Why this is exactly-once without a separate outbox:

* **Crash before commit** → the data files exist but no snapshot references them. They are
  orphans, invisible to readers, and reclaimed by orphan-file removal. The deltas are recomputed
  from the same source range on restart: same input, same output.
* **Crash after commit** → the new snapshot *is* the durable record of the delta and of the new
  watermark, in one atomic metadata update. Restart reads the last snapshot's `veladb.delta.to`
  and continues. There is no window in which the view advanced but the emitted delta was lost —
  the failure mode the earlier design had to solve with an outbox.
* **The in-memory watermark must be initialized from the table, not from local state.** On
  startup: load the output table, read the current snapshot summary, and start the source from
  `veladb.delta.to`. If the table is empty, start from the source's current position and treat
  the first refresh as a full build (position 0), not an incremental one.
* **A repeated commit of the same range** (e.g. a manual retry that re-reads the source before
  the summary was observed) produces a duplicate snapshot. `veladb.delta.hash` lets consumers
  detect it; the sink should also refuse to write when `from`/`to` do not match the source's
  current position. Treat this as a bug, not a normal path.

Limits, stated plainly:

* **Single writer per output table.** `Transaction::commit` reloads the table and re-applies the
  actions on the refreshed base, with catalog-level CAS on the metadata. That makes concurrent
  *appends* safe mechanically, but two writers that each computed a delta from the same source
  position would both append it. There is no global sequencer here; the source snapshot
  assertion in step 1 is the guard.
* **No multi-table atomicity.** One view, one output table, one commit. Two views cannot be
  advanced atomically; a consumer requiring that must read both and reconcile, or wait for a
  multi-table commit protocol Iceberg does not have.
* **Retention interacts with replay.** `expire_snapshots` exists (`transaction/expire_snapshots.rs`);
  expiring the snapshot that holds the current watermark removes the only copy of `positions`.
  The sink must either keep the latest snapshot's summary forever or maintain a side table of
  watermarks. M1 keeps history and does not expire.

## 5. Weight → operation mapping

For an unkeyed view, the mapping is the identity:

* `weight > 0` → INSERT row with multiplicity `weight`
* `weight < 0` → DELETE row with multiplicity `-weight`

When the view declares a key (`KeySpec`, not implemented yet), a further fusion is possible:

* a `-old` and `+new` pair with equal keys inside one delta → a single UPDATE record carrying
  before/after images. Until the view has keys, do **not** guess: two unrelated changes to rows
  with equal non-key content must not be fused.

Group-drop (an aggregate group that disappears) is just its deletes; no special case.

## 6. Files, batching, and small-file growth

* One Parquet file per delta per partition; deltas under the target size are batched: a sink may
  accumulate several refreshes and commit them as one snapshot whose summary records the full
  `(from, to]` range. Batching changes latency, not semantics, because `ordinal` is unique per
  delta — record `(from, ordinal)` for fully deterministic ordering if batches are used.
* Target 8–64 MB per file. A pathological workload (many tiny deltas) is handled by a separate
  compaction job that rewrites data files; it is not part of the CDC path.
* Sort order: none in M1. Consumers filter by summary/statistics; add a sort order only if a
  consumer needs range scans on the key.

## 7. Self-check using code we already have

Because the changelog table is append-only, the delta between two of its snapshots **is** the
rows we wrote for that range. So `IcebergSource::changes(p, p+1)` over the changelog table must
equal the delta that produced snapshot `p+1`. That is a free end-to-end test of the sink — the
read path and the write path validate each other — and it is how M1 should be accepted.

## 8. Failure modes

| Failure | State after | Recovery |
|---|---|---|
| Data file write fails | no snapshot; partial/orphan files possibly | recompute the delta, retry |
| Commit fails (conflict/IO) | no snapshot; files orphaned | `Transaction::commit` retries with backoff; on give-up, recompute from the same range |
| Crash between file write and commit | orphan files | restart resumes from the last summary, recomputes |
| Crash after commit | snapshot durable | restart reads summary, continues from `to` |
| Output schema changed by the view | sink refuses | explicit migration, or rebuild the output table |
| Same range committed twice | duplicate rows | `delta.hash` detects; sink refuses mismatched `from`/`to` |
| Old snapshot expired while consumers lag | history gap | pin retention in M1; later, a watermark side table |

## 9. Relationship to the `DeltaSink` design

The earlier CDC-output analysis listed three problems: weight→op mapping, exactly-once, and
destination. This document resolves two of them structurally:

* **exactly-once** stops being ours: Iceberg's atomic snapshot commit is the outbox, and the
  summary is the watermark. No side state, no two-phase write.
* **destination** is settled: a changelog table, readable by anything that speaks Iceberg.
* **weight→op** remains a schema decision (§3) plus an optional keyed fusion (§5).

`DeltaSink` remains the right trait shape — `emit(&Multiset, Position)` — with `IcebergCdcSink`
as one implementation. The trait should return the committed `Position`, not `()`: the caller
must not advance its watermark unless the commit succeeded.

## 10. Implementation order

* **M1 — minimal loop.** Unpartitioned, unkeyed, one file per delta, summary properties as
  specified, restart from summary, `IcebergSource` self-check. Uses `fast_append` only.
* **M2 — keyed updates.** `KeySpec` on views; fuse `-old`/`+new`; keep the changelog shape
  (updates are still rows, just paired).
* **M3 — layout and retention.** File batching, compaction job, partition by key bucket,
  retention policy with an explicit watermark side table if snapshots must expire.

## 11. Open questions

1. **Same catalog or a separate one?** The output table is write-heavy and its schema is owned
   by the engine, not by the source data. A separate catalog/warehouse keeps permissions clean.
2. **Consumers need upserts or changelog?** If downstream is a merge-on-read consumer, the
   changelog is enough; if someone expects to query "current state" directly, a materialized
   table is a different (and much more expensive) product.
3. **Retention.** Keeping every snapshot is fine for a prototype and unacceptable long-term;
   the watermark side table is the clean way out, but it reintroduces the two-write problem for
   the *retention* job only (not for CDC).
4. **v3 metadata columns.** Worth revisiting when iceberg-rust can write `_change_type`; at that
   point the explicit `weight` column becomes redundant for v3 readers.