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
* **That determinism is inherited, not guaranteed.** `Multiset` stores a `BTreeMap<Row, i64>`
  (`change.rs:54`), so iteration is in `Row` order. If the container is ever changed to a hash
  map, `ordinal` must be assigned by an explicit sort, or the "same input, same output" claim
  quietly stops holding.
* **`commit_seq` (`int64`) — required from M3 on.** The snapshot that introduced the row. It is
  redundant while one delta = one snapshot and consumers read only that snapshot's files, but
  compaction rewrites files across commits; without a row-level commit marker a consumer cannot
  tell whether rows re-added by a compaction snapshot were already applied (§6, §7.1).
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
4. The snapshot summary carries the resume point and the idempotency key:

   ```
   optd.view      = <view name / id>
   optd.delta.from = <source position before>       -- informational, not the resume identity
   optd.delta.to   = <source position after>        -- informational, not the resume identity
   optd.delta.rows = <number of changelog rows>
   optd.delta.hash = <hash over the canonically ordered delta>   -- idempotency key
   optd.base       = <per table {snapshot_id, sequence_number}>  -- durable resume point
   optd.bootstrap  = full | external
   optd.writer     = <lease holder id>
   ```

   `set_snapshot_properties` exists on `FastAppendAction`, and the properties come back from
   `snapshot.summary().additional_properties`. `delta.hash` is computed over rows in the
   `Multiset`'s canonical order (a `BTreeMap`, `change.rs:54`), so it is stable across runs.

Why this is exactly-once without a separate outbox:

* **Crash before commit** → the data files exist but no snapshot references them. They are
  orphans, invisible to readers, and reclaimed by orphan-file removal. The deltas are recomputed
  from the same checkpoint on restart: same input, same output.
* **Crash after commit** → the new snapshot *is* the durable record of the delta and of the new
  resume point, in one atomic metadata update. Restart reads the last summary and continues from
  `optd.base` — **not** from `delta.to`, which is source-instance-local (§4.2). There is no window
  in which the view advanced but the emitted delta was lost.
* **The in-memory watermark must be initialized from the table, not from local state.** On
  startup: load the output table, read the last summary, rebuild the view at `optd.base` (§4.2).
  If the table is empty, the first commit is the bootstrap record (§4.1), not an incremental
  refresh from an assumed position 0.
* **A repeated commit of the same range** is a duplicate. Before retrying after an ambiguous
  failure, read the last summary and compare range + hash (§4.3); if the range was already
  committed, treat it as done. `delta.hash` is the idempotency key, not just diagnostics.

Limits, stated plainly:

* **Single writer per output table.** `Transaction::commit` reloads the table and re-applies the
  actions on the refreshed base, with catalog-level CAS on the metadata. That makes concurrent
  *appends* safe mechanically, but two writers that each computed a delta from the same source
  position would both append it. There is no global sequencer here; the source snapshot
  assertion in step 1 is the guard. In practice, enforce a per-view writer lease (a catalog
  property, a coordination service, or a lock row with an expiry) and record the writer id in the
  summary so a violated lease is visible after the fact. Without a lease this protocol is
  at-least-once, not exactly-once.
* **No multi-table atomicity.** One view, one output table, one commit. Two views cannot be
  advanced atomically; a consumer requiring that must read both and reconcile, or wait for a
  multi-table commit protocol Iceberg does not have.
* **Retention interacts with replay.** `expire_snapshots` exists (`transaction/expire_snapshots.rs`);
  expiring the snapshot that holds the current watermark removes the only copy of `positions`.
  The sink must either keep the latest snapshot's summary forever or maintain a side table of
  watermarks. M1 keeps history and does not expire.

### 4.1 The first commit must bootstrap the view

Nothing above makes the log replayable **from empty**. `View::load` builds the full contents at the
current position, but `refresh` only emits changes *after* that point; a consumer replaying from
zero would see the tail of the history and none of its beginning. Two contracts are workable:

* **Bootstrap record (default).** The first commit writes the whole view as weight `+1` rows, then
  normal deltas follow. Cost: one full copy, once. It is the only option where the log stands
  alone.
* **External base.** The log is valid only from a declared base snapshot the consumer already
  holds. Cheaper, but the base must be shipped out of band, and the summary must name it.

Record the choice in the summary (e.g. `optd.bootstrap = full | external`). Mixing them silently is
the failure mode: an empty output table plus a "resume at current position" restart looks like a
valid log that has lost its history.

### 4.2 Restart must rebuild at the checkpoint, not at "now"

`delta.to` is a `Position`, and for the only implemented source (`IcebergSource`) a position is an
**epoch index over the snapshot timeline** (`current_position() = epochs.len()`). Epoch indices are
source-instance-local: after a restart the timeline is rebuilt and the same index may denote a
different snapshot or none at all. The durable identity is the snapshot recorded in
`base.snapshot`, not `delta.to`.

Consequences:

* Store `{snapshot_id, sequence_number}` per base table in `base.snapshot`; the sequence number is
  monotone, the id selects the exact state.
* On startup, load the output table, read the last summary, and **LOAD the view at those
  snapshots**. Loading at the source's *current* position and then setting the watermark from the
  summary is wrong: `View::load` already includes the post-checkpoint changes, so the next refresh
  applies them a second time.
* This is a prerequisite, not an optimization: `View::load` only loads at `current_position()` and
  `DeltaSource` exposes no `snapshot_at`. Add `snapshot_at`/`load_at` before claiming crash
  recovery. `IcebergSource` already has the scan-by-snapshot-id path (`scan_at`), so this is an API
  exposure rather than a new capability.
* If the source has no durable snapshot identity, the safe restart is a **new log generation**
  (new table, or a generation id in the summary) after a full rebuild — never a replay into the
  old log.
* Longer term, make `IcebergSource`'s `Position` *be* the snapshot sequence number. That removes
  the epoch translation and the entire class of "the position moved after restart" bugs.

### 4.3 An ambiguous commit must be resolved by reading back

`Transaction::commit` can fail after the catalog has accepted the metadata update (timeout, lost
response). Retrying blindly appends the same delta twice. Before a retry:

1. reload the output table;
2. read the last snapshot summary;
3. if it already records the target range and hash, treat the commit as succeeded and drop the
   staged files;
4. otherwise commit again.

This read-back is also what distinguishes "our commit" from another writer's (§4, single-writer
bullet). It is why the summary carries both `delta.from/to` and `delta.hash`: together they are the
idempotency key, not just diagnostics.

### 4.4 Keep the delta across a failed commit

`View::refresh` advances the in-memory watermark as part of computing the delta. After a commit
failure, recomputing the range would apply it to state that already includes it. The retry loop must
hold the computed `Multiset` and retry **that** commit; if the process cannot continue, it should
exit and restart from the durable checkpoint (§4.2). A `preview()` that computes Δ without
committing the in-memory watermark makes this explicit and is the cleaner fix.

These four points are what separate the protocol from a demo. With them the failure table in §8 is
complete; without §4.2 and §4.3 the guarantee is at-most-once in the crash window and at-least-once
on a lost acknowledgement, respectively.

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
  `(from, to]` range. Batching changes latency, not semantics at the multiset level.
* Batching does break row-level ordering, though: `ordinal` is unique **per delta**, not per
  snapshot, and the rows do not carry the delta's `from`. Either keep M1 at one delta per snapshot,
  or add `delta_from` to the row schema so `(delta_from, ordinal)` is recoverable from the data.
  Recording `(from, ordinal)` in the summary is not enough — a summary describes the whole batch,
  not which row came from which delta.
* Target 8–64 MB per file. A pathological workload (many tiny deltas) is handled by a separate
  compaction job that rewrites data files; it is not part of the CDC path.
* Sort order: none in M1. Consumers filter by summary/statistics; add a sort order only if a
  consumer needs range scans on the key.

## 7. Self-check using code we already have

Because the changelog table is append-only, the delta between two of its snapshots **is** the
rows we wrote for that range. So `IcebergSource::changes(p, p+1)` over the changelog table must
equal the delta that produced snapshot `p+1`. That is a free end-to-end test of the sink — the
read path and the write path validate each other — and it is how M1 should be accepted.

### 7.1 The log is not directly a base table

The self-check diffs two snapshots; it does not mean the engine can consume the changelog as an
ordinary base table. Its rows are **oriented changes** (`weight`), not a relation, so `snapshot()`
over the log is not the view's state — it is the history of states. A downstream maintained view
needs a dedicated source:

```
CdcLogSource::changes(table, from, to):
    read only data files added by snapshots in (from, to]
    filter rows by (commit_seq, ordinal) > watermark      // commit_seq from M3
    return (row, weight) as a Multiset                    // weights preserved
CdcLogSource::snapshot(table):
    fold all changes up to the position                  // O(history)
```

Capabilities: `ordered`, `gap_free`, `expresses_deletes`; `before_images` only when key fusion
(§5) is enabled; `cross_table_position` only if several logs share one commit protocol. Two costs
have to be decided early:

* `snapshot()` by folding the whole log is called by join rules on every refresh. Without periodic
  state checkpoints (or a materialized serving table), a chain of maintained views degrades to
  folding the entire history at every level.
* Retention must cover the slowest consumer. Expiring snapshots that still hold unread rows is a
  real gap, not a performance issue.

This is also why `commit_seq` (§3) stops being optional once compaction exists: after a rewrite,
"files added by this snapshot" is no longer a faithful per-commit feed.

## 8. Failure modes

| Failure | State after | Recovery |
|---|---|---|
| Data file write fails | no snapshot; partial/orphan files possibly | recompute the delta, retry |
| Commit fails (conflict/IO) | no snapshot; files orphaned | retry **the same delta**; if that is impossible, restart from the durable checkpoint (§4.4) |
| Crash between file write and commit | orphan files | restart resumes from the last summary, recomputes |
| Commit times out after the catalog accepted it | snapshot exists, caller saw an error | read back the last summary; if range + hash match, treat as committed (§4.3) |
| Crash after commit | snapshot durable | restart reads summary, then **rebuilds the view at `base.snapshot`** (§4.2) |
| Restart using `delta.to` from a previous source instance | the epoch may denote a different snapshot | never resume from `delta.to`; rebuild at `base.snapshot` (§4.2) |
| View loaded at current position, watermark taken from summary | in-memory state is ahead of the watermark | the next delta double-applies; prohibited by §4.2 |
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

`DeltaSink` remains the right trait shape — `emit(&Multiset, CommitMeta) -> CommitMeta` — with
`IcebergCdcSink` as one implementation. Three details follow from the sections above:

* it returns the **committed** metadata, not `()`;
* it exposes `last_checkpoint(view) -> Option<CommitMeta>`, because restart begins by reading the
  output table, not local state (§4.2);
* `CommitMeta` carries the source snapshot ids/sequence numbers, the bootstrap flag, and the
  writer id, so the summary is part of the contract rather than a diagnostic afterthought (§4.1,
  §4.3).

## 10. Implementation order

* **M1 — minimal loop.** Unpartitioned, unkeyed, one delta per snapshot, summary properties as
  specified, bootstrap commit, `IcebergSource` self-check. Uses `fast_append` only. Add
  `snapshot_at`/`load_at` in this milestone — without them "restart from summary" is not
  implementable and M1 cannot be accepted as crash-safe (§4.2).
* **M2 — consumption.** `CdcLogSource` (§7.1) plus periodic state checkpoints; acceptance is a
  view over the log equalling a view over the base table at every step.
* **M3 — keyed updates.** `KeySpec` on views; fuse `-old`/`+new`; keep the changelog shape
  (updates are still rows, just paired). `commit_seq` enters the row schema here if it was not
  already added.
* **M4 — layout and retention.** File batching (with `delta_from`), compaction job, partition by
  key bucket, retention policy with an explicit watermark side table if snapshots must expire.

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