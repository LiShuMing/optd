# Block-Level Checkpoints and Delta Logs (R07a)

**Status:** accepted · 2026-11-21
**Scope:** `optd-incremental-sql` host-local SQL database store (`optd-sql-database/2`), `RowBlocks` block identity/serialization, unordered MV block outputs, table delta logging and O(1) request-receipt index. Standalone single-view `CheckpointStore` (`optd-incremental-sql-checkpoint/1,2`) and `optd-incremental` Iceberg CDC formats are unchanged.

## 1. Problem

In `optd-sql-database/1`, `TableRows` (`RowBlocks`) already partitions tables into bounded immutable leaves (at most `BLOCK_ENTRIES = 256` distinct entries per leaf) shared via copy-on-write AVL directories. However, on every modifying commit:

1. `DatabaseStore::commit` calls `t.rows.materialize()?`, flattening all leaves of every modified base table into a single `RecordBatch` and writing the entire table to `table-<id>.arrow`.
2. Every modified materialized view writes its complete flattened output to `view-<id>.arrow`.
3. `DatabaseStore::receipt` traverses the entire manifest history chain (`self.manifests()`) linearly to check request-ID idempotency.

For a 65,536-row table with a 1-row insert, commit writes scale as O(N) (>1 MiB) even though in-memory state updates touch only 1–2 leaves.

## 2. Constraints and evidence

* Preserve single-host atomic visibility via `CURRENT` rename + directory fsync (`PublishPoint::FilesDurable` and `PublishPoint::CurrentDurable`).
* Preserve exact typed multiset semantics: duplicate multiplicities, NULLs, signed zeros, NaN payload bits, empty tables, zero-column tables, and exact output-history replay (`verify_output_history`).
* Clean-break format upgrade (`optd-sql-database/2`) approved by owner: reject `optd-sql-database/1` explicitly with a typed error; no online migration.
* Reuse existing Arrow IPC + SHA-256 `FileEntry` + canonical JSON (`#[serde(deny_unknown_fields)]`). No new dependencies.
* Preserve strict `StoreWork` accounting: written, read and reused file/byte/row counters must match actual on-disk files.

## 3. Options considered

1. **Append-only WAL over monolithic snapshots**: reduces per-commit write, but requires periodic full rewrites and full-WAL replay to read current table state.
2. **Content-addressed block store mirroring `RowBlocks` leaves + per-epoch net `table_deltas` + cumulative receipt index**: selected. Unchanged `Arc<RowBlock>` leaves are reused by immutable identity with zero flattening and zero disk I/O; changed commits write only touched leaves (≤ 2 blocks for a single-row write).

## 4. Decision

### 4.1 Immutable Block Identity (`BlockUid`)

Assign every newly constructed `RowBlock` a monotonic process-local `BlockUid(u64)`. Because `RowBlock` is immutable inside `Arc<RowBlock>`, any leaf untouched by a transaction retains its exact `BlockUid` across candidate `RowBlocks` revisions.

`DatabaseStore` maintains an in-memory map `committed_blocks: BTreeMap<BlockUid, BlockEntry>` updated **only after** `CURRENT` is durably published. A failed or rolled-back commit never inserts speculative block entries.

### 4.2 Disk Layout (`optd-sql-database/2`)

* **Weighted block payload (`t-<id>-b-<seq>.arrow` / `v-<id>-b-<seq>.arrow`)**: Arrow IPC encoding `RowBlock::to_weighted_batch()` (complete typed columns + non-null positive `__optd_weight: Int64`), avoiding expansion of high-multiplicity duplicate rows.
* **`BlockRelationEntry`**:
  * `schema`: reusable 0-row Arrow IPC `Payload` preserving exact field names, types, nullability and metadata even for empty relations.
  * `order_mode`: `Encoded | SingleLeaf`.
  * `blocks`: `BlockListRef` — inline `Vec<BlockEntry>` when `len <= 128`, or an external hashed JSON `Payload` (`blocks-<id>.json`) for larger relations so `manifest.json` stays compact.
* **Unordered vs Ordered MV outputs**:
  * Unordered MVs (`ordered == false`, the default for strict incremental views) store `ViewOutput::Blocks(RowBlocks)` in memory and `ViewOutputEntry::BlockRelation(BlockRelationEntry)` on disk, sharing identical O(Δ) block reuse with base tables and lazy chunked `RowTable` scans for `SELECT`.
  * Ordered MVs (`ordered == true`, root `ORDER BY`/`LIMIT`) store `ViewOutput::Ordered(RecordBatch)` and `ViewOutputEntry::OrderedPayload(Payload)` to preserve presentation sequence.
* **Per-epoch `table_deltas`**: persist the consolidated net signed changes (`TransactionWorkspace::changes`) for every modified base table as weighted Arrow IPC (`tdelta-<table_id>.arrow`) in `ManifestV2.table_deltas`, enabling bounded delta replay in `R07b`.
* **请求回执。** 新的带 ID 写入发布不可变的分片回执索引，查找路径的深度有上限。首次成功执行带 ID 的写入时，旧版累计 JSON 索引随该次写入一起迁移。格式、回退条件和验证结果见[增量 SQL 生产就绪评估](./production_readiness.md)。

### 4.3 `StoreWork` Accounting

* `tables` and `view_outputs` record actual per-block reads, writes and unchanged-block reuses (exact on-disk IPC bytes and logical expanded rows).
* Add optional `table_deltas: Option<PayloadIoWork>` to `StoreWork` (and `StoreWork::since`) so base-table delta writes are measured separately and transparently.
* Unchanged whole tables/views reuse their existing descriptors with zero serialization and zero flattening (`table_materialization.misses == 0`).

## 5. Failure modes and recovery

* **Legacy v1 database**: `read_manifest` checks `m.format == "optd-sql-database/2"` and fails explicitly before mutating any file.
* **Crash before `CURRENT` rename (`FilesDurable`)**: new epoch directory and block files remain unreferenced; reopen restores predecessor `CURRENT` and its intact blocks.
* **Crash after `CURRENT` rename (`CurrentDurable`)**: reopen loads the new `ManifestV2`, validates every referenced schema/block/delta/receipt-index SHA-256, and reconstructs `RowBlocks` directly from persisted weighted blocks without flattening.
* **Corrupted block or boundary key**: `from_persisted_blocks` re-verifies positive weights, schema equality, distinct-entry bounds, `row_count`/`distinct_entries` metadata and `min_key` ordering; any mismatch fails open atomically.

## 6. Acceptance criteria

1. Fixed 1-row `INSERT` / `UPDATE` / `DELETE` at 1,024 / 8,192 / 65,536 base rows writes at most 2 table blocks (≤ 512 distinct entries) and 0 full-table concatenation misses (`table_materialization.misses == 0`), while `tables.reused.rows` grows with N.
2. Unordered MV output commits write only changed view blocks; `SELECT * FROM mv` and `verify_output_history()` match independent full SQL across duplicates, NULLs, empty tables and zero-column tables.
3. Request-ID retry after 20 commits performs `history_traversals == 0` and rejects ID reuse with conflicting SQL.
4. Opening an `optd-sql-database/1` directory returns an explicit format error.
5. Real subprocess `SIGKILL` tests at `FilesDurable` and `CurrentDurable` pass with exact recovery and replay.

## 7. Rollout / migration

Clean break to `optd-sql-database/2`. No online migration from v1 is provided; existing single-view `CheckpointStore` formats are untouched.

## 8. Open questions

Orphan directory cleanup and physical compaction of unreachable historical blocks belong to `R07c`.
