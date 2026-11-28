# Persistent Operator State and Bounded-Replay Recovery (R07b)

> **当前磁盘格式。** 本文记录 v2 阶段的设计与验证。当前 SQL 数据库从空仓库使用 v3，结果块与输出变更日志的存储布局见[视图合并持久化设计](./view_output_bundle.md)。

**Status:** accepted · 2026-11-21
**Scope:** `optd-incremental-sql` persisted view operator states (`optd-operator-state/1`), plan/rule fingerprinting, checkpoint + bounded delta replay recovery in `SqlSession::open`, and `RecoveryWork` accounting.

## 1. Problem

Even with block-level table checkpoints (`R07a`), `SqlSession::open` currently reconstructs every materialized view by materializing all dependency base tables and calling `SqlView::new_with_optimizer`, which re-executes `eval_full` over the entire SQL DAG and rebuilds every `AccumulatorState`, `KeyedState`, `GroupState`, `RankState` and intermediate `Relation` from scratch. Restart latency and I/O therefore remain O(N · |views|).

## 2. Constraints and evidence

* Never silently apply persisted operator state to a different SQL query, schema, rule version or compiled physical plan.
* Do not reintroduce O(N) writes on every normal DML commit: normal commits must remain O(Δ).
* Preserve exact typed Arrow values (including signed zeros, NaN payloads, NULLs, duplicates, window peer ordering and numerical moment states) across restart.
* Zero new third-party dependencies: encode typed payloads with existing Arrow IPC + SHA-256 `FileEntry` and structural metadata with canonical JSON (`#[serde(deny_unknown_fields)]`).

## 3. Options considered

1. **Write full operator state on every commit**: makes recovery O(1) replay, but turns every 1-row DML commit back into O(N) disk write. Rejected.
2. **Always rebuild from base tables on open**: O(Δ) commits, but O(N) full-SQL recovery. Rejected (status quo).
3. **Periodic state checkpoints (every `K` affected commits, default `K = 16`, plus `CREATE MV`) + bounded replay of `≤ K` persisted `table_deltas` on open**: selected.

## 4. Decision

### 4.1 Plan and Rule Identity Fingerprint (`PlanFingerprint`)

Add `IncrementalPlan::fingerprint(sql, source_schemas) -> Result<String>` computing SHA-256 over:

1. `STATE_CODEC_VERSION = "optd-operator-state/1"`;
2. exact SQL definition string;
3. sorted dependency table names and canonical Arrow schemas;
4. for each graph node in postorder: `(node_id, operator_display, ordered, RuleIdentity { name, version, priority }, Strategy, StateSpecKind)`.

### 4.2 Persisted View State Format (`optd-operator-state/1`)

A view state checkpoint directory (`states/view-<id>/`) stores a hashed `state-manifest.json` containing:

* `format`: `"optd-operator-state/1"`
* `checkpoint_epoch`: database epoch at which this state is exact
* `plan_fingerprint`: SHA-256 hex string
* `view_epoch`: `SqlView`'s logical epoch
* `node_relations`: per-node `BlockRelationEntry` (reusing `R07a` `BlockUid` deduplication for intermediate `Relation` blocks!)
* `operator_states`: per-node optional `PersistedOperatorState`:
  * `Accumulator`: schema + ordered groups (`key_hex`, `group_scalars` Arrow IPC, `Cell` encodings covering `Native`, `ExactIntegerAverage(states, mass)`, `Moments(count, origin, mean, m2, values)`, `Values(OrderedValues)`, `Distinct(values, states)`).
  * `Keyed`: per-port ordered `Bucket`s (`key_hex`, weighted `bag` IPC, ordered `batch` IPC, `key_has_null`), optional `ResidualState` per-row match counts, and optional window `outputs` buckets.
  * `Group`: ordered `(key_hex, input_bag IPC, output_bag IPC)` buckets.
  * `Rank`: ordered `(key_hex, rows_bag IPC, count, rank)` peers.

`SourceState` for each dependency table is restored directly from the epoch's persisted table blocks without duplicating base-table files.

### 4.3 Checkpoint Cadence and Open Policy

* `OpenOptions` (default used by `SqlSession::open`):
  * `checkpoint_interval: u64` (default `16`; `1` checkpoints every affected commit, useful in tests).
  * `incompatible_state: IncompatibleStatePolicy`:
    * `Fail` (default, fail-closed): missing, corrupt, version-mismatched or fingerprint-mismatched state returns a typed error without mutating disk.
    * `Rebuild`: explicitly falls back to full base-table rebuild and records `rebuilt_views += 1` (never disguised as `restored_views`).
* On `CREATE MATERIALIZED VIEW`, an initial state checkpoint is published in the same atomic commit.
* On subsequent commits affecting view `v`:
  * If `affected_commits_since_checkpoint < checkpoint_interval`, `ManifestV2.view_states[v]` carries forward the existing `ViewStateRef` with zero operator-state disk writes.
  * When `affected_commits_since_checkpoint >= checkpoint_interval`, a new `ViewStateCheckpoint` is written atomically in the epoch directory.
* 压缩时，先推进旧检查点，再清理历史版本。未变化的视图可复用已有状态文件；发生变化的视图则根据当前已提交状态写入新检查点。新检查点和历史保留下界在同一数据库逻辑版本一起发布。详见[增量 SQL 生产就绪评估](./production_readiness.md)。
* On `SqlSession::open`:
  1. Compile each view's strict `IncrementalPlan` from table schemas only (`SqlView::compile_plan`, zero row scans).
  2. Verify `ViewStateRef.plan_fingerprint == compiled_fingerprint`.
  3. Load the `checkpoint_epoch` operator states and intermediate relations.
  4. 遍历检查点之后保留的元数据，仅解码该视图依赖的表增量。按正常检查点策略恢复时，通过 `SqlView::prepare` 和 `acknowledge` 最多重放 `K - 1` 次影响视图的提交。检查点与数据库当前版本之间仍可能夹有与该视图无关的元数据版本。
  5. Verify final view output equals committed `ViewData.output` (exact bag equality, plus exact order when `ordered == true`).

### 4.4 `RecoveryWork` Accounting

Extend `RecoveryWork` with additive optional fields:

* `restored_views: Option<u64>`
* `replayed_epochs: Option<u64>`
* `replayed_delta_rows: Option<u64>`
* `state_payload_reads: Option<PayloadWork>`

Incremental restore records `rebuilt_views = 0`, `dependency_rows = 0`, `restored_views = 1`, and exact replay/read counts.

## 5. Failure modes and recovery

* **Crash before `CURRENT` (`FilesDurable`)**: new checkpoint files are unreferenced; reopen uses previous `CURRENT` and its valid state checkpoint + lineage.
* **Crash after `CURRENT` (`CurrentDurable`)**: new `CURRENT` points to the synced state checkpoint or carried-forward `ViewStateRef` + synced `table_deltas`.
* **Tampered state file / mismatched checksum**: SHA-256 verification rejects load before installing any view.
* **Rule version or SQL change**: `PlanFingerprint` mismatch triggers explicit `Fail` or opted-in `Rebuild`.

## 6. Acceptance criteria

1. Restart after 1..`K` DML commits restores `Accumulator` (including MIN/MAX, DISTINCT, exact integer AVG, Welford STDDEV), `Keyed` (scoped, match-count, residual, partitioned window), `Group`, and `Rank` views with `rebuilt_views == 0`, `restored_views == 1`, `dependency_rows == 0`, and exact equivalence with independent full SQL and subsequent DML commits.
2. Checkpoint rollover at commit `K` resets `replayed_epochs` on reopen to `0`.
3. Tampered checksum or modified `PlanFingerprint` fails cleanly under `IncompatibleStatePolicy::Fail` and rebuilds transparently under `IncompatibleStatePolicy::Rebuild`.
4. Real subprocess `SIGKILL` tests pass across both checkpoint and non-checkpoint commits.

## 7. Rollout / migration

Part of `optd-sql-database/2`. No legacy v1 state format exists to migrate.

## 8. Open questions

Fine-grained copy-on-write block persistence inside `AccumulatorState`/`KeyedState` directories (so even periodic checkpoints at `K` write only changed buckets) can build directly on this codec in a future increment if checkpoint latency at large `|State|` becomes a bottleneck.
