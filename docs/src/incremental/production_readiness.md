# Incremental SQL production readiness

**Status:** accepted for the first storage increment · 2026-11-28
**Scope:** Bound the local SQL store's receipt and recovery work, qualify ordered SQL and Iceberg ingestion, and measure the effect on a target workload. This proposal specifies the first storage increment and its acceptance criteria. Later increments have their own reviewed designs.

## 1. Problem

At baseline `166eb5f82cbd4af6fa5319f26b85e73410356bf0`, every identified write clones and serializes all request receipts. The cumulative JSON file has a 16 MiB cap. Crossing it permanently prevents further identified writes until the stored receipt representation changes. Compaction preserves the receipt index. A view whose checkpoint has stopped advancing pins the retention floor even when subsequent writes touch only unrelated tables. The history walker admits at most 100,000 manifests.

`ORDER BY ... LIMIT` can block strict MV creation after a valid incremental aggregate. The typed Iceberg adapter decodes both endpoints for each changed source and requires its qualified local catalog and a stable, delete-free source profile. Local transaction and crash tests leave capacity, tail latency, and host-loss recovery unqualified. The original DataFusion bridge has panic paths and takes the shared optimizer out of its mutex while converting a physical plan.

## 2. Constraints and evidence

The owner accepted the first storage increment and selected controlled local comparison for this run. Target production qualification remains a separate gate.

- Preserve exact request identity, original response and conflict rejection forever under the current API. Any future receipt expiry must define a request fencing contract before allowing a formerly used ID to execute again.
- `CURRENT` remains the local publication authority. A failed candidate must change neither visible rows nor which request IDs are acknowledged. A lost response after publication must remain unambiguous after reopen.
- Retain the requested trailing history and every block, receipt and operator-state payload referenced by it. No reader or writer visibility change is allowed during compaction.
- The baseline local receipt fixture with 15,155 synthetic entries was 128 bytes below the cumulative cap. A normal identified write failed, the exact `CURRENT` bytes stayed intact, and an old request replayed after reopening. A separate 12-write fixture produced 1,110 bytes after the first request and 13,320 bytes after the twelfth. An idle view checkpoint at epoch 3 forced compaction at epoch 15 with a retention request of one to keep 13 manifests.
- The baseline unit and integration runs on macOS arm64 and Rust 1.100.1 passed 365 default tests and 379 Iceberg-feature tests. The recorded TPC plan counts describe plan admission. Target workloads require separate correctness and capacity qualification.

## 3. Options considered

- Increase the receipt byte limit or give receipts a TTL. Raising the cap leaves write amplification and a later failure. Expiry without fencing changes retry semantics.
- Write each receipt beside its transaction and rebuild a receipt index by scanning epochs. Lookup after restart and compaction would then depend on unbounded history.
- Publish a root for an immutable sharded receipt index and refresh old view checkpoints during compaction. The same local transaction root commits both operational records and data. Select this option.

## 4. Decision

### 4.1 Store receipts in bounded shards

Keep reading existing `optd-sql-database/2` manifests with the legacy `receipt_index` payload. Introduce an optional, versioned receipt root whose nodes are referenced by hashed payload descriptors. New roots and legacy index payloads are mutually exclusive. Migrate the legacy map on the first successful identified write by including every existing ID and original report in the same publication as the new write. A reader encountering an unknown root version fails before admitting writes.

Route by a stable SHA-256 digest of the exact request ID. Use narrow internal nodes and bounded sorted leaf shards. Split by the next digest nibble when a leaf reaches its target byte size. A large individual receipt uses a leaf by itself, subject to the existing individual JSON byte budget. Check IDs for exact equality inside leaves and return an error on a conflicting fingerprint or a directory or hash invariant violation. Use the existing JSON and verified-file helpers. Limit fanout and stop at the digest width. Report theoretical digest collisions without executing a duplicate ID. Storing a new ID writes the affected leaf and its ancestor path. Requests without an ID reuse the root. Open and retry use the root and a bounded path without traversing historical manifests on behalf of the receipt lookup.

Before publication, make the new receipt nodes and manifest durable. After publication, old roots remain valid for retained history. Walk the union of reachable receipt payloads from the retained roots before sweep and deduplicate shared subtrees by verified payload identity. Missing or corrupt referenced node data aborts the sweep. Account for receipt files and bytes read, written, and migrated independently of Arrow payloads and manifest bytes.

### 4.2 Advance recovery during compaction

Determine the floor from the requested trailing epochs and current database head. For each checkpoint older than that floor, move its reference forward without writing a state snapshot only if its validated state and source dependencies already represent the head. Initially use the persisted zero-affected-commits invariant and an admitted view identity for that check. For stale checkpoints with relevant writes or unsupported fast promotion, export the current committed view state under the exclusive store lock. Preserve each view's engine epoch, plan fingerprint and ordered output. Reuse existing state export and block persistence code.

Publish the updated checkpoints and retention floor together at the same logical database epoch. Keep old payloads and deltas reachable until the new `CURRENT` is durable. A failure after publication is resolved by reopen. Resume or repeat compaction after a crash. Ensure retained output replay starts from the retained baseline and that recovery loads only the table deltas it needs to restore each view. Report checkpoint bytes, reclaimed bytes, retained epochs, and recovery metadata and payload reads.

### 4.3 Follow-on increments

| Increment | Implementation boundary | Acceptance witness |
| --- | --- | --- |
| Strict ordered SQL, R05b | Start with finite `ORDER BY`/Top-K over qualified scalar keys. Maintain an ordered index with signed duplicate counts and seek the post-change prefix to refill deletions. Preserve SQL NULL ordering, direction, offsets, membership, and physical output sequence. Require proven or checked total ordering when boundary ties would expose ambiguous membership. Admit only covered plan shapes and persist versioned state. | Use the exact original SQL against a full SQL oracle. Include cutoff ties, NULLs, duplicate rows, deletes, order-only changes, rollback, restart, and fixed-delta scaling. Republish both original and relational-core plan catalogs separately. |
| Incremental Iceberg input, R08 | First qualify immutable Parquet file additions and removals under the existing typed snapshot contract. Bind a validated file inventory to the committed source position and read changed files. Revalidate metadata lineage and projected schema. Account separately for metadata, decoded rows, checkpoint writes, and source-cache work. | On the same exact snapshot tuples, compare signed deltas and the final result against full endpoint scans. Cover append, overwrite, compaction of equal rows, retries, missing files and a source cursor recovery interruption. |
| Target Iceberg integration | Design remote catalog and delete-file handling against the selected production catalog, storage and writer modes. Specify snapshot retention and per-file delete applicability, authenticated configuration, and output publication independently of the local source cursor. | In the approved target environment, compare position and equality delete effects to the existing full snapshot reader and exercise retained snapshots, lost responses and retries. |
| Storage limits and operations | Add workload-specific resource admission and checkpoint telemetry, and exercise capacity under checkpoint, compaction, long-running randomized workloads and restart. Design spillable state and multi-host operation independently if the target cannot meet its envelope on one host. | Publish a measured capacity envelope and verify the required restart, restore, and rollback paths within the target SLO. |
| Original Optd/DataFusion bridge | Replace supported error-path panics with typed errors, preserve optimizer ownership on fallible or canceled planning, and qualify explicit native planner fallback before execution. Use a separate branch increment from the SQL engine. | Differential SQL results, forced conversion errors, cancellation, concurrent sessions, planner fallback, and measured planning and execution latency on the chosen workload. |

## 5. Failure modes and recovery

- Before `CURRENT` is durable, keep the previous receipt root and retention floor authoritative. Orphaned attempts can be collected later.
- After `CURRENT` is durable, reopening yields the new checkpoint and receipt root even if the operation returned an error. The same request returns its stored outcome. A different request body under that ID stays rejected.
- Retention release requires reachable checkpoint payloads and an exact committed head. A failed state export, receipt migration, or validation leaves the older retained lineage available.
- A single outstanding request report can still exceed the individual JSON budget. Reject it before publication and preserve prior request identities. Do not use dropping receipts as recovery.

## 6. Acceptance criteria

### First storage increment

1. Read a baseline warehouse, replay old IDs, migrate on the first new identified write and reopen. Include a valid legacy fixture within 128 bytes of the legacy cap. Confirm the same write failed on the baseline, then succeeds on the candidate while every old ID remains resolvable and conflicting bodies stay rejected.
2. Vary prior IDs at fixed statement, table size and request report shape. Record per-write receipt bytes and files, lookup work, process memory and durable commit latency. Prove that each successful post-migration write modifies only the route and leaf, including shard splits. Ensure the store reopens after cumulative receipts exceed the old cap.
3. Exercise unrelated writes after view creation and after one relevant write. With retention of one and compaction at the same logical head, demonstrate floor advancement, bounded recovery metadata work and exact full SQL results. Include a transaction with relevant input changes and empty output delta to validate fast promotion eligibility.
4. Inject failures on both sides of `CURRENT` publication for identified commits, migration and compaction. Run real process death checks as well as deterministic fault tests. Confirm old and new epoch visibility, request replay, post-compaction writes, and output-history verification. Validate that corrupt receipt paths fail before sweep and never look like a new request.

### Controlled and production evidence

- Pin the baseline commit, candidate commit, lockfile, toolchain, release build profile, hardware, storage filesystem and input fixture identities. Run both binaries in isolated new warehouses on the same target machine, alternate run order, and record warm-up, sample counts, every run and timeouts.
- Report the effect of each increment independently. For receipts compare physical bytes and commit latency across prior-ID counts. For compaction compare retained epochs, reclaimed bytes and reopen time. For Top-K and source ingestion compare planned or decoded work and result equality as separate measurements. Use repeated unchanged-work and changing-work windows.
- Compare incremental maintenance with a full-query oracle over identical source epochs and the same original SQL. Check typed results, signed changes and recovery output before interpreting any latency measurement. Report p50/p95/p99 where sample counts support them, plus peak RSS, disk growth, failover or restart duration and result freshness.
- For production claims run a read-only or shadow replay of authorized inputs on the actual target storage or a user-approved equivalent. Keep output and checkpoint locations isolated. Agree the target query set, source types, peak change sizes, and acceptance limits ahead of time. Report synthetic or development-hardware measurements as controlled evidence and record the target production result separately.

## 7. Rollout and migration

Use separate commits for receipt storage and checkpoint-aware compaction. Exercise a copied warehouse during the format transition. Keep a pre-migration backup for rollback to the old binary. When the new receipt root is published, use a compatible binary to preserve and replay the new request IDs. The store remains host-local and requires a single owner during compaction.

Run target validation in shadow mode with the current production output still authoritative. Compare every agreed epoch and rehearse removing the shadow process and rebuilding from the approved source position. Consider a consumer cutover only after the target correctness, resource and recovery gates pass.

## 8. Open questions for review

- Agree the warehouse compatibility and rollback contract for the optional receipt root. Choose the shard byte target using measured tradeoffs in commit I/O and lookup work.
- Identify the target placement, supported source/catalog and delete modes, exact SQL and change profile, and tolerable freshness, memory, commit and recovery limits. Decide whether an isolated replay or a production shadow run is authorized.

## 9. Observed local storage qualification

**Verified with synthetic fixtures.** The baseline was `166eb5f82cbd4af6fa5319f26b85e73410356bf0`. Both binaries used Rust 1.95.0 and the locked dependencies in optimized builds on macOS 14.8.9, Apple M2 Pro, 10 CPU cores, 16 GiB RAM and APFS. Baseline binary SHA-256 was `2439def957cb13f2a9af5fd97ddf30be8046e63299327896cf5b7aa7df3e8587`. Candidate binary SHA-256 was `159047a076c9f32302dfa686d334f8624a4c240c3b9fe3241ad863ab14861869`.

Build the two example binaries at their respective revisions, preserving each binary under a distinct path. Then run the paired fixture generator from the candidate checkout.

```sh
CARGO_INCREMENTAL=0 cargo +1.95.0 build --release --locked -p optd-incremental-sql --example sql_database
python3 optd-incremental-sql/benchmarks/tools/qualify_storage.py \
  --baseline "$OPTD_BASELINE_SQL_DATABASE" --candidate "$OPTD_CANDIDATE_SQL_DATABASE" \
  --output "$OPTD_STORAGE_COMPARISON_JSON" --samples 8 --repeats 2 --history-epochs 128
```

The near-limit fixture seeded 15,100 synthetic receipts in 16,777,088 bytes. The baseline rejected the next identified write without changing `CURRENT`. The candidate committed that same request through migration and returned the original outcomes for both old and new IDs after reopening.

The receipt comparison used 16 timed updates per binary at each prior-ID count. Each update changed one row in a one-row table, and each binary received the same copied seed before warm-up. The byte figures are lengths of newly written receipt files. CLI times include reopening, hashing, SQL execution and writing the result. They are indicative local observations affected by process and test-run scheduling.

| Prior IDs | Baseline receipt bytes, P50 | Candidate receipt bytes, P50 | Baseline CLI P50 | Candidate CLI P50 |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 23,322 | 23,331 | 42.54 ms | 59.95 ms |
| 4,096 | 4,556,202 | 24,104 | 2,084.56 ms | 55.60 ms |
| 8,192 | 9,106,858 | 40,768 | 4,656.53 ms | 120.41 ms |

For the 8,192-ID case the receipt write fell by 99.55%. Maximum sampled CLI RSS was 68,780,032 bytes for the baseline and 30,162,944 bytes for the candidate. Small indexes incurred a small representation overhead. The migration was outside the timed sample and still performs a one-time copy of the legacy index.

Compaction followed 128 unrelated database commits and requested retention of one epoch. Both binaries reopened and returned the same full SQL and MV results. The candidate's unchanged checkpoint reused its existing state files. Its changed checkpoint wrote 3,867 bytes of new state files.

| View shape | Retained epochs, baseline and candidate | Old epoch payload bytes reclaimed, baseline and candidate | Table-delta epochs replayed, baseline and candidate |
| --- | --- | --- | --- |
| Idle | 129 / 1 | 8,888 / 648,175 | 0 / 0 |
| One relevant write | 130 / 1 | 8,888 / 658,219 | 1 / 0 |

**Evidence boundary.** These measurements cover stored receipt-file work, logical history retention, observed local CLI latency and a single-host process-recovery contract. Receipts are retained indefinitely; aggregate disk usage still grows with new IDs. Compaction must run often enough to keep history within the configured operational envelope. Target workload latency percentiles, host-loss recovery, full TPC execution, strict Top-K, and production Iceberg remain separate acceptance gates.

### Implementation and release gates

The code under test was `41fa259`, including the receipt migration in `412e186`. A separate staged build of the receipt-only change passed both release regression tests. The complete storage candidate passed the following commands on macOS arm64 with Rust 1.95.0, using a separate target directory for local verification.

```sh
CARGO_TARGET_DIR="$OPTD_CHECK_TARGET" CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  bash scripts/agent-check.sh sql
RUSTDOCFLAGS='-D warnings' CARGO_TARGET_DIR="$OPTD_CHECK_TARGET" \
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +1.95.0 doc --locked -p optd-incremental-sql --features iceberg \
  --no-deps --document-private-items
MDBOOK_BUILD__BUILD_DIR="$OPTD_BOOK_TARGET" mdbook build docs
```

The SQL script exited 0 with format and both all-target Clippy checks clean. The default suite passed 369 tests with 2 ignored. The Iceberg suite passed 383 tests with 3 ignored. Both suites had zero failures. The full TPC qualification and large Q21 recovery witness were ignored. The Iceberg helper marked ignored is run as a child by its process-death test. Rustdoc and mdBook 0.4.52 exited 0.
