# CDC Output to Iceberg — Protocol v1

**Status:** accepted · 2026-10-28
**Scope:** final CDC row format, batch identity, consumption semantics and Iceberg commit contract.
Finalized at the owner's request to reference Snowflake and MaxCompute Streams. Implementation
and runtime acceptance remain pending; design acceptance is not evidence of a working sink.

## 1. Problem and decision

An incremental view emits a signed multiset, not a source DML audit trail. Its output must be
understandable to SQL consumers while preserving duplicate rows, retractions and restart identity.
An operation-only format loses multiplicity unless every copy is expanded.

**Decision:** persist an append-only Iceberg v2 change table with full view-row images followed by
operation/update metadata, signed 64-bit weight and durable batch coordinates. An UPDATE remains
two rows. A stream is a separate consumer bookmark over this table; querying it does not consume it.

Protocol name: **`optd-cdc/1`**. One output table contains exactly one view generation. This revision
replaces this document's earlier `weight int32` / `ordinal int32` draft.

## 2. Constraints and reference designs

### 2.1 Official references checked on 2026-10-28

| Aspect | Snowflake Streams | MaxCompute Streams | optd decision |
| --- | --- | --- | --- |
| Row shape | Source columns plus metadata | Source columns plus metadata | Complete view image plus system columns |
| Operation | `METADATA$ACTION`: INSERT/DELETE | `__meta_op_type`: 1/0 | `__meta_op_type`: 1/0 |
| Update | `METADATA$ISUPDATE` on both images | `__meta_is_update`: 1 on both images | Boolean flag on both proven update images |
| Identity/time | `METADATA$ROW_ID` | `__meta_timestamp` | Optional logical key identity; preparation timestamp |
| Consumption | Query leaves offset; consuming DML commits it | Query leaves offset; consuming DML advances version | Peek leaves cursor; committed consumer effects advance it |

Snowflake standard streams expose net differences between offsets; an insert followed by delete
can disappear. Its immutable row ids come from source change tracking. We borrow the before/after
shape and transactional bookmark, without claiming its identity implementation.
[Snowflake: Introduction to streams](https://docs.snowflake.com/en/user-guide/streams-intro).

MaxCompute CDC includes a write timestamp and operation/update flags. Its append mode can include
updated after-images while omitting deletes; Snowflake append-only omits updates too. These modes
are not interchangeable. v1 defines complete CDC only, with no ambiguous `append` mode.
[MaxCompute: Stream objects](https://help.aliyun.com/zh/maxcompute/stream).

### 2.2 Repository and pinned dependency evidence

`change.rs` stores `BTreeMap<Row, i64>`. `View::refresh` advances memory while returning a delta.
The Iceberg reader captures a source-local epoch timeline once. Public historical LOAD, prepared
refresh and the sink remain implementation work.

The pinned `iceberg-rust` revision is `5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3`:

* `FastAppendAction` accepts data files and snapshot properties. This is M1's write primitive.
  [Pinned append action](https://github.com/apache/iceberg-rust/blob/5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3/crates/iceberg/src/transaction/append.rs).
* `SnapshotProducer::produce_manifests` permits zero added files when snapshot properties are
  nonempty. Use a properties-only snapshot for empty deltas and empty bootstrap. This is verified
  from source, not from a runtime storage test.
  [Pinned snapshot producer](https://github.com/apache/iceberg-rust/blob/5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3/crates/iceberg/src/transaction/snapshot.rs).
* `Transaction::commit` retries retryable errors and reloads/rebases actions. Disable these retries
  with `commit.retry.num-retries=0`, verify the effective setting, and let the CDC driver resolve
  unknown outcomes before another append.
  [Pinned transaction implementation](https://github.com/apache/iceberg-rust/blob/5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3/crates/iceberg/src/transaction/mod.rs).

## 3. Options considered

| Format | Decision |
| --- | --- |
| Weight and ordinal only | Replace with descriptive metadata and durable identity; retain weights |
| Flags only, expanding every copy on write | Reject as physical default: duplicates unnecessarily multiply storage |
| Single UPDATE row with partial columns | Reject: before-images and actual NULLs become ambiguous |
| Vendor-specific format as sole protocol | Reject: define mappings over a canonical contract |
| Full images, flags and weighted storage | Accept, with an explicit row-expanded projection |

## 4. Final row format and semantics

### 4.1 Iceberg schema

Payload columns appear first in declared view order. System columns follow in the order below.
Types are Iceberg types; `long` is signed 64-bit and `timestamptz` has microsecond precision.

| Field id | Column | Type | Required | Meaning |
| --- | --- | --- | --- | --- |
| 1000 + index | `<view columns>` | Declared output type | Per schema | Complete before/after row image |
| 1 | `__meta_op_type` | int | yes | 0 = DELETE, 1 = INSERT |
| 2 | `__meta_is_update` | boolean | yes | Participates in a proven logical UPDATE pair |
| 3 | `__meta_weight` | long | yes | Signed multiplicity, with sign matching operation |
| 4 | `__meta_row_id` | string | no | Generation-scoped logical key identity; NULL if unavailable |
| 5 | `__meta_batch_id` | string | yes | Durable batch identity from §5 |
| 6 | `__meta_batch_seq` | long | yes | Contiguous generation-local sequence starting at 1 |
| 7 | `__meta_ordinal` | long | yes | Zero-based physical row index within a batch |
| 8 | `__meta_timestamp` | timestamptz | yes | UTC preparation time, fixed across retries of the prepared candidate |

Ids 9–999 are reserved. Payload indices start at zero; check field-id arithmetic. Reject
case-insensitive `__meta_` payload names, duplicate names/ids, unexpected width, incompatible types
and schema mismatch. NULL means a real NULL, never an omitted field. No partial images are allowed.

M1 payload representation is `value-v1`: `Value::Int → long`, `Value::Str → string`, and typed NULL.
Require an explicit output schema, including all-NULL columns. Original source decimals, floats
or times currently mapped to Int/Str are not silently restored to native SQL types. Aggregate
payloads remain the declared accumulator-state layout until a finalized-result contract exists.

### 4.2 Four change kinds

| Kind | `__meta_op_type` | `__meta_is_update` | `__meta_weight` | Image |
| --- | --- | --- | --- | --- |
| INSERT | 1 | false | +n | Entire inserted row |
| DELETE | 0 | false | -n | Entire removed row |
| UPDATE_BEFORE | 0 | true | -1 | Entire old row |
| UPDATE_AFTER | 1 | true | +1 | Entire new row |

Reject zero and `i64::MIN`; accepted weights are nonzero integers in
`[-9223372036854775807, 9223372036854775807]`. The symmetric range permits checked negation and
absolute multiplicity. Check weight, join and aggregate arithmetic before publishing a candidate.
Unknown operation codes and operation/weight sign disagreement are errors.

A physical `(row, +3)` means three logical inserted copies. Apply signed weights to row counts,
then validate the resulting relation. Publish the whole batch atomically; intermediate row-by-row
states are not committed relations.

### 4.3 UPDATE pairing and row identity

**M1 is unkeyed:** `__meta_row_id = NULL` and `__meta_is_update = false` for every row. An input
update can become ordinary DELETE/INSERT changes in a derived view. We preserve those changes
and their before-images without inventing lineage or an immutable id for duplicate rows.

**Keyed profile, implemented in M3:** `KeySpec` names ordered, non-null payload fields and requires
one row of weight 1 per key in both endpoint relations. Validate the full endpoint invariant,
not just the changed rows. For a key present at both endpoints with different payload, emit one
BEFORE/AFTER pair with the same row id and adjacent ordinals, BEFORE first. An unchanged row emits
nothing. A changed key emits an unpaired DELETE and INSERT, both with `is_update=false`.
Uniqueness or key-null violations fail; they never choose an arbitrary match.

Row id is unpadded base64url of C1 encoding (§5.1) of
`["optd-row/1", view_id, generation_id, [[field_id, key_value], ...]]`, in KeySpec field order.
It identifies a logical key within this generation, not a physical source-row lifetime. Deleting
and recreating a key can reuse the id. A net key-preserving replacement is an UPDATE here regardless
of which source DML produced it; original statement provenance is not available from a multiset.

Do not use a full-row hash as an immutable id, or equal non-key columns as pairing evidence.
KeySpec and profile are immutable per generation; enabling keyed annotation starts a new generation.

### 4.4 Range semantics, ordering and examples

Every persisted batch is the **net view delta of one refresh range**, not every intermediate
source write. Insert/delete may cancel; insert/update may produce a plain INSERT of the final row.
Across committed batches, history is preserved: a raw stream read concatenates batches rather
than recompacting them into one Snowflake-style net interval. A downstream DeltaSource may sum
weights into a net multiset when that is its declared interface.

Unkeyed batches sort DELETE before INSERT, then use `Row::Ord`: NULL before Int before Str,
numerical integers, UTF-8 byte string order and lexicographic rows. Keyed batches sort keys by the
same value order, then emit each key's BEFORE/DELETE before AFTER/INSERT. Assign contiguous
ordinals after sorting. File order, file names and timestamps are not replay order. Coordinates
are `(generation, batch_seq, ordinal)`; sequence and ordinal overflow fail before writing.

The timestamp is identical on all batch rows. It is preparation time, not source event time,
source commit time or Iceberg commit time. Retrying one candidate preserves it. Recomputing an
uncommitted batch after restart may choose a new timestamp without changing batch identity.

```text
# Unkeyed duplicate insertion; batch id and timestamp omitted only in this example.
id  value  op_type  is_update  weight  row_id  batch_seq  ordinal
7   new    1        false      3      NULL    2          0

# Keyed replacement; K7 abbreviates the specified encoded logical identity.
id  value  op_type  is_update  weight  row_id  batch_seq  ordinal
7   old    0        true      -1      K7      2          0
7   new    1        true       1      K7      2          1
```

### 4.5 Vendor-shaped projections

These are interface mappings, not vendor SQL/connector compatibility guarantees:

| Projection | Mapping |
| --- | --- |
| Snowflake-shaped | `METADATA$ACTION` from op type as INSERT/DELETE; `METADATA$ISUPDATE` from flag; `METADATA$ROW_ID` from logical id |
| MaxCompute-shaped | `__meta_op_type` as tinyint 0/1; `__meta_is_update` cast to tinyint 0/1; `__meta_timestamp` with explicit UTC conversion |

A weighted projection MUST retain `__meta_weight`. A row-expanded projection emits `abs(weight)`
copies with unit weight before it may omit that column. Its event identity adds zero-based
`copy_ordinal` to the physical coordinate. Complete every copy before acknowledging the batch;
size limits fail rather than truncate the export. An adapter requiring non-null immutable row
identity must reject unkeyed input, not manufacture ids. After-image-only exports are outside v1.

## 5. Envelope, identity and checkpoint

### 5.1 Canonical encoding C1

All hashes use SHA-256, formatted as `sha256:` plus 64 lowercase hex digits. Hash a C1 list with
an explicit domain string as first element. Never hash Rust display output or a default hasher.
C1 is recursive; lengths and counts are unsigned 64-bit big-endian:

| Tag byte | Encoding after tag |
| --- | --- |
| 0 | NULL: no bytes |
| 1 | Boolean: one byte 0 or 1 |
| 2 | Signed i64: eight-byte two's-complement big-endian |
| 3 | String: UTF-8 byte length, then exact bytes; no Unicode normalization |
| 4 | Bytes: byte length, then exact bytes |
| 5 | List: element count, then C1 elements |
| 6 | Map: entry count, then C1 string keys and values; keys sorted by UTF-8 bytes |

Map keys are unique. UUID strings use canonical lowercase hyphenated form. Timestamps encode as
i64 microseconds since Unix epoch. Reject invalid UTF-8, unknown tags, noncanonical maps, trailing
bytes and overflow. C1 ordering is an encoding rule, not the row-sort rule in §4.4.

Schema fingerprint input is `["optd-schema/1", payload_representation, profile, key_field_ids,
fields]`, where fields is the complete ordered list of maps `{id, name, type, required}` for
payload and system columns. Type names are the Iceberg types in §4.1. Plan fingerprint input is
`["optd-plan/1", node]`: each node is a map `{kind, params, children, predicates}`; expressions
are maps `{kind, params, children}`. Kinds are the explicit IncNodeType/Expr variant names, params
include every variant field in typed form, and child/predicate order is preserved. Referenced
tables are bound to stable UUID/reference identities, not local TableId. Unknown variants or
unserializable parameters fail; changing a descriptor version requires a new generation.

### 5.2 Required snapshot envelope

Summary keys are `optd.protocol = optd-cdc/1` and `optd.commit`, a UTF-8 JSON object with the fields
below. Parse integer tokens losslessly as i64, never through floating point, and reject duplicate
keys. JSON formatting is not hash input: decode typed values and apply C1.

| Field | Contract |
| --- | --- |
| `view_id`, `generation_id` | UUID strings, also pinned by output table properties |
| `batch_seq` | Positive long, predecessor + 1; bootstrap is 1 |
| `previous_batch_id` | Previous identity, or NULL for bootstrap |
| `batch_id` | Batch hash defined below |
| `plan_fingerprint`, `schema_fingerprint` | Hashes of the descriptors in §5.1 |
| `profile`, `payload_representation` | `unkeyed`/`keyed`, and `value-v1` for the current interpreter |
| `source_from`, `source_to` | Ordered durable checkpoint arrays; from is NULL for bootstrap |
| `kind` | `bootstrap` or `delta`; no external-base bootstrap in M1 |
| `record_count` | Physical CDC row count, not total signed weight |
| `delta_hash` | Canonical row payload digest |
| `prepared_at_us` | Timestamp shared by the batch's rows |
| `writer_id` | Diagnostic owner id; not a fence |

Each summary repeats the complete envelope; application keys are not inherited from a previous
snapshot. Table properties pin `optd.protocol`, `optd.view_id`, `optd.generation_id`,
`optd.plan_fingerprint` and `optd.schema_fingerprint`, and include the canonical plan/schema
descriptors for inspection. Readers verify these bindings, recompute both fingerprints, and
validate every row's batch id, sequence and timestamp against its envelope. Record count and
contiguous ordinals must match; the recomputed delta digest must match before effects are published.

A source record contains `table_id` (stable UUID), `reference` (branch name), `snapshot_id`,
`sequence_number` and `schema_fingerprint`. Snapshot and sequence are longs, or both NULL for
no snapshot. Arrays sort by `(table_id, reference)` and contain no duplicates. Catalog resolution
must match the configured UUID: recreation under the same name is a different source. M1 reads
one source table on Iceberg `main`; multiple tables require a separate consistency protocol.
Epoch indices never identify durable checkpoints.

```text
batch_id = SHA256(C1(["optd-batch/1", view_id, generation_id, batch_seq,
    previous_batch_id, plan_fingerprint, schema_fingerprint, source_from, source_to, kind]))

delta_hash = SHA256(C1(["optd-delta/1",
    [[payload_values, op_type, is_update, weight, row_id], ...]]))
```

Digest rows use §4.4 order. Profile, key fields and representation are covered by the schema hash.
Timestamps, file paths and batch metadata are excluded from delta_hash. Distinct source ranges
with identical deltas have different batch ids. Same identity with a different digest is a
corruption/determinism error, never permission to append again.
For a delta, source_from must equal the predecessor's source_to, the source identity set is
unchanged, and each target must be on the validated forward mainline. At least one source
position advances. Repeating a source endpoint does not manufacture another batch sequence.

### 5.3 Generations, bootstrap and cursors

A generation fixes its plan, schema, representation and profile. Full bootstrap exports every
view row at source_to with its actual positive multiplicity, including an empty bootstrap envelope.
A later source range whose view delta is empty commits a new zero-record envelope. No new source
position means no new batch. No fake payload row is needed for checkpoint-only commits.

A changed definition/schema/profile creates a new generation and table. Missing input history,
rollback or an unexpected reference stops refresh; a rebuild explicitly starts a new generation.
Never load at current state and then substitute an old watermark.

A durable consumer cursor is `(output_table_uuid, generation_id, last_batch_seq, last_batch_id)`.
Zero/NULL denotes before bootstrap. Acknowledge complete batches, including empty ones, checking
predecessor identity and contiguous sequence. Timestamps are never offsets.

## 6. Commit, failure handling and consumption

### 6.1 Producer state machine

1. Acquire lifetime OS-backed exclusive ownership keyed by canonical output table identity.
   M1 permits one cooperating writer on one host; no other writer or maintenance job may append.
2. Read/validate the latest output envelope and LOAD at its exact source_to snapshots. With no
   batch, pin input and prepare full bootstrap. Hold required input snapshots for recovery.
3. Observe fresh metadata, pin target snapshots and prepare immutable delta, candidate state and
   envelope without advancing memory. Every read uses the pinned endpoints.
4. Write Parquet for actual rows. With zero rows, write no data file but still apply fast_append
   with nonempty snapshot properties. Use `ApplyTransactionAction::apply` before transaction
   commit. Disable library retries and verify the effective table retry property.
5. Validate commit/read-back metadata, then acknowledge the candidate. Repeated or stale
   acknowledgement cannot apply it twice. Only then observe another source target.

The output snapshot atomically publishes data and the producer checkpoint: it is the outbox.
It does not provide consumer transactions, writer fencing or multi-table atomicity. Distributed
failover needs a separate commit-time fence; a TTL lease/writer id is insufficient. Source and
driver APIs must return errors and have explicit async runtime ownership; panics/nested block_on
are not a recovery interface.

### 6.2 Resolving commit outcomes

On an unknown result, keep candidate and file ownership and reload authoritative catalog state:

* Same batch id and digest: success; acknowledge it, never delete its referenced files.
* Same id but different digest: stop with a deterministic-replay error.
* Still at the predecessor: retry only once the prior request is definitively not in flight
  and catalog reads are authoritative. An early negative read after timeout is insufficient.
* Unexpected successor, ownership violation or unresolved request: stop and reconcile; never
  blindly rebase/append the candidate.

M1's catalog fixture must persist across process death and provide definitive completion and
read-after-write semantics. E0 selects and qualifies this local adapter. Remote catalogs with
unresolved late commits are outside M1's exactly-once scope. Retries in any catalog/transport layer
must be idempotent or disabled, not just retries in the outer loop.

After crash, recover from the committed envelope before computing. Recomputing an uncommitted
range preserves batch identity and logical digest, although files and preparation time can differ.
Reclaim only unreferenced files after uncertainty is resolved and a conservative age window.

### 6.3 Consumer streams

Each consumer has its own cursor. Peek pins a committed upper batch and returns complete batches
in `(cursor, upper]`, with envelopes. Repeating the pinned read returns that range. An unpinned
later read can see new commits. Queries never advance cursors; empty batches still advance the
available upper boundary.

Consume applies effects and cursor in one destination transaction, or writes a batch-id
idempotency ledger atomically with the effects. Rollback keeps the old cursor. A separate
source-side acknowledge after an unrelated external write cannot claim exactly-once consumption.
CdcLogSource folds signed weights; it cannot treat change-table rows as positive base-table rows.

## 7. Acceptance criteria

These are required implementation tests, not results of this documentation revision:

1. Golden schema and C1/hash fixtures: system ids/types, four kinds, complete NULL images,
   duplicates and identities. Check independently expected rows/bytes/digests, not just codec
   round trips. Verify row-expanded and weighted vendor-shaped adapters preserve multiplicity.
2. Reject invalid codes/signs, zero/minimum weights, overflow, missing columns, schema collisions,
   duplicate/null keys and changed generation/definition before committing anything.
3. Bootstrap with duplicate and empty input; cancellation; different ranges with identical deltas;
   properties-only empty commits with durable advancing checkpoints and no phantom rows.
4. Source/write failure leaves prepared view state unchanged; stale/repeated acknowledge is safe;
   a new source commit appears only in the next observation.
5. Real subprocess kills after file close/before commit and after commit/before acknowledge,
   including bootstrap. A new process folds output to exactly the view at the durable checkpoint.
6. Lost commit response, retryable errors below the driver and delayed completion: no duplicate
   append, referenced-file deletion or blind retry. Assert no hidden transaction retries.
7. A second local writer cannot enter; killing the first releases the lock.
8. Pinned peek repeats, queries do not consume, rollback keeps cursor, successful consumption
   advances effects/cursor together, and independent consumers do not share a bookmark.
9. Keyed updates preserve pair identity/adjacency; key changes remain DELETE/INSERT; complete
   endpoint key invariants are enforced before labeling an update.

Use real catalog/warehouse fixtures and bounded timeouts. After every recovery point compare
weighted log fold with a full evaluation at committed source snapshots, plus hand-calculated
semantic cases so a shared interpreter bug cannot validate itself.

## 8. Rollout, migration and deferred work

No sink or deployed v1 data exists in this repository, so implementation starts with this format.
No dependency/runtime code changes are part of this design revision.

* **M1:** unkeyed, one source/view/output generation, full bootstrap, one batch per snapshot,
  properties-only empty snapshots, stable row coordinates and real recovery acceptance.
  Unpartitioned; pin required input snapshots and all output history.
* **M2:** separate consumer cursors, CdcLogSource, destination transaction/idempotency contract
  and periodic state checkpoints to avoid repeated full-history folds.
* **M3:** keyed profile and paired update annotations, starting a new generation. UPDATE stays
  two complete weighted rows, never a fused partial record.
* **M4:** batching/compaction/retention preserving every logical coordinate and envelope, including
  empty batches. Multi-batch snapshots need an atomic envelope index, not just the last summary.

Compaction preserves logical ids, ordinals and preparation time; new files are no longer evidence
of new CDC. Retention covers the slowest consumer and input snapshots needed for recovery. Future
v3 operation metadata still cannot encode multiplicity: retain weights or explicitly expand copies.
Append-only exports, distributed fencing, in-generation schema evolution and serving-table SQL
semantics require separate designs.

**Open format questions:** none for v1. The first implementation loop qualifies the local catalog,
codec vectors and properties-only commits described below. Remote delayed-commit behavior remains
outside M1. See the [execution record](./execution_plan.md) for observed commands and limits. A
failed future probe requires a documented amendment, not an unannounced weaker guarantee.

## 9. First implementation loop

The owner authorized implementation on 2026-10-28. The initial writer owns the protocol,
engine and storage lane. Use serde/serde_json, SHA-256 and async-trait versions already present
in the workspace lockfile; tempfile is the reproducible test-directory helper. No new package
version is requested. Use Rust's OS-backed File locking rather than adding a lock dependency.

For the M1 fixture, implement a deliberately limited local filesystem catalog: immutable Iceberg
metadata files plus an fsynced atomic metadata-pointer rename, serialized by an OS-held catalog
lock. No background server can finish a request after the writer process dies. Unsupported catalog
operations return errors. It is an M1 local adapter, not a remote/distributed catalog.

Keep relational evaluation synchronous over pinned, already materialized snapshots. Iceberg reads
are async and complete before a pinned source is handed to the evaluator; typed source errors
and historical snapshot loading do not require nested runtimes. Existing synchronous callers can
use the in-memory source. Persisted checkpoints always use Iceberg UUID/snapshot identities.

Each loop records commands, observed test results and outstanding gates in the execution plan.
M1 remains incomplete until real subprocess kills, ambiguous outcomes and the storage path pass.

The pinned creation helper renumbers schema field ids. The local adapter therefore restores the
requested schema in staging and removes the unused provisional schema before first publication.
No data or committed metadata ever uses the provisional CDC layout. The ordinary catalog path
refuses snapshot removal and backward moves, and atomically reserved attempt directories prevent
retry files from overwriting earlier data.

## 10. Streams and maintenance implementation loop

The owner-authorized continuation implements local consumer destinations, keyed annotation,
logical batching, physical compaction and conservative snapshot expiration under
[the accepted streams design](./streams_and_maintenance.md). Single-envelope snapshots remain
readable. `optd.commits` carries a strict JSON `{envelopes, checksum}` index; its checksum is the
C1 fingerprint of `["optd-commit-index/1", envelopes]`. A replace snapshot carries the complete
`optd.envelopes` archive with the same shape and domain `"optd-envelope-archive/1"`. Readers stop
walking parents at that verified archive, allowing earlier physical snapshots to expire without
losing an empty logical batch or a slow consumer's bookmark. All entries still obey §5's chain.

Each local destination atomically stores its weighted materialized relation and complete-batch
cursor. A trusted state checkpoint plus suffix feeds downstream views; physical CDC IO still scans
the full table. The first maintenance implementation retains every CDC record and all files. It
expires older output snapshot metadata only, after a verified current compaction archive. It does
not claim bounded CDC storage, remote transactions or distributed fencing. See
[the runbook](./cdc_loop.md) and [execution evidence](./execution_plan.md#11-implementation-loop-2--2026-10-28).
