# Changed-group Aggregate Maintenance

**Status:** accepted · 2026-10-29
**Scope:** operator-local maintenance of ordinary SQL `GROUP BY` in
`optd-incremental-sql`; no CDC v1 change, native accumulator claim, global
aggregate optimization, grouping sets, distributed execution or performance promise.

## 1. Problem

The typed SQL graph propagates changes through linear operators and inner joins,
but a dirty Aggregate currently executes over its complete maintained child.
Nested aggregation is correct, yet changing one customer can reread every other
customer's rows at that aggregate boundary. The next independently verifiable
increment is to recompute only groups whose input multiset changed.

This is **changed-group recomputation**, not a native arithmetic update to SUM or
COUNT. DataFusion still evaluates each affected group's aggregate. The graph
continues to report all unsupported aggregate shapes as `OperatorRecompute`.

## 2. Constraints and evidence

* The accepted typed execution design is [Typed SQL Incremental
  Execution](./sql_execution.md). Its prepare/publish/acknowledge and exact local
  epoch recovery boundaries remain unchanged.
* `engine.rs` owns DataFusion planning, qualified schemas, ordered-cache
  invalidation and graph strategy selection. `bag.rs` owns signed typed row
  multiplicities and materialization limits. The new module owns a group index
  and its candidate update; it does not mutate either existing component.
* A SQL group key uses SQL equality. The existing Bag's bit-preserving scalar
  identity is **not** a general SQL grouping comparator: NaNs, signed float zeros,
  dictionaries and collations require their own equality decisions.
* A changed-group index is derived state. Recovery rebuilds it from the committed
  typed child relation and aggregate output. The checkpoint format does not need
  a new persisted group-key encoding in this increment.
* Current graph preparation copies full state, and local checkpoints write full
  source/output snapshots. Reducing aggregate input work does not establish a
  delta-sized complete refresh, reduced checkpoint I/O or a benchmark speedup.

## 3. Options considered

1. Implement custom COUNT/SUM/AVG accumulators immediately. This eventually
   avoids rereading large individual groups, but first requires matching pinned
   DataFusion precision widening, NULL/empty semantics, overflow and AVG
   finalization. Defer to a separate design and acceptance increment.
2. Filter the full aggregate child for changed keys on every refresh. This reduces
   aggregate work while retaining a full child scan. Reject as the index's
   implementation: do not label a full scan as an indexed lookup.
3. Maintain an input multiset and one cached aggregate output per SQL group.
   Re-evaluate only touched groups with the existing DataFusion aggregate. Select.

## 4. Decision

### 4.1 Qualification and equality

The first increment admits only ordinary, nonempty grouping-expression lists.
`GROUPING SETS`, `ROLLUP`, `CUBE`, grouping-id expansion and global aggregation
retain `OperatorRecompute`. All expressions must already have passed the graph's
immutable-expression and subquery checks.

Evaluate the complete grouping expression list with a DataFusion Projection over
the child relation. It must return exactly one typed key row per input row, in
the same order. The same projection evaluates positive and negative input
changes; hand-written expression evaluation is forbidden.

Encode those projected rows with Arrow `RowConverter` and a fixed list of key
types. A `GroupKey` owns its encoded bytes and compares those bytes. Rebuilding
the converter for a later batch must produce the same encoding for admitted
types; dictionary interning is therefore excluded. NULLs of a fixed key type
encode as one key and belong to the same group. Ordinary GROUP BY emits one row
per remaining key, including an all-NULL key.

The initial key whitelist is Null, Boolean, signed/unsigned integers, Decimal128/256
with fixed precision/scale, Date32/64 and binary UTF-8 strings. Timestamp keys may
be added only with a fixed admitted unit/timezone and a round-trip/equality test.
Float16/32/64, dictionary, nested, extension, interval, duration and every
unrecognized type retain `OperatorRecompute`. Float aggregate inputs are also excluded from the initial admission below;
floats are never silently normalized as group keys.

String keys require the default binary DataFusion equality and no collation
annotation or custom collation configuration. Nonbinary or unprovable collation
semantics retain `OperatorRecompute`. Byte identity intentionally distinguishes
`A` from `a` and composed from decomposed Unicode under binary SQL equality.
No Unicode normalization or case-folding is introduced. The caller supplies an
explicit binary-key policy only for the fixed default context; the module does
not infer collations by looking at scalar values.

The first aggregate whitelist is the built-in COUNT, SUM, AVG, MIN and MAX,
including DISTINCT variants that DataFusion accepts. Arguments must be nonfloat
and order-insensitive; custom UDAFs and order-sensitive signatures retain the
fallback. The engine checks the actual built-in implementation identity rather
than admitting a custom function merely by its name. Floating aggregate
arithmetic needs independent oracle cases and an accepted comparison policy
before enabling those signatures.

### 4.2 State and module interface

Add a private `group_state.rs`. The integration owner adds the module declaration
and engine strategy; one writer implements this module and its direct tests.
No additional dependency or public storage protocol is required.

```rust
pub(crate) struct KeyPolicy {
    pub binary_strings: bool,
}

pub(crate) struct GroupState { /* schemas and key -> Arc<GroupBucket> */ }
pub(crate) struct GroupUpdate { /* immutable candidate and touched keys */ }

impl GroupState {
    // None means a known unsupported key type/policy, not an execution failure.
    pub(crate) fn try_new(
        input: &RecordBatch,
        projected_keys: &RecordBatch,
        aggregate_output: &RecordBatch,
        policy: KeyPolicy,
    ) -> Result<Option<Self>>;

    pub(crate) fn prepare(
        &self,
        positive_rows: &RecordBatch,
        positive_keys: &RecordBatch,
        negative_rows: &RecordBatch,
        negative_keys: &RecordBatch,
    ) -> Result<GroupUpdate>;

    pub(crate) fn total_groups(&self) -> usize;
}

impl GroupUpdate {
    pub(crate) fn input_after(&self) -> Result<RecordBatch>;
    pub(crate) fn touched_groups(&self) -> usize;
    pub(crate) fn aggregate_input_rows(&self) -> usize;
    pub(crate) fn finish(
        self,
        recomputed_output: &RecordBatch,
    ) -> Result<(GroupState, Bag)>;
}

```

`GroupBucket` owns the positive typed child multiset and its one cached output
row. `GroupState` records input/key/output schemas; ordinary aggregate output
group-key columns are its leading key columns. Initialization validates the
complete correspondence: one output row per input key, no missing or unknown
keys, compatible key types, and no duplicated output group. It does not accept
an arbitrary prefix of output columns without validating that correspondence.

Untouched buckets are shared immutably with `Arc`; touched buckets are copied
into the candidate. Cloning the ordered key map still costs O(number of groups)
in this implementation, which is disclosed independently of aggregate work.
The candidate is not installed until the owning SQL view acknowledges its epoch.

The engine reuses its existing `eval_inputs` evaluation boundary with the touched
input batch. It replaces exactly the aggregate child while retaining qualified
schemas and expressions, and retains the graph node's logical schema contract.
If DataFusion produces an incompatible physical type, execution rejects that
shape rather than silently casting it; propagating physical schema changes
through the graph remains separate work. The module
does not introduce another SQL planner or aggregate evaluation helper.

### 4.3 Update algorithm

1. Check input/key batch lengths and schemas before looking up any group.
   Consolidate positive and negative rows per encoded group key. The input bags
   come from an already consolidated signed child delta, but the group module
   must still tolerate repeated keys and duplicates.
2. A touched key is one whose complete input multiset changes. Cancellation
   within the atomic batch does not touch or evaluate that group. For each
   touched key, update its candidate input multiset with checked arithmetic;
   negative resulting multiplicities are errors. Original buckets remain intact.
3. Materialize only surviving touched groups' candidate input rows, using the
   existing row/byte limits and original child schema. This does not scan or fold
   untouched buckets. Evaluate the original aggregate once over their union.
   Grouping semantics separate those groups; no per-group SQL text is generated.
4. `finish` validates the returned output's schema and exact key set. Each
   surviving touched group must have exactly one output row. A deleted group
   must have no output row. Unknown, duplicate or omitted keys are errors.
5. Compute `new_touched_output - old_touched_output` as a typed signed Bag and
   replace only touched buckets in the candidate. A group whose SUM becomes zero
   remains if input rows remain; COUNT(expr)=0 and SUM(expr)=NULL are delegated
   to DataFusion instead of conflating them with an absent group.
6. The engine applies that delta to its cached complete aggregate relation and
   propagates it normally. HAVING, projection and nested aggregates continue to
   be independent graph nodes. The prepared next GroupState travels with the
   candidate and becomes visible only on acknowledge.

If every touched group is deleted, use a correctly typed empty aggregate result
or execute the grouped aggregate over the typed empty affected input. A grouped
aggregate returns zero rows here; the one-row empty-global-aggregate rule belongs
to the unchanged global-aggregate fallback.

### 4.4 Honest work reporting and later native state

Add `ChangedGroupRecompute` as a distinct strategy. For each such node record
touched groups, total groups, changed input rows, actual aggregate input rows and
produced aggregate rows. A one-customer update that rereads that customer's 1,000
rows reports 1,000 aggregate input rows, not one native update. Global aggregates
remain `OperatorRecompute` and do not claim changed-group speedups.

A later native-accumulator design can reuse this key/index ownership boundary.
COUNT(*) needs group row count; COUNT(expr) and SUM(expr) need nonnull counts;
SUM must distinguish all-NULL from numeric zero. AVG needs typed sum plus nonnull
count and pinned DataFusion result precision/division semantics. DISTINCT needs
per-value multiplicities; retractable MIN/MAX needs a value multiset or index.
Those changes require their own design, differential tests and recovery evidence.

## 5. Failure modes and recovery

Unsupported shape/key type/collation/function selects the existing
`OperatorRecompute` **before** building a partial index. An execution error,
schema mismatch, overflow, absent-row deletion, unexpected output key or resource
limit aborts the prepared epoch; it must not silently switch algorithms midway
through an error or publish a partial group index.

Restart reconstructs each admitted index in postorder from the committed source
vector and the full child/output caches rebuilt by the SQL graph. The durable
output comparison and complete committed delta history remain independently
validated. No persisted group-key bytes are trusted across a DataFusion/Arrow
version change. No cleanup or new background worker is introduced.

## 6. Acceptance criteria

1. Key tests compare group partitions with independent DataFusion GROUP BY for
   NULL, binary UTF-8 case/Unicode, booleans, signed/unsigned integers and exact
   Decimal precision/scale. Float +0/-0 and multiple NaN payloads, dictionaries
   and nonbinary/unprovable collations explicitly select fallback.
2. Direct state tests cover duplicate rows, multirow changes to one group,
   changing a grouping key, cancellation, deleting the final group row,
   SUM=0 versus empty, all-NULL SUM, unknown output keys and candidate nonmutation.
3. At least 100 groups exist; one batch changes only one group. The aggregate
   callback receives only that group's surviving rows, and untouched groups'
   complete input/output buckets are reused. Count this work directly rather
   than inferring it from elapsed time.
4. SQL tests compare multiple epochs with an independent full physical SQL plan
   for nested GROUP BY/HAVING, duplicate and NULL keys, Decimal SUM/AVG, DISTINCT
   and final-row deletes. Names/types/values are exact; legitimate cross-plan
   nullability annotations remain separate from each plan's schema stability.
5. Existing native join/order/scalar-subquery tests remain green. Actual
   subprocess death before/after epoch publication is followed by index rebuild,
   next refresh and committed-delta folding against independent full SQL output.
6. Fresh build, scoped warnings-denied lint, formatting and the relevant default
   SQL/store suites pass. Record commands/toolchain/results. No performance,
   Iceberg multi-table or native accumulator claim follows from these tests.

## 7. Rollout / migration

Review this proposal and settle the private interface first. Implement the index
and direct tests in one lane; integrate plan admission, group-expression
projection, strategy/counters and candidate ownership in the engine lane.
Qualify an explicit aggregate/type subset before enabling its strategy, then run
the complete benchmark manifests with strategy and nonempty/changed evidence.
The existing fallback remains available for every unqualified shape.

## 8. Decisions and later questions

* Initial aggregate admission is built-in COUNT/SUM/AVG/MIN/MAX with nonfloat,
  order-insensitive arguments and no UDAF. DISTINCT remains admitted under those
  same restrictions. Unsupported signatures use the existing fallback.
* Timestamp keys are deferred. A later accepted change needs fixed unit/timezone
  admission plus equality and round-trip tests.
* The caller explicitly selects default binary string equality. Unknown or
  nonbinary equality returns fallback.
* An immutable persistent map is deferred until measured cloning cost justifies
  another dependency. The first implementation shares individual buckets using
  `Arc` and honestly records O(number of groups) key-map cloning.
