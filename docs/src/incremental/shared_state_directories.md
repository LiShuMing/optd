# Shared state directories

**Status:** accepted · 2026-10-31; implementation pending
**Scope:** ROADMAP R02c; SQL table-leaf, grouped aggregate, accumulator, join input
and window input/output directories. Full relation Bags/caches remain R02d.

## 1. Problem

R01a and R02b measure complete ordered-directory copies despite shared bucket
payloads. Engine candidate preparation clones every installed directory, physical
state preparation clones again, and each SQL table write clones its leaf directory.
One changed key therefore copies pointers and encoded keys for unrelated groups.

## 2. Constraints and evidence

Preserve deterministic ordered iteration, exact byte-key identity and predecessor
lookup for table leaves. Candidate clone, mutation and discard cannot alter old
snapshots or other candidates. No new dependency or persisted/wire format. Retain
cached actual window Before outputs and every current scalar/resource guard.
Source-name maps, graph-vector headers, full Bags and payload work are separate.

## 3. Options considered

Arc around a BTreeMap merely defers a complete clone to the first write. Chained
overlays avoid clones but require unbounded lookup depth or periodic full merges.
Fixed hash shards still copy an unbounded touched shard and lose ordered lookup.
Use an internal persistent balanced binary directory with shared immutable branches.
AVL balance gives logarithmic maximum path length without randomized priorities,
external dependencies or an unbounded overlay chain.

## 4. Decision

Introduce `StateDirectory<V>` with `Vec<u8>` keys and cloned values (all production
values are Arc payloads). Nodes hold key, value, two Arc branches and cached
structural height/length invariants bounded by addressable owned nodes. A snapshot clone only shares its root. Insert,
replace and remove copy shared nodes on the modified search path using `Arc::try_unwrap`;
an explicit clone counts even if another owner concurrently drops its root;
rotations may additionally copy shared sibling roots. Ordered iteration and
predecessor lookup read shared nodes. Empty/missing-key removal must avoid copies.

Keep the implementation internal and limited to these needed operations. No public
persistent-container API, generic comparison policy, locking or new dependency.
Iterators borrow the immutable revision. Structural lengths/heights are bounded by
owned memory and AVL invariants; mutation cannot introduce cycles.

Count an old directory entry only when its owning shared node is actually cloned,
including rotation siblings and delete successors. Root/Arc clones, new entries,
shared payloads and moved exclusive nodes count zero. A cloned directory starts a
fresh candidate-local copy counter. Record counters after physical updates finish,
including window-output updates; remove pre-mutation inferred full-copy accounting.
Bootstrap counts are not refresh counts. SQL table diagnostics use the same actual
node-copy boundary. Keep existing counter names and documented units.

Integrate aggregate, accumulator, join/window and table-leaf directories. Grouping
of a delta may still use ordinary BTreeMaps: these maps contain only changed keys.
No relation-cache or checkpoint optimizations are inferred from this change.

## 5. Failure modes and recovery

Arc copy-on-write keeps prior roots immutable. All row/state validation and fallible
physical execution remain candidate-local; failed and stale candidates are discarded
under the existing publication protocol. The derived directory is rebuilt on reopen
from the existing format. Memory allocation follows existing standard collection
behavior; no new fallible persistence phase or acknowledgement is introduced.

## 6. Acceptance criteria

* Compare insert, replacement, missing/existing deletion, predecessor and sorted
  iteration against an independent BTreeMap after deterministic mixed operations.
  Assert exact heights, subtree lengths and AVL balance after each operation.
* Retain multiple old roots through rotations, deletions and all-node removal;
  prove their ordered contents and unaffected payload pointers remain unchanged.
* At fixed changed keys/bucket/output size, grow unrelated keys; copied entries
  must remain bounded by AVL path work, not total key count. Empty/canceled updates
  copy zero directory entries. Count rotation work at the actual clone boundary.
* Default/Iceberg SQL semantics, concurrent/failed/stale candidates, window peers,
  duplicate/NULL/key moves, retry/reopen and process-death recovery pass.
* Default/Iceberg fmt/lint/tests, private rustdoc, mdBook and work-audit tool tests
  pass. Selected current-binary TPC SQL loops retain strict-plan and result checks.
  These checks do not qualify controlled performance or all TPC queries.

## 7. Rollout / migration

Commit this design before implementation. Replace one internal map family at a
time, preserving callers and diagnostics. R02c stays in progress until every named
family and acceptance gate is verified. No serialized state or migration changes.

## 8. Review

Source review covers engine candidate clones and executor physical updates, including
`CompleteGroups` and window `CompleteIndex` output mutation. Recording before these
updates would report zero incorrectly. Table-leaf lookup needs inclusive predecessor
and fallback to the first leaf; grouping maps only need point lookup/iteration.
The review also rejects a fixed-shard design because it cannot meet the stated
untouched-data scaling criterion. This records local design review, not independent
review or implementation evidence. Remaining complete Bag copies, Arrow caches,
selector scans and checkpoint writes stay visible in their existing domains.
