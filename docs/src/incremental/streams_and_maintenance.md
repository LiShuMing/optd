# Local Streams, Keyed Updates and Maintenance

**Status:** accepted · 2026-10-28
**Scope:** implementation decisions for the owner-authorized continuation of M2–M4, preserving
`optd-cdc/1`. One host and cooperating local processes remain the deployment boundary.

## 1. Problem

M1 publishes a recoverable log but has no consumer bookmarks, keyed annotation or file maintenance.
The next loop must add these without splitting a cursor from its destination effects, inventing
row identity, or treating rewritten files as new logical changes. It also removes the reported
workspace lint blockers and runs the wider gates to expose additional integration debt.

## 2. Constraints and evidence

The accepted output protocol specifies full images, C1 identities, complete-batch cursors and an
atomic envelope index when multiple logical batches share a physical snapshot. Current readers
assume append-only snapshots and acquire the producer lock; they must gain a separate read-only
entry point. The local catalog rejects all expiration and backward movement. No remote commit or
distributed fencing capability is introduced. Reuse the existing locked base64 codec for keyed ids;
no package version changes are intended.

## 3. Options considered

* A source-side cursor followed by an external write cannot provide atomic consumption.
* Reconstructing all state on every downstream refresh defeats a chain of incremental views.
* Removing CDC records merely because an Iceberg snapshot expires creates consumer gaps.
* A local destination checkpoint containing both state and cursor, stable keyed images, and
  metadata-only expiration after verified compaction keeps each operation independently testable.

## 4. Decision

### Consumer transaction and source adapter

A read-only `CdcLogReader` can run alongside the producer. Peek pins immutable validated batches
and their upper boundary without writing a bookmark. Each named consumer holds a separate OS lock.
Its local destination is one checksummed state file containing output UUID/generation, consumer id,
last batch sequence/id and the full weighted materialized relation. Atomic fsynced replacement
publishes effects and cursor together; pre-publication failure keeps both old, and post-publication
uncertainty is resolved from that file. Instrumentation is not an external-effects callback.
Repeated consumption is idempotent; stale peeks and changed table/generation are errors.

Every consume creates a state checkpoint. `CdcLogSource` starts at a validated checkpoint and folds
only its suffix to materialize pinned states for synchronous downstream evaluation; requests before
that checkpoint fail explicitly. Independent consumers have independent bookmarks. Arbitrary
external destinations still need their own transaction or idempotency ledger.

### Keyed generation

A nonempty ordered KeySpec names required payload field ids, with no repeated fields. It is part
of the schema fingerprint. Preparing keyed output requires both complete endpoint relations:
validate unit multiplicity and unique non-null keys across all rows before encoding changes.
Stable key replacements produce adjacent BEFORE/AFTER images and the same C1/base64url row id.
Key changes remain unpaired DELETE/INSERT. Storage read-back verifies annotations against actual
endpoint state; enabling keys changes generation/schema bindings as already specified by v1.

### Physical batching, compaction and conservative retention

`emit_many` commits a contiguous sequence of prepared batches in one snapshot. Summary key
`optd.commits` stores all new envelopes plus a domain-separated C1 checksum; `optd.commit` remains
the last envelope for inspection. Empty batches remain in the index. Retries resolve the entire
submitted prefix and may never append already committed logical batches.

Compaction writes the same logical rows to new Parquet files, preserving every row's batch id,
sequence, ordinal and preparation time. An Iceberg replace snapshot atomically publishes the new
manifest set and `optd.envelopes`, a checksummed complete envelope archive. Readers resume walking
at that archive, not at file addition timestamps. A catalog CAS checks the exact metadata pointer;
maintenance and producer share ownership, and readers hold a shared scan lock across IO. Concurrent producer/consumer scans coexist;
maintenance requires the exclusive form of that same lock.

Snapshot retention may expire older output snapshots only once the current snapshot contains that
verified complete archive and all logical CDC rows remain readable. It preserves the current input
checkpoint and refuses source-history expiration through the ordinary catalog path. Consumers
continue to resolve old sequence/id bookmarks from the archive. **This loop does not delete logical
CDC rows or orphan files:** those need a consumer low-watermark, physical reader pins and an age
policy. Conservative metadata retention is not advertised as bounded CDC storage.

### Workspace gate portability

The full gate exposed a benchmark root finder that asserts the origin URL contains the upstream
owner. Locate the Git top-level directory and verify the workspace's known Cargo manifests instead;
forks, nested working directories and checkouts without an origin are valid. Return an IO error for
unrelated repositories or invalid command output, and add temporary Git-fixture regressions. Do not
change the user's remote or suppress the integration test.

## 5. Failure modes and recovery

* Kill before/after consumer state replacement: recover old/new state and cursor as a unit.
* Duplicate/stale consume: no second application and no skipping an unread range.
* Crash before compaction pointer publication: old layout remains authoritative; new files orphaned.
* Crash after publication or expiration: archive and stable coordinates reconstruct the same log.
* Changed schema/identity, malformed index or missing retained data: error before publishing effects.
* Reader contention: maintenance fails/retries rather than deleting files under an active scan.

## 6. Acceptance criteria

Use real Parquet and the existing subprocess harness. Add independent-consumer, repeatable-peek,
rollback, restart and destination-state/cursor tests; compare a downstream view with the base-table
oracle. Exercise key replacement/key change, duplicates/null keys and malformed pairs. Batch empty
and nonempty commits, compact them, expire old snapshots, and compare identities, rows, consumer
results and restart behavior before/after. Run workspace lint and appropriate tests, recording any
remaining failures rather than suppressing them.

## 7. Rollout and migration

Unkeyed single-envelope M1 snapshots remain readable. New readers understand multi-envelope and
archive snapshots; old readers fail closed on maintenance. Keyed logs use a fresh generation.
No maintenance runs implicitly. Expose explicit library/demo actions and retain source history.
No external system, publication or merge is part of this implementation loop.

## 8. Open questions

Logical CDC TTL, orphan-file deletion, key-bucket partition counts, distributed ownership and
arbitrary destination connectors require separate workload/retention decisions. They do not block
consumer correctness, keyed annotation, batching or conservative snapshot retention.
