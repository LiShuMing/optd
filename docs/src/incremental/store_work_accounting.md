# Database storage and recovery work accounting

**Status:** accepted · 2026-10-31; implementation verified for SQL database scope
**Scope:** ROADMAP R01c, host-local SQL database store and SQL session recovery.
Native source ingestion and the standalone engine checkpoint stores are outside this increment.

## 1. Problem

Refresh/DML counters omit durable payload reuse, full changed-table writes, lineage
traversal and view reconstruction. A retry returns the original receipt but still
reads history. That work must remain distinguishable from original SQL execution.

## 2. Constraints and evidence

Keep the current manifest/IPC/CURRENT formats, checksum and lineage validation,
writer lock, publication barriers and fencing. Diagnostics must not introduce IO,
read all history again, or serialize themselves into receipts. Such self-reporting
would make manifest byte counts recursive and confuse retry work with original work.

## 3. Options considered

Descriptor estimates alone cannot establish writes. Sampling OS disk counters
cannot attribute shared cache/IO to a session. Instead count successfully completed
file operations at their existing boundaries, using returned FileEntry lengths and
batch rows. Count reads, writes and reuse separately, without claiming disk traffic.

## 4. Decision

Expose cumulative session-local StoreWork via SqlSession::store_work(), including
completed pointer/manifest reads and writes, checksum-verified file bytes, payload
reads/writes/reuse and their expanded rows/file lengths. Repeated reads count again;
reused bytes are referenced file sizes, never reads or writes. Manifest bytes read
for checksum and parsing are separate logical passes, not measured physical IO.
Counts exclude partial operations that fail before the existing helper returns,
allocator work, directory operations/fsync and comparison CPU work. They remain
available from fenced sessions to diagnose completed work before a failed barrier.
Use checked additions with atomic per-event updates, without global counters.

A fixed RecoveryWork records store work captured during open, successfully rebuilt
views, expanded dependency input rows per view, reconstructed output rows/Arrow
buffer sizes and durable rows compared to reconstructed outputs. Count repeated
view dependencies separately. These measure reconstruction inputs/results, not
internal optimizer/operator CPU visits. An empty database reports measured zeros.
Keep this record distinct from later store work and from CREATE MV bootstrap,
which remains unmeasured rather than fabricated zero in unified reports.

Diagnostics are deliberately not persisted. Reopen resets cumulative store work
to the actual open work; retry adds actual lookup reads without payload writes or
new recovery counts. Historical receipts are unchanged. Consumers take before/after
snapshots for one invocation; they must never add original receipt work on retry.

## 5. Failure modes and recovery

A failed publication may leave completed payload/manifest writes, and may have
published CURRENT. Count completed operations even when a later observer fails;
retain the existing session fence. Failed open returns no usable session or success
report. No diagnostic snapshot implies a successful commit or recovery. Counter
overflow returns an error and never silently wraps; publication errors stay fenced.

## 6. Acceptance criteria

* Check actual written payload/metadata bytes against files in successful attempts.
* Fixed one-row changes and growing unchanged data expose current full writes;
  unchanged table/view payloads count as reuse, and deltas count independently.
* Reopen reads only current payloads but traverses all metadata; history verification
  reads historical payloads; exact retries add metadata reads and zero writes.
* Duplicate/NULL values and view results match independent SQL endpoints after
  reopen. Isolated/concurrent roots never share counters.
* Callback failure before/after CURRENT keeps observable completed writes and fences
  the session; process-death recovery and checksum guards remain green.
* Default/Iceberg format/lint/tests, private rustdoc and mdBook pass.

## 7. Rollout / migration

No dependency, persisted format or receipt schema change. New public read-only
statistics APIs are additive. File lengths and Arrow buffer sizes use different
units; neither is measured allocation traffic, device reads or performance proof.

## 8. Open questions

Partial failed syscall byte accounting, internal bootstrap state work, standalone
checkpoint/source stores and per-invocation SQL/CLI reports remain separate work.

## Design review

Reviewed existing read/write helpers and publication ordering. A successful
write_batch already returns the actual serialized file length after sync and hash;
read_batch verifies the complete payload before decode. Counting these boundaries
preserves existing validation and avoids extra IO. A session-local mutex permits
existing &self history APIs without making SqlSession non-Sync. Counters retain
completed failed-attempt work; they are never durable commit evidence.
