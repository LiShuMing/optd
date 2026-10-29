# Writer I/O Reduction

**Status:** proposed · 2026-10-29
**Scope:** removes repeated full-history passes from one CDC refresh and from restart recovery in
`optd-incremental`. It does not change the CDC v1 wire format, batch identity, durability
boundaries, or the accepted output protocol.

## 1. Problem

[Measured costs](./measurements.md) show that one `CdcLoop::step` scales with output history, not
with the delta, and that only tens of milliseconds of a 6–11 s step are catalog commit. On the
same warehouse, at 302,000 output rows in 12 batches, one full pass costs about 4.2 s and a step
performs three of them (spot-check on 2026-10-29; the measurement page lists 3.7 s at 300,000
rows, within run-to-run variance).

The three passes in `emit_many_with_observer` (`iceberg_sink.rs:495-608`) are:

1. a pre-write `read_batches()` used only to detect an already-committed batch;
2. an `apply_to` replay of the whole history whose accumulated relation is never read — the call
   still validates `endpoint_records(before, after) == rows` (`cdc.rs:1037-1050`);
3. a post-commit `read_batches()` that runs even when the catalog returned a definitive success.

Recovery repeats the same work three times: `IcebergCdcSink::open` validates by scanning
(`iceberg_sink.rs:245`), `last_checkpoint()` scans again (`:667-673`), and the non-bootstrap
branch scans a third time to fold and compare (`:738`). The measured single-read model is about
one third of today's recover time.

None of these passes creates new information on the normal path: the batch was built from two
endpoint relations, the commit is exclusive and sequential, and the committed envelope was
already written into the snapshot summary.

## 2. Constraints and evidence

**Verified by measurement** (`measurements.md`, release, rustc 1.96, warm page cache):

* one validated full pass fits ≈ 9.7 µs/row + 73 ms/batch; the scan alone ≈ 3.5 µs/row;
* decoding the commit index from the current snapshot summary takes ≈ 2.4 ms for 12 batches;
* `read_since` still scans physically, so a scan predicate would attack only the cheaper term;
* recover at 122k/300k rows takes 6.96/12.56 s, of which one read plus fold models 2.33/4.31 s.

**Contract constraints** (accepted [CDC output protocol](./iceberg_cdc_output.md)):

* one generation has one output table and one writer; the writer lock is held for the sink lifetime;
* batch identity excludes `prepared_at_us` and the payload digest, so retrying a prepared batch
  keeps one identity;
* the local catalog returns a definitive commit outcome; unknown outcomes must be resolved by
  read-back before another append;
* `optd.commit` and `optd.commits` are stored in each append snapshot's summary; a compaction
  snapshot stores the full envelope archive under `optd.envelopes`.

**Inferred:** the startup validation in `open` plus writer exclusion is sufficient evidence that
the previously committed chain is intact; re-validating it on every step is defence in depth, not
a durability requirement.

**Known limitation:** page cache is warm; these are fresh-process, not powered-on cold-start
figures.

## 3. Options considered

| Option | Benefit | Decision |
| --- | --- | --- |
| Keep every full-history audit | No semantic change | Rejected: refresh cost remains O(history) for O(delta) work |
| Delete the three passes outright | Fastest | Rejected: loses the endpoint-delta and lost-ack checks |
| Producer-side folded-state checkpoint | Bounded recovery | Deferred until the cheap reductions are measured |
| Replace history re-reads with summary lookups and delta-sized validation | Removes most cost, keeps fail-closed checks | Chosen |

## 4. Decision

Four increments, each its own commit and each measured with the existing `cdc_bench` harness
against the same warehouse configuration. Commit messages link this page.

**Landing order: 4.3 → 4.1 → 4.2 → 4.4.** The numbering names the features, not the sequence.
4.1 cannot remove the normal path's pre-write read on its own: that read also supplies the
predecessor relation for the `apply_to` replay, so removing it first would delete the endpoint
check. 4.3 lands first and keeps the read; 4.1 then removes it, replacing the replay with the
trusted relation. A/B therefore reports two points: after 4.3 (one replay removed) and after 4.1
(the scan removed as well).

### 4.1 Pre-write idempotency from the snapshot summary

Replace the pre-write `read_batches()` with a `latest_commit_index()` that loads table metadata
and decodes `optd.commit` (one envelope) or `optd.commits` (the commit group) from the current
snapshot summary. The step loads metadata once and uses the same `Table` for the checks and the
append.

The fast path covers **the latest commit group only**; older groups are not in the summary.

* If `requested.first.batch_seq` is greater than the summary's commit sequence, no requested
  batch can be committed: append all of them (the normal path). The condition is on the **first**
  batch: when the request starts at or before the summary sequence but ends after it, an already
  committed prefix is possible.
* If `requested.last.batch_seq` equals the summary commit sequence, the batch ids and
  `delta_hash` values match, **and** the summary supplies an envelope for every requested batch
  (`optd.commits` covers the slice, or the request has one batch and `optd.commit` matches it),
  return those committed envelopes without reading data.
* If the request straddles the summary sequence (`first <= summary < last`), or the requested
  sequence is older than the summary commit, or the summary cannot supply every requested
  envelope, fall back to the full `read_batches()`; never reconstruct envelopes from partial data
  and never append on a mismatch.

Checks before returning: `optd.protocol`, generation identity, `follows()` continuity within the
slice, and `delta_hash` equality per batch. Row equality is not available on this path, so the
fast path must not be described as equivalent to `same_logical_batch` (which also compares rows,
`iceberg_sink.rs:1065-1069`).

The pre-write validation of the older chain is not repeated on the fast path; `open` performs it
once at startup. `optd.envelopes` is deliberately not decoded on the normal path because its size
grows with history; after compaction, a multi-batch retry therefore falls back unless the
compaction snapshot's single `optd.commit` already answers a one-batch request.

Emit reports which validation path ran (a `CommitStage` variant, or an equivalent test counter) so
tests and measurements can prove the fast path is actually taken rather than silently falling
back on every step.

### 4.2 Post-commit read-back only for an unknown outcome

The fast return requires **both** a definitive `commit` success **and** a successful
`CatalogCommitted` observer. If either returns an error, keep the current read-back resolution:
an observer failure after a durable commit must still be resolved by inspecting the log, exactly
as today.

"`Ok` means durable" is stated for `LocalCatalog` only: it fsyncs every new file, writes the
pointer to a staging file, fsyncs, renames and fsyncs the directory
(`local_catalog.rs:139-165`). A future remote or renamed catalog must restore the read-back unless
it can make the same definitive claim.

Keep an `audit` switch (default off) that forces read-back after a successful commit. It is an
operational verification tool as much as a test hook.

### 4.3 Validate only the new batches against a trusted predecessor relation

Do not delete the `apply_to` call: it verifies that each batch is exactly the endpoint difference
between its predecessor and successor. Instead, add a separate entry point
`emit_many_against(trusted_before, batches, observer)`; `emit_many_with_observer` keeps its
current signature and `DeltaSink::emit` is unchanged. In `CdcLoop::step` the trusted relation is
`self.view.contents()` — the view at the predecessor checkpoint.

The trusted relation is only valid when **nothing from the request is already committed**
(`skip == 0`). With a partial overlap the committed prefix is not part of the trusted relation and
replaying only `remaining` would fail a correct batch. Rule: use the trusted relation only when
`skip == 0` and it was supplied; any partial overlap falls back to the current full replay.
Group commits are atomic, so partial overlap is a defensive path and does not cost normal-path
performance.

If no trusted relation is supplied, keep the current full replay. A mismatch still fails before
the commit with `InvalidRow`; the durable state and the checkpoint are unchanged.

### 4.4 Read the log once during recovery

Return the validated batches from the sink constructor (or add an internal constructor that
accepts them) so `recover` derives the last checkpoint from `batches.last()` and folds the same
vector for the digest comparison. `CdcLogReader::open` already validates the envelope chain
without scanning data; use the same separation for the producer.

`DeltaSink::last_checkpoint` also uses `latest_commit_index()` instead of walking every snapshot,
since it only needs the current commit group. Recovery still performs its one full read for the
digest comparison; the summary lookup does not replace that read.

## 5. Failure modes and recovery

| Scenario | Required result |
| --- | --- |
| Retried emission after a lost acknowledgement | Summary index finds the whole group; returns the stored envelopes; no append |
| Partial overlap with committed batches | Fast path handles the prefix it can resolve; otherwise full read; no double append |
| Compaction happened before the retry | Archive decode resolves the envelopes, or the full read fallback runs; no append on mismatch |
| Definitive commit followed by process death | Prepared envelopes are returned and the caller acknowledges; recovery reads the log as today |
| Commit outcome unknown | Unchanged: read-back resolves success or reports `UnknownCommit` |
| Predecessor relation passed by the caller is stale | `apply_to` endpoint comparison fails before writing |
| Log corrupted between startup and the next step | Not detected on the fast path by design; `audit` mode and startup validation cover tests and operational checks |
| Summary has no `optd.commit` (older table or foreign snapshot) | Full read; no append unless the full read validates |
| Compaction happens between startup and the next step | The next step sees the archive snapshot's `optd.commit`; a one-batch request can use it, a multi-batch retry falls back |
| Observer fails after a durable commit | Read-back resolves the outcome, as today; the fast path is not taken |

## 6. Acceptance criteria

* Before/after numbers from `cdc_bench` on the same warehouse: step latency at 122k and 300k,
  pre-commit/commit split, recover, and the single-read model.
* Regression tests that fail before the change: lost-ack retry, partial overlap, retry after
  compaction, stale trusted relation, and `audit` mode detecting an injected envelope mismatch.
* Path assertions: the normal path takes zero full reads, and each fallback case takes exactly one.
  A test that injects a summary decode failure must show the fallback path was selected.
* Existing real process-kill windows (bootstrap, incremental, post-commit) still pass.
* `scripts/agent-check.sh full` passes on the recorded toolchain; 1.95 and 1.96 are reported
  separately.
* No wire-format, identity or schema change; golden vectors remain byte-identical.

## 7. Rollout

Local implementation and measurements first. No push until the owner authorizes it. The `audit`
switch is documented as an operational verification tool, not only a test hook; the normal
semantics are unchanged.

## 8. Open questions

* Whether the producer-side folded-state checkpoint is still worth designing after 4.1–4.4.
* The `read_since` scan predicate is **deferred**. The scan costs about 3.5 µs/row while validation
  costs about 10.2 µs/row, and the pinned Iceberg release has no row-level filter
  (`row_selection_enabled` defaults to false, so only file/row-group pruning is available).
  Revisit it after 4.1–4.4, when the consumer scan becomes a measured first-order cost.