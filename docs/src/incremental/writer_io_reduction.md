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

Four independent increments. Each lands as its own commit and is measured with the existing
`cdc_bench` harness against the same warehouse configuration. Commit messages link this page.

### 4.1 Pre-write idempotency from the snapshot summary

Replace the pre-write `read_batches()` with a `latest_commit_index()` that loads table metadata
and decodes `optd.commit` (one envelope) or `optd.commits` (the commit group) from the current
snapshot summary. Use it to decide the already-committed prefix and to return committed envelopes
when every requested batch is present.

Fallbacks and checks:

* verify the decoded envelopes belong to this generation and form a `follows()` chain;
* if the requested prefix cannot be resolved from the summary (for example a compaction archive
  that does not contain it), fall back to the current full read; never append on a mismatch;
* `optd.envelopes` decoding after compaction is accepted as a slow path; it is not on the normal
  refresh path.

### 4.2 Post-commit read-back only for an unknown outcome

When `Transaction::commit` returns a definitive success, return the prepared envelopes directly.
Read back only when the commit result is an error or otherwise unresolved, preserving the rule
that an unknown outcome is resolved before another append.

Keep an `audit` switch (test-only by default) that forces read-back after a successful commit so
the crash and corruption tests can still exercise the independent check.

### 4.3 Validate only the new batches against a trusted predecessor relation

Do not delete the `apply_to` call: it verifies that each batch is exactly the endpoint difference
between its predecessor and successor. Instead, let `emit_many` accept the caller's trusted
predecessor relation (in `CdcLoop::step`, `self.view.contents()`) and replay only the batches
being written from that relation. The check stays, its cost drops to O(delta).

If no trusted relation is supplied, keep the current full replay. A mismatch still fails before
the commit with `InvalidRow`; the durable state and the checkpoint are unchanged.

### 4.4 Read the log once during recovery

Return the validated batches from the sink constructor (or add an internal constructor that
accepts them) so `recover` derives the last checkpoint from `batches.last()` and folds the same
vector for the digest comparison. `CdcLogReader::open` already validates the envelope chain
without scanning data; use the same separation for the producer.

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

## 6. Acceptance criteria

* Before/after numbers from `cdc_bench` on the same warehouse: step latency at 122k and 300k,
  pre-commit/commit split, recover, and the single-read model.
* Regression tests that fail before the change: lost-ack retry, partial overlap, retry after
  compaction, stale trusted relation, and `audit` mode detecting an injected envelope mismatch.
* Existing real process-kill windows (bootstrap, incremental, post-commit) still pass.
* `scripts/agent-check.sh full` passes on the recorded toolchain; 1.95 and 1.96 are reported
  separately.
* No wire-format, identity or schema change; golden vectors remain byte-identical.

## 7. Rollout

Local implementation and measurements first. No push until the owner authorizes it. The accepted
protocol page needs one paragraph only if the `audit` switch becomes an operational feature; the
normal semantics are unchanged.

## 8. Open questions

* Whether the compaction archive decode is cheap enough to stay on the fast path, or should always
  fall back once the archive exceeds a measured size.
* Whether the producer-side folded-state checkpoint is still worth designing after 4.1–4.4.
* Whether `read_since` should also take a scan predicate, now that the validation cost is known to
  dominate the scan cost.