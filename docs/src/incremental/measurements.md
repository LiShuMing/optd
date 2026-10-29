# Measured Costs (E10)

This page records what the implementation actually costs, so that the next
performance change is argued from numbers rather than from the storage format.
It is a measurement log, not a design: the protocol and the semantics are
unchanged by anything here. The design that follows from it is
[Writer I/O Reduction](./writer_io_reduction.md).

Two shapes are measured, both with the same trivial view (`id > 0` over a
single `Long` column):

| name | source base | refresh | output at the measurement point |
|------|------------:|---------|--------------------------------:|
| A | 100,000 rows | 11 × 2,000 rows | 124,000 rows in 13 batches |
| B | 100,000 rows | 10 × 20,000 rows | 320,000 rows in 12 batches |

B has about ten times the rows of A's incremental part at almost the same batch
count, which is what separates a per-row cost from a per-batch one. The batch
counts include the bootstrap batch and the one that `phases` appends.

## How it was taken

`optd-incremental/examples/cdc_bench.rs` is the measuring binary;
`scripts/e10.py` sequences it and appends one JSON record per configuration:

```
export PATH=$HOME/.rustup-new/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH
python3 scripts/e10.py --dir /home/admin/tmp/e10A --label "A: 100k base, 11 x 2k" \
  --record /home/admin/tmp/e10.jsonl
python3 scripts/e10.py --dir /home/admin/tmp/e10B --base-rows 100000 --batch 20000 \
  --steps 10 --label "B: 100k base, 10 x 20k" --record /home/admin/tmp/e10.jsonl
```

Release build, rustc 1.96.0, on dev1 (96 CPUs). Warehouses live outside the
repository. Each record carries the commit, a `dirty` flag, the rustc version,
host, configuration, every per-step refresh measurement, and the summary
objects below.

Page cache is **warm** for every number here: dev1 does not give an
unprivileged process `drop_caches`, so these are fresh-process figures, not
powered-on cold-start ones.

The binary on its own:

```
cdc_bench <warehouse> init [--rows N]
cdc_bench <warehouse> append [--rows N]
cdc_bench <warehouse> info
cdc_bench <warehouse> refresh [--steps K] [--batch N]
cdc_bench <warehouse> phases [--batch N]
cdc_bench <warehouse> recover
cdc_bench <warehouse> parts
cdc_bench <warehouse> read-since [--fraction F]
cdc_bench <warehouse> observe-vs-recompute [--batch N]
```

## Refresh latency

One `CdcLoop::step`, split at the two durability boundaries
(`FilesWritten`, `CatalogCommitted`):

| history | delta | pre-commit | commit | total |
|--------:|------:|-----------:|-------:|------:|
| 102k rows (2 batches) | 2,000 | 1.16 s | 0.067 s | 1.83 s |
| 124k rows (12 batches) | 2,000 | 4.04 s | 0.10 s | 6.36 s |
| 124k rows | **200** | 4.44–5.62 s | 0.08 s | 6.94–8.79 s |
| 320k rows (12 batches) | 20,000 | 7.29 s | 0.07 s | 11.35 s |

The two things to read off this table:

* **Refresh cost follows history, not the delta.** A 200-row delta costs the
  same as a 2,000-row delta at the same history size, and ten times the delta
  rows at 320k history costs roughly twice, not ten times.
* **Commit is not the cost** (tens of milliseconds). Almost everything before
  `FilesWritten` is the writer re-reading and re-validating the log it already
  wrote.

The writer does three full-history passes per step
(`iceberg_sink.rs:495-608`, `emit_many_with_observer`):

1. `read_batches()` before writing — the "is this batch already committed?"
   idempotency check.
2. `apply_to` replay of `existing.chain(remaining)` into a `Multiset` whose
   value is never read afterwards. The call is not dead code: it checks
   `endpoint_records(before, after) == rows` (`cdc.rs:1037-1050`), i.e. that the
   batch really is the difference between its predecessor and successor
   relations.
3. `read_batches()` after the commit — lost-ack read-back, run on the normal
   `Ok` path as well as the error path.

At B's 320k rows the same three passes measure 4.21 s (`scan_s`), 3.48 s
(`apply_to_s`), and the remainder of the 11.35 s step. At A they are 2.29 s,
2.03 s, and 2.3 s.

## The cost of one full pass

Fitting `cost = a·batches + b·rows` to `read_batches()` at both shapes
(A: 2.29 s, 13 batches, 124k rows; B: 4.21 s, 12 batches, 320k rows) gives
**≈ 10.2 µs per output row + ≈ 79 ms per batch**. The per-row term is the row
digest check and canonical-JSON payload handling; the per-batch term is
metadata and envelope-chain work.

`read_since` still physically scans the whole table, but it does not rebuild or
validate the batches the cursor has already consumed:

| history | `read_batches` | `read_since` (90 %) | suffix |
|--------:|---------------:|--------------------:|-------:|
| 124k rows, 13 batches | 2.29 s | 0.38 s | 2 batches / 4k rows |
| 320k rows, 12 batches | 4.21 s | 1.07 s | 2 batches / 40k rows |

Fitting those two points gives **≈ 3.5 µs per row** and no measurable per-batch
term. The physical scan is therefore cheap; the expensive part is validation,
which `read_since` already skips for consumed batches. A `with_filter` on
`__meta_batch_seq` would attack the 3.5 µs/row term, not the 10.2 µs/row one.

## Cold start (`CdcLoop::recover`, non-bootstrap)

| history | recover | sink open | last_checkpoint | read_batches | fold | model: one read |
|--------:|--------:|----------:|----------------:|-------------:|-----:|----------------:|
| 124k rows | 7.42 s | 2.34 s | 2.39 s | 2.29 s | 0.33 s | 2.62 s |
| 320k rows | 14.10 s | 4.26 s | 4.21 s | 4.21 s | 0.70 s | 4.92 s |

`IcebergCdcSink::open` ends with `read_batches()`, `last_checkpoint()` runs a
second one, and the non-bootstrap branch runs a third to fold and compare
against the view. The three passes are identical work; `model: one read` is the
first scan plus folding, i.e. what a recovery that read the log once would pay
— **37–39 % of today's cost**.

The remaining terms are not part of the three scans: source observation
0.16 s / 0.41 s and `View::load_at` 0.09 s / 0.26 s for A / B.

## Observe, Δ evaluate, recompute

| shape | `IcebergSource::observe` | `View::prepare_refresh` | `View::recompute` |
|-------|-------------------------:|------------------------:|------------------:|
| A, 126k rows, +2,000 | 0.146 s | 0.191 s | 0.095 s |
| B, 340k rows, +20,000 | 0.359 s | 0.518 s | 0.278 s |

For this shape the incremental delta is **slower** than recomputing the view.
That is not an argument against the engine: the view is a filter over one
column, the delta path pays row-identity hashing and multiset algebra a filter
does not need, and the value being sold is the durable change log, not the
arithmetic. It does mean no performance claim should be made for the Δ path
from this shape; a join or aggregate view is where Δ can win and has not been
measured here.

## Writer increment 4.3: before and after

[Writer I/O Reduction](./writer_io_reduction.md) §4.3 replaces the full `apply_to`
replay with delta-sized validation against the caller's predecessor relation. The
two arms below run from fresh copies of the same frozen warehouse, with the same
command and the same toolchain (rustc 1.95.0); both report `dirty=False`. The
before arm is a worktree at `9249541`, the after arm is `e98ad04`.

```
scripts/e10.py --dir <copy> --reuse --steps 3 --batch 2000    # shape A
scripts/e10.py --dir <copy> --reuse --steps 3 --batch 20000   # shape B
```

| shape | metric | before | after | change |
|-------|--------|-------:|------:|-------:|
| A, 134k rows / 17 batches | step | 8.84 s | 6.11 s | −31 % |
| | pre-commit | 5.63 s | 3.04 s | −46 % |
| | commit | 0.063 s | 0.11 s | — |
| | recover | 9.86 s | 9.78 s | — |
| B, 420k rows / 16 batches | step | 19.31 s | 13.76 s | −29 % |
| | pre-commit | 12.43 s | 7.01 s | −44 % |
| | commit | 0.10 s | 0.059 s | — |
| | recover | 22.44 s | 22.09 s | — |

Recovery is unchanged because the single-read change is a separate increment.

At shape A the remaining step decomposes as 3.04 s pre-commit + 0.11 s commit +
≈2.96 s post-commit read-back, and the pre-commit part is itself about one full
scan (3.04 s). The full replay is gone from the step; what remains is exactly the
pre-write scan (increment 4.1) and the post-commit read-back (4.2). Those two
numbers are the baseline the next increments are argued against, not a promise:
the projected step after 4.1 and 4.2 is roughly the delta-sized write plus the
commit, and the A/B for each increment will report it rather than assume it.

The measured shape is unkeyed, so it exercises the delta-sized path. Keyed
generations still build a key index per call (`O(n log n)`); that path is not
measured here and no claim is made for it.

## Writer increment 4.1: idempotency from the snapshot summary

§4.1 removes the pre-write `read_batches()` from the normal path: the sink
answers a retry from the current snapshot summary and otherwise appends against
the caller's trusted relation. The before arm is the 4.3 measurement above
(`e98ad04`), the after arm is `0b9df71`; both run from fresh copies of the same
frozen warehouse, with the same command, and both report `dirty=False`.

| shape | metric | before (4.3) | after (4.1) | change |
|-------|--------|-------:|------:|-------:|
| A, 134k rows / 17 batches | step | 6.11 s | 3.37 s | −45 % |
| | pre-commit | 3.04 s | 0.53 s | −83 % |
| | commit | 0.11 s | 0.06 s | — |
| | recover | 9.78 s | 9.53 s | — |
| B, 420k rows / 16 batches | step | 13.76 s | 8.02 s | −42 % |
| | pre-commit | 7.01 s | 1.71 s | −76 % |
| | commit | 0.059 s | 0.06 s | — |
| | recover | 22.09 s | 21.91 s | — |

The pre-write scan is gone from the step. What is left of the step is almost
entirely the post-commit read-back: 3.37 − 0.53 − 0.06 ≈ 2.8 s at A and
8.02 − 1.71 − 0.06 ≈ 6.2 s at B, matching the measured full scan. That is the
target of increment 4.2.

`pre-commit` is the whole step up to `FilesWritten`, so it also contains source
observation and Δ evaluation. The `phases` record splits it at 4.1:

| shape | observe | Δ evaluate | write (by subtraction) | pre-commit |
|-------|--------:|-----------:|-----------------------:|-----------:|
| A, 2,000-row batch | 0.193 s | 0.254 s | 0.297 s | 0.745 s |
| B, 20,000-row batch | 0.480 s | 0.683 s | 0.721 s | 1.884 s |

So the writer's own share after 4.1 is 0.30 s and 0.72 s. The small growth of
`pre-commit` across B's three steps (1.59 → 1.71 s) is source observation and Δ
evaluation over the source table, which also grows by one batch per step — not
writer work. That distinction matters when 4.2 is credited: the writer is
already delta-sized, and any remaining growth in a `refresh` step after 4.2
should be attributed to the source side before it is called a regression.

## Writer increment 4.2: read-back only for an unknown outcome

§4.2 stops reading the log back after a definitive commit and a successful
`CatalogCommitted` observer; the read-back remains for unknown outcomes and is
forced by `set_audit(true)`. The before arm is the 4.1 measurement
(`0b9df71`), the after arm is `4f18b71`; same command, fresh copies, and both
report `dirty=False`.

| shape | metric | before (4.1) | after (4.2) | change |
|-------|--------|-------:|------:|-------:|
| A, 134k rows / 17 batches | step | 3.37 s | 0.63 s | −81 % |
| | pre-commit | 0.53 s | 0.53 s | — |
| | commit | 0.06 s | 0.06 s | — |
| | recover | 9.53 s | 9.65 s | — |
| B, 420k rows / 16 batches | step | 8.02 s | 1.92 s | −76 % |
| | pre-commit | 1.71 s | 1.71 s | — |
| | commit | 0.06 s | 0.09 s | — |
| | recover | 21.91 s | 21.94 s | — |

With increments 4.1–4.3 in place, a writer step no longer scales with the output
history: at 134k rows it is 0.63 s and at 420k rows 1.92 s, and the difference
is the larger batch (20,000 vs 2,000 rows), not the history. The `phases` split
of the writer's own share is 0.21 s and 0.72 s; the rest of the step is source
observation and Δ evaluation (0.36 s / 1.16 s), which follow the *source*
table. A refresh step is therefore no longer the right place to look for a
history-dependent regression; recovery still is, until 4.4.

## Writer increment 4.4: one read during recovery

§4.4 merges the three startup scans into one: the constructor returns the
validated batches, `recover` derives the checkpoint from the last one and folds
the same vector for the digest comparison, and `last_checkpoint` decodes the
commit index from the summary instead of walking every snapshot. The before arm
is the 4.2 measurement (`4f18b71`), the after arm is `d007577`.

| shape | metric | before (4.2) | after (4.4) | change |
|-------|--------|-------:|------:|-------:|
| A, 134k rows / 17 batches | recover | 9.65 s | 3.70 s | −62 % |
| | sink open | 3.01 s | 3.01 s | — |
| | last checkpoint | 3.01 s | 0.001 s | — |
| | fold | 0.37 s | 0.38 s | — |
| B, 420k rows / 16 batches | recover | 21.94 s | 8.76 s | −60 % |
| | sink open | 6.70 s | 6.64 s | — |
| | last checkpoint | 6.66 s | 0.001 s | — |
| | fold | 1.09 s | 1.05 s | — |

Recovery now decomposes as one read (3.01 s / 6.64 s) + fold (0.38 s / 1.05 s)
+ source observation and view load (≈ 0.3 s / 0.9 s), which sums to the measured
recover. **Recovery is still `O(history)`** — it reads the log once for the
digest comparison, by design — but 62 % of the old cost is gone.

## Cumulative: pre-4.3 to 4.4

Same two shapes, same fresh copies, pre arm is the worktree at `9249541` (before
any of the four increments), after arm is `d007577`.

| shape | metric | before | after | change |
|-------|--------|-------:|------:|-------:|
| A, 134k rows / 17 batches | refresh step | 8.84 s | 0.68 s | **−92 %** |
| | recover | 9.86 s | 3.70 s | −62 % |
| B, 420k rows / 16 batches | refresh step | 19.31 s | 1.85 s | **−90 %** |
| | recover | 22.44 s | 8.76 s | −61 % |

Both report `dirty=False` on rustc 1.95.0, and all three `agent-check.sh` modes
pass at `d007577`.

## What this implies, and what it does not

After increments 4.1–4.4 the writer no longer follows the output history: a
2,000-row batch costs 0.68 s and a 20,000-row batch 1.85 s at 134k and 420k rows
respectively, with the writer's share at about 0.2 s and 0.7 s. Recovery reads
the log once instead of three times and costs 3.70 s / 8.76 s, but it is still
`O(history)` because that read is the digest comparison. A producer-side state
checkpoint — the structural change that would make restart independent of
history — is therefore still the remaining option, and the numbers leave it a
much smaller prize (3.7 s / 8.8 s rather than 9.9 s / 22.4 s). It stays
undesigned until someone needs bounded recovery.

The figures below this section describe the state before 4.1–4.3 and are kept
as the baseline the increments are argued against.

None of these figures is a regression gate yet. They are one run per shape on a
shared machine; repeated runs are needed before any threshold is set.