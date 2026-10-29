# Measured Costs (E10)

This page records what the implementation actually costs, so that the next
performance change is argued from numbers rather than from the storage format.
It is a measurement log, not a design: the protocol and the semantics are
unchanged by anything here.

Two shapes are measured, both with the same trivial view (`id > 0` over a
single `Long` column):

| name | source base | refresh | output history |
|------|------------:|---------|---------------:|
| A | 100,000 rows | 11 × 2,000 rows | 122,000 rows in 12 batches |
| B | 100,000 rows | 10 × 20,000 rows | 300,000 rows in 11 batches |

B has ten times the rows of A's incremental part at almost the same batch
count, which is what separates a per-row cost from a per-batch one.

## How it was taken

`optd-incremental/examples/cdc_bench.rs`, release build, rustc 1.96, on dev1
(96 CPUs). Warehouses live outside the repository under `/home/admin/tmp/e10*`.
Page cache is **warm** for every number below: dev1 does not give an
unprivileged process `drop_caches`, so these are not powered-on cold-start
figures, only fresh-process figures. Every command prints one JSON object; the
driver is a shell loop.

```
cargo build --release --example cdc_bench -p optd-incremental --features iceberg
B=target/release/examples/cdc_bench
$B DIR init --rows 100000
$B DIR refresh --steps 11 --batch 2000
$B DIR phases
$B DIR parts
$B DIR read-since --fraction 0.9
$B DIR recover
$B DIR observe-vs-recompute --batch 2000
```

## Refresh latency

One `CdcLoop::step`, split at the two durability boundaries
(`FilesWritten`, `CatalogCommitted`):

| history | delta | pre-commit | commit | total |
|--------:|------:|-----------:|-------:|------:|
| 102k rows (2 batches) | 2,000 | 1.14 s | 0.046 s | 1.80 s |
| 120k rows (11 batches) | 2,000 | 3.78 s | 0.068 s | 5.88 s |
| 122k rows | 2,000 | 4.16 s | 0.069 s | 6.47 s |
| 300k rows (11 batches) | 20,000 | 5.66–7.26 s | 0.10 s | 8.78–11.25 s |
| 124k rows | **200** | 4.44–5.62 s | 0.08 s | 6.94–8.79 s |

The two things to read off this table:

* **Refresh cost follows history, not the delta.** A 200-row delta costs the
  same as a 2,000-row delta at the same history size, and ten times the delta
  rows at 300k history costs about twice, not ten times.
* **Commit is not the cost** (tens of milliseconds). Almost everything before
  `FilesWritten` is the writer re-reading and re-validating the log it already
  wrote.

The writer does three full-history passes per step (`iceberg_sink.rs`,
`emit_many_with_observer`):

1. `read_batches()` before writing — the "is this batch already committed?"
   idempotency check. Scans and validates every batch.
2. `apply_to` replay of `existing.chain(remaining)` into a `Multiset` whose
   value is **never read** afterwards — a validation pass only.
3. `read_batches()` after the commit — lost-ack read-back, run on the normal
   `Ok` path as well as the error path.

Measured on the same warehouse, taking B's 300k rows: `scan_s` 3.71 s (pass 1),
`apply_to_s` 3.09 s (pass 2), and the post-commit pass is the remainder of the
11.25 s total. On A: 2.06 s, 1.74 s, and 2.1 s respectively.

## The cost of one full pass

Taking `read_batches()` at both shapes and fitting `cost = a·batches + b·rows`:

* A (12 batches, 122k rows): 2.06 s
* B (11 batches, 300k rows): 3.71 s

gives **≈ 9.7 µs per output row + ≈ 73 ms per batch**. The per-batch term is
metadata and envelope-chain work over the batch's snapshot; the per-row term is
the row digest check and canonical-JSON payload handling.

`read_since` still physically scans the whole table, but it does not rebuild or
validate the batches the cursor has already consumed:

| history | `read_batches` | `read_since` (90 %) | suffix |
|--------:|---------------:|--------------------:|-------:|
| 122k rows, 12 batches | 2.10 s | 0.38 s | 2 batches / 4k rows |
| 300k rows, 11 batches | 3.77 s | 1.01 s | 2 batches / 40k rows |

Fitting those two points gives **≈ 3.5 µs per row** and no measurable per-batch
term. So the physical scan is cheap and inexpensive to prune; the expensive part
is validation, which `read_since` already skips for consumed batches. A
`with_filter` on `__meta_batch_seq` would attack the 3.5 µs/row term, not the
9.7 µs/row one.

## Cold start (`CdcLoop::recover`, non-bootstrap)

| history | recover | sink open | last_checkpoint | read_batches | fold | model: one read |
|--------:|--------:|----------:|----------------:|-------------:|-----:|----------------:|
| 122k rows | 6.96 s | 2.09 s | 2.06 s | 2.06 s | 0.27 s | 2.33 s |
| 300k rows | 12.56 s | 3.75 s | 3.70 s | 3.71 s | 0.59 s | 4.31 s |

`IcebergCdcSink::open` ends with `read_batches()`, `last_checkpoint()` runs a
second one, and the non-bootstrap branch runs a third to fold and compare
against the view. The three passes are identical work; `single_read_model_s` is
the first scan plus folding, i.e. what a recovery that read the log once would
pay — **about one third of today's cost**, matching the two-thirds saving
expected before measuring.

The remaining terms are not part of the three scans: source observation
0.15 s / 0.38 s and `View::load_at` 0.10 s / 0.24 s for A / B.

## Observe, Δ evaluate, recompute

At 124k rows, one 2,000-row source append:

| step | time |
|------|-----:|
| `IcebergSource::observe` (materialises both endpoint snapshots) | 0.147 s |
| `View::prepare_refresh` (Δ plan over the snapshot diff) | 0.188 s |
| `View::recompute` (full evaluation of the view plan) | 0.093 s |

For this shape the incremental delta is **slower** than recomputing the view.
That is not an argument against the engine: the view is a filter over one
column, the delta path pays row-identity hashing and multiset algebra that a
filter does not need, and the value being sold is the durable change log, not
the arithmetic. It does mean no performance claim should be made for the Δ
path from this shape; a join or aggregate view is the case where Δ wins and
has not been measured here.

## What this implies, and what it does not

The measured ceiling on any throughput claim today: a refresh is
`O(total history)` because it re-validates the history three times, and a
restart is `O(total history)` because it reads it three times. Both fixes are
local and protocol-preserving:

* **Refresh:** the pre-write check needs the last envelope, not the whole log
  (`optd.commit` is already in the snapshot summary); the unused `apply_to`
  replay can be dropped; the post-commit read-back is only needed when the
  commit outcome is unknown.
* **Restart:** read the log once, take `last()` as the checkpoint, fold the same
  vector.

Neither is implemented as part of this measurement. A producer-side state
checkpoint (the structural fix that makes restart independent of history) stays
where the design puts it: after these local fixes, and only if the numbers still
justify it.

None of these figures is a regression gate yet. They are one run per shape on a
busy shared machine; repeated runs are needed before any threshold.