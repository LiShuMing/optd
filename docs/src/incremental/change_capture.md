# Δ Source — Where the Changes Come From

`scan.md` §1 lists this as the design's first open question, and it is the one that decides whether
the rest of the design is buildable at all. This page answers it: **what the change feed must look
like, how to obtain it from four different source shapes, and which of them are usable for which
views.**

The short version:

> Define a **narrow source contract** (a change feed with a position and three capability flags),
> let the engine depend on nothing else, and let the capability flags — not the operator set —
> decide whether a view can be maintained incrementally.

---

## 1. The contract

Everything else in this design assumes a `Δ` can be obtained for a base table. That assumption
belongs in exactly one place:

```
DeltaSource {
    // The changes that happened in the half-open range (from, to].
    changes(from: Position, to: Position) -> ChangeBatch

    // The current end of the feed. Used to pick `to` for a refresh.
    current_position() -> Position

    // What this feed can and cannot express. Drives the decisions in §7.
    capabilities() -> Capabilities

    // The consistent starting point for an initial load (§6).
    snapshot_position() -> Position
}
```

```
ChangeBatch {
    rows:        Δ relation     // weighted, per overview.md §2.1
    position:    Position       // watermark to record on success
    schema:      SchemaVersion  // to validate against the view's expectation
}

Capabilities {
    ordered:        bool        // the feed is totally ordered by its position
    gap_free:       bool        // no missing ranges are possible
    expresses_deletes: bool     // deletions are representable
    before_images:  bool        // updates carry the old row, not just the new one
    cross_table_position: bool  // positions are comparable across tables (§5)
}
```

**The engine should depend on nothing else.** In particular it must not know whether the feed is a
log, a diff of snapshots, or a query over a timestamp column — those are four implementations of the
same trait, with very different capability flags.

The value of putting the flags in the contract is that **the "can this view be maintained?"
decision becomes a mechanical check** (§7) instead of per-storage special-casing scattered through
the rules.

---

## 2. Four ways to actually get the changes

| # | Shape | How | Capabilities | Blind spots |
| --- | --- | --- | --- | --- |
| **A** | **Change log / CDC stream** | The source already emits ordered change records (log, WAL, log table, stream) | ordered, gap-free, deletes, before-images (if it carries them) | Requires the source to have one. Schema of the log must be stable. |
| **B** | **Snapshot diff** | `Δ = S(t₂) ⊖ S(t₁)` — difference of two versions of the table | deletes (as −1 weights) | Needs **both** snapshots. Cost is the table size, not the change size — the cost win of incremental maintenance disappears at the source. Only viable if versioned snapshots are cheap. |
| **C** | **Timestamp / version column** | `WHERE row_version > watermark` | ordered (by the column) | **Misses deletes entirely** (a deleted row cannot report itself). Misses updates that do not touch the column. Breaks if the clock is not monotonic or backfilled. |
| **D** | **Audit / shadow table** | The writer maintains a side table of changes | whatever the writer records | Requires every writer to cooperate; easy to miss a writer. |

**Shape C is the trap.** It is by far the easiest to implement and it looks like CDC, but it cannot
maintain any view over a table that is deleted from — and it fails *silently*, producing a view that
is stale rather than an error. If the engine supports C at all, `expresses_deletes = false` must
propagate to "this view cannot be incrementally maintained, rebuild instead" (§7), not to "assume
nothing is deleted".

**Shape B is the fallback that costs nothing extra to support** if the storage already keeps
versions, and it is the only shape that works with **no cooperation from the source at all**. It is
worth supporting precisely because it makes the engine usable against sources that will never give
you a log.

**Shape A is the one the design should be built around**, with B as the safety net.

---

## 3. What a change record must carry

This is the part that is easy to get subtly wrong, and every omission shows up as a wrong view
rather than an error:

| Field | Why it is needed |
| --- | --- |
| **Operation** (insert / delete / update) or a **weight** | The whole algebra is oriented (`overview.md` §2.1). An insert-only feed cannot express retraction. |
| **After-image** | What the row now is: needed by every branch that joins `Δ` against a snapshot. |
| **Before-image** | Needed whenever the **changed columns participate in matching or grouping** — a join key or group key update is not "−1 old, +1 new" in the *output*; the row leaves one group and enters another. Without the before-image the rule cannot compute that. |
| **Position** | Must be monotonic and usable as a watermark. `join.md` §6.2 notes that a multi-table batch needs this to be *cross-table* comparable (§5). |
| **Schema version** | Between the watermark and now, the base table may have gained or dropped a column. The delta's rows must be projected to the schema version the view was built with, or the rule sees a row shape it does not expect. |

**On before-images:** requiring them for *every* update is expensive (it doubles the record size).
A useful middle ground is to require them **only for columns that participate in the view's
matching/grouping** — which the planner can determine from the view definition. That keeps the
common case (a payload column changed) cheap, and pays for the before-image exactly where
correctness requires it.

---

## 4. The three hard problems

### 4.1 Positions and cross-table consistency — the deepest one

A view joining `A` and `B` needs `ΔA` and `ΔB` **from a consistent point in time**, or the
"before/after" pairing in `join.md` §1 is meaningless: the delta was computed against a state of the
other table that never existed.

* If both tables live in **one transactional system**, use a **global position** (commit timestamp /
  transaction id) so that "as of position `p`" is well-defined for both. `cross_table_position =
  true`, and the engine can require it for multi-table views.
* If they do **not**, then per-table watermarks produce a genuinely **torn** view — it corresponds to
  no consistent snapshot of the world. That is not necessarily fatal (the view converges once the
  lagging table catches up), but it must be an *explicit* documented property, not an accident.

**Design position:** require `cross_table_position = true` for any view over more than one base
table, and support single-table views with weaker positions. This is a clean, checkable rule and it
eliminates the largest class of silent wrongness.

### 4.2 Idempotence and the watermark's transactionality

Three requirements that must be designed together:

* The watermark update must be **atomic with the view update**. If a refresh applies the delta and
  then dies before recording the position, replaying is inevitable; if it records the position first
  and dies, changes are lost permanently. Neither is fixable after the fact — it has to be one
  transaction (or a two-phase commit with a recovery step).
* Replaying a range must be **safe**, which is exactly what weighted deltas give: applying `+1` twice
  is wrong, but the correct replay is to apply `⊖` of the first attempt before re-applying, or to
  make the whole refresh idempotent by construction. **This is the strongest practical argument for
  the weighted encoding** (`overview.md` §2.1).
* Collapsing a range must be safe: if the same row changed 5 times inside one batch, the feed may be
  compacted to the **net** effect — but only if no intermediate state was ever observed by a
  consumer. For a view refresh, only the endpoints matter, so compaction is usually valid and is a
  significant cost win. Worth doing deliberately.

### 4.3 Deletes, and feeds that cannot express them

Covered by the capability flag, but the consequence deserves stating: **a view over a table whose
feed cannot express deletes must be rebuilt, not incrementally maintained.** The engine cannot
compensate for a missing retraction by looking harder. Detect it from `expresses_deletes` and route
to the rebuild path (`overview.md` §8).

---

## 5. A simplification worth taking: you may not need versioned snapshots

`join.md` §4 lists "a way to express `Σ(child, before)`" as a prerequisite, which sounds like it
requires versioned storage. It usually does **not**:

```
Σ(B, before)  =  Σ(B, after)  ⊖  ΔB
```

The *before* snapshot is the current snapshot with the batch's changes subtracted — which is
expressible with **weighted deltas** and ordinary relational operators, no versioned storage
required.

So the prerequisite is weaker than it first appears: **a complete delta plus multiset difference
gives you both snapshots.** The engine still reads the current snapshot (`Σ(B, after)`) — that cost
is unavoidable, as `join.md` §5 notes — but it does not need the storage to keep history.

The one case where this breaks is when the delta is **incomplete** (a feed that only reports
inserts, or that lost a range). That is precisely what the capability flags in §1 are checking.

---

## 6. Two phases: load, then catch up

The first delta is undefined without a starting point, so a view's life is two-phase:

```
1. LOAD:      build the view from a snapshot taken at position P₀
              (snapshot_position() — must be a consistent position, §4.1)
2. CATCH-UP:  repeatedly apply changes in (P₀, P₁], (P₁, P₂], …
              advancing the watermark on each successful refresh
```

Two traps here:

* **The snapshot must be taken *at* `P₀`, not "around" it.** Taking a snapshot while the table is
  being written, then claiming it is at position `P₀`, means changes between the read and `P₀` are
  lost (or double-applied). The source must be able to produce a snapshot and a position together.
* **`P₀` must be established before the snapshot read completes**, not after — otherwise changes
  arriving during the read are missed.

---

## 7. Turning capabilities into a decision

This is the table the engine's planner should consult, and it is the payoff for putting the flags in
the contract:

| View shape | Required capabilities | Why |
| --- | --- | --- |
| Single table, append-only | `ordered` | Nothing is retracted. |
| Single table, mutated | `expresses_deletes` | Retraction is required. |
| Multi-table | `cross_table_position` (+ the above) | §4.1 — otherwise torn. |
| Any view, key columns updated | `before_images` (at least for matching/grouping columns) | §3. |
| Any view, source with possible gaps | `gap_free` | A missing range is unrecoverable (§4.2). |

If any required flag is false → **rebuild**, not a degraded incremental refresh.

This is also why the contract in §1 is worth having even if only one source type is implemented
first: the flags are what make the "incremental or not" decision mechanical, and they stop the
engine from quietly assuming the convenient case.

---

## 8. Unsettled

1. **How is a consistent, cross-table position obtained in practice** when the sources are
   independent? Options: a global commit clock, a coordination service, or accepting torn views with
   documented convergence. This is the decision with the largest downstream effect.
2. **Compaction within a batch** (§4.2) — is it always safe, or does something consume intermediate
   states (a downstream view, an audit log)? If a view is built on another view, compaction in the
   lower one may be observable.
3. **Schema evolution**: does a schema change invalidate the view (rebuild) or is it projected
   through? Projection is better for availability and much harder to get right.
4. **Push vs pull**: the contract in §1 is pull-shaped (`changes(from, to)`). A push-shaped feed
   (the source tells you) needs the same contract with a different driving loop; worth designing the
   interface so the engine does not assume it initiates.
5. **Where does `Position` come from for shape B (snapshot diff)?** Two snapshots do not naturally
   yield a comparable position — the position may have to be *assigned* by the engine (a refresh
   counter), which then makes it per-table by construction and unusable for multi-table views.
