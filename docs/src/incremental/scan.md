# ΔScan — Table Scan

## 1. Semantics and the delta form

A scan is the **source** of changes: it is where `Δ` originates, not where it is transformed.

```
ΔScan(T)              = the batch of changes to T between two versions
ΣScan(T, v)           = the full contents of T as of version v
```

Where do the changes come from? Three possible sources, and this choice determines whether
`ΔScan`'s rule is a **leaf** or a **rewrite**:

| Source | ΔScan is… | Notes |
| --- | --- | --- |
| A change log / CDC feed exposed by the table | a leaf scan of the log | Cheapest; only works if the storage provides one. |
| A diff of two snapshots | a rewrite: `ΣScan(T, after) − ΣScan(T, before)` | Needs a cheap way to subtract multisets — i.e. the weighted delta of `overview.md` §2.1. |
| An explicit delta supplied by the caller | a leaf parameter | Puts the burden on whoever triggers the refresh. |

**Recommendation:** support the leaf form first (explicit delta or change log), because it keeps
`ΔScan` from needing `Σ`-minus-`Σ`, which would itself need the union/negation machinery in place
before the scan rule can work.

## 2. Correctness conditions

* **The change feed must be ordered.** Two changes to the same row in one batch must be applied in
  a defined order, or be commutative under the encoding. With weighted deltas, `+1` and `-1` on the
  same row cancel and order does not matter — this is one of the concrete arguments for weighted
  deltas.
* **The feed must be complete for the version range.** A refresh that reads a *partial* change
  range produces a view that is consistent with nothing. The watermark (§4) is what makes this
  checkable.
* **Deletes must be representable.** A change feed that only reports inserts (a common limitation of
  append-only logs) cannot maintain a view over a table that is mutated; the engine must detect
  this and fall back to a rebuild.

## 3. State that must be maintained

The scan itself needs no state beyond what the source provides, but the **engine** must record, per
base table:

* the **applied watermark** — the version up to which changes have been folded into the view;
* enough to detect a **gap** in the feed (a missing range), since a gap means the view is stale in a
  way that a later successful batch will not repair.

## 4. Rule shape in optd

```
Pattern:    Δ( Scan(T) )
Rewrite:    ΔScan(T, from = watermark, to = watermark')
```

This is a **leaf rule** — it produces a scan of the change set and resolves the marker. It is also
the rule that *advances* the watermark, so the watermark update must be tied to the commit of the
refresh, not to the rewrite (see §5 of `overview.md` — the failure contract).

A prerequisite: the plan node must be able to carry a **version range**. optd's `Scan` has no such
field today, so either the node gains one or `ΔScan` is a distinct node type.

## 5. Cost and trade-offs

* Reading a bounded change range is proportional to the **change volume**, not the table size — this
  is the entire win of incremental maintenance, and it is entirely at the scan.
* If the change volume approaches the table size (a bulk load, a mass update), the delta stops being
  cheaper than a rebuild. The refresh planner should compare the two and pick the rebuild
  (`overview.md` §8).

## 6. Unsettled

1. **Is `ΔScan` a new node type, or `Scan` with an extra field?** A separate node keeps the
   existing rules untouched; an extra field avoids a parallel scan family but touches every rule
   that matches `Scan`.
2. **Where is the watermark stored** — in the optimizer's context, in catalog metadata, or on the
   view definition? It must be transactional with the view update.
3. **Gap detection**: is a missing change range detectable from the feed itself, or does the engine
   need a separate low-watermark probe?
