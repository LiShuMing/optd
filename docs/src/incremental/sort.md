# ΔSort

Sort is the first operator where the honest answer is **"usually do not maintain this"**. The page
exists mostly to make that answer precise, and to separate the two things people mean by "the delta
of a sort".

## 1. Semantics and the delta form

A sort has two aspects, and they behave completely differently:

| Aspect | Delta behaviour |
| --- | --- |
| **Contents** (the multiset of rows) | Sort does not change it: `multiset(Sort(X)) = multiset(X)`. So `Δ` passes straight through: `Δ(Sort(X)) = ΔX`. |
| **Order** (the sequence) | Not a function of `ΔX`. One inserted row can change the position of every row after it. There is no `Δ` for an ordering. |

So the practical rule splits on **whether the order is semantically required**:

* **Order not observable** (the sort exists for some downstream purpose that does not require it —
  a sort that no `LIMIT` consumes and that the view does not declare): **drop the sort** on the
  delta path. `Δ(Sort(X)) → ΔX`.
* **Order observable** (feeds a `LIMIT`, or the view declares an order): the sort is
  **terminal** — it has to be recomputed, because producing the new order needs the whole ordered
  relation, which is the entire result.

Note the asymmetry with `Projection`/`Filter`: those *transpose* the marker and stay in place.
A meaningful sort does not transpose; it either disappears (when order is unobservable) or blocks
the marker and forces a recompute.

## 2. Correctness conditions

* **A total order is mandatory** if the order is observable. If the sort key has ties, the output
  order is unspecified, and:
  * the *view contents* are still well-defined (ties produce the same rows),
  * but anything that consumes the order (`LIMIT`) is **non-deterministic**, which makes the whole
    incremental refresh unsound (`overview.md` §6.1), not just imprecise.
  This is the most common way an incremental top-n silently returns different rows each refresh.
* **Determinism of the sort key expression** — same argument as projection/filter.
* **`ORDER BY` inside the view definition**: if the view *declares* an order, the order is part of
  the contract and cannot be dropped on the delta path even if nothing downstream reads it.

## 3. State that must be maintained

* If the order is unobservable and the sort is dropped: **none**.
* If the order is observable: the state is **the entire ordered result** — which is to say there is
  no state *smaller* than the answer. This is the formal reason incremental maintenance does not
  help here: any correct rule must read the whole sorted relation anyway.

## 4. Rule shape in optd

```
Pattern:    Δ( Sort( child ) )                       -- order unobservable
Rewrite:    Δ( child )

Pattern:    Δ( Sort( child ) )                       -- order observable
Rewrite:    (no rule) → marker unresolved → refresh aborts, or the sub-plan is marked
            "recompute" and the delta passes through as a full replacement
```

The second case is the important design decision: **should "cannot maintain" be an abort or a
degrade-to-rebuild?** Aborting is safer (`overview.md` §4.2); degrading is friendlier. Either way it
must be an *explicit* decision taken by the planner, recorded in the plan, and not a silent
fallback.

## 5. Cost and trade-offs

* Maintaining an order costs more than recomputing it: a correct rule must merge the delta into the
  ordered relation, which is `O(n)` on the whole result per batch, versus `O(n log n)` once per
  rebuild. Per-batch `O(n)` beats a rebuild only if rebuilds are rare — i.e. it is a function of
  refresh frequency, not of the delta size.
* Dropping an unobservable sort is free and strictly beneficial: it is the one clear win on this
  page.

## 6. Unsettled

1. **How does the engine know whether the order is observable?** This is a property that has to be
   derived (a sort is observable iff it feeds a `LIMIT`, or the view declares order) and propagated
   down the plan — optd has a physical-properties mechanism, but "is the order semantically
   required" is a *semantic* property, not a physical one, and needs its own treatment.
2. If the planner degrades to rebuild above a sort, does the delta still need to be computed for the
   part of the plan *below* the sort, or is the whole sub-plan replaced? Replacing is simpler and
   probably correct; it also means the change marker terminates at the sort.
3. Ties: does the engine require the view definition to be tie-free (reject otherwise), or does it
   add a synthetic tie-breaker? Adding one silently changes the declared result.
