# ΔLimit

`LIMIT` is where "the delta of the output" stops being monotone in the delta of the input, and it is
the clearest example of an operator that should usually be recomputed.

## 1. Semantics and the delta form

The naive rule is wrong for a reason that is easy to miss:

```
Δ( Limit_k(X) )   ≠   Limit_k( ΔX )          ← WRONG
```

`LIMIT k` **evicts**. Consider `X` with rows `{1,2,3}` and `k = 2`; the view holds `{1,2}`. Now
insert `{0}`. The new view is `{0,1}` — so the change to the view is `+{0}`, `−{2}`: **a pure insert
into the input produced a deletion in the output.**

So:

```
Δ( Limit_k(X) )  =  Limit_k(X')  −  Limit_k(X)
```

and computing it requires both the **old** top-k and enough of the new input to determine the new
top-k. It is not expressible as a function of `ΔX` alone.

### With an order it is a threshold problem; without one it is not defined

`LIMIT` without `ORDER BY` returns an unspecified subset — the *contents* are only determined up to
which rows the engine happens to pick. Incrementally maintaining an unspecified subset is
meaningless (there is nothing to maintain), so **`LIMIT` is only maintainable when paired with a
total order** (`sort.md` §2).

With a total order, this becomes the classical **top-k maintenance** problem:

* keep the current top-k and the **boundary value** (the k-th key);
* a new row whose key beats the boundary enters the result and evicts the current k-th;
* a retraction of a row currently in the result must be followed by promoting the best remaining
  candidate — which requires access to the rows *below* the boundary.

## 2. Correctness conditions

* **A total order** (no ties at the boundary). If the k-th and (k+1)-th rows tie, which one is in
  the result is arbitrary, so the refresh result is non-deterministic → unsound
  (`overview.md` §6.1). **This is the same trap as `sort.md`, and it is worse here** because a tie
  at the boundary changes the *contents*, not just the order.
* **`OFFSET`** makes it strictly harder: the boundary is between `k` and `k+1` rows *of the offset
  window*, and rows above the window can be evicted by changes far outside it.
* **Stable state across refreshes**: the top-k must be persisted. If the view is rebuilt from
  scratch each time the "boundary" is not available and the incremental path has nothing to start
  from.

## 3. State that must be maintained

| State | Why |
| --- | --- |
| The current top-k | It is the view; but it must be *readable as input* to the maintenance step, not just as output. |
| The **boundary key** (k-th value) | To decide, per new row, whether it enters without touching the rest. |
| For retractions: access to the rows just below the boundary | Otherwise a deletion inside the top-k cannot be repaired without re-scanning the input. |

That third row is the crux: **because `LIMIT` evicts, maintaining it needs rows the view does not
contain.** That is a stronger form of the "state larger than the output" problem than `Sort` has.

## 4. Rule shape in optd

```
Pattern:    Δ( Limit( child ) )        with an ordered child

Rewrite:    (no transposition) — terminal
            compute:  new_topk = topk( old_topk ∪ candidate_rows_from( Δ( child ) ) )
            emit:     weighted delta of (new_topk − old_topk)
```

Note the shape is genuinely different from every other operator here: rather than pushing `Δ` down
and combining results on the way up, the rule **consumes the delta as candidate input** and produces
the change to the result directly. It has to read `old_topk` (the view) *and* the delta, and it
needs a way to look **below** the boundary.

Because that "look below the boundary" is not generally expressible against an arbitrary child plan,
the practical rule is:

* **If the child supports ordered access with a boundary seek** (e.g. an index on the sort key),
  top-k maintenance is cheap and worth doing.
* **Otherwise**, mark the sub-plan as recompute.

## 5. Cost and trade-offs

* **Cheap case**: append-only workload, small `k`, a boundary test that rejects most new rows. Each
  batch touches only the rows that beat the boundary. This is the case where `LIMIT` is genuinely
  worth maintaining (leaderboards, "latest N" views).
* **Expensive case**: updates/deletes inside the top-k, or a child with no ordered access. Then
  repairs need a rescan of the input, and the rebuild is simpler *and* faster.
* `OFFSET` generally pushes the cost back toward a rebuild.

## 6. Unsettled

1. **Is this in scope at all?** A defensible position: support `Sort+LIMIT` incrementally only in
   the append-only, index-backed case, and recompute otherwise. That covers the real use cases while
   keeping the rule count small.
2. **How is "the child supports a boundary seek" determined** — a physical property of the child
   plan, or an explicit annotation? This is the same semantic-vs-physical-property question as
   `sort.md` §6.1.
3. **Where does the recompute boundary sit** for `Sort` above `Limit` above `Filter`… — does
   everything below the `Limit` become a full recompute, or only the `Limit` itself?
4. **Interaction with `OFFSET`** — support, or reject the incremental path when `OFFSET` is present?
