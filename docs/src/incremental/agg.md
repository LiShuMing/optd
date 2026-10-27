# ΔAggregate

Aggregation is the operator where the naive rule is not merely inefficient but **wrong**, and where
the design has to choose between three levels of ambition.

## 1. Semantics and the delta form

The naive rule fails:

```
Δ( γ_{G; f}(X) )   ≠   γ_{G; f}( ΔX )          ← WRONG
```

`γ(ΔX)` aggregates only the changed rows. But a group in the view also contains the **unchanged**
rows that still belong to it, so the changed-only aggregation has forgotten most of the group. The
rule is not an approximation; it produces nonsense (e.g. `SUM` of only the deltas, taken as if it
were the new total).

### The three levels

**Level 1 — invertible aggregate functions, updated in place.**

For functions that have an inverse, the delta can be applied directly to the stored value:

| Function | Retraction | Behaviour |
| --- | --- | --- |
| `SUM(x)` | `value − x·w` | Exact, no extra state. |
| `COUNT(x)` / `COUNT(*)` | `value − w` | Exact. |
| `AVG(x)` | keep `(SUM, COUNT)` and re-derive | Needs two accumulators, not the average itself. |
| `MIN(x)` / `MAX(x)` | **no inverse** | See level 2. |
| `DISTINCT`-aggregates, percentile, median | **no inverse** | See level 3. |

Two things that bite even here:

* **Group emptiness.** If a retraction makes a group's contributing rows vanish, the **group itself
  must be deleted** from the view. `SUM` going to `0` is *not* the same as "group is empty" — a
  group can legitimately sum to `0`. Emptiness must be tracked (usually by the row count), not
  inferred from the value.
* **NULL handling.** `COUNT(x)` ignores NULLs while `COUNT(*)` does not; a delta rule that treats
  them alike is wrong on NULLs.

**Level 2 — non-invertible but maintainable with auxiliary state (`MIN`/`MAX`).**

A common construction stores, per group, the extremum **together with how many rows attain it**:

* insert `x` where `x < min` → new min, count = 1;
* insert `x` where `x == min` → count += 1;
* delete `x` where `x > min` → nothing;
* delete `x` where `x == min` → count −= 1; **if count reaches 0, the new extremum is unknown** and
  must be recovered by scanning the group's remaining rows.

So `MIN`/`MAX` are maintainable **in the common case and degrade to a rescan in the worst case**.
That is an honest statement of their cost, and it should be documented as such rather than
advertised as "incremental".

**Level 3 — not maintainable: recompute the group.**

`DISTINCT` aggregates, exact percentiles/medians, and any aggregate over a fully ordered multiset
have no bounded-state incremental form. The options are to keep the group's complete multiset (which
is to say, to not aggregate), or to recompute the affected groups.

**Recommended landing point:** support **level 1 fully**, **level 2 with the rescan fallback**, and
**level 3 by recomputing the affected groups** (and refusing incremental maintenance if the affected
groups cannot be computed cheaply).

### The merge formulation (what the rule should actually emit)

Rather than updating values in place, the clean formulation is to **store the aggregate's
intermediate state** and merge:

```
stored_state:      per group g, the accumulator S_g of the view
partial_state:     per group g, the accumulator p_g of  ΔX  restricted to g
new_state:         S_g ⊎ p_g            (⊎ = the accumulator's merge function)
view value:        finalise(S_g)        (the aggregate's "finish" step)
```

and symmetrically for the snapshot side. This has two advantages: it composes with multi-phase
aggregation (`partial`/`merge`/`finalise` is already the shape of a distributed aggregate, so the
incremental state is the *same* state the engine already computes), and level 1 falls out as the
case where `⊎` is subtraction.

**Consequence: the view must persist intermediate state, not just the final values.** That is a
storage-format decision, and it must be made before the rule is written — a view that stores only
`AVG(x)` cannot be maintained at level 1.

## 2. Correctness conditions

* **Determinism** of the aggregate arguments (`overview.md` §6.1). `SUM` over a non-deterministic
  expression is not maintainable.
* **Bag semantics**: the aggregate sees multiplicities. `SUM` over a bag with duplicates is not
  `SUM` over the deduplicated set.
* **Group key ↔ NULL**: a NULL group key forms its own group (`GROUP BY` treats NULLs as equal *for
  grouping*, unlike join equality). The merge step's join on the group key must reproduce that,
  which it will **not** if it uses ordinary `=` on NULL-able keys. Either encode the key (so NULL
  becomes a value) or handle NULLs explicitly.
* **`HAVING`** is a filter above the aggregate and transposes normally (`filter.md`) — but it must
  run **after** the merge, not on the partial state, or groups that should survive will be dropped.
* **Aggregate over an empty input**: the merge must produce the correct identity (`COUNT → 0`,
  `SUM → NULL`, not `0` — the SQL semantics of the empty set are not the algebraic identity).

## 3. State that must be maintained

| State | Why |
| --- | --- |
| Per group: the **intermediate accumulator** | So `Δ` can merge into it. Required for level 1 and 2. |
| Per group: **contributing row count** (or equivalent emptiness witness) | To delete groups that become empty, and to distinguish "empty" from "sums to zero". |
| For `MIN`/`MAX`: the extremum + **count at the extremum** | Level 2. |
| For level 3: the group's **multiset** or nothing (recompute) | Choose deliberately. |

## 4. Rule shape in optd

```
Pattern:    Δ( Agg( child ) )          -- only valid at the view root / where the state lives

Rewrite:    Projection(
              Merge( partial_agg( Δ( child ) ),  Scan( view state ) )
            )
```

where `Merge` is an **outer** join on the (encoded) group key — outer, because a group may be
present only in the delta (new group) or only in the stored state (group untouched by this batch;
it must be carried through unchanged). The `Projection` applies `finalise`.

This is a **terminal** rule: it consumes the marker at the aggregate. The marker is still pushed
into the `Δ(child)` inside `partial_agg`, so the rest of the plan continues to be rewritten
normally — the aggregate rule restructures the top of the tree and re-arms the marker below.

Two prerequisites beyond `join.md`'s:

* **A `Merge` that expresses "outer join + combine accumulators"** — this is not expressible with
  optd's current `Join` + `Agg` alone, since no operator combines two accumulator columns.
* **The view's stored state must be readable as a relation** so it can appear as the right child of
  the merge. That is what makes the view self-referential here.

### Milestone 1: the aggregate must be the view root

The implementation merges the partial aggregate into the view's stored contents, which are the
accumulator rows **only when the aggregate is the root**. Anything above the aggregate — a
projection that drops the accumulator column, or a `HAVING` filter — makes the stored rows
something else, and the merge would silently subtract the wrong relation. `View::load` therefore
rejects such plans with `RefreshError::UnsupportedShape` instead of loading a view that will drift
from a recompute.

The consequence is deliberate: **`HAVING` views are rebuild-only in milestone 1.** Maintaining them
needs the aggregate's state to be addressable independently of the view's final rows — i.e. the
state-location question in §6 — plus running the filter *after* the merge. Both are deferred; the
refusal is not a silent degradation, it is a fallback with a name.

## 5. Cost and trade-offs

* Cost is proportional to **the number of affected groups**, not the number of input rows — which is
  the real win, and it is largest when the delta touches few groups (a point update in a
  high-cardinality grouping).
* If the delta touches **most groups** (a schema-wide update, a low-cardinality grouping), the merge
  reads the whole stored state and there is no win. Rebuild may be cheaper.
* `MIN`/`MAX` at level 2 can silently degrade to a rescan (the count-at-extremum reaching zero). The
  cost model should assume the degraded case, or the engine will be optimistic in exactly the
  workload where it is slowest.

## 6. Unsettled

1. **Where does the intermediate state live** — in the view table itself (extra columns), or in a
   side structure? This determines whether the view is human-readable.
2. **Level 2's rescan**: fine-grained (rescan the one group) or coarse (rebuild the view)? Rescanning
   one group needs a way to re-read that group's input rows, which may be more machinery than it is
   worth.
3. **`DISTINCT` inside aggregates** (`COUNT(DISTINCT x)`) is extremely common in practice and is
   level 3. Is "recompute the affected groups" acceptable, or does this need a design of its own?
4. **Multi-phase aggregation**: if the view's original plan already had a partial/final split (for
   distribution), does the incremental rule attach at the partial level or the final level? The
   answer changes where the state lives.
5. **Interaction with `HAVING` and with projections above the aggregate** — the marker must end up
   above the merge, not between partial and final.
