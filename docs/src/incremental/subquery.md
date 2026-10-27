# ΔSubquery (DepJoin / RawDepJoin)

Subqueries are the open problem of this design. This page states the problem precisely, argues for
**unnest-then-maintain** as the only tractable path, and recommends a default of *recompute*.

## 1. Semantics and the delta form

optd represents subqueries with a **dependent join**: `RawDepJoin`/`DepJoin` where the right input
is parameterised by rows of the left input. A scalar, `EXISTS`, or `IN` subquery is a plan whose
result for one outer row depends on that row.

The difficulty is structural:

```
Δ( DepJoin(outer, inner(param)))  ≠  any combination of Δouter and Δinner
```

A change to the **inner** relation affects the subquery's result for **many** outer rows at once,
and *which* outer rows are affected is not known until the subquery is evaluated — i.e. the delta of
the sub-plan is not a function of the delta of a table, but of a *correlated* evaluation. This is
unlike every operator in the previous pages, where the delta of the output depended only on the
delta of the input plus a snapshot of the other input.

There is no clean algebra for "the change to a correlated subquery's per-outer-row result" that is
smaller than re-evaluating it.

## 2. Correctness conditions

If a subquery is maintained at all, the conditions are severe:

* **Unnesting must be semantics-preserving** before any delta rule is applied. Unnesting scalar
  subqueries into outer joins, or `EXISTS`/`IN` into semi/anti joins, has well-known traps:
  * a scalar subquery returning **more than one row** must raise an error, but an unnested outer
    join silently multiplies rows;
  * `NOT IN` with NULLs in the subquery result is **never true**, while `NOT EXISTS` over the same
    data is often true — the unnesting must reproduce that, not "fix" it;
  * duplicate rows in an `IN` subquery must not multiply the outer rows.
* **Cardinality**: even a correct unnesting can turn "one subquery evaluation" into "a join with a
  much larger relation", so the delta may *exceed* the input. The cost argument for incremental
  maintenance disappears.
* **Determinism** — as ever, and correlated subqueries make it more likely that an expression
  secretly depends on evaluation order.

## 3. State that must be maintained

Whatever the **unnested** plan needs — because the recommended design never maintains a subquery
*as* a subquery. If unnesting succeeds, the subquery is gone and the state is that of the resulting
join/aggregate. If unnesting fails, there is no state, because there is no incremental rule.

## 4. Rule shape in optd

**Recommended: no delta rule for `DepJoin`/`RawDepJoin` at all.** Instead:

```
Pattern:    Δ( DepJoin( … ) )
Rewrite:    (no rule)

Planner:    detect an unresolved marker under a dependent join
            → mark that sub-plan as "recompute" and carry the delta through as a full
              replacement, or abort the refresh (policy decision, overview.md §4.2)
```

and rely on the **existing unnesting rules** to remove dependent joins before the incremental pass
runs:

```
1. ordinary rewrite pass:  unnest subqueries (semi/anti/scalar → joins)
2. incremental pass:       apply the delta rules above to the unnested plan
```

This ordering matters: if the incremental pass runs first, it will find a `DepJoin` it cannot
handle. Running unnesting first converts the problem into one the previous pages already solve.

The cost of this choice is that **`DepJoin` is a hole in the rule set**: any plan where unnesting
does not apply cannot be maintained incrementally at all. That is an acceptable, explicit limitation
— much better than a rule that is right most of the time.

## 5. Cost and trade-offs

* Unnesting often makes the plan **worse**: a scalar subquery evaluated once per outer row becomes a
  join, and a correlated `EXISTS` becomes a semi-join that may be far more expensive. So
  "unnest then maintain" can lose to "leave it correlated and recompute".
* For views where the subquery is on a **small, slowly-changing** relation (a lookup table), the
  delta of the subquery's *contents* is cheap, but applying it still requires finding the affected
  outer rows — which is the expensive part, not the subquery delta.

## 6. Unsettled — this is the genuinely open area

1. **Is there a bounded-state incremental form for a correlated subquery at all**, or is the honest
   answer "no, and the interesting question is when recomputation is cheap enough"? My position is
   the latter, but this deserves to be challenged.
2. **Which unnestings are safe to do before the incremental pass** — a concrete, checkable list is
   needed, since the failure modes in §2 are silent (wrong rows, not errors).
3. **Marker placement**: if the incremental pass runs *after* unnesting, how does the planner know
   the unnesting happened (so it does not try to maintain a `DepJoin`)? A plan-level annotation, or
   simply "the unnesting rules are in the same rewrite rule set and run to fixpoint first"?
4. **Multi-level / correlated-by-multiple-levels subqueries** — almost certainly out of scope, but
   worth stating explicitly so the limitation is documented rather than discovered.
