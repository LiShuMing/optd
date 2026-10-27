# ΔProjection

## 1. Semantics and the delta form

Projection is **bag-homomorphic**: `π` distributes over multiset union.

```
Δ( π_exp(X) )  =  π_exp( ΔX )
```

That is the whole rule. There is nothing to merge, no state to keep: apply the projection
expression to each changed row, and the weights ride along unchanged (`+1` stays `+1`).

```
Σ( π_exp(X), v )  =  π_exp( Σ(X, v) )
```

so the snapshot form is just the ordinary projection.

## 2. Correctness conditions

* **Determinism — the only real precondition.** If `exp` contains a non-deterministic subexpression
  (`now()`, `rand()`, a sequence), then the same input row does not produce the same output row, and
  applying `exp` to the delta is not the same as the change in `π_exp(X)`. The incremental rewrite
  is **unsound**, not merely imprecise. This must be caught by the determinism check
  (`overview.md` §6.1) and the refresh refused.
* **No `DISTINCT`.** `π` here is bag projection. A de-duplicating projection is an aggregate
  (`GROUP BY` the projected columns with no aggregate functions) and must go through `agg.md`,
  because retracting one copy of a row must not remove it while other copies remain.
* **Row width / encoding**: if the delta is encoded as "key + changed columns" (`overview.md` §2.2),
  the projection must be evaluated only on columns the delta carries, or the delta must be widened.
  This is a corner worth deciding before implementation.

## 3. State that must be maintained

**None.** Projection holds no cross-batch state. This makes it the cheapest operator to maintain
and the natural one to validate the whole rule pipeline against first.

## 4. Rule shape in optd

```
Pattern:    Δ( Projection( child ) )
Rewrite:    Projection( Δ( child ) )        -- and Σ( child ) if exp references anything else
```

If `exp` is computed purely from the child's columns, no `Σ` branch is needed. If it references
anything outside the child (a correlated value, a parameter), that reference must be pinned to a
version, otherwise the same determinism problem reappears at the rule level.

## 5. Cost and trade-offs

* Cheaper than recompute whenever the delta is smaller than the input — which is the common case,
  since projection does not amplify rows (no fan-out).
* A projection containing a **volatile but deterministic-looking** expression (e.g. a
  locale-dependent cast) is a trap: it may pass a naive determinism check and still differ between
  the delta path and the rebuild path. Worth a conservative list of "functionally stable" functions
  rather than an allow-list by name.

## 6. Unsettled

1. How strict is the determinism check for expressions — reject on any function not marked stable,
   or maintain an allow-list?
2. Does the delta carry all columns the projection needs, or do we join back to the base table for
   missing ones (which would make `ΔProjection` need a `ΣScan`)?
