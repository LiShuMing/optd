# ΔFilter

## 1. Semantics and the delta form

Filter is bag-homomorphic, exactly like projection:

```
Δ( σ_p(X) )  =  σ_p( ΔX )
```

Apply the predicate to the changed rows; keep the weights; drop the rows that fail the predicate.

```
Σ( σ_p(X), v )  =  σ_p( Σ(X, v) )
```

## 2. Correctness conditions

* **Determinism of the predicate** — same argument as `projection.md`. A predicate that is not a
  function of the row (time-dependent bounds, random sampling) makes the delta unsound.
* **NULL semantics.** The predicate must be evaluated with the same three-valued logic on the delta
  path as on the rebuild path. This is the classic place where a hand-written delta rule diverges
  from the original plan: `WHERE x > 5` must drop both `FALSE` and `UNKNOWN` rows, and a rule that
  keeps `NOT (x <= 5)` behaves differently for NULL. **The rule should reuse the same predicate
  expression object, not reconstruct an equivalent one.**
* **The predicate must reference only the filtered child's columns.** If it references something
  outside (a correlated outer value, a subquery), the filter is not `σ_p` in the sense used here and
  the delta has to be derived in the context of that correlation — see `subquery.md`.

## 3. State that must be maintained

**None**, as with projection.

## 4. Rule shape in optd

```
Pattern:    Δ( Filter( child ) )
Rewrite:    Filter( Δ( child ) )
```

Note this is a **transposition** rule: it moves the `Δ` marker below the filter and leaves the
filter in place. In a Cascades setting this is the simplest possible rule — a pure re-association
of the marker — which is why filter and projection are the right operators to bring the rule
pipeline up on.

**A subtlety in the rule system:** if the filter is later pushed *into* a scan by an existing
predicate-pushdown rule, the `Δ` marker must not be lost or run before the predicate. Ordering
between the incremental rewrite pass and the ordinary optimization passes has to be pinned down
(see `overview.md` §9 item 5).

## 5. Cost and trade-offs

* Strictly cheaper than recompute when the delta is a small fraction of the input.
* If the predicate is *selective* (drops most rows), the delta side is even cheaper — a good case
  for incremental maintenance.
* If the predicate is *unselective* (keeps nearly everything), the delta is as large as the input,
  and a rebuild may be no worse. The refresh planner's volume heuristic (`overview.md` §8) covers
  this.

## 6. Unsettled

1. Where does the `Δ` marker live relative to predicate pushdown — before or after that pass?
2. Should the rule return the same predicate object (recommended, for NULL-semantics safety) and
   does optd's expression representation make that cheap?
