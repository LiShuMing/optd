# ΔJoin

The join is where incremental maintenance stops being a marker-transposition exercise and starts
needing real state. It is also the operator that forces the **snapshot** family (`overview.md` §3)
into existence.

## 1. Semantics and the delta form

### Inner / cross join

Write `A` for the left input as of the *previous* version, `A' = A ∪ ΔA` for the current one, and
likewise `B`, `B'`, `ΔB`. Then

```
A' ⋈ B'  =  (A ∪ ΔA) ⋈ (B ∪ ΔB)
         =  A⋈B  ∪  A⋈ΔB  ∪  ΔA⋈B  ∪  ΔA⋈ΔB
```

and the delta of the join is what is *new*:

```
Δ(A ⋈ B)  =  A'⋈B' − A⋈B
          =  Σ(A, before) ⋈ ΔB   ∪   ΔA ⋈ Σ(B, after)
```

Expanding the second form gives `A⋈ΔB ∪ ΔA⋈B ∪ ΔA⋈ΔB` — note the `ΔA⋈ΔB` term **is** covered, by
virtue of `Σ(B, after)` containing `ΔB`. So the two-branch form is complete; there is no third
branch to add.

The symmetric equivalent,

```
Δ(A ⋈ B)  =  ΔA ⋈ Σ(B, before)  ∪  Σ(A, after) ⋈ ΔB
```

is the *same* multiset (both expand to `A⋈ΔB ∪ ΔA⋈B ∪ ΔA⋈ΔB`). **Pick one and document it**; mixing
them within one engine is how sign errors get introduced.

**The load-bearing observation:** each branch pairs a **delta** with a **snapshot of the other
side**. There is no formulation using deltas on both sides — the changed rows of `A` must meet the
complete `B`. This is why `overview.md` §3 insists on two operator families.

Weights multiply: a `+1` row of `ΔA` meeting a `-1` row of `Σ(B)` contributes `-1` to the join's
delta. That falls out of the weighted encoding for free — a strong argument for it
(`overview.md` §2.1).

### Outer joins — a different problem

For a left outer join, the delta is **not** the inner-join delta plus NULL padding, because a change
on the *inner* side can retroactively change rows already in the view:

* `B` row deleted where it was `A`'s only match → `A`'s row must **change from matched to
  NULL-padded** (a `-1/+1` pair, not a simple insert).
* `B` row inserted where `A` had no match → `A`'s NULL-padded row must be **retracted** and replaced
  by the matched row.
* `A` row deleted → if it was NULL-padded, a simple `-1`; if it was matched, `-1` per matching `B`
  row — which requires knowing **how many** matches it had.

Full outer joins combine both directions.

## 2. Correctness conditions

* **Inner / cross join**: no precondition beyond determinism of the join condition. This is the
  only join type that is straightforwardly maintainable.
* **Outer join**: maintainable **only if the state of §3 is kept**. Without per-row match counts,
  the rules above cannot be written down, and an outer-join delta rule that ignores them produces a
  view that is wrong exactly in the cases that matter (the ones involving unmatched rows).
* **Join-key updates**: if a column that participates in the join condition is updated, the change
  is *not* "−1 old row, +1 new row" in the output — the row leaves one match set and joins another.
  This is correct automatically with weighted deltas applied to the two branches, but it is a
  useful test case; it is the first thing to check when validating the rule.
* **NULL in the join condition**: as with filter (`filter.md` §2), the condition must be evaluated
  with the same three-valued logic on the delta path as on the rebuild path. In particular
  `NULL = NULL` does not match, so NULL join keys must not accidentally pair up.

## 3. State that must be maintained

| Join type | Extra state |
| --- | --- |
| Inner, cross | **None.** Only the view's own contents are needed (as `Σ` of each side). |
| Left / right outer | **Per preserved-side row: the count of matches** on the other side (or enough to derive "was matched before / is matched now"). |
| Full outer | Match counts on **both** sides. |

The inner-join case needing no extra state is a genuine property, not an approximation: `Σ(B, ·)` is
just `B` as it stands, which the storage already has.

## 4. Rule shape in optd

```
Pattern:    Δ( Join( left, right ) )            with inner/cross join type

Rewrite:    Union(
              Join( Δ( left),  Σ( right, before) ),
              Join( Σ( left, after), Δ( right) )
            )
```

Three things this rule needs that optd does not have today:

1. **A multiset `Union`** — **not currently an optd operator** (`overview.md` §7 item 1). The rule
   literally cannot produce its result without one. This is the single blocking prerequisite.
2. **A way to express `Σ(child, ·)`** — a version-annotated snapshot of a sub-plan.
3. **Sub-plan duplication**: each branch needs its own copy of the child sub-trees (with fresh
   column references). In a Cascades memo, "duplicate the subtree" is not free — the two branches
   share structure but must not share *column identity*, or the union's schema will be inconsistent.

The marker is **not** consumed here: it is pushed into each branch's `Δ` child, and those are
resolved by the child operators' own rules. The join rule is a **transposition** rule, not a
terminal one.

Precondition check: the rule must verify the join type. If it is an outer join and the engine has no
outer-join support, the rule **must not fire**, and the unresolved marker must cause the refresh to
abort (`overview.md` §4.2) — not silently degrade to an inner join.

## 5. Cost and trade-offs

* Cost is roughly `|ΔA|·|B|/…` versus `|A|·|B|` for a rebuild — i.e. the win is the ratio of the
  delta to the base table, on both sides.
* **Each branch reads the full other side.** For a large dimension table joined to a small changing
  fact table, the budget is dominated by re-reading the dimension. If the dimension is
  hash-partitioned and cached, this is cheap; otherwise an "incremental" join can read more than a
  rebuild.
* The two branches independently re-read both sides, so the rule doubles the snapshot traffic
  compared to a single join. Worth measuring before assuming the delta wins.

## 6. Unsettled

1. **Which of the two equivalent forms** to standardise on (§1) — and whether to emit both as
   alternatives for the cost model to choose between. They are equal in result but not in cost if
   one side is much larger.
2. **Multi-table batches**: if the same refresh produces `ΔA` and `ΔB` together (a transaction
   touching two joined tables), does the "before/after" pairing still hold? The formula assumes each
   branch sees a consistent `Σ`; a combined batch may need the snapshot pinned to the *batch start*
   version, not per-table watermarks. This is the most likely source of subtle wrongness.
3. **Outer joins**: worth supporting at all, given the match-count state? A defensible position is
   to support inner/cross join incrementally and **recompute** any sub-plan containing an outer
   join — cheaper to build and much easier to trust.
4. **Rule interaction with join reordering**: the delta branches produce new join trees that the
   ordinary join-order rules will want to reorder. Do the incremental branches go back through the
   optimizer, and does re-optimising them risk re-introducing the join being maintained?
