# ΔEmptyRelation

## 1. Semantics and the delta form

The empty relation has no rows, so it never changes:

```
Δ( EmptyRelation )  =  ∅
Σ( EmptyRelation )  =  ∅
```

The rule exists to keep the rewrite pass **total**: a plan that contains an `EmptyRelation` — which
appears legitimately after constant folding, after a contradiction predicate such as `WHERE 1=0`, or
as the empty branch of a `UNION`/`LEFT JOIN` — must not leave an unresolved `Δ` marker behind.

This is why it earns a page despite being trivial: **an unresolved marker is an error**
(`overview.md` §4.2), and every operator in the plan needs a rule, including the ones that are
obviously empty.

## 2. Correctness conditions

* None beyond "the node really is the empty relation". If a rewrite can turn an `EmptyRelation`
  into a non-empty relation (it should not), this rule would be wrong.
* In a bag semantics, `∅` has multiplicity zero and carrying a weight is meaningless; the produced
  delta must be a well-formed empty delta (typed, with the right schema and weight column), not
  simply "no plan".

## 3. State that must be maintained

**None.**

## 4. Rule shape in optd

```
Pattern:    Δ( EmptyRelation )
Rewrite:    EmptyRelation  with the delta schema/weight column
```

The distinction from the ordinary empty relation is only the schema — the delta form carries the
weight column, the snapshot form does not. If the weight is carried as a hidden column, this rule
is a no-op; if it is a separate node type, the rule must produce the delta-typed node.

## 5. Cost and trade-offs

Zero. The only cost is correctness bookkeeping in the rule set.

## 6. Unsettled

1. Is the empty delta represented as an `EmptyRelation` with a weight column, or is there a
   dedicated "empty change set" node? Choosing the former means fewer node types; the latter makes
   "this branch contributes nothing" explicit and cheaper to detect.
