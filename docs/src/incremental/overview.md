# Incremental Engine — Overview

This section designs an **incremental engine** on top of optd: for a view `V` defined over base
tables, maintain `V` as the base tables change, by computing *how much* `V` changes instead of
recomputing it.

The documents here are **design, not implementation**. Each operator gets its own page so the
operator-level decisions can be argued about independently. `overview.md` holds the decisions
that are shared and must be made first.

**Current implementation, 2026-10-29:** the original `optd-incremental` crate
supplies the local CDC, consumer/keyed-state and conservative maintenance loop.
The separate `optd-incremental-sql` crate supplies typed SQL graph maintenance,
atomic named-table epochs, durable output deltas and recovery. Its optional
Iceberg adapter binds explicit source snapshot tuples to the same checkpoint.
See [SQL execution](./sql_execution.md), [typed Iceberg usage](./typed_iceberg_loop.md)
and the [observed query qualification](./typed_iceberg_qualification.md).
The SQL graph's maintenance implementations are compiled by independently
registered [rewrite rules](./rule_rewrite.md), with a typed expression verifier
and [public extension and explanation APIs](./rule_rewrite_usage.md).
This implementation uses native delta propagation, changed-group recomputation
and explicit nonlinear operator recomputation; the general operator designs
below are not evidence that every operator has a native incremental algorithm.

> **Scope note.** These documents describe a general incremental-maintenance design. They are
> deliberately engine-neutral: the deltas, algebra, and correctness conditions below follow from
> relational semantics and are written from scratch. No third-party engine's internal naming,
> code, or identifiers appear here by design, and none should be introduced.

---

## 1. The central question

Recomputing `V` on every change costs the full plan. Incremental maintenance instead computes
`ΔV` — the *change* to the view — and applies it. Everything else is a consequence of two
questions:

1. **How do we represent a change?** (Section 2)
2. **How do we push that change through a plan?** (Section 4)

The whole design reduces to answering these per operator.

---

## 2. Representing changes

### 2.1 Changes are a multiset, not a set of inserts

The naive encoding — "a list of newly inserted rows" — is not sufficient. A change to a base
table can be:

* an **insert** (row becomes present),
* a **delete** (row becomes absent),
* an **update** (delete + insert, or an in-place edit that changes grouping/join keys).

So a change must be an **oriented multiset**: each row carries a *weight* (or an *action* that
means +1 / −1). Two encodings are workable:

| Encoding | Shape | Notes |
| --- | --- | --- |
| **Weighted delta** | `(row, weight: int)` | `+1` = insert, `-1` = delete. Cancellation (`+1` then `-1`) is free. |
| **Action column** | `(row…, action)` | Carries the operation explicitly; keeps room for future ops (e.g. partial update). |

**Design decision to make:** a weighted delta makes joins and unions *much* cleaner (`Δ(A⋈B)` is a
union of products whose weights multiply), and makes the aggregate merge a plain sum. An action
column is friendlier on the surface but needs a mapping to weights before any of the algebra
below works. **Recommendation: carry weights internally, and keep the action column (if used at
all) as a surface encoding that is lowered to weights early.**

### 2.2 Why an update is not "just a delete and an insert"

It usually *is* equivalent for the algebra, but two consequences matter and must be handled:

* **Volume**: an update that touches one non-key column should not ship both full rows. This is a
  partitioning/encoding concern (send the key + the changed columns), not an algebra concern.
* **Determinism**: if an update is decomposed, the *order* of the −1 and +1 must not matter. With
  weighted deltas it does not; with an action column care is needed when the same row appears
  twice in one batch.

### 2.3 One batch, one delta

Each refresh processes a **set of changes between two states** (a version range, an LSN range, a
commit range). The delta does not have to be a single snapshot-consistent set if the algebra is
insensitive to it, but it does have to be **idempotent per version**: reprocessing the same range
must not double-apply. That requires recording the applied watermark (§6).

---

## 3. Two operator families

The single most important structural decision: **every operator that participates in an
incremental plan gets two forms.**

| Family | Meaning | Example |
| --- | --- | --- |
| **Δ (change)** | "the change to this sub-plan" | `ΔScan(T)` = the batch of changes to `T` |
| **Σ (snapshot)** | "the *whole* value of this sub-plan, as of a version" | `ΣScan(T)` = `T` as of version `v` |

Why the snapshot family is unavoidable: the delta form of most operators needs the *unchanged
side in full*. For an inner join,

```
Δ(A ⋈ B)  =  ΔA ⋈ Σ(B, before)   ∪   Σ(A, after) ⋈ ΔB
```

Both terms reference a **snapshot** of the other side, not its delta. There is no formulation of
the join delta that uses only deltas on both sides — the unmatched rows have to meet *something*
complete. Any engine that only implements Δ operators will get wrong answers on joins.

**Consequence for optd:** `Σ` can usually be *the ordinary operator* (a plain scan of the current
table) — the snapshot family is often "the operator as it already exists, evaluated at a version".
It is the Δ family that has to be added. But this must be an explicit decision per operator,
because it is not always true (see §5 for `Sort`/`Limit`, where the snapshot form is the same but
the Δ form is not maintainable).

---

## 4. Getting the change from the root to the leaves

### 4.1 Rule-driven transposition

The change starts as a marker at the **root** of the view plan (the view *is* the thing whose
delta we want) and is **pushed down** until it reaches the scans. This is exactly the shape of a
Cascades transformation rule, which is why optd is a good host:

```
Pattern:   Δ( Op( child… ) )        -- a change marker wrapping an operator
Rewrite:   Op'( Δ(child)…, Σ(child)… )   -- operator-specific delta form
```

Each operator contributes its own rule, and the rules are applied repeatedly until every change
marker sits directly on a scan (or is eliminated by an operator that consumes it, §4.3).

**This is the part optd already gives us.** optd's rule engine matches on a pattern and produces a
replacement expression; a delta rule is a rule whose pattern has the change marker at the root.
No new rule-engine machinery is needed — only new operators, new node types, and the rules.

### 4.2 Everything must be resolved, or the refresh must fail

If, after the rewrite pass, a change marker remains somewhere it cannot be resolved — an operator
with no delta rule, or a rule whose preconditions failed — the engine **must abort the refresh**.

The failure mode to avoid is precise: a partially rewritten plan will *still execute*, and it will
produce the **changes-only** result of the part that was rewritten while silently dropping the part
that was not. That is not an error the user sees; it is wrong data in the view. **Emitting an
error is the only safe behaviour**, and it should name the operator that could not be resolved.

### 4.3 "Terminal" operators that consume the marker

Some operators do not push the marker down; they **consume** it because the delta cannot be
expressed below them:

* **Aggregate** — `Δ(Agg(X))` is not `Agg(ΔX)`: an aggregation over only the changed rows forgets
  the rows that are still contributing to the existing groups. The standard resolution is to
  compute the *intermediate* aggregate of the delta, then **merge it with the stored aggregate
  state** of the view and re-finalise. See `agg.md`.
* **Limit / Sort** — the delta of a top-k is not a top-k of the delta. See `limit.md`, `sort.md`.

Terminal operators are where incremental maintenance earns its complexity, and where it most often
should *not* be attempted.

---

## 5. Which operators can be maintained incrementally

A first-pass classification, refined in each operator's page. "Cost" is relative to recomputing
that sub-plan.

| Operator | Δ form exists? | Cost vs recompute | Notes |
| --- | --- | --- | --- |
| **Scan** | Yes — it is the source of changes | Cheaper | Requires the source to expose a change feed at a version. |
| **Projection** | Yes — push Δ below | Cheaper | **Only if the projection is deterministic.** |
| **Filter** | Yes — push Δ below | Cheaper | Same determinism caveat. |
| **Join** (inner / cross) | Yes — §3 formula | Usually cheaper | Needs a Σ side per branch. |
| **Join** (outer / full) | Partly | Often *not* cheaper | NULL-padded rows make deletes expensive; see `join.md`. |
| **Aggregate** | Yes, via merge-with-state | Usually cheaper | Only for *invertible* aggregate functions, or when the merge can absorb retractions (see `agg.md`). |
| **Union** | Yes — Δ distributes | Cheaper | **Not currently an optd operator** — see §7. |
| **Sort** | Weak — order is not a delta | Usually *worse* | A stable order costs more to maintain than to recompute. |
| **Limit** | Weak | Usually *worse* | Top-k changes non-monotonically; see `limit.md`. |
| **EmptyRelation** | Trivial | N/A | |
| **Subquery / DepJoin** | Hard | Case-dependent | Delta of a correlated sub-plan is the open problem; see `subquery.md`. |

The honest summary: **`Scan`, `Projection`, `Filter`, `Join(inner)`, `Aggregate` and `Union` are
the maintainable core; `Sort` and `Limit` usually are not; outer joins and subqueries are
case-dependent and should default to "recompute this sub-plan".**

---

## 6. Preconditions — the three that actually bite

Incremental maintenance is *unsound* unless these hold. They are not implementation details; they
are the difference between a correct view and a silently wrong one.

### 6.1 Determinism

**Every expression used in the view definition that participates in an incremental rewrite must be
deterministic.** If the view contains `now()`, `rand()`, a sequence, an unstable sort key, or a
row-number without a total order, then the same input can produce different outputs, and "the
change to the output" is not a function of "the change to the input".

* Where it bites: `Projection`, `Filter`, `Aggregate` (non-deterministic aggregate arguments),
  `Sort`/`Limit` (ties broken non-deterministically).
* What to do: **check determinism of the view definition up front and refuse the incremental
  refresh** if it fails, falling back to a full rebuild. This check is cheap and prevents the worst
  class of bug (a view that is wrong only sometimes).

### 6.2 Multiset semantics and aggregate invertibility

The relational model here is **bag (multiset)**, not set. Consequences:

* Duplicate rows are meaningful; a change cannot be applied as a set-union.
* `π`, `σ`, `⋈` are all bag-homomorphic (they distribute over multiset union) — this is *why* they
  are easy.
* Aggregation is **not** bag-homomorphic. `SUM`/`COUNT` are invertible (subtract the retraction);
  `MIN`/`MAX` are not (retracting the current minimum requires knowing the *next* minimum, i.e.
  keeping a full multiset or an indexed structure). See `agg.md` for the split.

### 6.3 Keys and uniqueness

* **Dedup / `DISTINCT`**: retracting one copy of a row must not remove it if other copies remain.
  Needs multiplicity tracking.
* **Outer joins**: a delete on the preserved side can *remove* a NULL-padded row (if it was the
  only match) or *convert* a matched row into a NULL-padded one. Neither is a simple ±1 on the
  join output unless the state records match counts.
* **Updates to a join/group key**: an update is not "−1 old, +1 new" *in the output* when the key
  participates in matching — it is a change of *which group* the row belongs to.

---

## 7. What optd is missing today

optd's logical operator set is:

```
Projection, Filter, Scan, Join(JoinType), RawDepJoin(SubqueryType), DepJoin,
Sort, Agg, EmptyRelation, Limit
```

Against the design above:

1. **There is no `Union`.** This is the biggest gap, because multiset union is *the* connective of
   delta algebra: `Δ(A⋈B)` is a union of two branches, and `Δ(union)` is a union of deltas. Without
   a union operator the join and union delta rules have nothing to produce. **Adding a multiset
   `Union` is a prerequisite for the incremental engine.**
2. **There is no explicit change/weight representation.** The weighted delta of §2.1 needs a carrier
   — either a new node type or an extra hidden column. This is a representation decision that
   affects every rule, so it should be settled before writing any rule.
3. **There is no version/watermark concept.** `Σ(·, v)` needs a "as of version `v`" notion on scans,
   and the engine needs to record the applied watermark (§2.3).
4. **No determinism checker.** §6.1 needs one; it is a small, self-contained pass over the view
   definition's expressions.

Items 1–2 are blocking; 3–4 are required for correctness but can be added incrementally.

---

## 8. Refresh strategy: when *not* to go incremental

An incremental engine should refuse work it cannot do well. The decision per refresh:

| Condition | Action |
| --- | --- |
| View definition is non-deterministic | **Full rebuild.** |
| Plan contains an operator with no Δ rule | **Full rebuild** (or abort, per policy). |
| Change volume is a large fraction of the base table | **Full rebuild** — the delta is not smaller than the input. |
| `Sort`/`Limit` is on the critical path to the view root | **Full rebuild** of that sub-plan. |
| Otherwise | Incremental. |

This table is the design's escape hatch and should be treated as a first-class feature, not a
fallback: an engine that always tries to go incremental will be slower *and* harder to trust.

---

## 9. Open questions to settle before implementation

1. **Weighted delta vs action column** — §2.1. Affects every rule; decide first.
2. **How is the version/watermark carried and checked?** Per base table or per source system?
3. **Where does the change feed come from** for `Scan` — is it a change log, a diff of two
   snapshots, or an explicit `ΔScan` input? This determines whether `Scan`'s Δ rule is a leaf or a
   rewrite.
4. **Multi-statement / multi-table refresh**: one delta per table per refresh, or one combined
   delta? The join formula in §3 assumes deltas arrive per side independently — a combined batch
   needs care with the `ΔA ⋈ ΔA` interaction.
5. **Interaction with optd's adaptive/partial-exploration machinery** — an incremental plan is a
   *different* plan shape, not a better cost for the same shape; does the re-optimization loop need
   to know that?
6. **What is the failure contract?** If a refresh aborts mid-way, is the view left at the previous
   watermark (recommended) or partially updated?

---

## 10. Operator pages

Each page follows the same skeleton so they can be compared side by side:

1. semantics and the delta form,
2. correctness conditions,
3. state that must be maintained,
4. the rule shape in optd (pattern → replacement),
5. cost and trade-offs,
6. what is unsettled.

| Operator | Page |
| --- | --- |
| Scan | [scan.md](./scan.md) |
| Projection | [projection.md](./projection.md) |
| Filter | [filter.md](./filter.md) |
| Join | [join.md](./join.md) |
| Aggregate | [agg.md](./agg.md) |
| Sort | [sort.md](./sort.md) |
| Limit | [limit.md](./limit.md) |
| EmptyRelation | [empty_relation.md](./empty_relation.md) |
| Subquery / DepJoin | [subquery.md](./subquery.md) |
