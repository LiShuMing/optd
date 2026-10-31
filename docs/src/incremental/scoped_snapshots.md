# Key-scoped Snapshot/Delta Rules

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** accepted · 2026-10-30 (design review; window-output and zero-column conditions incorporated)
**Scope:** indexed equijoins (outer/semi/anti), partitioned windows, DISTINCT
normalization, explicit snapshot/delta algebra and strict planning.

The [Optd plan/execution design](optd_plan_execution.md) supersedes the opaque
snapshot execution mechanism as of 2026-10-30. Snapshot requests expand through
Optd rules into explicit key projections, index preparation, per-port lookups
and typed operator evaluation; the SQL semantics below remain applicable.

## 1. Problem

Executable rules currently retain whole-operator recomputation for nonlinear
operators. Correct query results do not establish bounded maintenance scope.
An extension must declare both its snapshot semantics and the input-key state
that makes its affected snapshots available without filtering complete inputs.

## 2. Constraints and evidence

Preserve typed schemas, signed multiplicities, immutable candidates, independent
full-SQL comparisons and durable/source-aware reconstruction. No dependency,
MSRV or checkpoint-format change. Existing complete-relation caches and source
endpoint scans remain; operator-local scope is not total-refresh complexity.
Implement independently using public SQL/Arrow interfaces, without copying
external source or internal identifiers.

## 3. Options considered

1. Filter complete child relations for changed keys every epoch: reject as indexed
   execution; it retains full input scans.
2. Add bespoke output construction for every join/window function: later work;
   risks duplicating NULL, residual-predicate and window-frame semantics.
3. Maintain typed input buckets, evaluate the original operator on complete
   affected buckets before/after, and express their difference in rule IR: select.

## 4. Decision

Introduce `KeyScope::{JoinKeys, WindowPartitions}` and
`DeltaExpr::ScopedSnapshot { scope, version }`. Each primitive returns a positive
operator-output snapshot over the union of affected keys. Rules emit:

```text
Sum(ScopedSnapshot(scope, After), Negate(ScopedSnapshot(scope, Before)))
```

The two snapshots use exactly the same affected-key union and original typed
operator. Window Before reads cached committed partition output, never a
re-evaluation that could reassign ties. Verification checks scope qualification, output type, local arity,
ordering, and forbids mixed key scopes in one program. The selected expression
declares the derived index; there is no executor-side strategy override. Expose
`KeyedRecompute` separately from native delta and complete-operator recompute.
Custom expressions may read either or both snapshots; evaluate only requested
versions, at most once per node/epoch. Empty affected sets execute neither.

An immutable index stores each input's typed key -> positive relation bucket.
Candidate preparation projects keys only on consolidated positive/negative
changes, updates touched buckets, and shares untouched buckets. Preserve the
bootstrap input subsequence inside each window bucket; canonicalize updated
buckets consistently with maintained unordered child relations. Materialize only
affected buckets for the operator. Index-map/cached-result copying is reported
as remaining overhead, not hidden inside a delta-complexity claim.

### Join scope

Qualify ordinary left/right/full outer and left/right semi/anti equijoins with
one or more equal-key pairs of matching admitted SQL key types. Composite and
NULL keys, null-safe equality and residual predicates use the original join's
semantics. Union keys changed on either side; update both sides atomically.
Every matching pair must share its equijoin bucket, so subtracting local before
from local after handles outer NULL-row retractions, last-match deletion,
semi existence and anti nonexistence without counting duplicate matches as
duplicate semi output. Reject unsupported key equality before index creation.
Existing inner-join algebra remains available; this increment does not claim
its complete-side snapshots are indexed probes.

### Window scope

Qualify a window node only when every window expression has the same nonempty
PARTITION BY expression list and admitted key types. Re-evaluate complete touched
partitions with original ordering/frame/function expressions; leave other
partitions untouched. Moving a row touches both old and new partitions.
No-partition or incompatible-partition windows remain explicitly unsupported in
strict mode. A changed partition can require all its output rows to change.
Cache each window partition's actual committed output. Its Before snapshot is
that exact output, so tie-sensitive ROW_NUMBER/LAG/ROWS cannot manufacture
retractions by re-evaluating old inputs. Apply the executed expression's signed
output delta to these cached buckets, including custom expressions. After uses
the original function/frame semantics. SQL with non-total ordering retains its
SQL-defined tie ambiguity for new results; oracle tests use deterministic
ordering or peer-invariant functions, plus a tie-sensitive cached-Before test.

### DISTINCT and strictness

Normalize any remaining `Distinct::All` into grouping on all output columns
before graph compilation. Existing changed-group state then handles membership,
duplicates, NULLs and final-row deletion. Preserve schema and qualifiers; do not
reinterpret DISTINCT ON as ordinary set membership. Exclude zero-column
Distinct::All: empty GROUP BY would change empty-input existence semantics.
Unsupported equality types
retain explicit compatibility behavior and fail in strict mode.

`IncrementalOptimizer::require_incremental()` disallows any selected
`Maintenance::Recompute` before bootstrap. Compatibility constructors retain
their behavior. Strictness is not a promise that arbitrary SQL is supported;
it prevents unsupported full-node recomputation from masquerading as incremental.
Global aggregates, global sort/top-k, non-equijoins and unqualified windows are
not silently admitted. Existing typed group recomputation remains accepted.

### Work and recovery

Each keyed node reports total input buckets before/after, affected keys, changed
input rows, rows supplied to each requested before/after operator evaluation,
snapshot evaluation count and full-input scans (zero for this indexed path).
Zero means no traversal of the full child to locate affected keys; when all
keys are affected the selected buckets can still contain every input row.
These counters exclude source acquisition, full cache copying/materialization,
checkpoint writes and bootstrap/reconstruction scans; disclose those boundaries.
Indexes are derived from committed child caches on reopen. Candidates carry
the new indexes and become visible only on acknowledgement. Failed planning,
execution or publication leaves the committed index unchanged.

## 5. Failure modes and recovery

Reject invalid/mixed scope, mismatching key types, unsupported equality,
out-of-range ports, ordered-native bypass and missing derived indexes. Existing
row/byte/arithmetic bounds apply before materialization. No fallback after an
index/physical-evaluation failure. Reopen rebuilds indexes under the supplied
registry and independently checks committed output; no new trusted disk state.

## 6. Acceptance criteria

1. Inspect actual snapshot expression trees, strict rejection and malformed scope
   rejection; custom snapshot formulas demonstrably control execution.
2. Differential multi-epoch left/right/full outer and left/right semi/anti joins,
   residual predicates, composite/NULL/null-safe keys, duplicates, simultaneous
   changes, key moves, first/last matches and self joins. Fold signed output
   deltas independently and assert source/candidate nonmutation.
3. At least 100 unrelated keys/partitions; changing one key supplies only affected
   buckets to original operators. Assert row counts, empty/no-op behavior and
   zero operator full-input scans, not elapsed-time inference.
4. Partitioned rank/aggregate/frame windows, NULL partitions, partition moves,
   multiple expressions, unaffected partitions and strict no-partition rejection.
5. DISTINCT plans normalize to grouping and preserve multiplicities across final
   deletes and NULLs. Default and custom strict registries reopen then refresh;
   source-aware and existing process-recovery tests remain green.
6. Final default/feature strict lint/tests, workspace regression, fresh feature
   library check, tools and isolated documentation build; preserve all failures.
   Existing full query-suite evidence is historical until a new executable rerun.

## 7. Rollout / migration

Deliver checked snapshot primitives, capability-qualified rules, derived indexes,
actual interpreter integration, normalization, strict option and tests together.
Keep unrelated concurrent files/index entries intact. No remote publication is
authorized by this design; the previous automatic push rejection remains a
separate explicit-authorization boundary.

## 8. Open questions

No blocker in the admitted profile. Per-row match-count optimization, native
accumulators, indexed inner-join primitives, general key-equality support,
global ordering and source changelogs need separate acceptance evidence.
