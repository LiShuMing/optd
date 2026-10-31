# Reference-counted DISTINCT aggregates

**Status:** accepted · 2026-10-31; local source/design review, implementation pending
**Scope:** R03b single-argument builtin COUNT/SUM/AVG DISTINCT over already qualified
exact types, mixed with ordinary aggregates and MIN/MAX. No dependency, persisted
format, grouping-set, floating arithmetic or multi-argument COUNT contract change.

## 1. Problem

Global DISTINCT aggregates that remain after SQL optimization have no scoped group
fallback. Mixed ordinary/DISTINCT aggregates can retain such a node, including
TPC-DS Q28's COUNT DISTINCT with Decimal AVG. Removing DISTINCT or recounting all
surviving values on every change would violate semantics or fixed-change work.

## 2. Constraints and evidence

Retain the existing accumulator rewrite, Optd state declaration and operation,
exact full-row signed child deltas, group cardinality, cached Before output,
candidate isolation and atomic SQL publication. A SQL optimizer may remove redundant
MIN/MAX DISTINCT or split distinct aggregates into multiple ordinary nodes; test
both original-node eligibility and the actual selected executable plan.

R03a supplies counted persistent ordered argument values. Their counts represent
all occurrences; native DISTINCT aggregate inputs must represent only membership.
Do not reuse a native non-retractable COUNT DISTINCT accumulator or emit one
contribution per duplicate input row. Keep original float SUM/AVG and ordering
boundaries; qualification of new numeric types belongs to R03e.

## 3. Options considered

Recomputing a changed group's complete DISTINCT values remains a valid older
fallback, but scans hot groups. A whole-map clone isolates candidates but copies
untouched values. Counted persistent directories with native accumulator updates
only on membership zero crossings meet both deletion and isolation requirements.

## 4. Decision

Admit canonical pinned builtin COUNT/SUM/AVG DISTINCT with exactly one argument,
no FILTER, ORDER BY or explicit null treatment, and a qualified exact argument
encoding. Preserve existing result/arithmetic checks, including Decimal-only AVG.
MIN/MAX DISTINCT can use their existing counted extrema representation because
multiplicity never changes their result. Never admit a same-name user aggregate.

For SQL-optimizer DISTINCT lowering and normalized SELECT DISTINCT, an aggregate
with nonempty qualified grouping keys and no functions uses the existing group
entry row count as its membership refcount. Admit this form to the accumulator
rule instead of changed-group input recomputation. It emits a key only at group
creation/removal, preserves real NULL keys and never invents a global empty row.
The degenerate empty-key/empty-function node remains outside this qualification.

Maintain an exclusive DISTINCT cell containing the shared value directory and
ordinary retractable native accumulator state. Native expressions are built with
DISTINCT disabled only inside this proven membership-maintenance cell. First
consolidate positive/negative value changes; unchanged counts cause no native
work. For each nonzero net change, validate old/new checked counts and update its
persistent path. Forward exactly one positive value for 0->positive, one negative
value for positive->0, and no value for positive->positive. NULL is absent from
single-argument COUNT/SUM/AVG DISTINCT input. Retract leaving values before adding
entering values using the existing native arithmetic/type implementation.

Refactor only the R03a counted directory responsibilities required to return value
membership transitions and share exact encoding/count checks. Keep extrema endpoint
selection and DISTINCT native states separate. Neither path enumerates historical
values during refresh. A grouped zero-row state can be removed without visiting
its directory; global zero rows retain native COUNT=0, SUM/AVG=NULL.

Expose optional DISTINCT work separately from existing MIN/MAX counters: argument
rows, nonzero value updates, actual directory copies and native entering/leaving
values. Old absent metrics remain unknown. Keep native-only metric behavior.
Restart bootstraps from committed child relations under the existing protocol;
bounded persisted-state recovery remains R07b.

## 5. Failure modes and recovery

Overflow, negative value counts, invalid types/identity or native update failure
reject the private candidate before publication. Preserve discarded/stale candidate
isolation and installed state. Full-row deletion existence remains the graph's
responsibility. Never select full recomputation after admitting this state.
Request retries retain original diagnostics; restart must restore refcounts before
deleting one versus the final duplicate.

## 6. Acceptance criteria

* RED regression exposes global/mixed DISTINCT fallback on R03a; green selects the
  existing accumulator plan without relaxing strict mode.
* Independent full SQL and integer-map/Decimal endpoint oracles cover duplicates,
  zero crossings, NULL, empty global/grouped input, key moves, mixed aggregates,
  projected cancellations, multiple DISTINCT functions and independent arguments.
* Growing untouched groups and hot-group distinct cardinality at fixed changes
  retain zero historical aggregate reads, logarithmic path copies and exact native
  transition counts. Nonfinal duplicate deletion performs zero native updates.
* Randomized epochs and signed delta replay verify discard, failure and stale
  candidates. SQL INSERT/DELETE/UPDATE, retry/reopen and output history preserve
  the final-duplicate behavior after reconstruction.
* Test admitted ordered argument types and existing modifier/type/custom identity
  rejection. Default/Iceberg format, Clippy, tests, private rustdoc, mdBook and report
  tools pass. Freeze a default binary and run a fresh isolated three-round TPC-DS
  Q28 strict SQL/full oracle/reopen/output-history witness.
* Full TPC and controlled performance qualification remain distinct R10 gates.

## 7. Rollout / migration

Review and accept this design against R03a before implementation; link and commit
it first. Preserve public imports and persisted formats; diagnostics are additive.
Update tests that intentionally expected DISTINCT changed-group/full fallback to
assert the new state while preserving independent SQL and history checks. Retain
explicit changed-group coverage with qualified-key FILTER aggregates.

## 8. Open questions

No blocking numerical or format decisions within this scope. Multi-argument COUNT
DISTINCT, unqualified numeric types and aggregate modifiers remain explicit separate
boundaries, not silently claimed coverage.


## 9. Local design review

Inspected the accepted R03a cell/value-directory implementation at `0c30be5`,
DataFusion DISTINCT normalization, builtin aggregate expression builder, rule
priority, exact argument/result guards and TPC-DS Q28's actual SQL. Native builders
already default to non-DISTINCT; explicit membership maintenance must preserve
that setting. Empty-function grouping nodes can use the existing group row count
without retaining complete historical rows. Critical checks are zero-crossing
versus duplicate counts, NULL exclusion, native empty result types, candidate
isolation and preserving fallback for unqualified types/modifiers. No dependency,
format or numerical-policy change is needed. This review accepts implementation
scope; all new implementation and witness acceptance remains unverified.
