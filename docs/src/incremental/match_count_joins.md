# Stateful outer/semi/anti join maintenance from match-count transitions

**Status:** accepted · 2026-10-31; local source/API design review, implementation pending
**Scope:** R04a typed equijoin keys, all seven outer/semi/anti orientations, optional
SQL residual conditions and null-safe equality. Reuse Optd plans and derived
candidate state. Mark/range joins, NULL-aware anti-join global dependencies and durable operator-state formats are separate.

## 1. Problem

Current keyed outer/semi/anti rules evaluate After minus Before for all complete
changed-key buckets. This is semantically qualified but repeatedly evaluates
unchanged peer combinations. Maintaining match counts allows direct output deltas
and membership changes only when counts cross zero.

## 2. Constraints and evidence

Preserve the existing inner-join orientation:
Delta-left join Before-right plus After-left join Delta-right. Signed full-row
multiplicities, duplicates, SQL NULL and residual TRUE-only matching remain exact.
Source changes are atomic net row images, not ordered SQL statement prefixes.
The pinned DataFusion 55 Join API exposes null_equality and null_aware separately.
NULL-aware anti joins require a global right-side NULL dependency; a local equal-key
closure alone cannot prove their scope. Audit this existing admission boundary
first and add a failing independent regression before any correction. Ordinary
anti joins and null-safe equijoins remain supported; a null_aware plan must use a
separately proven global algorithm or retain explicit strict refusal.
Scoped snapshot maintenance remains an explicit qualified alternative; it is not
a substitute for the new algorithm's acceptance evidence.

## 3. Options considered

Caching affected output and diffing it still reevaluates all After pairs. A rule
containing an imperative join engine would violate rule/executor separation.
Instead declare a typed match-maintenance state and operation in Optd; keep all
execution, indexes and count transitions in dedicated physical/state leaf modules.

## 4. Decision

### Plan and state

Add an explicit maintenance frontend, StateSpec and physical operation for
match-count joins. A deterministic rule checks original join type, complete typed
key closure and SQL schema, then declares original operator and input/key ports.
The executor runs only that selected operation; it never chooses algorithms from
SQL node text. Plan explanation and actual strategy identify match-count execution.
No remaining complete-node recomputation is introduced in strict mode.

State contains immutable candidate-shared per-key input buckets. Pure equijoins
need only each bucket's total positive row multiplicity: every eligible row has
the same peer match count. With residual predicates, retain per-distinct-full-row
peer match multiplicities. Full-row identity uses existing ScalarValue identity,
not a lossy key encoding. Maintain counts for preserved membership as needed.
Share untouched directories/payloads and copy only touched paths/buckets; record
any full touched-bucket input/count-map copies honestly. This increment need not
prove bounded hot-bucket storage updates or end-to-end delta work.

### Two one-sided transitions

Apply the consolidated left delta against committed right inputs, then the right
delta against candidate left inputs. Each stage:

1. Probe only changed rows and their peer key buckets. For pure equijoins use
   bucket totals without reading peer rows when neither matched output nor a zero
   crossing requires them. Respect the original null-equality mode exactly.
2. For inner/outer matched output, emit changed-row weight times matched-peer
   multiplicity. Semi/anti plans emit no concatenated matched pairs.
3. Emit changed preserved rows according to their peer match count: semi when
   positive, anti when zero, outer NULL extension when zero. Full outer preserves
   both sides. Keep exact original output types/nullable schema and USING behavior.
4. Accumulate each unchanged peer's match-count change across all changed rows in
   the stage before applying it. Emit/retract peer semi/anti/NULL-extended rows
   only on zero crossings, multiplied by that peer's existing row multiplicity.
5. Install only final valid positive input multiplicities and counts into the
   isolated candidate. Removed rows lose their count entry; duplicate rows change
   multiplicity without inventing extra identities. Combine both stage outputs
   into the ordinary checked signed Bag.

The right stage sees A-after, so the two-sided cross term is counted once. Never
mix Before/After versions or count transient per-delta ordering as SQL semantics.
Checked arithmetic and final source/bucket nonnegativity errors abort the private
candidate without modifying committed input/count/output state.

### Residual evaluation

Compile the original deterministic residual against the combined qualified input
schema using DataFusion physical expressions, with source-row pairs evaluated in
bounded batches. A candidate pair is one changed distinct row and one peer row;
unchanged-left/unchanged-right pairs are not reevaluated. Predicate NULL is not a
match. Pair multiplicities remain separate from physical predicate rows so large
duplicate counts do not require expanding every duplicate. Bootstrap may need
all initial candidates; refresh must expose and separately account its probes.
Do not copy dependency internals or hand-implement SQL predicate semantics.

### Metrics and recovery

Record actual changed entries, bucket/row probes, residual candidate evaluations,
matched emitted pairs, match-count updates/zero crossings, copied directory and
bucket/count entries, and signed output work. Preserve source/cache/SQL/store
measurements separately. Output work may necessarily grow on first/last matches.
No benchmark latency claim follows from fewer snapshot evaluations alone.

Derived states are reconstructed on reopen under existing strict plans; no stored
format changes. Failed/discarded/stale refresh candidates cannot advance counts.
Durable atomic publication, request retry and every output-history prefix retain
the existing protocol. R07b separately persists operator states.

## 5. Failure modes and recovery

Null-safe vs ordinary equality, residual false/NULL, duplicate weighted matches,
full-join simultaneous first/last matches, identical visible NULL outputs,
projections, computed/composite keys and self-joins require independent oracles.
A diagnostic or invalid rule cannot acquire state. Unsupported scope remains
explicit; never coerce an outer join into an inner join or omit a residual.

## 6. Acceptance criteria

* RED shows existing scoped snapshot evaluation and unnecessary unchanged peer
  visits; green plans execute a declared match-count state/operation for every
  admitted outer/semi/anti form, including residual/null-safe variants.
* Independent small nested-loop SQL three-valued predicate oracle over all seven
  join types, insert/delete/update/key moves, both-side changes, duplicates,
  real/synthetic NULL collisions, empty relations and first/last matches.
* Randomized signed endpoint/output-prefix comparisons plus fresh full DataFusion
  SQL/schema, discarded/failed/stale candidates and overflow refusals.
* Fixed change/output and growing untouched keys keep scope work bounded. Hot
  pure-equi semi/anti updates without zero crossing read zero unchanged peer
  rows/pairs; residual work evaluates changed-vs-peer candidates only. Count and
  retain all remaining touched-bucket copy/materialization/storage costs.
* SQL CREATE MV/DML, identified retry, per-round reopen and output history retain
  atomicity for outer/semi/anti chains and downstream aggregate/window consumers.
* Default/Iceberg fmt/Clippy/tests, private rustdoc, mdBook and report tools pass.
  Fresh isolated original-query-directed witnesses retain source/binary identities;
  full TPC and controlled performance remain separate R10 gates.

## 7. Rollout / migration

Source/design review, commit accepted design/index, RED tests, physical state and
Optd declarations, independent semantics/scope tests, then fresh SQL witnesses.
Keep current scoped implementation as an explicit alternative for later R09a cost
selection. Mark R04a verified only after residual and all orientation gates pass.

## 8. Local source/API design review

Pinned API review confirms Join::null_equality, Join::null_aware, DFSchema::join,
and physical expression construction are available. Original key-scope admission,
keyed bucket updates, scalar row identity, physical plan verification and output
schema handling were reviewed. DFSchema::join provides the
combined qualified input schema for residual evaluation without executing a cross
join. Complete output shape, USING semantics and null-aware admission require the named
regressions before implementation acceptance. Implement the null-aware scope audit
and any independently reproduced repair as a separate prerequisite increment;
then add match-count state/operations. This accepted direction is not completed
implementation or numerical/performance qualification.
