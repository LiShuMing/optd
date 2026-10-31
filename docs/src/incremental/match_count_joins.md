# Stateful outer/semi/anti join maintenance from match-count transitions

**Status:** accepted · 2026-10-31; local source/API design review; pure/residual opt-in implemented, default/SQL acceptance pending
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


## 9. Prerequisite scope repair and observed verification

The independent NOT IN regression reproduced a wrong result: left `(1),(2)`,
right `(1)`, then insert right NULL. Full SQL returns zero rows; the previous
changed-key snapshot algorithm retained `(2)`. Key-scope admission now excludes
Join::null_aware. Strict planning/CREATE refuses with the global dependency reason;
non-strict explicit recomputation stays correct. Ordinary anti/NOT EXISTS and
null-safe equijoins retain strict keyed plans.

`ROADMAP.md` records the exact RED/green and final commands. Three SQL integration
cases and a typed scope-verifier test cover duplicates/NULL/empty transitions,
discarded candidates and pre-publication refusal. Final default/Iceberg gates
passed 267/281 tests (2/3 ignored), both Clippy configurations/fmt, private rustdoc
and mdBook. Logs use `target/roadmap-r04a-null-aware-*20261031.log`.
No new match-count algorithm, full TPC or controlled performance is claimed.
R04a remains in progress and globally incremental NULL-aware anti joins remain
explicit R06 work. This boundary must also guard the future match-count rule.


## 10. Initial physical rollout: pure equijoin match counts

Implement the first physical slice as an opt-in registered transformer through
IncrementalOptimizer::with_match_count_joins(). Pure equijoins (including null-safe
keys) use the existing indexed bucket row cardinalities as their match counts;
no extra engine state-slot vector or persisted state is needed. The selected
frontend compiles to an explicit MaintainMatchJoin operation between one shared
PrepareIndex and CompleteIndex, using StateSpec::Keyed. Retain consolidated
per-key changes in its candidate; never reconstruct the delta by diffing full
untouched buckets. Record actual emitted pair/membership and peer-row work.

The first slice admits no residual filter and no null_aware flag. Those queries
retain their existing qualified scoped algorithms (or explicit global refusal).
This API registers a concrete physical alternative, not a semantic opt-out. Keep
it opt-in until residual support and the complete R04a acceptance matrix qualify
default selection; no default coverage/performance claim follows from this slice.
Add its RED plan test before implementation. R04a stays in_progress throughout.

The shared index declaration carries an explicit `IndexAlgorithm`: `Scoped` or
`MatchCounts(RelationOperator)`. Preparation retains original consolidated deltas
only for the latter. Verification binds `MaintainMatchJoin` to that exact operator,
state token and Before/After permissions. Buckets retain a key-NULL flag derived
from projected arrays, never inferred from encoded key bytes.


## 11. Pure-equijoin slice and remaining acceptance

The opt-in registry now lowers `Maintenance::MatchJoin` to shared PrepareIndex,
MaintainMatchJoin and CompleteIndex steps. State and operator identities are
verified before bootstrap. The runtime consumes original per-port net deltas,
uses O(1) bucket cardinalities, and emits peer membership only at zero crossings.
Typed key NULL flags come from projected Arrow arrays. Ordinary equality never
matches those buckets; NULL-safe equality may match them. No residual is discarded.

The independent nested-loop oracle exercises all seven orientations with ordinary,
NULL-safe and composite keys over 756 signed epochs. Tests also cover deterministic
first/last matches, empty inputs, duplicates, stale/discarded/invalid candidates,
checked overflow, forged physical bindings, computed/USING/self joins, downstream
aggregate/window consumers and per-round DurableSqlView reopen. Fresh full SQL
comparisons verify schemas and bags independently of incremental state.

Work regressions grow untouched keys while keeping delta and output fixed. Pure
semi/anti hot-bucket changes without zero crossings visit zero unchanged peer
entries and emit zero pairs. These tests also expose the remaining complete
changed-bucket copies and expanded materialization; source/cache/store work is
separate. No end-to-end complexity or latency claim follows from this slice.

The database SQL session still uses its default registry. It does not exercise
this opt-in physical operation yet. SQL CREATE MV/identified retry/output-history
and original-query-directed witnesses must be rerun after residual qualification
and default selection. R04a stays in_progress; no full-TPC or performance acceptance
is claimed. Final gate commands/results are recorded in ROADMAP.md.

## 12. Residual slice: state and transition review

Extend the opt-in algorithm to qualified deterministic equijoins with residuals.
`IndexAlgorithm::MatchCounts` still declares the complete original operator. At
bootstrap derive a physical Boolean expression against the combined qualified
input schema, plus two persistent directories of per-key full-row match-count
maps. Pure equijoins retain cardinality-only state. No new persisted format is
introduced. Reopen reconstructs the derived counts, including zero-count rows.

For each stage, copy only touched count maps. Evaluate changed-distinct-row times
fixed-peer-distinct-row residual candidates in batches of at most 1024 rows; keep
multiplicities outside predicate arrays. SQL TRUE is the only match. Accumulate
peer count changes across the entire stage before emitting zero crossings. Own
changed rows use their computed fixed-peer match count, then retain/remove their
entry according to final own multiplicity. Unchanged own rows keep their counts;
fixed peers receive the accumulated count delta. Removed buckets lose their count
map. Stage 0 produces A-after counts vs B-before and B-before counts vs A-after;
stage 1 produces both final count directories. Check cached counts against computed
counts for existing changed rows, and reject negative/overflowed counts.

The physical operation mutates only the private prepared candidate. Evaluation or
arithmetic errors discard the candidate without installing input or count state.
Record actual residual candidates/rows, count writes, zero crossings, count-map
entry copies and persistent count-directory path copies. Refresh counters exclude
bootstrap pair evaluation, source/cache IO and storage; recovery remains a rebuild.
Keep the opt-in API until SQL/default-rollout acceptance. Preserve strict NULL-aware
refusal and typed key admission. Reject forged operator/state/version bindings.

Source review: existing persistent directories share untouched roots, Bag full-row
identity is exact, and DataFusion's public create_physical_expr/evaluate/into_array
APIs provide SQL Boolean/null semantics without copying dependency implementation.
Add a RED residual-plan test, independent integer/NULL residual oracle, randomized
two-sided signed replay, zero crossings/cancellation and hot/untouched scope tests,
then both feature gates. Final strategy diagnostics must report any actually executed
snapshot evaluations even when a custom plan also contains match maintenance.


## 13. Residual implementation slice

The opt-in rule is now version 2 and includes deterministic residuals. Bootstrap
compiles a DataFusion Boolean expression and derives per-key full-row count maps.
The physical operation evaluates only changed-row/fixed-peer candidates in chunks
of at most 1024 rows, then applies consolidated peer-count transitions. Counts and
inputs remain isolated in the prepared candidate; reopen reconstructs both maps.
Pure equijoins retain the cardinality-only path. NULL-aware anti joins still require
R06's global dependency algorithm and remain explicitly refused in strict mode.

Thirteen SQL integration tests cover seven join orientations over 1764 signed
randomized epochs with an independent integer/NULL oracle, periodic full SQL,
NULL-safe and arithmetic residuals, FALSE/NULL, duplicate rows, empty first/last
matches, discard/stale/failure, downstream consumers and durable reopen. A second
stage division-by-zero regression proves first-stage candidate writes do not leak.
Hot semi/anti replacement consolidates zero net peer counts before crossings;
refresh evaluates exactly changed-distinct-row times peer-distinct-row candidates.
Growing untouched keys keeps pair/count-map work fixed, with count-directory path
copies separately bounded. Remaining full touched input/count-map copies are
measured. Bootstrap pair work, source/cache/store IO and latency are not qualified
by these refresh counters.

An explicit custom Optd rule regression adds S-S snapshot evaluation to an otherwise
match-maintained program. Its result remains equal to full SQL and diagnostics
report KeyedRecompute plus the actual snapshot and match counters. This prevents
frontend labels from hiding a final program's work.

The RED residual-plan regression selected the former scoped rule. Current gate
commands/results and local implementation identity are recorded in ROADMAP.md.
Default SQL session selection, identified request/history acceptance and directed
TPC witnesses remain the next rollout slice; R04a remains in_progress.
