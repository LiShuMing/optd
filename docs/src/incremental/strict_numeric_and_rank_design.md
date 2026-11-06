# Float grouping, integer averages and ordered ranks

**Status:** accepted owner direction, implementation review recorded · 2026-11-06
**Scope:** S2/S3 qualification; S4 numerical boundary; S5 ordered rank state.

## 1. Problem

Continue the strict-gap execution specification without conflating floating
arithmetic with grouping or weakening complete-row/delta comparison. Section 10
records the owner's subsequent approval of a bounded numerical result contract.

## 2. Constraints and evidence

The pinned DataFusion 55 primitive grouping normalizes negative zero and retains
NaN payload bits. Arrow 59 row encoding preserves IEEE total-order bits. Source
bags retain all complete-row bits. Join equality and aggregate argument eligibility
must remain separate from group-key eligibility. Public implementation references:
<https://docs.rs/datafusion-physical-plan/55.0.0/datafusion_physical_plan/aggregates/index.html>.

The seven AVG refusals are integer quantities cast to Float64, alongside existing
decimal averages. Binary64 additions of integral values are exact if the sum of
absolute contributions never exceeds 2^53, including temporary retraction/addition
states. This permits a narrower qualification than general floating averages.

## 3. Options considered

Removing the float guards is unsound. Retracting general floating SUM/AVG or
Welford variance can differ from full-query evaluation because floating arithmetic
is order dependent. Replaying a global group is full input work and cannot be
called delta-only aggregation. SQL changes are not selected. The original exact
oracle was superseded only for the numerical result columns listed in section 10.

## 4. Decision

S2 introduces group-specific eligibility and zero-normalized group encoding.
Accumulator grouping uses canonical +0 output representatives, exact NaN payloads,
and existing persistent reference counts. Empty aggregate lists implement DISTINCT
without retaining historical inputs; output occurs only at 0/1 crossings. Other
function/key domains keep their original admission. Float16 remains unqualified.

S3 admits canonical AVG of an explicit integer-to-Float64 cast. A separate cell
retains the native count/sum and checked absolute integral mass. Bootstrap and
refresh reject any argument or candidate mass outside the exact domain before
publication; no fallback or approximate answer is returned. Retractions reduce
mass before additions. NULL contributes neither mass nor count. Empty/global
states retain DataFusion's native NULL behavior. Native division rounds once in
the same way as full AVG. General float inputs, DISTINCT averages, modifiers and
custom UDAFs remain rejected. An integer cast is a structural qualification; the
mass check is a runtime arithmetic validity condition, analogous to overflow.

S4 uses the separately approved numerical contract in section 10. A differential
cancellation witness documents native SQL order dependence; mathematical oracles,
exact endpoint replay and canonical recovery are separate acceptance gates.

S5 must use ordered peer counts, prefix cardinality and the first changed peer's
suffix, with ASC/DESC/NULL encoded in the order key. It must not qualify all empty
partition windows as one recompute bucket. Detailed rank implementation review and
range counters are required before admission.

## 5. Failure modes and recovery

All updates operate on private candidates. Invalid deletes, mass overflow, output
or state errors discard the candidate. Existing checkpoints persist source/output,
then rebuild and verify state; no persisted-format migration is introduced.

## 6. Acceptance criteria

RED-before/GREEN-after tests for float DISTINCT (+/-0, NaN payloads, NULL,
last deletion), strict integer AVG (global/grouped, cancellation, limits, invalid
candidate), signed-delta replay, retry/reopen, and fixed delta with growing
untouched groups. Compare to independently executed full SQL using exact scalar
bags. Re-run Q36 and the seven AVG queries on directed and generated data with
fresh state, original/core identities and explicit timeouts. Record incomplete
results separately. Default/Iceberg tests, fmt and warnings-denied lint remain gates.

## 7. Rollout / migration

Keep previous benchmark artifacts frozen. Publish each batch's exact commands,
source identity and observed work separately from performance qualification.

## 8. Open questions

General floating SUM/AVG and VAR_SAMP/VAR_POP remain open. STDDEV has the
owner-approved numerical contract below rather than native bitwise equivalence.
Ordered rank and original Top-K are separate from group-key and AVG qualification.

## 9. Rank implementation review

A rank state owns an immutable ordered directory per partition. Each peer contains
its complete-row bag, cardinality and one-based rank. A directory predecessor
provides the prefix count (`rank + peer cardinality`) without reading preceding
peers. Bootstrap evaluates typed order expressions and builds peer counts in order.

Refresh consolidates exact signed rows into changed peers, starts at the earliest
changed peer, and merges ordered old peers with changed keys. The running prefix
computes candidate ranks. An unchanged peer is skipped whenever its rank is
unchanged; when the net shift returns to zero, seek directly to the next changed
key rather than traverse the intervening peers. After the last changed key, stop
as soon as the shift is zero. Emit old/new rows only for changed contents or rank.
Deletion of an entire peer emits its committed rank. Insertion and deletion at the
end therefore do not revisit the untouched prefix. Insertion at the start can
legitimately update every later peer. Store the committed ranks so the Before
output is never reconstructed by rerunning a window.

The first qualification is one canonical builtin RANK per Window node, nonempty
ORDER BY, no PARTITION BY. Existing partition-window rules remain available.
Float32/64 order keys retain Arrow total-order identity (including signed zero and
NaN); rank peers in the pinned SQL executor use those same encoded sort keys.
Only qualified scalar sort types are admitted. Ordinary full-row identity remains
exact. StateSpec::Rank and UpdateRank are distinct from aggregate/group operators;
physical verification binds the exact original delta input. RankWork records peers
visited, changed peers, input rows read, emitted output rows and actual directory
copies. Growing-prefix and necessary-suffix tests are separate acceptance cases.
No new checkpoint format: rebuild rank state and compare committed output on reopen.

## 10. Owner-approved variance contract (2026-11-06)

The owner explicitly selected stable numerical algorithms with separately stated
error bounds. This supersedes S4's bitwise-oracle requirement for STDDEV and its
paired floating mean, but leaves exact bags, source images, integer/decimal values,
group identities, multiplicity and signed-delta replay unchanged. The native
full-SQL permutation witness yields 2.449489742783178 versus 2.0 for the same
three near-1e16 observations. Neither value is an absolute accuracy oracle.

Use origin-centered Welford state `(n, origin, mean_offset, M2)`; the translated
coordinate avoids losing small deviations near large values. Retractions reverse
the mean/M2 update, reset at n=0/1, and never use raw `sum(x*x)-sum(x)^2/n`.
Retain counted finite argument values in persistent ordered state so numerical
rebasing can visit only a touched group's distinct values. Canonically rebase
each changed cell before publication, as required by the recovery review below.
Record those state-value visits explicitly. Nonfinite inputs or
unrepresentable moments reject the private candidate. NULL is ignored; sample
STDDEV is NULL for n<2, population STDDEV for n<1; duplicates retain multiplicity.

Admit only canonical stddev/stddev_pop and Float64 AVG paired with the same
STDDEV argument in that aggregate node. Other floating AVG/SUM stays rejected.
Bootstrap must publish the numerical cell's output before downstream operators;
subtracting a stable old value from an independently rounded bootstrap is invalid.
Recovery rebuilds this same state, then compares its exact committed endpoint.

Focused mathematical oracles use known deviations, large origins, singleton/empty
states and deletion sequences with abs <= 1e-10 + 1e-9*abs(reference). Benchmark
comparison uses that bound only for result columns with numerical lineage from
these cells. All other columns and source comparisons stay exact. Exact signed
output replay compares maintained Before/After endpoints, while independent full
SQL checks numerical agreement separately. A changed filter/Join membership or
multiplicity remains a failure. Native full SQL outside the bound is reported as
an oracle disagreement, never automatically accepted by widening the tolerance.

Recovery review refined the selected implementation: because checkpoints currently
persist sources/output rather than operator cells, every changed moment cell is
canonically rebased in ordered argument-value order before publication. This makes
rebuild output independent of refresh history, permitting exact checkpoint and
signed-log validation. The state still counts argument insertions/retractions;
only touched groups visit their counted distinct values. This is explicitly
O(delta + distinct argument values in affected groups), not an O(delta) numerical
accumulator claim. A global aggregate can visit all its argument values. Removing
these rebases depends on persisting/recovering numerical operator state (R07).
