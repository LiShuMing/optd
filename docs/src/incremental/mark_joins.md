# Mark joins and quantified-subquery truth

**Status:** accepted admission-repair design · 2026-10-31 after local API/source and independent truth-table review; mark-state implementation remains proposed
**Scope:** R04b correctness prerequisite shared by typed SQL planning and SQL reads/DML. Subsequent mark-count and global/correlated truth state require their own implementation evidence; this document does not certify R04b or R06.

## 1. Problem

The pinned DataFusion 55 JoinType documents LeftMark/RightMark as Boolean match
existence, never NULL. Its decorrelation code uses these joins for disjunctive IN
and EXISTS, despite a contradictory comment about three-valued mark semantics.
Incremental eligibility cannot rely on either comment as a semantic proof.

A frozen current default SQL runner reproduces this input:

```sql
CREATE TABLE a(k BIGINT);
CREATE TABLE b(k BIGINT);
INSERT INTO a VALUES (1),(2),(NULL);
INSERT INTO b VALUES (1),(NULL);
SELECT k FROM a WHERE k=9 OR k NOT IN (SELECT k FROM b);
```

The independent SQL truth table requires zero rows. Actual ordinary SQL returns
`2` and `NULL`: the Boolean mark loses UNKNOWN before negation. Positive IN,
EXISTS and NOT EXISTS in the same filter context match their independent expected
rows. Projected IN is explicitly unsupported by the pinned physical planner.
Evidence: `target/roadmap-r04b-mark-semantics-probe-20261031/individual-summary.json`,
with SQL, exact command, frozen runner hash and individual stdout/stderr artifacts.
These are read-only SQL probes after fixture DDL/DML, not incremental mark tests.

## 2. Constraints and evidence

Inspect the analyzed typed plan before optimization loses IN/EXISTS provenance.
`planning::compiler::compile` already has that plan before `optimize`;
`database::sql::plan_query` provides the same boundary for SELECT and UPDATE/DELETE
selectors. INSERT SELECT binds its input directly in `dml_compiler` before calling
`sql::immutable`; it must call the same shared query validator before collection.
The typed graph otherwise rejects remaining scalar/IN/EXISTS expression subqueries
once optimization has run. No parser-string inspection or executor-side guessing.

A WHERE/HAVING filter observes only whether its predicate is TRUE. Mapping UNKNOWN
to FALSE preserves that membership through positive AND/OR, including NULL-valued
other terms. It does not preserve NOT, IS FALSE, comparisons against Boolean
values, CASE, or a projected Boolean result. Plain conjunctive NOT IN remains the
existing NULL-aware anti path; do not disable its independent correctness tests.

## 3. Options considered

* Admitting all mark joins by equikey scope would promote a known wrong answer.
* Treating every mark as three-valued would invent information already lost by
  the upstream plan. Whole-node recomputation through that plan also gives the
  wrong answer.
* Rejecting every IN would unnecessarily remove existing qualified positive
  membership filters and plain conjunctive anti paths.
* Selected prerequisite: shared typed admission before decorrelation. Preserve
  the proven filter-membership contexts; explicitly refuse unknown-sensitive
  quantified contexts until the semantic descriptor/state can represent them.

## 4. Decision

Add a focused internal graph-validation leaf with a documented entry point over
`LogicalPlan`. Call it on analyzed plans in the common incremental compiler and
SQL query binder and direct INSERT-input validation, before evaluation, table
replacement or CREATE publication. Keep immutability checking explicit in the
shared SQL query validator; do not broaden a function named `immutable` silently.
Traverse subquery plans too. Do not mutate or rewrite their expressions.

Use explicit expression contexts:

* Conjunctive filter position: root Filter predicate and its AND spine. Admit
  positive IN and syntactically negated IN here; the latter keeps NULL-aware anti.
* Positive filter membership: after OR, propagate only through AND/OR and aliases.
  Admit positive IN; refuse negated IN because UNKNOWN can affect membership.
* Observed Boolean value: all other operators or plan expression positions.
  Refuse quantified IN descendants, including positive IN under NOT/IS FALSE,
  Boolean comparison or projection. EXISTS remains a separate two-valued construct.

Do not flatten NOT into an allowed context without proving NULL semantics. Limit
validation work and avoid unbounded recursive custom traversal. Ordinary scalar
expressions and subqueries without IN are unaffected. Diagnostic text identifies
three-valued quantified truth as R04b/R06 work, rather than pretending full
recomputation is a sound fallback. No public API, dependency or durable format
changes are required for this prerequisite.

For the later mark algorithm, preserve a typed semantic descriptor before
optimization: EXISTS is two-valued; quantified IN/NOT IN/ANY needs TRUE, FALSE and
UNKNOWN plus correlation-partition totals/NULL dependencies. A true match wins;
without true matches, unknown comparisons yield UNKNOWN; an empty comparison
relation yields FALSE for IN even for a NULL left value. Separate correlation keys
from compared values. R06 must declare and measure global/partition impact when
first/final NULL or empty/nonempty transitions affect unchanged source keys.
Logical rules declare this state and impact; physical operators execute it.

## 5. Failure modes and recovery

Admission failures must precede source execution and durable changes. Rejected
SELECT changes no epoch; rejected DML leaves its target unchanged; rejected CREATE
publishes no view, CURRENT change or request receipt. Reopen of an incompatible
previously admitted definition fails explicitly without changing committed data.
Existing successful request retries still return their original durable receipt.

Test SQL aliases, nesting, NOT wrappers, AND/OR combinations, nullable left/right
values, empty peers, duplicate NULLs and correlated partitions. Do not accept the
pinned full-query engine as the sole oracle for the reproduced wrong-answer case.

## 6. Acceptance criteria

* RED shows the unsafe negative-IN/disjunction executing and returning extra rows;
  after repair it is explicitly rejected at typed admission.
* Independent truth tables retain positive IN/EXISTS/NOT EXISTS filter membership
  and existing conjunctive NOT IN behavior across NULL, duplicates and emptiness.
* Unsafe IN under NOT, IS FALSE/Boolean observation and projection is refused;
  related expressions with no quantified subquery still execute.
* SELECT, DML selectors/INSERT SELECT, preflight and CREATE share admission; refused
  writes/CREATE preserve source rows, epoch, CURRENT, catalog and retry identity.
* Default/Iceberg fmt/lint/tests, private rustdoc and mdBook pass. Fresh directed
  TPC classification retains all blockers rather than changing SQL or expectations.

## 7. Rollout / migration

Commit the reviewed design/index, add RED cases, implement the shared guard and
qualify both configurations. This is a correctness repair, not completed mark
maintenance. Then specify/implement explicit mark truth/count state and R06 impact
prerequisites. TPC-DS Q10/Q35 use correlated EXISTS disjunctions; Q45 uses positive
IN under OR. Q35 also has floating-output integer AVG and must retain its R03e
blocker until numerically qualified, even after its mark operators become eligible.

## 8. Open questions

The full mark implementation must decide how its typed truth descriptor survives
upstream normalization and how its bootstrap oracle avoids the Boolean-only mark
kernel for observable UNKNOWN. No current design decision permits silently
substituting Boolean match existence for those semantics. R04b verification waits
for the complete three-valued/correlated acceptance matrix, not only this guard.


## 9. Admission implementation and observed verification

The shared analyzed-plan guard is implemented before decorrelation for incremental
compilation, SELECT and DML. Independent truth tests preserve positive membership,
EXISTS/NOT EXISTS and conjunctive NOT IN; unsafe observed UNKNOWN is explicitly
refused. CREATE and rejected DML publish no changes or request receipts.

ROADMAP.md records RED (three failures), final four focused tests per feature,
294/308 complete default/Iceberg tests, both lint modes/private rustdoc and fresh
CLI/classification evidence. The initial classification driver's exit-code
assertion was incorrect: benchmark returns 1 for unsupported strict plans. Final
classification checks that explicit refusal contract and all blockers; no rejected
query is reported passed. Q35 retains its independent numerical AVG blocker.

Mark execution remains unimplemented and R04b stays in_progress. No full-TPC,
standard-scale or controlled performance certification follows from admission.

## 10. Boolean mark-count slice: reviewed transition design

**Status:** accepted · 2026-10-31 after typed schema, index and emitter review.
**Scope:** the pinned Boolean LeftMark/RightMark relational operators only. SQL
admission in section 9 stays mandatory. Observable quantified UNKNOWN and global
NULL dependencies remain R04b/R06 work; Boolean qualification cannot close R04b.

Register a separately named `builtin.boolean_mark_join` version 1 at priority 20.
Its matcher accepts only mark types with the same qualified, nonempty equikey
scope, deterministic expressions and non-NULL-aware contract as match-count
state. Keep `builtin.match_count_join@2` restricted to the seven qualified
outer/semi/anti types. Both declare `Maintenance::MatchJoin` and lower to existing
Optd PrepareIndex/MaintainMatchJoin/CompleteIndex with operator-bound index state.
No SQL-string classification in rules/runtime and no new persistence format.
The existing scoped alternative may remain available only with the same scope
proof; removing the mark rule must preserve strict correctness and clear plans.

Pinned public typed builders and physical join schema utilities agree: LeftMark
outputs left fields followed by one nonnullable Boolean `mark`; RightMark outputs
right fields followed by that field. Test both by executing actual typed plans,
not just assuming RightMark symmetry. Key NULLs do not match ordinary equality;
NULL-safe equikeys may match. Residual TRUE alone contributes to match counts.
Boolean mark remains nonnullable even for NULL input rows and NULL residuals.

Extend the output policy with explicit preserved-side mark behavior. An own
preserved-row delta emits that row plus its fixed-peer positivity, with original
multiplicity. Peer count zero crossings retract the old marked row and insert
the new marked row. Both flags have membership one: subtracting semi/anti
membership indicators cannot implement this replacement. Non-preserved rows emit
nothing unless they change preserved peer flags. Consolidate all peer count
changes in each one-sided stage before crossings; multiplicities count matching
peer rows but never multiply the preserved row by pair cardinality. Stage order
stays delta-left/before-right then after-left/delta-right, including both-side
inserts/deletes and moved keys. Share crossing/own emission between pure and
residual algorithms so the seven existing join orientations retain their laws.

For pure keys with unchanged positivity, no unchanged peer entries need visiting.
At a zero crossing visit only affected-key preserved rows and account both output
contributions. Residuals still evaluate changed-distinct-row times fixed-peer
candidates and retain per-full-row counts; do not repeat unchanged/unchanged pairs.
Keep candidate-private failure/discard/stale semantics and actual count-map,
input-bucket, materialization and directory-copy measurements. Reopen rebuilds
count state from committed inputs; R07 state persistence is separate.

Acceptance: RED strict EXISTS/positive-IN disjunction plans; independent Boolean
nested-loop oracles over both orientations/predicates, duplicates/NULL, first/last
matches, both-side signed key moves and complete delta-prefix replay. Execute
actual typed LeftMark and RightMark bootstrap kernels, verify field order/type and
nullability, and compare incremental state output against the independent oracle.
Add pure hot-key/no-crossing and fixed-delta/growing-untouched scope regressions,
residual cancellation/failure/isolation coverage and default SQL CREATE/DML,
identified retry, atomic multi-source writes, reopen/history. Re-run both feature
gates and fresh frozen Q10/Q45 witnesses; retain Q35 numerical blockers. Keep
UNKNOWN-sensitive SQL refused and no-equikey/global joins explicit. Record every
remaining gate without promoting this slice to full R04b or performance acceptance.

## 11. Boolean physical slice: observed qualification

The independent version-1 Boolean mark rule now lowers qualified LeftMark/RightMark
through explicit Optd match-count state operations. Preserved row multiplicities
remain independent of pair counts. A zero crossing replaces the old Boolean row
with the new Boolean row; residual and pure paths share that output policy.

Typed kernels and independent bags pass 384 raw signed epochs across both mark
orientations and four key/predicate modes, plus 192 SQL membership epochs. SQL
retry/atomic writes/reopen/history, discarded/stale candidates and second-stage
failure are covered; fixed-delta/growing-untouched pure-key tests read zero
preserved peers without zero crossings. Existing outer/semi/anti oracles still pass.
Default/Iceberg complete gates pass 301/315 tests (2/3 ignored), both lint modes,
format, private rustdoc and 46 report-tool tests. ROADMAP.md retains exact commands,
RED and driver-failure evidence and all scope exclusions.

Fresh Q10/Q45 directed SQL witnesses pass three actual nonempty write commits
including UPDATE, strict planning, independent full comparisons and reopen/history.
The original one-row fixture attempt had a no-op UPDATE stage; stronger separately
identified two-image fixtures retain a row after exact-row DELETE. SQL and schema
are unchanged. Frozen code/binary identities and unified work reports are retained.
Q35 now plans both mark nodes incrementally but remains refused by its independent
floating AVG qualification gate. No full-TPC or controlled performance is claimed.

R04b remains in_progress. The section-9 guard still rejects observable quantified
UNKNOWN; Boolean mark qualification does not supply missing truth information or
prove global/correlated NULL dependencies. The next design must preserve that
provenance before decorrelation and represent its global/partition impact in R06.

## 12. Observable uncorrelated truth relation rollout

The typed normalizer specified in `quantified_truth_normalization.md` now lowers
qualified uncorrelated observed IN/NOT IN in Projection/Filter into ordinary
member/count/flag relations and CASE before this document's admission guard.
This supersedes blanket refusal for that exact fragment in the earlier historical
sections. The guard remains mandatory for unqualified contexts; Boolean mark
kernels retain their two-valued contract and qualified positive-filter path.
Original volatility checking precedes normalization in both compiler and SQL
binding, including direct INSERT SELECT input preparation.

Independent tests cover SQL UNKNOWN truth, empty comparison inputs, duplicate
counts, global first/last NULL transitions and signed two-source prefixes. SQL
writes/retries/reopen/history, schema metadata/nullability and candidate failure
are qualified. Default/Iceberg suites pass 311/325 tests (2/3 ignored); an initial
test-only Clippy error and its passing follow-up are retained in ROADMAP.md.
These are uncorrelated semantic/scope witnesses, not correlated/global dependency
contract completion or fresh full-TPC/performance certification. R04b/R06 stay open.

## 13. Ordinary correlated truth partitions

The relation normalizer now additionally extracts proven ordinary equality
correlations through row-preserving projection/filter/alias boundaries. Members
are grouped by correlation keys plus compared value; exact count flags are grouped
by correlation keys alone. Missing flag rows mean empty input, and ordinary NULL
keys never match. Typed extraction refuses unproved row-changing/computed/range/
NULL-safe scopes before publication, with a specific correlation diagnostic.

Independent partition truth passes 128 signed epochs and durable multi-view SQL
writes/retry/reopen/history. Growing untouched partitions 8 -> 512 keeps match-peer
visits at 12 and output delta rows at 2 for the fixed hot partition. Default/Iceberg
complete gates pass 318/332 tests, with 2/3 existing ignored. These observations
are recorded in ROADMAP.md; NULL-safe/mixed policies, binding sharing, reusable R06
impact descriptors and numerical/TPC/performance completion remain separate.

## 14. NULL-safe and mixed correlation policies

The section-10 truth-normalization design now extends direct correlation keys to
IS NOT DISTINCT FROM. Per-key policy is retained while ordinary-key NULL rows
are filtered from mixed-policy inner relations before typed NullEqualsNull joins.
The member relation still excludes NULL compared values, preserving scalar
UNKNOWN independently of correlation equality. No executor or durable format is
added. Computed/range/row-changing scopes remain refused before publication.

Independent tests cover four policies, 128 signed epochs, four views across six
durable SQL write/retry/reopen/history rounds, and fixed hot-partition work while
untouched partitions grow 8 -> 512. A parenthesized direct NULL-safe regression
fails against the archived prior source and passes with this implementation.
Commands, source identity, complete gate results and remaining acceptance limits
are recorded in ROADMAP.md. Binding sharing and R04b/R06 completion remain separate.
