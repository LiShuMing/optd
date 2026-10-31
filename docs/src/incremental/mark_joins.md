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
