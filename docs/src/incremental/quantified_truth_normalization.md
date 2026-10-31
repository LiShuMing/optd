# Quantified truth normalization into typed relations

**Status:** accepted · 2026-10-31 after local typed API and transition review
**Scope:** R04b/R06 implementation sequence. Preserve observable scalar IN/NOT IN truth before Boolean decorrelation, then qualify correlated partition and global dependency propagation. No new dependency or durable format.

## 1. Problem

The Boolean-only mark kernel cannot supply UNKNOWN after IN provenance has been
lost. Section 9 of mark_joins.md therefore rejects observed IN truth. The next
transformer must encode all three values with ordinary relational expressions so
existing Optd aggregate/join rules derive executable delta plans.

## 2. Constraints and evidence

DataFusion's public typed InSubquery exposes its scalar comparison expression,
single-column subquery and negation. LogicalPlan::transform_up_with_subqueries
can normalize nested plans before optimization. Logical builders provide typed
aggregates, left/inner equijoins, projections and CASE. Existing group/reference
counts, exact COUNT and delta joins already maintain these relations. The shared
compiler and SQL binder must use the same transformation before guard validation;
INSERT SELECT binds a separate input and must join this path too.

Keep immutability checking on the original subquery plan. Preserve full row
multiplicity, exact parent fields/qualifiers/metadata and nullable Boolean output.
Admission remains explicit for any correlation/type/expression position lacking a
proved lowering. Positive membership/EXISTS keeps the qualified Boolean path.

## 3. Options considered

* Boolean mark plus negation loses truth and is rejected.
* An opaque new runtime SQL/mark interpreter duplicates expression and state APIs.
* Selected: ordinary typed relational normalization, followed by the existing Optd
  transformers. Any new physical dependency algorithms retain their own contracts.

## 4. Decision

The named internal `quantified.truth_relations@1` transformer runs over analyzed
plans before decorrelation. Start with observed IN expressions in Filter and
Projection, including negative IN under OR. For expression positions that already
pass admission, keep current lowering. An unsafe Filter expression may normalize
its positive IN siblings as well; do not reinterpret EXISTS. Unsupported positions
retain explicit guard errors. Bound input nodes, expansion and alias allocation.

For an uncorrelated `e IN S(v)`, construct:

1. Non-NULL compared values grouped by value, projected to one TRUE membership
   marker. Multiplicity changes within a value produce no marker delta.
2. Global COUNT(*) and COUNT(v), projected to `empty` and `has_null` flags. Duplicate
   changes that do not change these flags produce no downstream flag delta.
3. A left equijoin from original rows by comparison value to distinct members.
   Join the one-row global flags through an explicit constant key. The flags are
   always one row, including empty input. Each original row remains exactly once.
4. CASE in order: true match => TRUE; empty => FALSE; NULL left or has_null =>
   UNKNOWN; otherwise FALSE. Apply NOT to the complete nullable result for NOT IN.

Remove helper columns at the original node boundary and preserve original output
expression names/qualifiers/types/nullability/metadata. Allocate helper names
against all existing names, including nested plans and expression aliases. Refuse
unproved comparison type changes rather than silently changing group equality.
Typed numeric coercion needs an explicit reviewed proof before widening admission.

Global flags declare a real all-preserved-keys dependency: first/final NULL or
empty transitions can affect unchanged comparison keys. The constant-key join
makes that propagation executable and visible in plans/counters; it does not
prove end-to-end delta complexity. R06 must subsequently express this dependency
in reusable descriptors alongside group/join/partition/range scope. Record ordinary
key changes separately from these global transitions. Never mark R06 verified just
because constant-key indexes produce correct output.

The correlated follow-on separates correlation keys from compared value, with
partition total/non-NULL flags and distinct compared values. Remove only proven
OuterReference equality filters. Missing partitions mean empty comparison input;
ordinary correlation NULLs do not match. NULL-safe/mixed policies and non-equality
correlation need their own proofs. No unproved scope becomes a keyed plan.

## 5. Failure modes and recovery

Normalization is pure planning: invalid types, ambiguous names, unsupported scopes
or resource bounds fail before SELECT reads, DML collection or CREATE publication.
No epoch, CURRENT, table/view, request receipt or source binding may change on a
refused plan. Reopen rebinds stored SQL through the same transformer. All state is
derived through existing operators; no new serialized state interpretation occurs.
Failed candidates and identified retries retain their existing atomic contracts.

## 6. Acceptance criteria

* RED strict SELECT/projected truth and negative IN/OR tests fail under the guard;
  green results match an independent three-valued nested-loop oracle.
* Cover match precedence, NULL left/right, empty peers, duplicate NULL/value counts,
  first/final NULL/empty transitions, both-source updates, negative/positive output
  prefixes, and unchanged-key propagation. Verify raw nullable Boolean schemas.
* Test positive membership retains its Boolean rule, unsupported correlation stays
  refused, aliases cannot collide, and non-immutable hidden subqueries stay refused.
* SQL SELECT, INSERT SELECT, UPDATE/DELETE selectors and CREATE share normalization;
  retry/reopen/history and atomic two-source transactions match independent results.
* Measure no flag output for duplicate-count changes without truth transitions;
  global changes may widen scope and must be reported as such. No full node
  recompute is hidden. Both features, fmt/Clippy/private rustdoc/mdBook must pass.
* Complete R04b additionally needs correlated truth and Q35's numerical gate;
  R06 additionally needs reusable descriptors and range/global propagation proof.

## 7. Rollout / migration

Commit this reviewed design/index before RED and source changes. Implement the
uncorrelated relation lowering, its independent truth/global propagation tests,
then the partition/dependency follow-on. Update only the former refusal assertions
whose exact new truth and scope are qualified; keep independent refusal tests for
remaining unsupported contexts. R04b/R06 stay open throughout partial rollout.

## 8. Open questions

Typed comparison coercion, non-equality correlation, nested scope ownership and
range/global cost selection require subsequent evidence. A globally affected
bucket can contain all preserved rows; necessary output and avoidable intermediate
work must remain separately measurable. Full-TPC and controlled performance are
independent R10 gates.
