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

When both original comparison operands are nonnullable, membership equals the
nonnullable matched marker's IS NOT NULL test even for empty S. Omit the flags
branch in that proven case; this avoids an unnecessary global dependency and
preserves the original nonnullable Boolean field.

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

## 9. Correlated equality partitions: reviewed follow-on

**Status:** accepted implementation design · 2026-10-31 after local typed-plan and
algebra review. This section precedes correlated source changes and does not claim
correlated implementation acceptance.

Start with a single compared output over a Projection/Filter/SubqueryAlias chain
ending in one uncorrelated input. Extract only conjunctive `inner_column =
outer_column` predicates with identical qualified equality types and ordinary
NULL equality. Keep every inner-only predicate. Reject outer references in any
other position, non-equality/null-safe predicates, correlated aggregate/window/
limit/distinct/set/join boundaries, unproved scope or comparison coercion. Bounds
cover extracted keys, original expressions, generated copies and helper names.
No outer reference may remain in the extracted relation.

Carry each extracted inner key through projections with fresh hidden aliases and
through alias boundaries with its new qualifier. Preserve the original compared
output as position zero. Build a compared relation with unique helper columns
for value and correlation keys; the parent input retains the outer key columns.

Group distinct non-NULL members by `(correlation keys, compared value)` and LEFT
JOIN original rows on all keys plus the scalar comparison. Group total/non-NULL
counts by correlation keys, project only a nonnullable presence marker and
`has_null`, then LEFT JOIN these flags using correlation keys alone. A missing
flags row is an empty comparison partition, including a NULL ordinary outer key.
CASE ordering stays matched TRUE, absent partition FALSE, left NULL/partition
has_null UNKNOWN, otherwise FALSE. NOT negates the complete result. If both
compared operands are nonnullable, the matched marker alone remains sufficient.

The two joins intentionally have different scopes: member changes touch comparison
keys inside one partition; first/last partition NULL or empty transitions may
change all preserved comparison keys in that partition. Unchanged partitions must
never be looked up or recomputed because another partition's flag changed.
Duplicate count changes with unchanged member/flag truth emit no downstream delta.
These concrete joins prove execution scope only; reusable R06 descriptors remain
required separately.

Qualify projection and negative/disjunctive predicates with independent nested-loop
partition truth and signed prefixes; cover composite/NULL correlation keys,
filtered values, missing/all-NULL partitions, duplicates, key moves and two-source
atomic epochs. SQL selectors/INSERT SELECT/CREATE, retry/reopen/history and schema/
alias safety share the same normalizer. Test actual access counters while growing
untouched partitions, and prove a partition-wide transition changes unchanged
comparison keys in exactly that partition. Unsupported correlations must still
refuse before any publication or request receipt.

## 10. NULL-safe and mixed correlation policies: reviewed follow-on

**Status:** accepted implementation design · 2026-10-31 after key-policy and
truth-case review. This section is a separate follow-on to ordinary equality.

Recognize only direct `inner_column IS NOT DISTINCT FROM outer_column` in addition
to ordinary equality, retaining an explicit per-key Ordinary/NullSafe policy.
Comparison membership itself always retains SQL `=` semantics. Unknown/computed
policies remain refused. No new physical state or runtime interpretation is needed.

DataFusion's typed join has one NULL-equality policy for its whole key vector.
For an all-ordinary correlation, preserve the existing ordinary joins unchanged.
If any correlation key is NULL-safe, remove right rows having NULL in any
*ordinary* correlation key before building both member and flag relations, then
use NullEqualsNull for both joins. Such removed rows can match no outer row under
that ordinary predicate. Remaining ordinary right keys are non-NULL, so enabling
NULL equality cannot create a match for an outer ordinary NULL key. The NULL-safe
keys now compare correctly, including NULL/NULL. This proves mixed vectors.

The member relation separately filters NULL compared values before grouping.
Its compared join key is therefore non-NULL, so whole-vector NullEqualsNull cannot
turn a scalar NULL comparison into a true member match. Flags use only correlation
keys. Matched/empty/UNKNOWN/false CASE ordering is unchanged: a NULL-safe matching
partition containing only NULL compared values produces UNKNOWN, never TRUE;
a missing partition produces FALSE even for a NULL compared scalar.

Qualify each NULL-policy combination against independent partition truth, including
both NULL keys, mixed vectors, first/last NULL values, duplicate counts, absent
partitions, two-source moves, signed prefixes, SQL writes and retry/reopen/history.
Keep the ordinary scope regression; add fixed-delta growing-untouched NULL-safe
partition checks. Replace only the newly qualified direct NULL-safe refusal case
with exact oracle tests; retain computed NULL-safe, range and row-changing scope
refusals. Full R04b/R06/numerical/performance gates remain independent.

## 11. Share identical membership bindings within one row-preserving boundary

**Status:** accepted optimization design · 2026-10-31 after row-preservation and
scope review. Implement and qualify after the correlation policy slices.

A Projection may observe both `e IN S` and `e NOT IN S`, or repeat the same IN.
Generating separately named helper relations duplicates count/member states and
propagates one truth change through another equivalent membership's joins.
Maintain a node-local map keyed by the complete typed InSubquery with negation
canonicalized to false, including compared expression, subquery plan and scope
metadata. Bind the positive truth once and apply NOT separately for each caller.

All generated joins preserve every original input row exactly once: members are
unique per full comparison key and flags are unique per dependency key. Before a
Filter evaluates its final predicate, these joins only append helper columns;
original scalar values and multiplicities stay unchanged. Original immutability
checking excludes call-count-sensitive SQL functions. Therefore reusing the same
bound truth within that one input boundary is sound. Never reuse bindings across
Projection/Filter nodes, different compared expressions/plans or nested scopes.
Map iteration order is never observable; only keyed lookup and insertion occur.

Expansion limits count actual distinct bindings; input/alias/node bounds remain
mandatory. Replace the former 65-identical-expression overflow witness with 65
*distinct* subqueries, and separately prove 65 identical IN/NOT IN observations
share one member/flag binding with exact output schemas and signed results.
Retain independent ordinary/mixed-correlation truth, SQL/recovery and refusal tests.
Measure operator state/plan counts and actual peer visits before/after at fixed
changed partition and increasing untouched input; this is planning/operator work
evidence, not elapsed-time or end-to-end performance qualification.
