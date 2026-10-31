# Rule-based Incremental Plan Rewriting

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** accepted · 2026-10-29 (three independent agent reviews; clarifications incorporated)
**Scope:** extensible typed SQL maintenance planning, executable signed-change expressions,
explicit recomputation, plan explanation and recovery with caller-selected rules.

The [Optd plan/execution design](optd_plan_execution.md) supersedes this document's
interpreter and dependency decisions as of 2026-10-30. `Maintenance` and
`DeltaExpr` remain compatibility-rule input; expanded Optd logical/physical
plans now determine execution. The registry and transaction contracts remain.

## 1. Problem

The SQL graph selects maintenance using operator matches inside its executor. Source,
grouped aggregation and native join branches independently decide their implementation.
A registry that only supplies a strategy label would leave those branches unchanged.
Users need independently registered transformers whose output determines actual execution.

## 2. Constraints and evidence

* Preserve the typed DataFusion 55 graph, immutable multi-table prepare, ordered sequences,
  source-vector validation, output/delta semantics and checkpoint formats 1/2.
* Reuse existing arithmetic and expression execution. No new dependencies, toolchain changes,
  copied external implementation, internal identifiers or internal source excerpts.
* General design references suggest separate matching, replacement construction, versioned
  input references, staged binding and an explicit completeness verifier. All implementation
  is independently written for this repository's schemas and signed bags.
* The graph caches complete relations; source acquisition, cloning, materialization and
  local recomputation remain real work. This change makes no performance claim.
* More than eight files are expected. The planner, execution integration, tests and source
  coordinator/documentation have separate canonical writers.

## 3. Options considered

1. Move the existing strategy match into a trait: smallest patch, but rules would not construct
   or control executable delta plans. Reject.
2. Reuse the legacy optimizer representation: it has a deliberately smaller value/expression
   model and cannot retain the qualified typed SQL contract. Reject for this path.
3. Compile the existing typed graph into a signed-expression maintenance plan using a
   deterministic registry. Select. Keep a small checked execution algebra, with independent
   operator rules building combinations of it.

## 4. Decision

```text
DataFusion analyzed / optimized logical plan
  -> typed graph with immutable source bindings and ordered properties
  -> one delta request per logical node
  -> deterministic rule matching and replacement construction
  -> arity / schema / reference / ordering / capability verification
  -> immutable executable IncrementalPlan
  -> existing prepare -> publish -> acknowledge
```

### Rules and registry

A public `IncrementalRule` has a stable name, version and priority, separate `matches`
and `rewrite` methods, and a read-only `RuleContext`. The context exposes the logical
operator, node/input identities, ordered property and verified group-index eligibility.
Rules cannot mutate source caches, operator states or checkpoint storage.

An immutable, cloneable `IncrementalOptimizer` contains `Arc`-owned rules. Names must be
nonempty and unique. Selection orders descending priority then ascending stable name.
The first matching rule supplies the replacement; matching or rewriting errors are fatal
and retain rule/node identity. No match is an error. Explicit rule removal permits tests
and applications to reject implementations they do not want.

### Executable expressions

Each compiled node contains a selected rule identity, explanation, original input IDs,
ordering property and a `Maintenance` expression:

* `Delta(expr)` executes a signed relation expression.
* `ChangedGroups` updates the existing derived group index and recomputes touched groups.
* `Recompute` executes this logical operator on maintained child relations and diffs output.
  A TableScan has a distinct raw-source dependency: bind its full candidate source-after,
  preserving pushed filters, projection and fetch, never its original registered provider.
* `Constant` retains a source-independent constant relation.

Signed `DeltaExpr` primitives are local-port `InputDelta`, `InputSnapshot(Before/After)`,
`SourceDelta`, `Linear(inputs)`, `Join(left, right)`, `Sum(terms)`, `Negate(expr)` and typed
`Empty`. `Linear` executes the current operator independently on positive and negative
parts. `Join` is its bilinear signed lifting, combining signs correctly. These primitives
use the current node's original expressions and qualified schemas, never registered
original tables. `SourceDelta` has the raw source schema; an ordinary scan compiles to
`Linear([SourceDelta])`, which performs its pushed projection/filter. Scan dirty detection
uses the source dependency even though it has no graph-child ports. `Sum` and `Negate`
use checked bag arithmetic.

The inner-join rule emits
`Sum(Join(Delta(0), Before(1)), Join(After(0), Delta(1)))`.
An independently registered rule can instead emit
`Sum(Join(Before(0), Delta(1)), Join(Delta(0), After(1)))`.
Both formulas count the cross term once, and the interpreter executes the selected tree.
This is executable rewriting rather than a join-order label interpreted by another hidden
operator selector. Additional algorithms can combine existing primitives without changing
the refresh driver; a genuinely new stateful primitive needs its own checked interpreter.

Built-in rules explicitly cover constants, ordinary source deltas, fetch scans, each linear
operator family, inner joins, eligible groups, ordered chains and the enumerated nonlinear
operators already supported. They do not include a universal unknown-operator fallback.
DataFusion subquery lowering and immutable-expression checks remain in force.

### Verification and extension trust

Before SQL graph bootstrap execution, verify all compiled nodes, dependency order, local-port
bindings, exact Arrow schemas, expression arity, bounded expression depth/size, and legal
primitive/operator combinations. Empty expressions infer their schema from the expected
typed port (the node output at the root), and every nested expression is checked against
that port. Native expressions cannot bypass ordering-sensitive
maintenance. ChangedGroups requires its prechecked group-key/function eligibility; failure
to construct the expected index is an error, not an unreported strategy change.

Custom rules are trusted semantic extensions. Structural verification prevents malformed
references and incompatible execution, but cannot prove an arbitrary custom algebraic
identity. Extension authors must supply independent result/delta oracle tests. No serialized
diagnostic plan is accepted as an executable program.

### Runtime, explanation and recovery

`SqlView::new` delegates to the default optimizer; `new_with_optimizer` accepts a caller's
registry. `incremental_plan()` exposes an immutable diagnostic plan, whose explanation
includes rule/version/priority, inputs, strategy, expression tree and selection reason.
Refresh statistics identify the selected rule and actual maintenance strategy.

The executor dispatches only on compiled maintenance/expression primitives. It does not
repeat a logical-operator strategy match. Ordered invalidation, candidate state isolation,
and publish-before-acknowledge remain unchanged.

`DurableSqlView::open_with_optimizer` and source-aware bootstrap/reopen variants carry the
chosen optimizer through reconstruction. Existing entry points retain default behavior.
Compiled plans and derived group indexes are rebuilt from committed SQL/sources; output
is checked as before. Rule identity is not added to the stored protocol: a valid registry
may choose another equivalent implementation after restart. This does not certify identical
rule identity across restarts. Callers needing custom choices must supply them again.

## 5. Failure modes and recovery

* Duplicate rule identity, no match, rule error or invalid replacement: construction fails
  before source/operator state becomes visible.
* Unsupported operator: explicit planning error; no default whole-query recomputation.
* Malformed port/schema, illegal signed primitive or ordered native rewrite: verifier error
  naming rule/node; no candidate is installed.
* Arithmetic, input or execution failure: prepare fails with the prior epoch intact.
* Publication/crash behavior follows existing checkpoint protocols; rewrite compilation
  neither publishes state nor changes source consumption positions.
* A semantically wrong but structurally valid custom rule is an extension bug; independent
  oracle tests are the acceptance gate, not strategy labels or rule names.

## 6. Acceptance criteria

1. New tests inspect real plan expressions and independently verify multi-round contents and
   signed deltas for linear operators, simultaneous two-sided joins and self joins.
2. A custom reverse join identity and a custom local recomputation rule change actual compiled
   execution. Deterministic priority/tie ordering, duplicate names, rule removal, no match,
   match/rewrite errors and invalid plans are covered. A structurally valid, deliberately
   non-equivalent Empty/Negate test proves that the interpreter actually follows a custom
   expression; that test is execution-authority evidence, not a SQL-correctness claim.
3. Invalid references, schema/arity mismatches, ordering violations and unqualified grouped
   actions fail before visibility. Existing NULL/duplicates/Decimal/date semantics stay green.
4. Group work, bag-preserving sequence changes, windows/outer joins and explicit nonlinear
   recomputation keep their existing contracts.
5. Default and custom registries survive durable reopen and subsequent changes; source-aware
   bootstrap/reopen uses the supplied registry. Fetch scans retain their source-after
   dependency across multiple changes and strict reopen. Existing process-death tests remain green.
6. Fresh-target build, format, both default/feature warnings-denied lint and tests, workspace
   regression, Python tools and book build pass with exact evidence. Rerun fixed query suites
   on the new executable; retain failures and separate earlier qualification evidence.

## 7. Rollout / migration

Deliver one coherent increment: public planner and verifier, executable integration, source
and durable constructors, tests and usage/evidence documentation. Existing APIs default to
the built-in registry. No persisted format migration, new environment variable or service.
Rollback uses an ordinary revert without rewriting checkpoint data. Ordinary task-related
publication follows the existing continuous-task authorization and repository contract.

## 8. Open questions

No blocking decision remains for this increment. Cost-based alternative search, persisted
custom-rule identities, native aggregate accumulators, join indexes and distributed ownership
are outside this design and require separate implementation and evidence.

Observed implementation gates are recorded in the
[qualification report](./rule_rewrite_qualification.md), separately from design acceptance.
