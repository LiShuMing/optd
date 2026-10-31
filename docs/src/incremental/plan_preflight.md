# Complete incremental plan preflight before bootstrap

**Status:** accepted · 2026-10-31; implemented and locally verified
**Scope:** R09c schema-bound planning, complete blockers within the admitted SQL
graph, automatic strict CREATE MV rejection before source materialization/bootstrap,
and SQL-database benchmark eligibility. No numerical, persisted or dependency change.

## 1. Problem

Strict rewriting stops at its first complete recomputation. The benchmark instead
constructs a non-strict SqlView, executing all bootstrap operators merely to inspect
the plan. Q22 now has five known Float64 AVG blockers but its generic reason does
not identify their numerical policy. Planning evidence should require no data work.

## 2. Constraints and evidence

R03c is committed at 733a0eb. Rule selection and Optd lowering are separate from
executor/state bootstrap. Typed schemas are enough to bind logical expressions.
Caller-supplied rules and physical plan rules must retain identity and diagnostics;
preflight must inspect their final physical operations, including injected Replace.
The first-node Error::Rewrite envelope remains available in strict mode too;
Error::plan_preflight() retrieves its complete structured source report.
Existing non-strict missing-rule errors retain their typed Error::Rewrite contract.

## 3. Options considered

Executing an empty non-strict view still runs operators and creates state. A second
independent eligibility implementation can drift. Extract the actual compilation
pipeline into one planning-only module, collect each node's failure, and use the
same result for bootstrap and read-only preflight instead.

## 4. Decision

Expose SqlView::preflight(sql, schemas, optimizer) and SqlSession::preflight(query)
returning a serializable PlanPreflight with deterministic postorder nodes, actual
physical strategy when compilation succeeds, and every blocking node. Blockers
retain node/operator, rule identity, phase and concrete details. Aggregate admission
and explanations share qualification predicates; include function, argument/output
types, modifiers and key policy. Join/window blockers explain missing or unsupported
key/partition closure. Ordered/limited nodes retain their semantic reason.

Use the same immutable registry and physical rules. Suppress only early strict
policy refusal during analysis, never a failed matching/rewrite/verification rule.
Compile each admitted graph node independently, using schema-bound symbolic After
inputs; retain errors while inspecting siblings and parents. A failed binding is
represented only in diagnostic continuation, never installed or executed. Any rule
or physical error prevents constructing an executable IncrementalPlan. A strict
plan with any Replace is refused with the complete structured report before source
Bag conversion, state bootstrap or database publication. Non-strict execution may
retain existing recomputation behavior but still rejects malformed rules.

Parse/binding, volatility, normalization and unsupported graph/expression failures
remain typed errors before a complete admitted graph exists; do not label such an
incomplete analysis eligible or invent missing nodes. Preflight is structural
qualification, not a promise that arithmetic, resource limits or bootstrap will
succeed on future data. Normal SQL optimization still folds immutable constants;
preflight does not evaluate source rows or run DataFusion physical operators. Schema-only and data-bound compilation must agree in the
covered default SQL optimizer, with explicit empty/nonempty parity tests.

SqlSession preflight is read-only and uses committed base-table schemas. Reject it
inside a transaction to avoid ambiguous schema/visibility scope. CREATE MV uses the
same automatic strict compiler; add a schema preflight before dependency source
materialization. Rejected CREATE publishes no catalog, receipt or epoch. The SQL
database benchmark uses preflight instead of constructing a non-strict probe;
retain complete_recompute_blockers compatibility while adding typed details.
Report tools retain unique-profile known refusals even without semantic success;
multiple profiles retain per-profile preflight evidence without an arbitrary choice.
Strict rejection, actual complete-node replacement and failed compilation have
separate counts; a missing implementation must not masquerade as measured recomputation.

## 5. Failure modes and recovery

No report is executable or persisted. Failed rules are not bypassed by later rules.
The complete report cannot turn a refusal into fallback. Existing catalog/output,
CURRENT, request retry state and sources remain unchanged after refusal. Reopen
rebuilds using the same strict compilation. Diagnostic continuation never grants
state ownership or produces runtime rows.

## 6. Acceptance criteria

* RED proves one ordered/limited/float/unsupported-window query only reports its
  first blocker or executes bootstrap while probing. Green reports all actual nodes.
* Independent expectation of multiple Float64 AVG/variance/ordering/window scopes;
  exact function/types, deterministic node IDs and schema-only/full-plan parity.
* Custom unmatched, throwing, malformed and physical Replace rules remain blocked;
  collect independent sibling errors without installing partial programs.
* A query that fails if evaluated (division/cast) can be preflighted without row
  execution; no source/state/relation construction is necessary.
* Failed ordinary CREATE leaves epoch, CURRENT bytes, file counters and catalog intact;
  identified retries may read existing request-history metadata but never write
  files or materialize source/view state;
  valid SQL still passes DML, reopen and output-history checks.
* Default/Iceberg format/Clippy/tests, private rustdoc, report tools and mdBook pass.
  Fresh Q22 reports five numerical blockers with zero bootstrap rounds; an accepted
  Q28 witness still passes strict SQL/full-oracle/reopen/history.
* Full TPC and controlled performance remain R10 acceptance gates.

## 7. Rollout / migration

Commit this design/index before implementation. Extract compilation without changing
SQL semantics, then add complete diagnostics and read-only APIs, integrate CREATE
and the SQL database harness. Preserve previous refusal reports and count refusal
as classification only. No stored schema or receipt migration.

## 8. Local source/design review

Reviewed engine graph construction, rewrite/physical lowerers, exact aggregate and
group admission, database CREATE publication order and benchmark probe execution.
The key invariant is that a diagnostic continuation is never an executable partial
plan. The owner's continuous implementation instruction authorizes this local
increment. Implementation and observed verification are recorded below.


## 9. Implementation and observed verification

`src/planning/` holds the common planning-only compiler, report types and typed
restriction context. `SqlView::preflight` and `SqlSession::preflight` bind schemas;
CREATE MV preflights before dependency materialization. Actual bootstrap compiles
again against its real registered schemas. Caller physical rules are inspected,
including Replace introduced after a native frontend. Diagnostic placeholders
cannot be installed: compilation returns either the whole plan or an error.

`ROADMAP.md` R09c records exact commands, original failed test assumptions and final
logs. Six new integration tests preserve existing rule/error contracts, compare
schema-only and full-data plans, enumerate independent source failures and physical
replacements, prove no source-row evaluation, and verify failed CREATE/retry leaves
no writes/catalog changes. Identified retry metadata reads remain measured R07c work.
Report tools preserve unique-profile refusals without semantic success, retain
ambiguous profile evidence and distinguish strict rejection, failed compilation
and complete-node replacement.

Final gates: 263 default / 277 Iceberg tests pass (2/3 explicitly ignored), both
Clippy configurations/fmt, private rustdoc, mdBook and 44 report tools/fixture checks
exit 0. Frozen default-runner Q22 preflight lists five Float64 AVG/R03e blockers
with zero execution rounds; Q28 passes three strict SQL/full-oracle/reopen/history
rounds. Artifacts: `target/roadmap-r09c-final-preflight-live-20261031/`.
These are local directed witnesses; full TPC and controlled performance remain R10.
