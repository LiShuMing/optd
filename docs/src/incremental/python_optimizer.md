# Python relational optimizer and DBSP backend

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

**Status:** accepted · 2026-10-29
**Scope:** reusable Python relational optimization framework, a bounded SQL frontend,
incremental physical choices and executable differential tests. This extends the Python
operator experiment; it does not replace the existing typed Rust SQL runtime.

## 1. Problem

The first Python experiment hand-builds eight query-specific circuits. These establish
operator evidence, but do not form a reusable optimizer or compile arbitrary query text.
The next step is SQL/relational IR -> rule optimization -> costed physical plan -> DBSP.

## 2. Constraints and evidence

The local optd design (`docs/src/optimizer.md`, `docs/src/rule_engine.md`) separates logical
equivalence rules, memoized alternatives, physical implementations, statistics and costs.
`optd-core/src/cascades/memo.rs` and `optimizer.rs` implement its memo/task architecture.
Feldera's Calcite frontend separately lowers SQL into a circuit and incrementalizes that
circuit. Adopt these separations without claiming to port an entire Cascades/Calcite engine.
No local StarRocks checkout was supplied or searched; it is architectural background only.
Python uses standard-library dependencies and the existing in-memory `ivm.py` runtime.

## 3. Options considered

* Query-specific plan generators: retain as independent reference fixtures, not the optimizer.
* Full SQL grammar/type inference or a Rust binding first: too broad for this validation.
* Immutable generic scalar/relational IR, explicit rule registry, bounded memoized search,
  cost/statistics interfaces and a backend adapter: selected. The SQL subset is a separate
  adapter; callers can construct IR directly for other relational operators.

## 4. Decision

### Generic IR and semantics

Expressions have column references, literals and explicit SQL operations with three-valued
NULL logic. Relations include Scan, Filter, Project, inner/left/full/semi/anti Join,
Aggregate, Distinct, Union and Sort/Limit. Stable named column identities survive join
commutation; insert a restoring projection where physical tuple order changes. Reject
unknown/ambiguous columns and unsupported functions before mutating execution state.
Only deterministic expressions can be moved. Outer joins, aggregates, limits and subqueries
are rewrite barriers unless a specific proven rule applies. Do not move a WHERE predicate
under an outer join by generic inner-join logic.

### Optimizer

Separate deterministic normalization (split/push filters, eliminate identities and safely
compose projections) from exploration/implementation. Memoize candidate plans and costs;
enumerate bounded inner-join alternatives using commutation/association, never outer joins.
Keep the original schema at the boundary and explicit search budgets. Record applied rules,
alternative counts and the selected plan in EXPLAIN. A plugin registry enables new rules;
this is a small memoized optimizer, not a claim of complete Cascades task equivalence.

Statistics include cardinality and column distinct counts. Costs distinguish bootstrap,
expected epoch maintenance and retained state. Model the anticipated update frequency;
estimated cost is a heuristic, and measured work must be reported separately. Physical
equi-join alternatives are indexed delta probing and unindexed nested-loop delta scanning.
Both obey the same synchronous delta identity; implement both so the selected strategy is
executed, not just annotated. The loop implementation scans retained opposite inputs;
record those rows/pairs as real work. Reordering across equality regions must preserve all
predicates, duplicates and ordinary NULL non-matching semantics.

### Frontend and backend

Expose `parse_sql(sql, catalog)` for a documented SELECT subset: named tables/aliases,
comma/inner joins, WHERE, projections/arithmetic, aggregate functions, GROUP BY, DISTINCT,
ORDER BY and LIMIT. Support the date expressions needed by actual TPCH Q1/Q3 and TPCDS Q3.
Reject CTEs, correlated/derived subqueries, windows and unsupported grammar explicitly.
These features can later enter through a Calcite/DataFusion/sqlglot adapter into the same IR;
no source-string query-ID dispatch is allowed in the frontend or optimizer.

The DBSP backend binds named schemas to tuple indexes once and compiles any supported IR
node to existing delta operators. Retain named input identities for self-joins. Group
aggregation and partition TopK retain their honest affected-group execution strategy.
Expose final ordered result rendering separately from materialized bags. This first version
does not support arbitrary ordered intermediate subplans; reject shapes whose semantics
cannot be represented by this backend.

## 5. Failure modes and recovery

Parser/name/type/operator errors fail before building an active circuit. Bounded search
returns the best explored candidate and records exhaustion; it does not run indefinitely.
Bad statistics can select a slower plan but cannot change results. SQL NULL, duplicate bags,
global empty aggregates and ties are semantic gates. The Python backend retains the original
prototype's nontransactional callback-failure boundary; no durable recovery claim is added.

## 6. Acceptance criteria

* Independently review this design before implementation.
* Parse actual full TPCH Q1/Q3 and TPCDS Q3 source SQL through the generic frontend; optimize
  and execute both the original and selected plans through the DBSP backend. For every
  fixture epoch compare complete output and signed delta against independent SQLite SQL.
* Use unseen schemas/queries and seeded NULL/duplicate/multitable changes to verify generic
  behavior, rule correctness, schema restoration, outer-join barriers and rejection paths.
* Demonstrate at least one changed join order and an actual measured work reduction on a
  controlled selective workload. Exercise both physical join implementations and enforce
  exploration budgets; predicted costs and observed counters remain separate evidence.
* Record source hashes, exact commands, versions, search trace, before/after plans and tests.
  Full 22/99 compilation, complete SQL typing, windows/subqueries/CTEs and durable state
  recovery are visibly outstanding, not counted as passed.

## 7. Rollout / migration

Keep implementation local under `dbsp/`, preserve original files and existing Rust paths.
No dependency installation, commit, push, merge or deployment is part of this experiment.
Future work can substitute a mature SQL frontend and move qualified algorithms into the
existing Rust graph without changing the optimizer/backend separation.

## 8. Open questions

Typed DECIMAL/overflow/collation contracts, range joins, decorrelation, physical property
enforcers, shared arrangements, statistics feedback, adaptive reoptimization and durable
operator state need separate extensions and independent acceptance evidence.

Independent design review on 2026-10-29 found no blocking issue for this scope.
Implementation must share physical table sources across aliases, preserve all residual
predicates/schema during join search, distinguish weighted rows from distinct tuples,
and include candidate construction in the exploration budget with an original fallback.
