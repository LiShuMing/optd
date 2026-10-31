# Incremental GROUPING SETS through ordinary aggregate branches

**Status:** accepted · 2026-10-31; implemented and locally verified; Q22 retains the R03e Float64 AVG blocker
**Scope:** R03c GROUPING SETS / ROLLUP / CUBE normalization with exact grouping
identity, existing accumulator and scoped algorithms, and independent TPC-DS
qualification. No dependency, numerical policy, persisted format or SQL membership
change. Unsupported aggregate/type/window boundaries retain their own gates.

## 1. Problem

The accumulator and changed-group algorithms reject `Expr::GroupingSet`. A real
NULL key and a subtotal's synthetic NULL are distinct groups even when their
visible keys coincide. Maintaining only visible grouping keys merges these groups,
and removing the last input must retain a grand total's empty aggregate output.

The actual nine TPC-DS query files containing ROLLUP are Q5, Q14, Q18, Q22, Q67,
Q70, Q77, Q80 and Q86. They require separate original query identities and fresh
qualification; a textual removal of ROLLUP is not an incremental implementation.

## 2. Constraints and evidence

R03b (`88b36bd`) supplies ordinary exact aggregates, DISTINCT membership and
key-only group refcounts under existing Optd accumulator declarations/operations.
Linear projection and UNION ALL already propagate signed changes. Graph interning
shares identical source/child plans and preserves repeated union input positions.
Use these contracts rather than introduce an interpreter inside rewrite rules.

The pinned DataFusion aggregate schema contains the unique grouping expressions,
then its typed internal `__grouping_id`, then aggregate outputs. Low identity bits
mark omitted grouping expressions, with the rightmost expression at bit zero;
higher bits distinguish repeated occurrences of the same grouping pattern. The
GROUPING expression interprets only semantic mask bits. Preserve this existing
schema/identity contract and exact parent references, qualifiers, types, nullability
and weak functional dependencies. This is an API/semantic reference, not copied
implementation.

## 3. Options considered

A new physical grouping-set accumulator would need grouping identity, expression
expansion, per-set cardinality and group-output logic inside the existing state
implementation. Evaluating the complete grouping-set node on each change is a
full fallback. Instead lower one logical grouping-set aggregate to ordinary
aggregate branches and UNION ALL: each branch naturally owns isolated per-set
state, including empty grand totals, and existing Optd rules implement its deltas.
This introduces explicit plan fanout but reuses verified physical algorithms.

## 4. Decision

### Logical normalization and rule boundary

Add a focused graph-normalization leaf module and a named
`normalize_grouping_sets` transformation before graph rule selection, after the
existing deterministic SQL analysis/optimization. Keep original SQL for reporting,
persistence and independent full-query oracles. A normalization produces logical
expressions only; it never executes aggregate/state work. Existing incremental
rules and their Optd programs continue selecting and executing physical state.

Normalize bottom-up. Extract grouping expressions in the pinned schema order,
enumerate explicit sets, ROLLUP prefixes and CUBE subsets, and preserve duplicate
set occurrences. Mixed grouping forms already canonicalized by the SQL planner
must preserve its Cartesian set semantics. Check shifts, ordinal packing and
schema identity; never truncate masks or silently drop duplicate sets. Respect
pinned planner limits rather than inventing an unbounded eager expansion policy.
If an identity/schema cannot be proved representable, preserve explicit refusal
instead of emitting an unsound incremental plan.

For every occurrence, construct an ordinary aggregate over exactly that set's
expressions and the original aggregate expressions. Then project:

* included grouping keys from that branch's group outputs;
* omitted keys as typed NULLs, including nullable projection of original NOT NULL
  keys, preserving the original grouping-set output fields;
* the exact typed internal grouping identity, including its duplicate ordinal;
* branch aggregate outputs at their original positions/names.

Use UNION ALL with the original aggregate schema; a single occurrence returns its
identity projection directly, since a union requires at least two inputs.
Branch position/internal identity
separate synthetic NULL groups from real NULL groups. Identical branch plans may
share execution, but repeated union ports preserve their SQL output multiplicity.
Never use distinct UNION or discard internal identity because the outer SELECT
happens to omit GROUPING. No original internal ORDER BY/LIMIT/FETCH is removed.

For an empty grouping set with no aggregate functions, use a private COUNT(*)
solely to create the one-row empty/global cardinality behavior and project it away.
Do not replace it with grouping on a constant, which loses the empty-input row.
Do not normalize an unrelated empty-key/empty-function ordinary aggregate.

### Execution, state and work

Every ordinary branch is independently checked by the existing optimizer's exact
aggregate/key/modifier guards. Eligible branches use accumulator states, scoped
branches retain explicit existing algorithms, and unsupported branches remain
visible strict-plan blockers. Normalization must not relax floating SUM/AVG, volatility or canonical-function
checks. GROUPING masks must resolve through the exact internal grouping identity.

Group identity belongs to branch state plus its internal projected ID. Ordinary
nonempty grouping sets remove zero-row groups; a grand-total branch retains native
empty COUNT/SUM/AVG/MIN/MAX output. Cached Before rows and signed UNION deltas give
exact overall output changes, including equal visible rows from different sets.
Prepared/discarded/stale/failed candidates preserve all branch states atomically.

Use existing per-node `AccumulatorWork`, state-copy and source/cache/SQL/store
counters. Multiple sets legitimately update multiple states per changed input row;
measure this fanout, separate unchanged groups and necessary output changes, and
make no end-to-end performance inference. Explain plans must show the actual
ordinary aggregate/identity projections and union input positions. No derived
operator state or new persistent schema is introduced; reopen normalizes the
original committed SQL again under the existing checked bootstrap protocol.

## 5. Failure modes and recovery

Malformed grouping metadata, identity overflow, conflicting expressions or schema
mismatch fail before CREATE MV publication. Preserve strict refusal for any
remaining unsupported branch, with its actual node/rule. Parent expressions must
resolve the original internal identity after lowering; do not guess aliases or
weaken schema checks to make planning succeed. Full-row changes and every branch's
state update remain part of the same private refresh/publication candidate.

## 6. Acceptance criteria

* RED regression proves ordinary exact ROLLUP/GROUPING SETS currently select full
  recomputation or strict refusal; green exposes ordinary accumulator branches,
  typed grouping identities and UNION ALL with no hidden whole-node fallback.
* Independent full SQL/schema and explicit small grouping-set multiset oracles
  cover real/synthetic NULLs, GROUPING argument order, computed/nullable keys,
  NOT NULL sources, duplicates, repeated sets and sets whose visible rows coincide.
* ROLLUP, CUBE, mixed grouping forms, empty sets, empty/all-NULL inputs, group-only
  sets, grand-total lifetime, HAVING, DISTINCT and mixed native/extrema aggregates
  preserve semantics through inserts/deletes/key moves and output delta replay.
* Check typed identity boundaries, duplicate ordinal packing, schema/qualifier
  preservation, invalid/refused plans and retained unsupported numerical guards.
* Hold change size and required output fixed while growing untouched keys; assert
  zero historical aggregate input work for eligible branches and bounded copied
  paths. Account per-set fanout rather than interpreting node counts as speed.
* SQL CREATE MV and INSERT/DELETE/UPDATE, original request retry, reopen and output
  history, randomized/discarded/failed/stale candidates preserve atomicity.
* Default/Iceberg fmt/Clippy/tests, private rustdoc, mdBook and report tools pass.
  Freeze a default binary and independently run the nine named TPC-DS queries on
  their existing directed fixtures, with strict plans, three SQL/full-oracle rounds,
  reopen and output replay. Preserve original failed profiles; enumerate any
  additional non-grouping blockers and their dependent roadmap item explicitly.
  Do not claim all nine strict SQL passes when any remains refused or unexecuted.
* Full-TPC and controlled performance qualification remain separate R10 gates.

## 7. Rollout / migration

Commit this accepted design and index link before implementation. Add the RED
plan test, then logical normalization and independent semantic/work regressions.
Replace old grouping-set refusal assertions with genuinely unqualified numeric
cases while retaining the original grouping-set SQL under new passing regressions.
Keep old evidence and do not regenerate baselines to conceal a difference.

## 8. Local source/design review

Inspected graph normalization/interning, aggregate rule and native-state guards,
DataFusion's grouping schema/internal identity contract, original nine SQL files
and existing directed fixture manifests (all nine are present). The critical
tradeoffs are explicit logical fanout, schema-exact typed NULL/ID projections,
duplicate occurrences, empty grand-total behavior and refusing unrelated types.
The normalization never substitutes a simpler benchmark query. Source/design review is complete. Implementation and local verification are
recorded below; source review alone is not their acceptance evidence.


## 9. Implementation and observed verification

Logical lowering lives in `optd-incremental-sql/src/graph/grouping_sets.rs`, called
before graph/rule compilation. It preserves field metadata in aliases, projects
NOT NULL keys through a nullable typed expression where required, and avoids a
single-input UNION. No runtime interpreter or new persisted state was added.

The R03c record in `ROADMAP.md` contains exact environment/commands and failure
logs. Final default/Iceberg gates exited 0 (257/271 passed, 2/3 ignored), as did
warnings-denied private rustdoc, 41 report tools/fixture checks and mdBook. Nine
integration cases include independent full physical SQL/schema and randomized
integer-identity oracles, SQL recovery/history and fixed-delta scope assertions.

Fresh frozen-runner qualification independently classified all nine named queries:
eight passed three strict SQL/full-oracle rounds with reopen/history; Q22 was
refused for five Float64 AVG branches (R03e). The refusal has zero rounds and is
not counted as success. Explicit classification and source/binary/command/report
identities are under `target/roadmap-r03c-grouping-live-20261031/`.
This evidence covers directed local fixtures and grouping-set normalization;
full-TPC, floating qualification and controlled performance remain separate gates.
