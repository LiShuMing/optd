# Rule rewrite — observed qualification

**Status:** verified local evidence · 2026-10-29
**Scope:** executable typed SQL rewrite rules, custom extensions, local recovery,
and fresh query evidence on the rule-driven executor.

This records implementation evidence for the
[accepted rewrite design](./rule_rewrite.md). The
[usage guide](./rule_rewrite_usage.md) contains complete extension examples.
Earlier [source-loop qualification](./typed_iceberg_qualification.md) remains
historical evidence for its own executable, rather than a rerun of this change.

## 1. Implementation and extension boundary

The planner compiles the optimized typed SQL graph before bootstrap into an
immutable `IncrementalPlan`. Seventeen named built-in rules select checked
signed-expression trees, changed-group maintenance, explicit local recomputation
or source-independent constants. Refresh interprets the compiled maintenance
program; it does not repeat the old operator-to-strategy selection.

The public `IncrementalRule` interface separates matching from replacement
construction. Registrations freeze name/version/priority, reject duplicate names,
and deterministically sort descending priority then ascending name. Match,
rewrite and verification failures retain node and rule identity. Removing a
required rule causes construction to fail; no universal recomputation rule
silently takes its place.

The verifier checks local ports, exact Arrow schemas, input arity, topology,
ordering, primitive/operator legality, grouped-index eligibility, and inclusive
limits of 64 expression levels and 1,024 primitive occurrences. `SourceDelta`
uses the raw source schema; scans apply pushed filters/projection through
`Linear([SourceDelta])`. Fetch scans explicitly recompute from the candidate
source-after even though their logical graph has no child inputs. `Empty`
inherits its schema from each expected typed port.

Custom rules are trusted semantic extensions. The verifier rejects malformed
programs but cannot prove arbitrary algebraic identities. Rules and identities
are not persisted: callers pass the registry again to custom durable or Iceberg
reopen constructors, which rebuild the plan and check committed output. The
checkpoint format, dependency graph and declared toolchain contracts are unchanged.

## 2. Focused execution evidence

The dedicated registry/planner and execution targets contain 17 tests. They cover
deterministic selection, duplicate/removal/no-match behavior, fatal match/rewrite
errors, invalid plans and unchanged durable state after rejected reconstruction.
Actual refresh tests inspect selected rules and expression trees and independently
compare complete SQL output and signed deltas across multiple epochs, including
two-sided joins, self joins, NULLs and duplicate multiplicities.

Two custom join identities exercise both the reversed two-term formula and
`dL join Rbefore + Lbefore join dR + dL join dR`. A simultaneous duplicate
deletion has a hand-checked output weight of -3, requiring a positive
negative-times-negative cross term. Explicit custom recomputation overrides
both native joins and eligible grouped aggregation; the latter reports no
group-index work. Fetch scans use changed raw sources across multiple refreshes
and durable reopen. Deliberately non-equivalent `Empty`/`Negate` programs,
including Empty nested in both Join ports and a narrower-output Linear operator,
prove actual interpreter execution. Those deliberate programs are execution
authority tests, not SQL correctness evidence.

The real Iceberg custom-registry test checks bootstrap, two-table refresh,
failed reopen without the required rules while CURRENT stays unchanged,
successful custom reopen, overwrite/delete changes, independent full SQL,
hand-checked bags, complete source vectors and per-epoch output-log folding.

## 3. Environment and commands

The verified toolchain is Rust/Cargo 1.95.0 on Linux x86_64, with the repository
lockfile, DataFusion 55.0.0, Arrow 59.2.0 and the existing pinned Iceberg revision.
Builds use four jobs, debug information disabled, incremental compilation
disabled and offline Cargo resolution. The main regression target is
`/tmp/optd-sql-msrv-20261029-target`; reuse of that target is reported separately
from the fresh-target check.

The commands below run through `bash /tmp/optd-sql-env-20261029.sh`, which sets
the environment above. The fresh check additionally overrides `CARGO_TARGET_DIR`
after that wrapper.

| Actual command | Observed result |
| --- | --- |
| `env CARGO_TARGET_DIR=/tmp/optd-rule-fresh-check-20261029-target cargo +1.95.0 check -p optd-incremental-sql --features iceberg --lib --locked` | Exit 0 in an initially empty target, 2m36s. This is a fresh feature-library check, not a fresh all-targets build. |
| `scripts/agent-check.sh sql` | Exit 0 on the final tests; format, both all-target warnings-denied lint configurations; default 94 passed / 1 ignored, feature 107 passed / 2 ignored. |
| `scripts/agent-check.sh sql-tools` | Exit 0; offline pinned-source verification and 28 Python tests passed. |
| `cargo +1.95.0 check --manifest-path /tmp/optd-rule-doc-check-20261029/Cargo.toml --offline` | Exit 0; the two complete usage examples compile as an external consumer. |
| `scripts/agent-check.sh full` | Exit 0; legacy default 41 and feature 70 passed; workspace all-feature warnings-denied lint and 373 tests passed / 4 ignored / 1 explicitly filtered; isolated-input book build passed. |

The initial execution-test run failed because a test identified Projection using
a substring that also matched the scan's diagnostic `projection` attribute.
The test now checks actual operator prefixes and independently asserts the raw
scan dependency. The final SQL gate includes 8 planner + 9 execution + 11 real-source
tests, with one subprocess-only source driver ignored. The nested-Empty cases
force schema comparison through `Sum([Empty])` before a Join can skip an empty
input; the Linear case asserts an actual two-column input and one-column output.
No SQL benchmark query, fixture, expected output or implementation was changed to
hide this assertion failure.

The full gate explicitly filters the separate PostgreSQL performance fixture
`cli_run_cardbench_twice`. Existing ignored cases are two Parquet metadata tests,
the opt-in complete TPC-H fixture test and the source-coordinator subprocess
driver invoked by its successful parent recovery test. Ignored or filtered
cases are not counted as successful standalone executions. These are local
regression checks, not remote CI results.

For the book step, a shell function maps `mdbook build docs` to
`/tmp/optd-mdbook-0.4.52/mdbook build /tmp/optd-rule-book-source-20261029`.
That input copies tracked documentation and this change's three new documents,
with only this change's index additions; concurrent untracked documentation is
excluded. The final documentation build is repeated after the evidence update.
Final gate logs are `/tmp/optd-rule-sql-final-v02-20261029.log` and
`/tmp/optd-rule-full-final-v02-20261029.log`; other logs are
`/tmp/optd-rule-{fresh-check,tools,doc-check}-20261029.log`.

The two complete usage-guide examples also compile as a separate temporary
consumer at `/tmp/optd-rule-doc-check-20261029`. Its lockfile adds only that
consumer and drops unused workspace packages. All 393 common packages retain
their version/source/checksum identities; this is example compilation, not a
runtime certification of arbitrary custom rules.

## 4. Query rerun and artifact identity

The actual feature-built executable is copied and frozen before query runs:

```text
/tmp/optd-rule-benchmark-20261029-v01
SHA-256 d346ce154bfd074514c7216c3b8484babfc506d2f3485925d9a096d6e329950a
size 242794480 bytes
```

The observed file hash and stat identity prevent later Cargo operations from
silently replacing this executable. This is observed executable identity, not
embedded compile-time source provenance. Query suites use new isolated local
Iceberg roots and preserve unchanged query/schema inputs.

TPC-DS completes 99/99 passing profiles, 99 bootstrap comparisons and 297 mutation
rounds, with strict reopen and per-prefix replay. Independent audit rebuilds the
strict aggregate and compares its complete object, then verifies 5,916 indexed
unique file hashes (319,314,755 bytes), including executable, runner, input CSV,
CURRENT/predecessor manifests, source vectors and IPC inputs/output/deltas.
Each committed chain has epochs 3 -> 2 -> 1 -> 0. There are no failed DS profiles.
This is a separate audit of indexed evidence, not an additional SQL execution.

| Local artifact | SHA-256 |
| --- | --- |
| `/tmp/optd-rule-tpcds-suite-20261029/aggregate.json` | `3cafe95911c3cf12b7d3dfc36d9ab118b1d473dc76be71364e959259e472176b` |
| `/tmp/optd-rule-tpcds-suite-20261029/progress.json` | `b89ba48ae00abd1d3e68ab8d753ce38c01a584f32e19517d7b33b96ac6c2b079` |
| `/tmp/optd-rule-tpcds-independent-audit-20261029.json` | `a0a2fce2c0552cac8aada038952419f433a8736786cd680d4458f55f92d803da` |
| `/tmp/optd-rule-tpch-evidence-20261029.json` (initial failed orchestration) | `f563b914a493cd54fd381e0d28cd18b507785256618908ee76b948de855fce33` |

The DS aggregate is 46,038,232 bytes; use progress or selected fields for inspection.
TPC-H obtains 22/22 complete nonempty/changing selected profiles. The tiny run
finishes in 295.522 seconds with exit 1: 20 passes and unchanged original Q11/Q21
classified as empty-only, not complete coverage. The same repository SQL for
Q11/Q21 passes separately on the existing DuckDB 1.2.2 dbgen SF0.01 fixture,
using new real Iceberg tables. Q11 exits 0 in 18.347 seconds; Q21 exits 0 in
211.149 seconds. Its 600-second query / 660-second outer deadline is retained.
The strict aggregate selects a complete profile per query, never portions from
different profiles; it reports `with_pass=22`, `remaining=0` and zero failed
leaf profiles. The original orchestration failure remains separately retained.

Together, selected TPC-H 22 and TPC-DS 99 evidence provides **121 bootstrap
comparisons and 363 completed mutation rounds**, with strict reopen and complete
output-log replay. Counting the extra empty-only tiny Q11/Q21 executions yields
123 actual profile bootstraps, 369 rounds and 492 checkpoint epochs. Those extra
executions do not increase logical query coverage. The independent TPC-H audit
also checks 741 indexed unique files / 372,423,734 bytes; both aggregates are
rebuilt independently and compare as complete objects.

| Local artifact | SHA-256 |
| --- | --- |
| `/tmp/optd-rule-tpch-evidence-20261029-v02.json` | `26df6e53da2a20bdee209300e628ecd092792157b9eb3e3818e3d91b82cb9e56` |
| `/tmp/optd-rule-tpch-aggregate-20261029-v02.json` | `4ac1fa9ff8423a3677ab50e143094c6171a80e30e49a6cdf76c6ec4ff93801e8` |
| `/tmp/optd-rule-tpch-independent-audit-20261029-v02.json` | `cbc1acb46cacdfa13a1168311c98373644242f6dbc806e1dbb352715c4d8f65c` |
| `/tmp/optd-rule-query-independent-audit-20261029.json` | `ac604b553d8656dcc2ed44c7a7f4b10824a5ad4faa8c6e0b00099c80fe2bf743` |

The TPC-H evidence records exact per-profile commands, manifest/report hashes,
deadlines, backend/environment and the strict aggregation command. The aggregate
and independent audit retain individual profile identities. The executed local
orchestration is `python3 /tmp/optd-rule-tpch-orchestration-20261029-v02.py`;
the public repository tools and preparation instructions are in the
[benchmark guide](../../../optd-incremental-sql/benchmarks/README.md).
The TPC-DS run is:

```sh
timeout --signal=TERM --kill-after=5s 1800s \
  python3 optd-incremental-sql/benchmarks/tools/run_query_witness_suite.py \
  /tmp/optd-rule-tpcds-suite-20261029 \
  --binary /tmp/optd-rule-benchmark-20261029-v01 \
  --expected-sha256 d346ce154bfd074514c7216c3b8484babfc506d2f3485925d9a096d6e329950a \
  --expected-backend typed_iceberg_endpoint_vector \
  --iceberg-root /tmp/optd-rule-tpcds-sources-20261029
```

The benchmark was built with
`cargo +1.95.0 build -p optd-incremental-sql --features iceberg --locked --example benchmark`
and copied to the frozen path above. Generated reports, source tables, local
orchestration and audits stay outside Git; they contain no copied external source.

The first TPC-H orchestration attempt used an insufficient 180-second total
deadline for the 22-query tiny profile and timed out during Q13. Its orchestration
then rejected an aggregation invocation missing `--report`. Both are retained
as failed verification attempts; no completed 22-query evidence is inferred
from them. The second attempt gives the tiny suite a 1,200-second total deadline,
and the separate SF0.01 Q21 profile a 600-second query / 660-second outer deadline.

## 5. Practical limits

This change establishes extensible plan rewriting and execution. It does not
establish cost-based search, persistent custom-rule identity, indexed joins,
native aggregate accumulators, distributed ownership or a performance benefit.
Complete relations remain cached/materialized; eligible groups recompute touched
groups and nonlinear/ordered operators may recompute their local inputs.
Iceberg change acquisition still scans complete before/after endpoints.

Query evidence uses small directed TPC-DS witnesses and repository-specific
TPC-H SQL/parameters with separate data profiles. It is not standard-generator
full-scale coverage, TPC certification or performance evidence. The benchmark
compares typed bags and signed deltas; it does not individually certify ordered
presentation for all queries. Ordering has separate regression tests. Remote CI,
deployment and production runtime are not established by local verification.
