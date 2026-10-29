# Query-directed SQL witness fixtures

**Status:** accepted · 2026-10-29 (root review)
**Scope:** small, explicit local input relations that exercise nonempty, changing
results for the unchanged TPC-DS 99-query profile; no scale or performance claim.

## 1. Problem

The public DuckDB 1.2.2 dsdgen SF0.01 fixture is useful standard-generator
evidence, but does not contain all literal/key domains required by the fixed
DuckDB v1.4.3 queries. Repeating it cannot exercise results that are impossible
for its input domains. For example, item contains no manufacturer 128 (Q3),
manufacturer 977 (Q32), color peach (Q24), any of Q37's four manufacturers, or
manufacturer 738..778 (Q41). Store sales contains no store key 4 (Q44).

Other queries need connected groups rather than isolated random rows: more
than ten preferred customers with a non-NULL ZIP (Q8), common customer/date
membership in all three sales channels (Q38), or the same inventory item and
warehouse exceeding the coefficient-of-variation threshold in both January
and February (Q39). Small generated data does not guarantee these conditions.

An empty or unchanged result remains recorded as such. It is not promoted to
complete incremental coverage.

## 2. Constraints and evidence

* Retain the original 99 logical query IDs, variant IDs, SQL bytes and SHA-256
  digests from `benchmarks/tpcds/manifest.json`.
* Retain the original schema bytes/digest and Arrow types. No SQL parameter
  changes, casts to floating point, precision reduction, relaxed thresholds or
  comparison tolerances.
* The fixture is explicitly `directed_semantic_fixture_not_dsdgen`. It is not
  public-generator data, an official benchmark dataset, or independent SQL
  semantics proof.
* One profile belongs to one target query. An aggregate can choose that entire
  profile's successful evidence, but cannot combine rounds from different
  profiles or hide failures elsewhere.
* Use the existing benchmark maintainer, independent DataFusion full-query
  oracle, typed multiset comparison and signed endpoint-difference checks.
* All generated files live under the requested `/tmp` output directory. Source
  SQL/schema remain ignored local preparations; the generator does not publish
  their contents or mutate the committed manifest.

## 3. Options considered

1. Increase the public generator's scale: retain as separate future evidence,
   but it costs more input/runtime and still does not guarantee each selected
   parameter value and rare relationship.
2. Change query parameters to fit available data: reject; it changes the fixed
   query profile and conceals the present coverage gap.
3. Add random rows to every table: reject as an acceptance method; connected
   joins, statistical thresholds and multi-period inequalities remain accidental.
4. Explicit per-query relational witnesses: selected. Encode the actual input
   relationships and validate each witness through the unchanged query/harness.

## 4. Decision

Add a dependency-free Python generator under `benchmarks/tools/`. Its recipe
registry contains explicit rows keyed by the original schema's column names.
The generator verifies the original manifest/schema/query hashes before writing
anything. It rejects unknown columns, invalid scalar types, undeclared recipe
IDs, output paths outside `/tmp`, and nonempty existing output directories.

Every profile includes all 24 table schemas, with zero rows for tables unused
by the recipe. Unspecified fields are explicit SQL NULL, encoded using one
declared literal `\\N` marker. Integers remain integers; decimal values are
decimal strings validated with Python's exact `Decimal`; dates are ISO dates.
No random defaults or binary floating-point input generation is used.

The generated manifest preserves all 99 query/variant identities. It references
the already-verified original SQL/schema by absolute path, avoiding 99 copies
of upstream material. Each table CSV has an exact row count and SHA-256. Extra
metadata identifies the source manifest, target query, recipe/version, intended
relationships and explicitly non-dsdgen status. Generated data is never written
back into the repository.

The existing runner selects the target query and performs its normal bootstrap
and three multi-table epochs: insert the remaining half, delete the first row,
and restore that row. Recipe ordering can make the bootstrap empty; insertion
must then create an actual qualifying result. Removing referenced dimension or
fact rows exercises retraction and restoring them exercises reinsertion. The
acceptance gate remains the existing full three-round comparison with a
nonempty result and observed signed output change.

### Initial recipes

| Query | Explicit witness relationship |
| --- | --- |
| Q3 | Manufacturer 128 item, November date and matching store sale. |
| Q8 | At least eleven preferred customers at non-NULL ZIP 24128, store with the same ZIP prefix and a 1998 Q2 sale. |
| Q24 | Peach item and a matching sale/return/customer/address/store group satisfying its original birth-country and aggregate conditions. |
| Q32 | Manufacturer 977 item and dated catalog sales with discounts sufficiently different to exceed the original correlated-average threshold. |
| Q37 | One of manufacturers 677/940/694/808, price in 68..98, inventory quantity in 100..500, original 2000 date interval and matching catalog sale. |
| Q38 | One customer and date in month sequence 1200 present in all three sales channels. |
| Q39 | One item/warehouse with inventory samples [0,0,100] in each of January and February 2001; sample standard deviation divided by mean exceeds 1. |
| Q41 | Manufacturer in 738..778 with a same-manufacturer item satisfying one original category/color/unit/size combination. |
| Q44 | Store key 4, distinct item profit groups and a nonempty NULL-address reference group for its original ranking/threshold conditions. |
| Q4 | Same customer in 2001/2002; store totals 100/110, web 100/150 and catalog 100/200 make catalog growth larger than both other channels. |
| Q11 | Same customer in 2001/2002 with positive store and web totals; web growth exceeds store growth. |
| Q31 | Same county in three quarters of 2000; store totals 100/110/121 and web totals 100/150/225 satisfy both original growth comparisons. |

The initial table records the first observed gaps. The implementation now has
an explicit recipe for every numbered query, including correlated and scalar
subqueries, three-channel set operations, rollups, ranking and adjacent-period
windows. Each recipe's description states its original qualifying relationship.
Preparation tests validate all 99 recipes against original column types and
check supplied weekday names against their dates.

Recipes and preparation checks are not accepted query results. Each must be
corrected or rejected if the actual unchanged SQL and independent oracle do
not produce a complete nonempty changing qualification. Runtime reports,
executable identities and strict aggregates remain separate evidence artifacts.

### Reproducible suite runner

A dependency-free `run_query_witness_suite.py` orchestrator accepts an explicit
benchmark executable and expected SHA-256, refuses mismatches before writes,
and runs all 99 target recipes by default. An optional explicit subset is for
focused diagnosis and remains visibly incomplete in the aggregate. Output must
be a newly created child of `/tmp`; every query has a separate fixture, log and
immutable benchmark report.

The orchestrator rechecks the executable identity before and after each query,
uses a hard per-process timeout, records actual exit statuses and report hashes,
and updates progress after every query, including timeout/failure. It continues
independent target queries after a query-level failure; it stops if the selected
executable changes. The existing strict summarizer validates each report and
its original manifest/schema/SQL identities and selects only complete profiles.
The final suite succeeds only when every selected query passed, every expected
query was executed, and strict aggregation succeeded; a focused subset cannot
claim full 99-query coverage. Executable SHA is observed identity, not embedded
compile-time source provenance. Negative tests exercise bad hashes, output
paths, duplicate/out-of-range query IDs and timeout preservation.

## 5. Failure modes and recovery

* Wrong SQL/schema/source identity: refuse generation before creating output.
* Invalid column/type or accidental floating-point value: reject the recipe.
* A recipe still yields empty/unchanged output: retain the report and fix input
  relationships; do not change SQL, thresholds or the verifier.
* Engine/oracle mismatch or timeout: retain it as failure; a witness does not
  override or waive an execution defect.
* A failed generation leaves files: retain that exact output for diagnosis and
  use a new empty output path on retry; do not delete unrelated temporary data.
* Non-total-order LIMIT ties: make input values distinguish the relevant sort
  keys where possible; never insert a tie-breaker into the query text.

## 6. Acceptance criteria

1. The generator verifies fixed source identities and emits reproducible CSV
   and manifest digests for an empty `/tmp` output directory.
2. Invalid columns, invalid typed values, output escape/nonempty output, and
   source digest changes have meaningful negative tests.
3. Every implemented target query runs with the current executable identity
   recorded, completing bootstrap plus all three original mutation rounds.
4. The unchanged validator records `pass`, a nonempty result and an actual
   signed delta for that individual profile. Empty/unchanged/failure states are
   preserved and remain outstanding.
5. Coverage aggregation verifies report/manifest/schema/SQL identities and
   chooses a complete individual profile without combining partial rounds.
6. Public-generator runs and directed-fixture runs remain visibly distinct.
   No full 99-query or performance completion is claimed from recipes alone.

## 7. Rollout / migration

Root reviewed this design before implementation. Implement the small generator
and initial recipes, run focused target queries with the current ready binary,
then extend recipes only for remaining observed coverage gaps. Root owns the
design index and overall integration/publication. No engine or verifier change
is required by this design.

## 8. Open questions

* Tiny witness runtime and total generated bytes must be measured. Avoid
  extrapolating scale/performance from directed fixtures or older executable runs.
* No small witness set proves every SQL branch, arbitrary change sequence, or
  an independent implementation of DataFusion's SQL semantics.
