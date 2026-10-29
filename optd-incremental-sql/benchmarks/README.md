# SQL incremental query qualification

`tpch/manifest.json` identifies all 22 queries from this repository's SQL logic
tests, plus the existing eight-table CSV fixture identities. The preparation
command extracts ignored local SQL/schema files from already tracked source;
these duplicate generated artifacts are not committed. These queries have
repository-specific parameter choices. Q15's temporary view is represented by
an equivalent CTE with the same parameters. The historical CSV directory name
is not a verified scale factor: row counts and SHA-256 digests are recorded.

`tpcds/manifest.json` identifies the 99 numbered SQL files shipped by the public
DuckDB **v1.4.3** source tree. The preparation command downloads the exact
original SQL and generator schema header into ignored local `sources/`, and
derives ignored 24-table DDL retaining every column name/type without adding
primary-key assumptions. Every file has a committed digest and source URL.
Upstream SQL, schema headers, derived SQL/DDL and downloaded notices are
**not distributed in this repository**. This version supplies one statement per numbered query; the manifest
does **not** claim the additional a/b variants used by some other TPC-DS suites.
The network mirror is jsDelivr; Q71 was retrieved from its `fastly.jsdelivr.net`
alias after the default hostname returned an error.

DuckDB's [root MIT license](https://github.com/duckdb/duckdb/blob/v1.4.3/LICENSE)
does not by itself establish rights for all TPC-derived material. The fixed
[TPC Legal Notice in address.cpp](https://github.com/duckdb/duckdb/blob/v1.4.3/extension/tpcds/dsdgen/dsdgen-c/address.cpp)
reserves TPC rights and states a disclaimer; the examined notice contains no
redistribution grant. The complete public source SHA-256 is
`3b0f123b6fe948302baab9874d1846403cdddab3341ebd8b7c3e45dd101dd405`;
the verbatim leading notice SHA-256 is
`fcd2ac50ed4eab4c9d541120f4c2ef3c7ab5a2a7df46202f56678ba2389fea40`.
Preparation is an explicit local fetch; this project makes no broader license
or official benchmark certification claim for the fetched material.

No result in either manifest is a successful execution claim. SQL qualification,
nonempty result coverage, standard-data coverage, durability, and performance
are separate facts.

## Reproduce fixture identities

From the repository root:

```sh
python3 optd-incremental-sql/benchmarks/tools/extract_repository_tpch.py
python3 optd-incremental-sql/benchmarks/tools/fetch_duckdb_sources.py
```

Both preparation tools keep their committed manifests unchanged and reject
different source/data identities. TPC-H preparation is entirely offline and
retains the existing tracked SLT and CSV files. Its `--verify-only` option checks
existing extracts without writing them.

The TPC-DS source fetcher skips already-present files and uses a fixed public version,
three concurrent requests, and a 20-second deadline per request. The first
download is about 0.4 MB. It verifies all upstream and derived hashes against
the committed manifest, and never changes that manifest to accept a different
download. After preparation, verify all identities without network access:

```sh
python3 optd-incremental-sql/benchmarks/tools/fetch_duckdb_sources.py --verify-only
```

Prepare TPC-DS sources before generating its synthetic fixture or executing
the downloaded query profile. Generated data remains outside the repository.

## Generate data outside the repository

The dependency-free synthetic fixture is deliberately small. It is labelled as
an operator compatibility fixture and is not standard generated TPC-DS data:

```sh
python3 optd-incremental-sql/benchmarks/tools/generate_synthetic_tpcds.py /tmp/optd-tpcds-fixture
```

For public-generator data, use a separate Python environment with the exact
`duckdb==1.4.3` package and an available matching extension. The optional
`--install-extension` flag permits the named extension download; downloaded
extensions and all generated rows stay inside the specified output directory:

```sh
python3 optd-incremental-sql/benchmarks/tools/generate_duckdb_fixture.py tpcds /tmp/optd-tpcds-dsdgen --scale-factor 0.01 --install-extension
python3 optd-incremental-sql/benchmarks/tools/generate_duckdb_fixture.py tpch /tmp/optd-tpch-dbgen --scale-factor 0.01 --install-extension
```

DuckDB 1.4.3's Linux wheel requires glibc 2.26, while the current host has
glibc 2.17. The explicit `--duckdb-version 1.2.2` alternative selects its
compatible manylinux2014 wheel and records the different generator/source
version; it never silently substitutes versions. DuckDB **1.2.2** has been
verified on this host using an isolated wheel whose SHA-256 matches PyPI and
the official signed extensions. Actual generation succeeded for TPC-H SF0.01
(eight tables, 60,175 lineitems, 10,673,344 CSV bytes) and TPC-DS SF0.01
(24 tables, 28,810 store-sales rows, 26,554,945 CSV bytes). The TPC-DS SF0.001
attempt reached its 300-second process deadline and is not a valid fixture.
The 1.4.3 generator path remains unexecuted on this host. The tool validates the complete logical-query/table counts,
exports the actual extension SQL, and records the DuckDB build and extension
metadata. It does not certify an official TPC benchmark run.

When a signed extension is downloaded separately, keep its actual filename
(`tpch.duckdb_extension` or `tpcds.duckdb_extension`) and pass
`--extension-file /absolute/path/tpcds.duckdb_extension` instead of
`--install-extension`. The generated manifest records its SHA-256.

To retain all existing SQL parameters and their original typed schema while
using public-generator CSV data, combine the two profiles explicitly:

```sh
python3 optd-incremental-sql/benchmarks/tools/combine_query_data_fixture.py optd-incremental-sql/benchmarks/tpch/manifest.json /tmp/optd-tpch-dbgen/manifest.json /tmp/optd-tpch-original-sql-new-data
```

The output preserves every query/schema hash, verifies each input CSV hash,
records both manifest identities and references the original CSV files.

## Run qualification

```sh
cargo run -p optd-incremental-sql --example benchmark -- optd-incremental-sql/benchmarks/tpch/manifest.json /tmp/optd-tpch-coverage.json
cargo run -p optd-incremental-sql --example benchmark -- /tmp/optd-tpcds-fixture/manifest.json /tmp/optd-tpcds-coverage.json
```

An optional final logical query number limits a focused run. The runner checks
all input digests. For each query, an independent DataFusion analyzed logical
plan identifies base-table scans, including scans inside expression subqueries.
Only those referenced tables enter the maintenance/oracle snapshots and changes;
their complete original rows remain intact. The report lists `registered_tables`.
The runner starts each referenced table with half its rows, then applies three
atomic multi-table batches: insert the remaining half, delete the first row,
restore the first row. After every batch it compares both the typed result bag
and signed output delta with independently executed DataFusion SQL. It records
operator strategy statistics, query failures, rounds, and empty/nonempty final
result counts. `nonempty_result_seen`, `output_delta_seen`, and
`all_rounds_empty` distinguish data coverage from successful comparisons.
Queries whose results stay empty are `verified_empty_only`; queries with no
observed output changes are `verified_unchanged_output`. Neither counts as
complete incremental query acceptance. The process returns nonzero if any
selected query fails or remains incomplete in either way. Floating point
results retain exact comparison; no unmeasured tolerance hides differences.
CSV decoding uses Arrow's typed reader with the manifest's exact literal NULL
marker. This bypasses DataFusion 55's execution reader omission of `null_regex`;
it does not change source types or values to fit a query.
The maintainer and independent oracle both use one DataFusion target partition;
the coverage report records this deterministic qualification configuration.
Each selected query prints a short start/completion record to stderr and has a
120-second asynchronous deadline. `OPTD_SQL_BENCH_QUERY_TIMEOUT_SECS=SECONDS`
explicitly changes the deadline within 1..3600, and the report records it.
Timeouts remain failures and the suite continues. A blocked synchronous planner
cannot be preempted by Tokio's timer; an outer process deadline remains the
appropriate hard limit for such a case. Progress from a dropped query future is
reported as unknown, not as successful completed rounds.

The output is a bag. `ORDER BY` is exercised as part of LIMIT membership, but
presentation ordering is not independently checked. LIMIT with non-total-order
ties can have multiple valid answers; the runner never silently adds tie-break
columns. The same DataFusion scalar implementation is used on both sides, so
these checks establish incremental propagation consistency, not independence
from DataFusion's own SQL semantics. Hand-calculated semantic tests remain
necessary.

## Generate a directed query witness

Some fixed query parameters do not occur in the small public-generator data.
Explicit recipes cover all 99 numbered queries. Create a separate non-dsdgen
profile for the selected query:

```sh
python3 optd-incremental-sql/benchmarks/tools/generate_query_witness_fixture.py /tmp/optd-tpcds-q3-witness --query 3
cargo run -p optd-incremental-sql --example benchmark -- /tmp/optd-tpcds-q3-witness/manifest.json /tmp/optd-tpcds-q3-witness-coverage.json 3
python3 -m unittest discover -s optd-incremental-sql/benchmarks/tools -p test_query_witness_fixture.py
```

Prepare the original TPC-DS sources first. The witness generator verifies the
fixed original manifest, all 99 SQL hashes and the typed schema, and references
their absolute paths without copying or changing them. It writes only to a new
empty child directory of `/tmp`; all 24 input table CSVs receive row counts and
hashes, with unused relations empty and unspecified fields explicit SQL NULL.
Recipes use exact decimal strings, original integer ranges and ISO dates;
invalid columns, rounding or floating-point input values are rejected.
The preparation tests check every recipe against every original table/column
type and check explicitly declared weekday names against their dates. These
preparation tests do not replace running the SQL qualification.

The manifest labels these rows `directed_semantic_fixture_not_dsdgen`, records
the target query, recipe version, description and generator digest, and keeps
all original query IDs/variants. Run only that target query for the recipe's
intended qualification. The existing benchmark performs its unchanged
bootstrap and three mutation rounds; a recipe is accepted only after actual
nonempty results and signed output changes agree exactly with independent SQL.
The [accepted design](../../docs/src/incremental/query_witness_fixtures.md)
explains the boundary between directed semantic coverage and generator data.

Run all 99 target profiles with one already-built executable and an explicitly
supplied SHA-256 identity:

```sh
python3 optd-incremental-sql/benchmarks/tools/run_query_witness_suite.py /tmp/optd-tpcds-witness-suite --binary /absolute/path/to/benchmark --expected-sha256 EXPECTED_BINARY_SHA256
python3 -m unittest discover -s optd-incremental-sql/benchmarks/tools -p 'test_*.py'
```

The output directory must be new. `progress.json` retains per-query process
exits, hard timeouts, logs, report hashes and executable identity checks;
`aggregate.json` uses the strict report verifier described below. The default
hard deadline is 180 seconds per target; `--timeout-seconds` changes it
explicitly. Query failures remain recorded and subsequent independent targets
continue. A changed executable stops the suite. A repeated `--query NUMBER`
option runs an explicit subset, whose aggregate still shows the unexecuted
queries and cannot claim `full99_coverage`. All generated data stays in `/tmp`.

## Combine fixture coverage without combining rounds

```sh
python3 optd-incremental-sql/benchmarks/tools/summarize_coverage.py /tmp/optd-tpch-aggregate.json --report tiny /tmp/optd-tpch-coverage.json EXPECTED_REPORT_SHA256 --report generated /tmp/optd-tpch-generated-coverage.json EXPECTED_REPORT_SHA256
python3 -m unittest discover -s optd-incremental-sql/benchmarks/tools -p test_summarize_coverage.py
```

Replace each expected hash with that report's actual SHA-256. The tool checks
every report identity, manifest/schema/SQL digest, suite, successful round count
and summary counter. It enumerates `query_id` plus `variant_id`, retaining each
profile's source, SQL parameter identity, fixture status, all three rounds and
failures. A query receives `with_pass` only when one individual profile has a
verified bootstrap, all three successful rounds, a nonempty result and an actual
signed output change. It never combines partial rounds from different profiles.
The aggregate reports `total`, `with_pass` and `remaining`; a successful profile
does not erase failures in another profile or establish scale/performance results.

## Qualify through real Iceberg snapshots

The optional backend uses real local Iceberg v2/Parquet tables and a format-2
checkpoint. It writes the declared typed fixture values, freezes complete source
vectors and performs bootstrap plus the same three mutation rounds. Each round
compares independent scans of those exact endpoints, maintained source caches,
full SQL results and signed deltas, strictly reopens the view and checks output
log replay. VARCHAR's Arrow Utf8View representation is explicitly converted to
Utf8 for Iceberg without changing string bytes or NULLs; no numeric/date casts
or query edits occur.

```sh
cargo +1.95.0 build -p optd-incremental-sql --features iceberg --locked --example benchmark
OPTD_SQL_BENCH_ICEBERG_ROOT=/tmp/optd-tpch-iceberg-sources target/debug/examples/benchmark optd-incremental-sql/benchmarks/tpch/manifest.json /tmp/optd-tpch-iceberg-coverage.json
sha256sum target/debug/examples/benchmark
python3 optd-incremental-sql/benchmarks/tools/run_query_witness_suite.py /tmp/optd-tpcds-iceberg-suite --binary /absolute/path/to/feature-built/benchmark --expected-sha256 EXPECTED_BINARY_SHA256 --expected-backend typed_iceberg_endpoint_vector --iceberg-root /tmp/optd-tpcds-iceberg-sources
```

Prepare pinned query sources first. The two suite paths must be new, disjoint
children of `/tmp`; every query creates a separate catalog/store. For a manual
manifest run the artifact root's parent must exist and each query's child must
be new. Building without the feature while setting the environment variable is
an error. Do not overwrite the chosen executable during qualification; the suite
checks its identity before/after every target. `CARGO_TARGET_DIR` changes the
example path above and must be reflected in the selected executable.

Reports explicitly declare `typed_iceberg_endpoint_vector`, retain complete
bootstrap/round source vectors and their SHA-256, endpoint work, SQL strategies,
committed epochs and reopen/replay evidence. Aggregate this backend explicitly:

```sh
python3 optd-incremental-sql/benchmarks/tools/summarize_coverage.py /tmp/optd-iceberg-aggregate.json --expected-backend typed_iceberg_endpoint_vector --report PROFILE REPORT_PATH EXPECTED_REPORT_SHA256
```

The verifier rejects mixed backends or claimed Iceberg passes without complete
source/recovery evidence. Old reports lacking a backend remain Arrow evidence.
The suite records the selected backend, artifact root and only its relevant
environment variable; inherited conflicting configuration fails before writes.

These are whole-file source replacements and full endpoint differences, not
native input CDC or O(delta) IO. SQL strategies still include local nonlinear
operator recomputation. A source vector selects an explicit tuple, not an
upstream distributed transaction; the writer is host-local. See
[the source contract](../../docs/src/incremental/typed_iceberg_sources.md)
for type, schema, delete-file and resource boundaries.

`scripts/agent-check.sh sql` checks both default and Iceberg Rust configurations.
`scripts/agent-check.sh sql-tools` verifies prepared public source hashes and
runs all Python tool tests offline; it fails if sources are missing. CI prepares
those ignored public sources in an explicit preceding fetch step.
