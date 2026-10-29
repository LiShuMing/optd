# SQL incremental qualification evidence

**Status:** verified evidence · 2026-10-29

**Scope:** fixed TPC-H 22-query and TPC-DS 99-query profiles, typed local
Arrow inputs, bootstrap and three atomic multi-table mutation rounds.

This record accompanies the [query-directed fixture design](query_witness_fixtures.md)
and the [preparation and runner instructions](../../../optd-incremental-sql/benchmarks/README.md).
It records observed query correctness, not standard-scale performance,
official TPC certification, or qualification of the newer typed Iceberg
source-vector/coordinator path.

The later [typed Iceberg qualification](./typed_iceberg_qualification.md) supplies
its own complete 22/99 source-backed results, recovery checks and artifact hashes;
those results are independent of the historical Arrow evidence below.

## 1. Verified result and executable identity

Both qualifications below used the same observed executable SHA-256:

```text
ffc5a246eb1e496ed8b2e5c158bff65766a73a9eb8a76b343bf2889be8b25ffb
```

The executable was `/tmp/optd-sql-msrv-20261029-target/debug/examples/benchmark`.
That path is a build output and can subsequently change. The digest identifies
the executable actually observed during these runs; it is not embedded
compile-time source provenance or a claim about the current contents of that
path. Later source changes require their own build and execution evidence.

| Profile | Complete queries | Observed inputs and rounds |
| --- | --- | --- |
| TPC-H | 22/22, no remaining query or failed profile | The repository fixture supplies 20 complete queries; unchanged Q11 and Q21 separately pass on DuckDB 1.2.2 dbgen SF0.01 CSV data. Each chosen profile has bootstrap plus all three rounds. |
| TPC-DS | 99/99, no remaining query or failed profile | 99 independent query-directed fixtures; 674 input rows and 55,812 CSV bytes in total; 99 bootstrap comparisons and 297 successful mutation rounds. Every query has a nonempty result and an observed signed output change. |

The TPC-H repository-only run returns **exit 1**, correctly retaining Q11/Q21
as `verified_empty_only`: 22 bootstrap and three-round comparisons succeed,
but only 20 queries satisfy nonempty/change acceptance. The two SF0.01 runs
each return exit 0. The aggregate chooses a whole successful profile for each
query; it does not turn the repository-only run into a successful 22-query run.

The TPC-DS suite records 99 actual process exits of 0, `status=pass`,
`complete=true`, and `full99_coverage=true`. It checks initial/final executable
SHA-256 and per-query device/inode/size/mtime/ctime identity. Its measured
subprocess wall times total 133.657 seconds. These are debug-executable times
on small directed fixtures, with no performance baseline or scale inference.

## 2. Local evidence artifacts

These are exact local artifacts, not files distributed by the repository.
SHA-256 digests were rechecked when writing this record.

| Artifact | SHA-256 |
| --- | --- |
| `/tmp/optd-tpch-final-aggregate-20261029.json` | `0ec8f8d5c5f068dbb381282c61e2a6349defdbf6b4bb79f63336f88720d2f09c` |
| `/tmp/optd-tpch-final-evidence-20261029.json` | `e2ad109d3153187c6e3f1e45e48091d6195e7f37b535ec0bc16f2165ea6005d9` |
| `/tmp/optd-tpch-final-tiny-verified-exit-coverage-20261029.json` | `b434446773a74cf304a69752631cf2836c4d8f6ad16625cb99e7f3acb31eb15e` |
| `/tmp/optd-tpch-final-sf01-q11-coverage-20261029.json` | `f669fe798bf8f77d2139992dd253fefa0b9def068d3ea1b939aa3a415f6b271b` |
| `/tmp/optd-tpch-final-sf01-q21-coverage-20261029.json` | `1ae911c535136e57838b96321ac2957a2b6473038a6af0551fd21507da8562c0` |
| `/tmp/optd-tpcds-witness-suite-20261029/aggregate.json` | `1b5936b630824410dd7244fc5a1ce99bbd4af7b81ad71ec76e03044253655082` |
| `/tmp/optd-tpcds-witness-suite-20261029/progress.json` | `886b2135c6191134bd6ffe631f188c79a7cf79ceb7c5efaecf90b69917bb8d95` |
| `/tmp/optd-benchmark-fixture-evidence-20261029.json` | `e38f7965d3edc8d86ba569db514db12f9553876468d097edbc83e96d99d90ed5` |

`progress.json` indexes every TPC-DS command, fixture manifest, log, coverage
report, report digest, process exit and completed-round count. The strict
aggregate independently records the chosen profile and preserves unsuccessful
evidence. Its summary is `total=99`, `with_pass=99`, `remaining=0`,
`profiles_with_failures=0`; the TPC-H summary has the same counters with 22.

The observed TPC-DS runner SHA-256 was
`a5fa7acb1a9b501e7659271296857e690869b52d8a0017b4ccaeae92701fe224`.
Report digests and executable checks do not substitute for retained source
revision/build provenance.

## 3. Query, schema and data boundaries

| Identity | SHA-256 |
| --- | --- |
| `optd-incremental-sql/benchmarks/tpch/manifest.json` | `c95215e1e9c4b8b249b9ab73944f1cdd4d81794f674f1d6fd7484e6fb8dfde03` |
| Original TPC-H schema | `e99da0640b4f15c3381867d8b284476d0e3edb89a3739806d1dc99ce490f0534` |
| `/tmp/optd-tpch-repository-sql-dbgen122-sf01/manifest.json` | `f99b4b9266006c8de91d76997f2bc9e36b0e5d04e154201814713a44e45c24ad` |
| `optd-incremental-sql/benchmarks/tpcds/manifest.json` | `f175d057e27e3575dcff1b74ae6f246a0b7e6610fd2ce1be93aa043d59a27935` |
| Original TPC-DS schema | `e9e1df6e61b2de380a51960704fa60fcb3227ef0736b1b1dba11c01c496ff9ae` |

TPC-H retains the repository's original SQL parameters and typed eight-table
schema. Q15's temporary view has the explicitly documented equivalent CTE
representation. The repository fixture's historical directory name does not
establish a scale factor. The separately generated dbgen SF0.01 dataset has
60,175 lineitems and 10,673,344 CSV bytes; only Q11 and Q21 in this aggregate
use that dataset.

TPC-DS retains the fixed DuckDB **v1.4.3** 99 numbered SQL files and original
24-table column names/types. This profile contains one statement per numbered
query; it does not claim 103 a/b variants. Every directed fixture is labelled
`directed_semantic_fixture_not_dsdgen`, with qualification
`query_directed_nonempty_change_semantics_only_not_standard_data_or_performance`.
Each recipe targets one query. Unused tables are empty and unspecified fields
are explicit SQL NULL. Decimal strings, integer ranges and ISO dates are
validated without changing SQL parameters, precision, predicates or comparison
tolerances.

Separately, public DuckDB **1.2.2** signed dbgen/dsdgen extensions generated
SF0.01 data on this host. The TPC-DS data has 28,810 store-sales rows and
26,554,945 CSV bytes, with unchanged v1.4.3 queries/schema combined in
`/tmp/optd-tpcds-v143-sql-dsdgen122-sf01/manifest.json`. Generation success is
not query execution success: a later standard-data execution attempt reached
its process deadline (exit 124), and no complete 99-query standard-data
qualification under the executable above was established. A separate
SF0.001 generation attempt also timed out after 300 seconds and produced no
valid fixture manifest. Neither attempt supplies a missing PASS.

Upstream SQL/schema material and generated data stay in ignored preparation
files or `/tmp`; they are not redistributed as this project's MIT source.
The preparation README records the examined upstream notice and license scope.

## 4. What each successful query proves

An independently analyzed SQL plan identifies referenced base tables,
including scans inside expression subqueries. Only those tables enter the
query's inputs. Bootstrap loads the first half of each table. The runner then
applies three atomic multi-table batches:

1. `insert_remaining_half`;
2. `delete_first_row`;
3. `restore_first_row`.

At bootstrap and each resulting endpoint, an independent DataFusion full-SQL
execution reads the endpoint inputs without using incremental caches. The
runner compares the full typed result multiset and the signed endpoint output
difference. `pass` requires one complete profile with bootstrap, all three
rounds, a nonempty result and an actual output change. Empty-only, unchanged,
failed and timed-out profiles remain distinguishable and incomplete. The
strict summarizer verifies report, manifest, schema and SQL identities; it
cannot assemble one accepted query from different profiles' partial rounds.

Both sides use DataFusion `target_partitions=1` and exact typed bag comparison,
including exact floating-point comparison. They share DataFusion's scalar
implementation. This proves incremental propagation consistency with that
full execution, not independent SQL-engine semantic correctness. The output
contract is a bag: `ORDER BY` affects `LIMIT` membership, but presentation
ordering is not independently qualified. Non-total-order ties are not repaired
by rewriting the query.

The reported operator strategies include all three of:

* `NativeDelta`: delta propagation for supported linear operations and inner joins;
* `ChangedGroupRecompute`: recomputation of affected aggregate groups;
* `OperatorRecompute`: explicit local recomputation of affected operators.

For example, TPC-DS Q1 reports `OperatorRecompute` for a left-semi join and
ordered operations. The 99-query result therefore does not mean every operator
uses a native delta algorithm or that work is proportional only to change size.
The reference backend limits a local materialization to 1,000,000 rows and
256 MiB; this is an admission bound, not a proven scale/performance target.
The SQL fixture runner uses Arrow's typed CSV reader and the declared literal
`\N` NULL marker, preserving source schema and values.

## 5. Reproduction and verification environment

The observed host is Linux x86_64, glibc 2.17, Python 3.11.7. The workspace
locks DataFusion 55.0.0 and Arrow 59.2.0. The compiler is
`rustc 1.95.0 (59807616e 2026-04-14)`, checked directly with the selected
toolchain executable. The original target directory was fresh for its initial
build and reused for later gates; a later cached gate does not establish a
fresh build. The baseline local full/SQL gate logs are
`/tmp/optd-full-gates-20261029.log` and
`/tmp/optd-directory-durability-sql-20261029.log`; those gates are separate
from the query reports and from subsequent Iceberg-source changes.

Prepare the original SQL/schema identities from the repository root:

```sh
python3 optd-incremental-sql/benchmarks/tools/extract_repository_tpch.py
python3 optd-incremental-sql/benchmarks/tools/extract_repository_tpch.py --verify-only
python3 optd-incremental-sql/benchmarks/tools/fetch_duckdb_sources.py
python3 optd-incremental-sql/benchmarks/tools/fetch_duckdb_sources.py --verify-only
```

The TPC-DS fetch is explicit network preparation: about 0.4 MB, three concurrent
requests, 20-second curl deadlines and 25-second subprocess deadlines. Its
`--verify-only` mode performs no network requests. It verifies the committed
manifest rather than rewriting identities to accept different input.

Use an already-built executable with an explicitly verified SHA-256. A new
build requires a new observed identity; do not reuse the historical digest
without checking it. The actual successful TPC-DS suite command was:

```sh
python3 optd-incremental-sql/benchmarks/tools/run_query_witness_suite.py \
  /tmp/optd-tpcds-witness-suite-20261029 \
  --binary /tmp/optd-sql-msrv-20261029-target/debug/examples/benchmark \
  --expected-sha256 ffc5a246eb1e496ed8b2e5c158bff65766a73a9eb8a76b343bf2889be8b25ffb
```

For a rerun, select a **new** `/tmp` output directory and the executable's
actual digest. The suite defaults to all 99 queries and a hard 180-second
deadline for each subprocess; `--query NUMBER` explicitly selects a subset
whose report cannot claim full coverage. A changed executable stops the suite;
individual failures and timeouts remain in progress and aggregate evidence.

For the TPC-H public-generator inputs, use an isolated Python environment with
`duckdb==1.2.2` and the matching signed extension. The selected version is
explicit because the v1.4.3 Linux wheel requires glibc 2.26. Preparation is:

```sh
python3 optd-incremental-sql/benchmarks/tools/generate_duckdb_fixture.py \
  tpch /tmp/optd-tpch-dbgen122-rerun --scale-factor 0.01 \
  --duckdb-version 1.2.2 --install-extension
python3 optd-incremental-sql/benchmarks/tools/combine_query_data_fixture.py \
  optd-incremental-sql/benchmarks/tpch/manifest.json \
  /tmp/optd-tpch-dbgen122-rerun/manifest.json \
  /tmp/optd-tpch-original-sql-rerun
```

These are preparation commands for new output paths. The actual qualified
TPC-H commands, including their recorded historical output paths, were:

```sh
/tmp/optd-sql-msrv-20261029-target/debug/examples/benchmark \
  /home/admin/xwork/optd/optd-incremental-sql/benchmarks/tpch/manifest.json \
  /tmp/optd-tpch-final-tiny-verified-exit-coverage-20261029.json
/tmp/optd-sql-msrv-20261029-target/debug/examples/benchmark \
  /tmp/optd-tpch-repository-sql-dbgen122-sf01/manifest.json \
  /tmp/optd-tpch-final-sf01-q11-coverage-20261029.json 11
/tmp/optd-sql-msrv-20261029-target/debug/examples/benchmark \
  /tmp/optd-tpch-repository-sql-dbgen122-sf01/manifest.json \
  /tmp/optd-tpch-final-sf01-q21-coverage-20261029.json 21
```

Preserve each process's actual exit status; the first command intentionally
returns 1 for the two incomplete queries. Reruns must retain historical
artifacts and use new report paths. Recheck the recorded TPC-H coverage with:

```sh
python3 optd-incremental-sql/benchmarks/tools/summarize_coverage.py \
  /tmp/optd-tpch-rechecked-aggregate.json \
  --report tiny /tmp/optd-tpch-final-tiny-verified-exit-coverage-20261029.json b434446773a74cf304a69752631cf2836c4d8f6ad16625cb99e7f3acb31eb15e \
  --report sf01-q11 /tmp/optd-tpch-final-sf01-q11-coverage-20261029.json f669fe798bf8f77d2139992dd253fefa0b9def068d3ea1b939aa3a415f6b271b \
  --report sf01-q21 /tmp/optd-tpch-final-sf01-q21-coverage-20261029.json 1ae911c535136e57838b96321ac2957a2b6473038a6af0551fd21507da8562c0
```

This regenerates validated coverage counts. The recorded final aggregate also
contains separately added executable-evidence metadata, so this command alone
does not promise its identical file digest. New report/manifests likewise have
their own digests, including any output-path metadata.

## 6. Python tool checks and outstanding gates

After original TPC-DS sources are prepared, the minimum offline tool gate is:

```sh
python3 optd-incremental-sql/benchmarks/tools/fetch_duckdb_sources.py --verify-only
python3 -m unittest discover -s optd-incremental-sql/benchmarks/tools -p 'test_*.py'
```

Observed result when recording this document: **14 tests passed**, exit 0,
0.348 seconds. They exercise strict aggregation, typed recipe preparation and
real subprocess failure/timeout/executable-identity handling. They need Python's
standard library, not DuckDB, Cargo or a Rust benchmark executable. A fresh
checkout must first explicitly fetch the ignored TPC-DS SQL/schema. Without
those prepared files, only the four self-contained summarizer tests form the
currently supported fully offline fresh-checkout gate:

```sh
python3 -m unittest discover -s optd-incremental-sql/benchmarks/tools -p test_summarize_coverage.py
```

**Unknown or not established here:** full 99-query standard-generator data
acceptance; larger scale, resource/performance comparisons or all possible
mutation sequences; independent SQL-engine semantic certification; and all
121 queries through real typed Iceberg endpoint vectors, durable publication,
strict reopen and source acknowledgement. Those require their own explicit
commands, identities and runtime reports. Passing the Arrow SQL qualification
does not close those gates.
