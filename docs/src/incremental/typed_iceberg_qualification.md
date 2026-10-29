# Typed Iceberg SQL loop — observed qualification

**Status:** verified local evidence · 2026-10-29
**Scope:** explicit local Iceberg v2/Parquet source tuples, typed SQL epochs,
atomic source/output checkpoints, strict recovery and fixed query profiles.

This is implementation evidence for [the accepted source contract](./typed_iceberg_sources.md).
[Usage](./typed_iceberg_loop.md) and [benchmark preparation](../../../optd-incremental-sql/benchmarks/README.md)
contain reproducible interfaces and commands. The earlier
[Arrow-input qualification](./sql_qualification.md) remains separate evidence.

## 1. Result and implementation

**Verified:** TPC-H **22/22** and TPC-DS **99/99** have complete individual
Iceberg-backed profiles with nonempty results and actual signed output changes.
Each chosen profile completes bootstrap and three mutation rounds: insert the
remaining half, delete the first row, restore it. These selected profiles total
121 bootstrap comparisons and 363 successful mutation rounds. Each epoch checks
the intended typed fixture against independent exact-snapshot scans, maintained
source caches, full-query output and signed output differences; strict restart
and prefix-by-prefix committed-output replay also pass.

The implemented path is:

```text
explicit complete source tuple
  -> retained UUID/schema/descriptor/ancestry validation
  -> exact typed before/after endpoint scans and signed multiset difference
  -> one immutable multi-table SQL prepare
  -> immutable IPC + source-vector.json + hashed predecessor manifest
  -> fsync + atomic CURRENT -> acknowledge
  -> strict source re-read / graph rebuild / output validation on restart
```

`source_vector.rs` and the format-2 store metadata compile without the optional
catalog dependency. `iceberg.rs` and `coordinator.rs` are feature-gated. Format 1
and old single-input CDC v1 remain separate protocols; mixing modes/lineages is
rejected. Identical v1/v2 commit retries now repeat the real parent-directory
fsync before returning success after a previous post-rename sync failure.

## 2. Fixed query and data identities

Both suites observed this same copied, feature-built executable:

```text
/tmp/optd-iceberg-benchmark-20261029-v02
SHA-256 fd959b935a9d221d573a78ddff64e5599815ff975eca1381a608308efc3043fb
```

It was copied after an actual Rust 1.95 build so later Cargo operations could
not replace it. The TPC-DS suite checks initial/final SHA and each process's
executable file identity. The manually invoked TPC-H profiles retain their
commands and report identities, with a final independent executable hash check.
This is observed executable identity, not embedded compile-time source provenance.

| Suite | Chosen evidence | Remaining qualification boundary |
| --- | --- | --- |
| TPC-H 22 | Original repository SQL/parameters/schema. The repository CSV profile provides 20 passes; unchanged Q11/Q21 pass separately on DuckDB 1.2.2 dbgen SF0.01 input written to real Iceberg tables. | Q15 uses the documented equivalent CTE; this is not official TPC certification or one common full-scale dataset. |
| TPC-DS 99 | Original DuckDB v1.4.3 numbered SQL/schema; one explicit directed witness per query, 674 input rows / 55,812 CSV bytes in total. | `directed_semantic_fixture_not_dsdgen`; separate tiny relations per query, not standard-generator/full-scale coverage or extra a/b variants. |

The repository-only TPC-H run exits 1, retaining Q11/Q21 as empty-only. The
first real-source SF0.01 Q21 run also exits 1 at the declared 120-second async
deadline. Its progress is unknown after timeout, not counted as completed.
A new isolated run with `OPTD_SQL_BENCH_QUERY_TIMEOUT_SECS=600` and a 660-second
outer process deadline exits 0 and passes all three rounds and recovery checks.
The final aggregate retains the timeout: `with_pass=22`, `remaining=0`,
`profiles_with_failures=1`. A successful retry does not erase the earlier failure.

TPC-DS records 99 actual exits of 0, 99 bootstrap and 297 round comparisons,
99 nonempty/changing profiles, complete strict reopen/replay evidence,
`full99_coverage=true`, and zero failed profiles. The original query/schema
hashes and all leaf-report hashes were independently rechecked; rebuilding the
strict aggregate from those leaves reproduces the complete aggregate SHA.

## 3. Exact local artifacts

These generated artifacts are local evidence, not repository contents. The
TPC-DS aggregate is 45,293,538 bytes because it retains complete source-vector
proofs; prefer `progress.json` or selected JSON fields for routine inspection.

| Artifact | SHA-256 |
| --- | --- |
| `/tmp/optd-tpch-iceberg-final-aggregate-20261029.json` | `b0d18e732267238ec977a74fa7842974a53a1e4fd45607ddcf1a4a82db0fecdc` |
| `/tmp/optd-tpch-iceberg-final-evidence-20261029.json` | `0db7b21c164c9d8294ad83dfe4a124aa1eb3ca44caacb38da81f7759a06c3a9c` |
| `/tmp/optd-tpch-iceberg-generated-q21-coverage-20261029.json` (timeout) | `b707524e15e3773e78b8fb1818d66efd3ae538f13f20a56f869046f742052090` |
| `/tmp/optd-tpch-iceberg-generated-q21-coverage-20261029-long.json` (complete) | `8c24272f7be57535a01287df18aff736c2390b1481c67f058199dd375cc5b0f5` |
| `/tmp/optd-tpcds-iceberg-suite-20261029/aggregate.json` | `13e766124e0dc19af790273d5813fdc8a2ddb848826f386447dc3bc72f9926ae` |
| `/tmp/optd-tpcds-iceberg-suite-20261029/progress.json` | `ca666810a3ff1c80fcf9057b7fb5c49b31fe18f59f849d8b17ae894c85978a55` |

The TPCH evidence file retains all four profile commands, input/backend roots,
report hashes and final executable hash. The TPC-DS progress file retains its
backend/root/environment, commands, process exits, hard deadlines, executable
checks and every leaf manifest/report identity. Its historical runner SHA is
`f549f874e0c44871cb8425100392ef9b8ed7ed6efa664b39a66de8ee9b358f51`;
the subsequently committed runner differs only by two module-docstring lines,
whose removal reproduces that historical SHA. The execution code is identical;
the original report has not been rewritten.

The 99 Iceberg profiles instantiate 488 independent source tables/UUIDs and
396 complete vector epochs, representing 1,952 table-endpoint identities.
Maintenance counters total 2,928 endpoint scans, 2,574 decoded rows, 6,396,446
decoded Arrow bytes and 1,457 consolidated signed source rows. These counters
exclude bootstrap, independent oracle and strict-restart reads. Decoded bytes
measure Arrow memory, not physical storage traffic. No performance comparison
or bounded total process-memory claim follows from these fixture work counters.

## 4. Build, test and recovery gates

**Verified environment:** Linux x86_64; Rust/Cargo 1.95.0
(`rustc 1.95.0 (59807616e 2026-04-14)`), locked DataFusion 55.0.0 / Arrow 59.2.0 /
Iceberg 0.10.1 at `5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3`;
Python 3.11.7; mdBook 0.4.52 official static executable under `/tmp`.

Relevant Cargo environment:

```sh
export PATH="/opt/rust/bin:/tmp/optd-mdbook-0.4.52:$PATH"
export RUSTUP_HOME=/home/admin/.rustup
export RUSTC=/home/admin/.rustup/toolchains/1.95.0-x86_64-unknown-linux-gnu/bin/rustc
export RUSTDOC=/home/admin/.rustup/toolchains/1.95.0-x86_64-unknown-linux-gnu/bin/rustdoc
export CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_INCREMENTAL=0 CARGO_NET_OFFLINE=true
export CARGO_TARGET_DIR=/tmp/optd-sql-msrv-20261029-target
export MDBOOK_BUILD__BUILD_DIR=/tmp/optd-source-v2-book-20261029
```

| Actual command | Observed result |
| --- | --- |
| `CARGO_TARGET_DIR=/tmp/optd-source-v2-fresh-check-20261029-target cargo +1.95.0 check -p optd-incremental-sql --all-targets --features iceberg --locked` | Exit 0 in an empty target; the current Iceberg-feature library, tests and examples compile. |
| `scripts/agent-check.sh sql` | Exit 0; fmt and both all-target warnings-denied lint configurations; default 72 passed / 1 ignored, feature 84 passed / 2 ignored. |
| `scripts/agent-check.sh full` | Exit 0; existing default CDC 41 and Iceberg CDC 70 pass; workspace all-feature lint and 350 workspace tests pass; 4 ignored / 1 explicitly filtered; book builds. |
| `scripts/agent-check.sh sql-tools` | Exit 0; offline pinned-source verification plus 28 Python tests, including real subprocess timeouts, conflicting backend/environment and forged proof rejection. |
| `cargo +1.95.0 check --manifest-path /tmp/optd-typed-iceberg-doc-check-20261029/Cargo.toml --offline` | Exit 0; the complete usage-document example compiles with identical existing dependency version/source/checksum identities. Not a runtime test of arbitrary user tables. |

The default/feature/full tests reuse the previously created qualification target
and recompile changed crates; only the separately listed fresh-target check is
a fresh configuration build. Logs:
`/tmp/optd-source-v2-{fresh-check,sql-gates,full-gates,sql-tools}-20261029.log`.
The full gate's filtered case is the explicitly separate PostgreSQL performance
fixture `cli_run_cardbench_twice`. The ignored cases are two existing Parquet
metadata tests, the opt-in whole TPC-H fixture test and a subprocess driver
invoked explicitly by its successful parent recovery test. They are not counted
as successful standalone test executions. Remote GitHub workflow results are
not established by local commands; CI is configured to run both SQL features
and explicitly prepare public inputs before the Python tools gate.

Actual source tests exercise nine scalar types, NULLs/duplicates, 20,000-row
multi-batch scans, rejection beyond the cumulative endpoint budget, two-sided
join changes and self joins, overwrite/empty/rewrite snapshots, frozen mixed-time
tuples, UUID/schema/descriptor/retained-lineage failures and missing files.
Independent format-2 tests cover immutable bindings, malformed/corrupt/spliced
vectors, mode isolation, held writers and retry identity. Real OS process death
exercises both publication windows for bootstrap/successor store envelopes and
the typed source coordinator. Private production-path IO injection verifies that
post-rename directory-fsync failure and repeated failed syncs cannot return a
false successful retry. This is local crash/recovery evidence, not hardware
power-loss certification or distributed fencing.

## 5. Practical limits and next work

Native deltas cover linear operators and inner joins; eligible ordinary GROUP BY
uses changed-group recomputation. Global aggregates, outer/semi/anti joins,
grouping sets, windows and ordered/limited chains may recompute their local
operator inputs. Strategies/work are reported explicitly; there is no hidden
whole-SQL fallback or claim of native incremental algorithms for every query.

Changed-source acquisition reads full before/after snapshots. The qualified
source profile is local/main/v2/Parquet with fixed schemas and no delete manifests;
it rejects schema evolution, off-main/expired history and unsupported types.
Each endpoint is bounded to 1,000,000 decoded rows and 256 MiB of decoded/scalar
storage estimates, with separate manifest-list/task limits. The tuple is explicit,
not an upstream cross-table transaction. Output ownership and atomic publication
are host-local, and strict restart requires retained accessible sources.

Large-scale performance, native source changelogs/delete files, persistent
aggregate accumulators/indexes, remote/distributed ownership and production
deployment remain separate projects with their own designs and evidence.
