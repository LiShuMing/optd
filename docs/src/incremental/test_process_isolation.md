# Test Process Creation and Local File Locks

**Status:** accepted · 2026-10-29
**Scope:** the `optd-incremental/tests/cdc_iceberg.rs` test binary only; no
production lock change, retry, skipped test, dependency change or suite-wide
serialization.

## 1. Problem

Parallel acceptance tests both create subprocesses and acquire nonblocking local
file locks. After fork/vfork, a child inherits the parent's open file descriptions
until exec closes CLOEXEC descriptors. Another test can drop its owning File in
that interval while the child still legitimately retains its lock.

Observed: the 2026-10-29 Iceberg gate failed with `CdcError::Ownership` at the
`expire_snapshots()` immediately following `compact()` in
`all_empty_bootstrap_compacts_expires_and_resumes_from_no_source_snapshot`;
23 other tests passed. Both methods acquire the same short-lived scan writer
lock. `LocalCatalog::WriterLock` uses `File::try_lock`. The failure did not record
the actual inherited descriptor, so attributing that particular event to a child
is an inference, not a verified process trace.

Verified separately with Rust 1.95.0's exact `File::try_lock`: a descriptor has
FD_CLOEXEC set; after fork and parent drop, another lock attempt returns
WouldBlock while the child waits before exec; after child exec/exit the attempt
succeeds. Therefore a test-only correction must cover internal scan/catalog locks
as well as visible `drop(sink)` / reopen pairs.

## 2. Options considered

1. Retry Ownership in production or tests. Reject: this weakens the immediate,
   nonblocking ownership contract and can conceal real ownership failures.
2. Use one exclusive mutex for every test. Reject: unrelated warehouses should
   remain concurrent, and this would serialize the complete suite.
3. Use a shared activity lease for normal test work and an exclusive process
   creation window. Select. Ordinary test work runs concurrently; only fork/exec
   waits until other test work has released all its local lock descriptors.

## 3. Decision

Add one private, test-only activity gate implemented with `Mutex<usize>`,
`Condvar` and a non-cloneable RAII activity lease. Entering increments the active
count and immediately releases the mutex. Dropping the lease decrements the
count and notifies waiters when it reaches zero. No standard mutex guard crosses
an await, and no production API/dependency or Tokio feature changes.

Every ordinary async test obtains a lease before creating its fixture, so all
its local objects are dropped before its first-declared lease. These leases are
shared activities; multiple tests can hold them simultaneously.

The synchronous process-kill tests wrap each existing `runtime.block_on(...)`
phase in the same activity lease. Those phases return only lock-free fixture or
value objects; local drivers, sinks, scan pins and consumers are dropped inside
the protected phase. They do not hold a lease while spawning or waiting for a
child. Worker entry points may use the wrapper too: exec creates a separate gate
in their own process.

Both parent-side Command creation sites call one gate helper. It waits for an
active count of zero, then retains the mutex through synchronous
`Command::spawn()`. New activities cannot start until spawn returns after the
exec handshake. The helper releases the mutex immediately after that boundary;
child barrier waiting, OS kill, wait/reaping and recovery remain unchanged.
A 30-second real deadline bounds waiting for active work; expiry fails with the
remaining activity count and creates no child. No Ownership error is caught.

The activity gate's exclusive callback may never reenter the gate, await, invoke
runtime work or wait for child completion. Its only production use in this test
binary is the synchronous process creation call. Keep that invariant in code
comments.

## 4. Failure modes and recovery

A missing activity boundary could leave an uncovered inherited descriptor. Audit
all two Command::spawn sites, all ordinary async test bodies and all block_on
phases in this exact test binary. An activity returned from a phase while a local
file-lock-owning object escaped would likewise violate the boundary; the current
fixture/catalog/value return types hold no such file descriptor.

An active production owner must still cause immediate Ownership. Contention
assertions, snapshot/delta identities, subprocess publication barriers and
post-kill recovery assertions are retained verbatim. A gate timeout or lock
poisoning fails the test rather than silently proceeding. RAII releases a lease
when a test fails, without catching that test's failure.

## 5. Acceptance criteria

1. A direct gate test holds two ordinary activity leases concurrently. An
   exclusive callback cannot run after only one drops; it runs after the last
   drop. This test creates no unprotected child process.
2. All existing 24 Iceberg acceptance tests and the new gate test run with normal parallel test
   scheduling, including immediate ownership rejection, compaction/expiration,
   all-empty bootstrap, and every actual subprocess-kill window.
3. Repeat the parallel Iceberg test binary across multiple runs; report counts,
   toolchain and failures rather than claiming a single pass proves absence of
   all races. Existing SQL store isolation remains separately tested.
4. Formatting and warnings-denied scoped lint pass. No performance, distributed
   ownership or production retry claim follows from this correction.

## 6. Rollout and open questions

Obtain peer review before editing the test binary. The integration owner links
this design from SUMMARY.md and records actual verification results. No new
storage format or runtime migration exists. Future process creation in this
binary must use the helper and future IO tests must acquire an activity lease.

## 7. Observed verification · 2026-10-29

**Verified:** Rust 1.95.0, locked offline dependencies, reused
`/tmp/optd-sql-msrv-20261029-target`; both dev/test debug information and Cargo
incremental compilation disabled, four build jobs. This is an incremental source
rebuild plus actual test executions, not a fresh dependency/configuration build.
The exact Cargo environment was:

```bash
export PATH="/opt/rust/bin:$PATH"
export RUSTUP_HOME=/home/admin/.rustup
export RUSTC=/home/admin/.rustup/toolchains/1.95.0-x86_64-unknown-linux-gnu/bin/rustc
export RUSTDOC=/home/admin/.rustup/toolchains/1.95.0-x86_64-unknown-linux-gnu/bin/rustdoc
export CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_INCREMENTAL=0 CARGO_NET_OFFLINE=true
export CARGO_TARGET_DIR=/tmp/optd-sql-msrv-20261029-target
cargo +1.95.0 test -p optd-incremental --features iceberg --locked --test cdc_iceberg
cargo +1.95.0 clippy -p optd-incremental --no-deps --all-targets --features iceberg --locked -- -D warnings
```

* Test command: exit 0; 25 passed, zero failed/ignored/filtered, including the
  previous all-empty failure, immediate ownership assertions, the new shared
  activity test and all actual subprocess-kill windows.
* Feature lint command: exit 0, warnings denied.
* The compiled `cdc_iceberg-d653250a5ab8c37b` binary was then run directly 20
  times with default parallel scheduling and no test-thread override. Every run
  exited 0 with 25 passed and no ignored/filtered cases: 500 further test
  completions. The runner used a 30-second deadline per run; no deadline fired.
* Targeted Rust 1.95 rustfmt check and `git diff --check` exited 0.

Local evidence: `/tmp/optd-cdc-isolation-test-20261029.log`,
`/tmp/optd-cdc-isolation-clippy-20261029.log`,
`/tmp/optd-cdc-isolation-repeat-20261029.log` and its numbered per-run logs.
The initial test/clippy execution sessions and the repeat runner all completed;
no owned investigation or test process remains running.

**Boundary:** repeated passes validate this test correction under the observed
host scheduling; they do not prove all possible interleavings or change the
production file-lock ownership contract. Other workspace gates and the complete
`scripts/agent-check.sh iceberg` invocation remain the integration owner's gate.
