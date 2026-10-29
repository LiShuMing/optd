# Checkpoint Directory Creation Durability

**Status:** accepted after independent agent review · 2026-10-29
**Scope:** host-local checkpoint directory creation and its observable fsync chain.
This preserves the typed checkpoint format and recursive path creation.

## 1. Problem

`CheckpointStore::open` creates directories recursively but currently synchronizes
only the warehouse and its immediate parent. For a new path such as
`existing/new-parent/warehouse`, publishing CURRENT does not synchronize the
`new-parent` entry in `existing`. A power failure may therefore lose the directory
subtree despite synchronized checkpoint files. Existing SQL SIGKILL tests do not
prove or disprove power-failure persistence of directory entries.

## 2. Constraints and evidence

The [typed execution design](./sql_execution.md) requires durable publication
before acknowledgement. Keep existing recursive path behavior, relative paths,
and existing symlink aliases to directories. Use only standard filesystem APIs;
do not introduce a public API, dependency, durable format, cleanup, or service.
An existing directory is not a sufficient durability anchor: a previous failed
open may have created it without synchronizing its parent entry.

## 3. Options considered

1. Require an already-existing parent and create only the warehouse: correct but
   unnecessarily breaks recursive path compatibility.
2. Synchronize every canonical ancestor through filesystem root on every open:
   selected. This covers both initial recursive creation and retries after an
   interrupted or failed opening without adding a recovery sidecar.
3. Find the existing anchor before creation and synchronize only to that anchor:
   rejected after review found the failed-attempt/retry hole. A later opening
   cannot distinguish a durable old directory from one left by a failed attempt.

## 4. Decision

Before creation, convert a nonempty relative path to an absolute path using the
current directory. Run the existing recursive directory creation, then
canonicalize the resulting warehouse and verify it is a directory. This resolves
relative paths, `..`, and symlink-parent aliases to the actual destination.

Acquire the existing warehouse writer lock, then fsync the canonical warehouse,
its parent, and every ancestor through filesystem root inclusive, from child to
parent. Apply the same complete rule when the warehouse already exists, including
after an earlier opening created directories and failed before finishing fsync.
The exact ancestor chain is bounded by path depth; it does not scan directory
contents or search unrelated trees. This persists the writer-lock entry, each
newly created directory on the actual warehouse path, and every parent directory
entry naming that path. Incidental directories traversed and then escaped through
`..` are outside the resulting warehouse path and are not promised durable.

Keep the chain computation and synchronization in private helpers that the
store's opening path calls. A test-only injected synchronization callback records
the exact chain and injects an error at a selected ancestor. Production uses the
existing `sync_dir` function for each entry. This adds directory synchronization
and read-permission requirements on higher ancestors. Any permission or fsync
failure is explicit: opening fails and drops its writer lock. Failed attempts
retain their directories, matching existing orphan behavior.

## 5. Failure modes and recovery

* A missing ancestor can be created recursively; the full canonical ancestor
  chain is synchronized on this attempt and every subsequent retry.
* Existing files, non-directory parents, dangling aliases, permission failures,
  canonicalization failures, and fsync failures return errors before success.
* Existing relative and symlink paths identify the same canonical warehouse and
  continue sharing its writer lock.
* External processes concurrently renaming or retargeting the directory tree
  during bootstrap are outside the existing host-local writer contract. This
  change does not claim descriptor-relative path traversal or adversarial race
  protection.

## 6. Acceptance criteria

Private observable-chain tests cover multiple missing ancestors, existing root,
relative path, symlink-parent alias on Unix, path normalization with `..`, file
and non-directory rejection, and a synchronization failure that does not return a
store or retain its lock. A retry after that failure must still synchronize the
complete chain, including ancestors beyond the previous failure. Tests call the
actual directory-setup/opening helpers;
the callback records which directories production would synchronize and can
delegate to real `sync_dir`.

Run formatting, relevant SQL/store tests, and warnings-denied lint through the
integration owner's shared Cargo execution. A recorded chain plus successful
fsync calls is source/runtime evidence of the intended sequence, not an actual
hardware power-failure experiment. Do not report it as power-loss certification.

## 7. Rollout / migration

Change only store opening and private tests. No existing checkpoint is rewritten,
no new durable field is added, and epoch publication/recovery semantics remain
unchanged. Link this design in the documentation index after review acceptance.

## 8. Open questions

No decision blocks this bounded fix. Fault-injected filesystem or hardware
power-failure testing would provide stronger persistence evidence separately.

## 9. Observed verification

**Verified, 2026-10-29:** `scripts/agent-check.sh sql` exited 0 with Rust 1.95.0.
Formatting and warnings-denied all-target SQL Clippy passed. SQL tests reported
61 passed, 0 failed, and 1 explicitly ignored complete-benchmark qualification;
the nine directory-chain tests passed, including failure followed by full-chain
retry. SQL/store real-process recovery tests also passed. Documentation tests
ran with zero cases.

The command used `/opt/rust/bin` first in PATH,
`RUSTUP_HOME=/home/admin/.rustup`, and explicit `RUSTC`/`RUSTDOC` from
`/home/admin/.rustup/toolchains/1.95.0-x86_64-unknown-linux-gnu/bin`.
It used `CARGO_BUILD_JOBS=4`, `CARGO_PROFILE_DEV_DEBUG=0`,
`CARGO_PROFILE_TEST_DEBUG=0`, `CARGO_INCREMENTAL=0`, `CARGO_NET_OFFLINE=true`,
and `CARGO_TARGET_DIR=/tmp/optd-sql-msrv-20261029-target`.
Full output is `/tmp/optd-directory-durability-sql-20261029.log`.
This reused the integration owner's target directory and recompiled the changed
crate; it is not an additional fresh-target build or power-failure experiment.
