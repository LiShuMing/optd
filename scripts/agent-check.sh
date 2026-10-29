#!/usr/bin/env bash
# Verification entry point; does not silently skip a requested gate.
#
# Modes:
#   fast      - formatting, default-feature lint and tests
#   iceberg   - fast plus the iceberg-feature lint and tests
#   sql       - formatting, typed SQL lint, oracle and process-recovery tests
#   full      - everything, except the Postgres-backed perfbench integration test
#               (run the `perfbench` mode on a machine that has Postgres)
#   perfbench - optd-perfbench tests; requires a running Postgres and the
#               `test_user` role (see optd-perfbench/tests/cardtest_integration.rs)
#
# Every step runs even if an earlier one fails; the exit status is non-zero if
# any step failed, so one environment problem cannot hide the remaining gates.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mode="${1:-fast}"
toolchain="${OPTD_RUST_TOOLCHAIN:-1.95.0}"
failures=()
run() {
  printf 'CHECK %s\n' "$*"
  if ! "$@"; then
    failures+=("$*")
  fi
}
case "$mode" in
  fast|iceberg|sql|full|perfbench) ;;
  *) printf 'usage: %s [fast|iceberg|sql|full|perfbench]\n' "$0" >&2; exit 2 ;;
esac
if [[ "$mode" == perfbench ]]; then
  run cargo "+$toolchain" test -p optd-perfbench --all-features --locked
elif [[ "$mode" == sql ]]; then
  run cargo "+$toolchain" fmt --all -- --check
  run cargo "+$toolchain" clippy -p optd-incremental-sql --no-deps --all-targets --locked -- -D warnings
  run cargo "+$toolchain" test -p optd-incremental-sql --locked
else
  run cargo "+$toolchain" fmt --all -- --check
  run cargo "+$toolchain" clippy -p optd-incremental --no-deps --all-targets --no-default-features --locked -- -D warnings
  run cargo "+$toolchain" test -p optd-incremental --no-default-features --locked
  if [[ "$mode" != fast ]]; then
    run cargo "+$toolchain" clippy -p optd-incremental --no-deps --all-targets --features iceberg --locked -- -D warnings
    run cargo "+$toolchain" test -p optd-incremental --features iceberg --locked
  fi
  if [[ "$mode" == full ]]; then
    run cargo "+$toolchain" clippy --workspace --all-targets --all-features --locked -- -D warnings
    run cargo "+$toolchain" test --workspace --all-features --locked -- --skip cli_run_cardbench_twice
    run mdbook build docs
  fi
fi
if ((${#failures[@]})); then
  printf 'FAIL mode=%s toolchain=%s (%d step(s))\n' "$mode" "$toolchain" "${#failures[@]}"
  printf '  failed: %s\n' "${failures[@]}"
  exit 1
fi
printf 'PASS mode=%s toolchain=%s\n' "$mode" "$toolchain"
