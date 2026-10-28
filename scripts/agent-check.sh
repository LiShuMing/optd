#!/usr/bin/env bash
# Verification entry point; does not silently skip a requested gate.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mode="${1:-fast}"
toolchain="${OPTD_RUST_TOOLCHAIN:-1.95.0}"
run() {
  printf 'CHECK %s\n' "$*"
  "$@"
}
case "$mode" in
  fast|iceberg|full) ;;
  *) printf 'usage: %s [fast|iceberg|full]\n' "$0" >&2; exit 2 ;;
esac
run cargo "+$toolchain" fmt --all -- --check
run cargo "+$toolchain" clippy -p optd-incremental --no-deps --all-targets --no-default-features --locked -- -D warnings
run cargo "+$toolchain" test -p optd-incremental --no-default-features --locked
if [[ "$mode" != fast ]]; then
  run cargo "+$toolchain" clippy -p optd-incremental --no-deps --all-targets --features iceberg --locked -- -D warnings
  run cargo "+$toolchain" test -p optd-incremental --features iceberg --locked
fi
if [[ "$mode" == full ]]; then
  run cargo "+$toolchain" clippy --workspace --all-targets --all-features --locked -- -D warnings
  run cargo "+$toolchain" test --workspace --all-features --locked
  run mdbook build docs
fi
printf 'PASS mode=%s toolchain=%s\n' "$mode" "$toolchain"
