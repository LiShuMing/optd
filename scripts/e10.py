#!/usr/bin/env python3
"""Run the E10 measurement sequence and append one JSON record.

The measuring binary is `optd-incremental/examples/cdc_bench.rs`; this driver
only sequences it, stamps the record with the commit and toolchain, and keeps
the per-command JSON objects together so two runs can be compared later.

Warehouses are benchmark state, not repository fixtures: keep them outside the
checkout and pass one directory per configuration.

    scripts/e10.py --dir /home/admin/tmp/e10 --label "A: 100k base, 11x2k"
    scripts/e10.py --dir DIR --reuse --record /home/admin/tmp/e10.jsonl

`--reuse` skips `init` and continues an existing warehouse; the refresh history
then grows by `--steps` more batches.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
BINARY = REPO / "target" / "release" / "examples" / "cdc_bench"
SCHEMA = "optd-e10/1"


def run(argv, cwd=None):
    return subprocess.run(argv, cwd=cwd, text=True, capture_output=True)


def build():
    completed = run(
        [
            "cargo",
            "build",
            "--release",
            "--example",
            "cdc_bench",
            "-p",
            "optd-incremental",
            "--features",
            "iceberg",
            "--locked",
        ],
        cwd=REPO,
    )
    if completed.returncode != 0:
        raise SystemExit(f"build failed:\n{completed.stderr}")
    if not BINARY.is_file():
        raise SystemExit(f"build produced no binary at {BINARY}")


def measure(directory, args):
    completed = run([str(BINARY), str(directory), *args])
    if completed.returncode != 0:
        raise SystemExit(
            f"cdc_bench {' '.join(args)} failed ({completed.returncode}):\n{completed.stderr}"
        )
    records = []
    for line in completed.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            records.append(json.loads(line))
    if not records:
        raise SystemExit(f"cdc_bench {' '.join(args)} printed no JSON")
    return records


def toolchain():
    completed = run(["rustc", "--version"])
    return completed.stdout.strip() or None


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dir", required=True, help="warehouse directory (outside the repo)")
    parser.add_argument("--label", default=None, help="free-form note for the record")
    parser.add_argument("--record", default=None, help="append the JSON record here")
    parser.add_argument("--base-rows", type=int, default=100_000, help="rows loaded by init")
    parser.add_argument("--batch", type=int, default=2_000, help="rows per refresh step")
    parser.add_argument("--steps", type=int, default=11, help="refresh steps")
    parser.add_argument("--fraction", type=float, default=0.9, help="read_since consumed fraction")
    parser.add_argument("--reuse", action="store_true", help="continue an existing warehouse")
    parser.add_argument("--quiet", action="store_true", help="only print the summary")
    args = parser.parse_args(argv)

    if args.steps < 1 or args.batch < 1 or args.base_rows < 1:
        parser.error("--base-rows, --batch and --steps must be positive")
    if not 0.0 <= args.fraction <= 1.0:
        parser.error("--fraction must be between 0 and 1")

    directory = Path(args.dir).expanduser().resolve()
    if args.reuse:
        if not (directory / "demo").exists():
            raise SystemExit(f"{directory} has no warehouse to reuse; run without --reuse")
    elif directory.exists() and any(directory.iterdir()):
        raise SystemExit(
            f"{directory} is not empty; remove it or pass --reuse. A refresh history is "
            f"not resumable across differently configured runs."
        )
    directory.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    build()

    record = {"schema": SCHEMA,
              "time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "label": args.label,
              "commit": run(["git", "-C", str(REPO), "rev-parse", "HEAD"]).stdout.strip() or None,
              "dirty": bool(run(["git", "-C", str(REPO), "status", "--porcelain"]).stdout.strip()),
              "rustc": toolchain(),
              "host": {"node": platform.node(), "machine": platform.machine(),
                       "cpus": os.cpu_count()},
              "config": {"base_rows": args.base_rows, "batch": args.batch,
                         "steps": args.steps, "reuse": args.reuse,
                         "page_cache": "warm"},
              "dir": str(directory)}

    if not args.reuse:
        record["init"] = measure(directory, ["init", "--rows", str(args.base_rows)])[0]
    record["refresh"] = measure(
        directory, ["refresh", "--steps", str(args.steps), "--batch", str(args.batch)]
    )
    record["phases"] = measure(directory, ["phases", "--batch", str(args.batch)])[0]
    record["parts"] = measure(directory, ["parts"])[0]
    record["read_since"] = measure(directory, ["read-since", "--fraction", str(args.fraction)])[0]
    record["recover"] = measure(directory, ["recover"])[0]
    record["observe_vs_recompute"] = measure(
        directory, ["observe-vs-recompute", "--batch", str(args.batch)]
    )[0]
    record["harness_seconds"] = round(time.monotonic() - started, 3)

    history = record["parts"]
    if not history["fold_matches_view"]:
        raise SystemExit("folding the log did not reproduce the view; the run is invalid")

    if args.record:
        path = Path(args.record).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    if not args.quiet:
        last = record["refresh"][-1]
        print(
            f"{history['rows']} history rows in {history['batches']} batches: "
            f"refresh {last['total_s']:.2f}s (pre-commit {last['pre_commit_s']:.2f}s), "
            f"recover {record['recover']['recover_s']:.2f}s, "
            f"read_since {record['read_since']['read_since_s']:.2f}s vs "
            f"{record['read_since']['read_batches_s']:.2f}s full"
        )
    if args.record:
        print(f"record appended to {Path(args.record).expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))