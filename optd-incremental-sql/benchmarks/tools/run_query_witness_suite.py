#!/usr/bin/env python3
"""Run unchanged TPC-DS witness queries with a pinned benchmark executable.

All generated inputs, logs, progress and strict aggregate stay in a new /tmp
directory. Defaults to all 99 queries; --query selects a visibly partial run.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from generate_query_witness_fixture import checked_source, generate
from summarize_coverage import summarize


def file_digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def file_identity(path):
    value = path.stat()
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def same_file(path, identity):
    try:
        return file_identity(path) == identity
    except OSError:
        return False


def selected_queries(query_ids):
    queries = list(range(1, 100)) if query_ids is None else list(query_ids)
    if not queries or any(type(value) is not int or not 1 <= value <= 99 for value in queries):
        raise ValueError("query IDs must be integers in1..99")
    if len(queries) != len(set(queries)):
        raise ValueError("duplicate query IDs are not allowed")
    return queries


def save_progress(path, data):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_suite(binary, expected_sha256, output, query_ids=None, timeout_seconds=180):
    """Execute selected queries and return the recorded suite progress object."""
    queries = selected_queries(query_ids)
    if not math.isfinite(timeout_seconds) or not 0.01 <= timeout_seconds <= 3600:
        raise ValueError("per-query hard timeout must be within0.01..3600seconds")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise ValueError("expected binary SHA-256 must be64 lowercase hexadecimal characters")
    binary = Path(binary).resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("benchmark binary must be an executable regular file")
    identity = file_identity(binary)
    if file_digest(binary) != expected_sha256 or file_identity(binary) != identity:
        raise ValueError("benchmark binary SHA-256/identity mismatch")
    output = Path(output).resolve()
    if not output.is_relative_to(Path("/tmp")) or output == Path("/tmp"):
        raise ValueError("suite output must be a new child directory of /tmp")
    if output.exists():
        raise ValueError("suite output already exists; choose a new /tmp directory")
    source = Path(__file__).resolve().parents[1] / "tpcds/manifest.json"
    checked_source(source)
    output.mkdir(parents=True)
    progress_path = output / "progress.json"
    progress = {
        "format_version": 1, "kind": "directed_tpcds_witness_suite", "binary": str(binary),
        "observed_executable_sha256": expected_sha256,
        "executable_checks": "initial/final SHA-256; per-query device/inode/size/mtime_ns/ctime_ns",
        "provenance_boundary": "Observed executable identity, not embedded compile-time source provenance",
        "runner_sha256": file_digest(Path(__file__)), "selected_query_ids": queries,
        "full99_requested": sorted(queries) == list(range(1, 100)),
        "per_query_hard_timeout_seconds": timeout_seconds, "records": [], "complete": False,
        "full99_coverage": False, "status": "running",
    }
    save_progress(progress_path, progress)
    specifications = []
    for query_id in queries:
        if not same_file(binary, identity):
            progress["error"] = "benchmark binary changed before query execution"
            break
        fixture = output / f"q{query_id:02}" / "fixture"
        try:
            prepared = generate(source, fixture, query_id)
        except (ValueError, OSError) as error:
            record = {"query_id": query_id, "status": "fixture_error", "error": str(error),
                      "exit_code": None, "hard_timeout": False}
            progress["records"].append(record)
            save_progress(progress_path, progress)
            print(json.dumps({"query_id": query_id, "status": "fixture_error"}), flush=True)
            continue
        report_path = fixture.parent / "coverage.json"
        log_path = fixture.parent / "execution.log"
        command = [str(binary), prepared["manifest"], str(report_path), str(query_id)]
        record = {"query_id": query_id, "preparation": prepared, "command": command,
                  "log": str(log_path), "report": str(report_path), "exit_code": None,
                  "status": "report_missing", "hard_timeout": False}
        start = time.monotonic()
        with log_path.open("w", encoding="utf-8") as log:
            try:
                completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                                           timeout=timeout_seconds, check=False)
                record["exit_code"] = completed.returncode
                if completed.returncode != 0:
                    record["status"] = "process_failed"
            except subprocess.TimeoutExpired:
                record.update(status="hard_timeout", hard_timeout=True)
            except OSError as error:
                record.update(status="execution_error", error=str(error))
        record["wall_seconds"] = round(time.monotonic() - start, 3)
        if report_path.exists():
            record["report_sha256"] = file_digest(report_path)
            specifications.append((f"witness-q{query_id:02}", str(report_path), record["report_sha256"]))
            try:
                report = json.loads(report_path.read_text(encoding="utf-8"))
                observed = report["queries"]
                if len(observed) != 1 or observed[0]["query_id"] != query_id:
                    raise ValueError("focused report has wrong query count/identity")
                record["reported_status"] = observed[0]["status"]
                record["reason"] = observed[0].get("reason")
                record["completed_rounds"] = observed[0].get("completed_rounds")
                if not record["hard_timeout"] and record["exit_code"] == 0:
                    record["status"] = observed[0]["status"]
                elif not record["hard_timeout"]:
                    record["status"] = "process_failed"
            except (ValueError, KeyError, TypeError) as error:
                record.update(status="invalid_report", error=str(error))
        progress["records"].append(record)
        save_progress(progress_path, progress)
        print(json.dumps({"query_id": query_id, "status": record["status"],
                          "completed_rounds": record.get("completed_rounds")}), flush=True)
        if not same_file(binary, identity):
            progress["error"] = "benchmark binary changed during query execution"
            break
    if not same_file(binary, identity) or file_digest(binary) != expected_sha256:
        progress["error"] = "benchmark binary final SHA-256/identity mismatch"
    if specifications:
        try:
            aggregate = summarize(specifications, Path(__file__).resolve().parents[3])
            aggregate["observed_executable_sha256"] = expected_sha256
            aggregate["executable_evidence"] = str(progress_path)
            aggregate_path = output / "aggregate.json"
            aggregate_path.write_text(json.dumps(aggregate, indent=2) + "\n", encoding="utf-8")
            progress["aggregate"] = {"path": str(aggregate_path), "sha256": file_digest(aggregate_path),
                                     **aggregate["aggregate"]}
        except (ValueError, KeyError, TypeError, OSError) as error:
            progress["aggregation_error"] = str(error)
    else:
        progress["aggregation_error"] = "no completed reports available for strict aggregation"
    progress["complete"] = len(progress["records"]) == len(queries)
    passed = (progress["complete"] and not progress.get("error") and
              not progress.get("aggregation_error") and
              all(record["status"] == "pass" for record in progress["records"]))
    progress["status"] = "pass" if passed else "fail"
    progress["full99_coverage"] = bool(passed and progress["full99_requested"] and
                                        progress.get("aggregate", {}).get("with_pass") == 99)
    save_progress(progress_path, progress)
    return progress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--query", type=int, action="append")
    parser.add_argument("--timeout-seconds", type=float, default=180)
    args = parser.parse_args()
    try:
        progress = run_suite(args.binary, args.expected_sha256, args.output, args.query, args.timeout_seconds)
    except (ValueError, OSError) as error:
        parser.exit(2, f"witness suite: {error}\n")
    print(json.dumps({"output": str(args.output.resolve()), "status": progress["status"],
                      "full99_coverage": progress["full99_coverage"], "aggregate": progress.get("aggregate")}), flush=True)
    return 0 if progress["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
