#!/usr/bin/env python3
"""Compare local database storage behavior using two fixed sql_database binaries."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import time

MAX_LEGACY_BYTES = 16 * 1024 * 1024
WRITE = "UPDATE t SET k = 1 - k"
VERIFY = "SELECT SUM(k) AS k FROM t; SELECT * FROM mv;"


def encode(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def digest_file(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def current(root):
    pointer = json.loads((root / "CURRENT").read_bytes())
    directory = root / pointer["directory"]
    return pointer, directory, json.loads((directory / pointer["manifest"]["name"]).read_bytes())


def run(binary, root, *arguments, measure=False, check=True):
    command = [binary, root, *arguments]
    platform_name = platform.system()
    time_binary = Path("/usr/bin/time")
    if measure and time_binary.exists():
        if platform_name == "Darwin":
            command = [time_binary, "-l", *command]
        elif platform_name == "Linux":
            command = [time_binary, "-f", "OPTD_RSS_KIB=%M", *command]
    started = time.perf_counter_ns()
    result = subprocess.run(command, check=check, capture_output=True, text=True, timeout=120)
    elapsed = time.perf_counter_ns() - started
    if result.returncode:
        return result.returncode, result.stderr

    rss = None
    if measure and platform_name == "Darwin":
        match = re.search(r"^\s*(\d+)\s+maximum resident set size", result.stderr, re.M)
        if match:
            rss = int(match.group(1))
    elif measure and platform_name == "Linux":
        match = re.search(r"^OPTD_RSS_KIB=(\d+)$", result.stderr, re.M)
        if match:
            rss = int(match.group(1)) * 1024
    return json.loads(result.stdout), {"cli_ns": elapsed, "peak_rss_bytes": rss}


def script_file(path, contents):
    path.write_text(contents, encoding="utf-8")
    return path


def run_script(binary, root, script, contents, measure=False):
    return run(binary, root, script_file(script, contents), measure=measure)


def seed_receipts(baseline, root, script, count, near_limit=False):
    run_script(baseline, root, script, "CREATE TABLE t(k BIGINT); INSERT INTO t VALUES (0);")
    run(baseline, root, "--request", "control-00000", script_file(script, WRITE))
    pointer, directory, manifest = current(root)
    ref = manifest["receipt_index"]["file"]
    source = directory / ref["name"]
    original = json.loads(source.read_bytes())["control-00000"]
    entries = {"control-00000": original}
    if near_limit:
        per_entry = len(encode(entries)) - 1
        count = (MAX_LEGACY_BYTES - 256) // per_entry
    for number in range(1, count):
        request_id = f"control-{number:05}"
        entries[request_id] = dict(original, id=request_id)
    if near_limit:
        entries["control-00001"]["report"] = dict(original["report"], maintenance={"padding": ""})
        left = MAX_LEGACY_BYTES - 128 - len(encode(entries))
        entries["control-00001"]["report"]["maintenance"]["padding"] = "x" * left
    payload = encode(entries)
    if near_limit:
        assert len(payload) == MAX_LEGACY_BYTES - 128
    assert len(payload) <= MAX_LEGACY_BYTES
    source.write_bytes(payload)
    ref.update(bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())
    manifest_file = directory / pointer["manifest"]["name"]
    serialized = encode(manifest)
    manifest_file.write_bytes(serialized)
    pointer["manifest"].update(bytes=len(serialized), sha256=hashlib.sha256(serialized).hexdigest())
    (root / "CURRENT").write_bytes(encode(pointer))
    return {"receipts": count, "legacy_index_bytes": len(payload)}


def receipt_sample(binary, root, script, samples):
    script_file(script, WRITE)
    run(binary, root, "--request", "warmup-00001", script)
    observations = []
    for index in range(samples):
        result, measurement = run(binary, root, "--request", f"sample-{index:05}", script, measure=True)
        assert not result["replayed"] and result["commit"]["affected_rows"] == 1
        _, directory, _ = current(root)
        lengths = [
            path.stat().st_size for path in directory.iterdir()
            if path.name == "receipts.json" or path.name.startswith("receipt-node-")
        ]
        assert lengths
        measurement.update(receipt_files=len(lengths), receipt_bytes=sum(lengths))
        observations.append(measurement)
    retry, _ = run(binary, root, "--request", "control-00000", script)
    assert retry["replayed"]
    result, _ = run_script(binary, root, script, "SELECT SUM(k) AS n FROM t;")
    assert result["results"][0]["output"]["rows"] == [[str(samples % 2)]]
    return observations


def prove_receipt_capacity(binaries, root, script):
    baseline = binaries["baseline"]
    candidate = binaries["candidate"]
    fixture = root / "at-limit"
    result = seed_receipts(baseline, fixture, script, 0, near_limit=True)
    before = (fixture / "CURRENT").read_bytes()
    status, error = run(baseline, fixture, "--request", "new-00001", script, check=False)
    assert status == 1 and "oversized checkpoint JSON file" in error
    assert (fixture / "CURRENT").read_bytes() == before
    written, _ = run(candidate, fixture, "--request", "new-00001", script)
    assert not written["replayed"]
    old, _ = run(candidate, fixture, "--request", "control-00000", script)
    new, _ = run(candidate, fixture, "--request", "new-00001", script)
    assert old["replayed"] and new["replayed"]
    return dict(result, baseline_rejected=True, candidate_committed=True, both_ids_replayed=True)


def compaction_cases(binaries, root, script, epochs, repeats):
    outcomes = {}
    for dirty in (False, True):
        shape = "one_relevant_write" if dirty else "idle"
        fixture = root / f"compaction-{shape}"
        run_script(
            binaries["baseline"], fixture, script,
            "CREATE TABLE t(k BIGINT); INSERT INTO t VALUES (0); "
            "CREATE TABLE busy(k BIGINT); INSERT INTO busy VALUES (0); "
            "CREATE MATERIALIZED VIEW mv AS SELECT SUM(k) AS k FROM t;"
            + ("UPDATE t SET k = 1;" if dirty else "")
            + "UPDATE busy SET k = 1 - k;" * epochs,
        )
        outcomes[shape] = {}
        for label, binary in binaries.items():
            warehouse = root / f"compaction-{shape}-{label}"
            shutil.copytree(fixture, warehouse)
            before, _ = run_script(binary, warehouse, script, VERIFY)
            compacted, timing = run(binary, warehouse, "--compact", "1", measure=True)
            post = []
            for _ in range(repeats):
                after, measurement = run_script(binary, warehouse, script, VERIFY, measure=True)
                assert after["committed_epoch"] == before["committed_epoch"]
                result_rows = [statement["output"]["rows"] for statement in after["results"]]
                assert result_rows == [[[str(int(dirty))]], [[str(int(dirty))]]]
                post.append(measurement)
            run(binary, warehouse, "--replay")
            outcomes[shape][label] = {
                "report": compacted["compaction"],
                "compaction_cli": timing,
                "reopen_and_verify": post,
                "recovery_before": before["work"]["recovery"],
                "recovery_after": after["work"]["recovery"],
            }
    return outcomes


def percentiles(values):
    ordered = sorted(values)
    return {f"p{p}": ordered[math.ceil(p / 100 * len(ordered)) - 1] for p in (50, 95, 99)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--history-epochs", type=int, default=128)
    args = parser.parse_args()
    if min(args.samples, args.repeats, args.history_epochs) <= 0:
        parser.error("samples, repeats and history epochs must be positive")
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    identities = {label: digest_file(path) for label, path in binaries.items()}
    if identities["baseline"] == identities["candidate"]:
        parser.error("baseline and candidate binaries have identical hashes")

    args.output = args.output.resolve()
    with tempfile.TemporaryDirectory(dir=args.output.parent, prefix="storage-fixture-") as temporary:
        root = Path(temporary)
        script = root / "script.sql"
        result = {"binary_sha256": identities, "platform": platform.platform()}
        result["old_receipt_limit"] = prove_receipt_capacity(binaries, root, script)
        result["receipt_scaling"] = {}
        for prior in (16, 4096, 8192):
            fixture = root / f"receipts-{prior}"
            seeded = seed_receipts(binaries["baseline"], fixture, script, prior)
            by_binary = {label: [] for label in binaries}
            for trial in range(args.repeats):
                order = list(binaries)
                if (trial + prior // 4096) % 2:
                    order.reverse()
                for label in order:
                    warehouse = root / f"run-{prior}-{trial}-{label}"
                    shutil.copytree(fixture, warehouse)
                    by_binary[label].extend(receipt_sample(binaries[label], warehouse, script, args.samples))
                    shutil.rmtree(warehouse)
            result["receipt_scaling"][prior] = {
                "fixture": seeded,
                "results": {
                    label: {
                        "samples": samples,
                        "cli_ns": percentiles([sample["cli_ns"] for sample in samples]),
                        "receipt_bytes": percentiles([sample["receipt_bytes"] for sample in samples]),
                        "peak_rss_bytes": max((sample["peak_rss_bytes"] or 0 for sample in samples)) or None,
                    }
                    for label, samples in by_binary.items()
                },
            }
            print(f"Completed receipt comparison with {prior:,} prior IDs.", flush=True)
        result["compaction"] = compaction_cases(binaries, root, script, args.history_epochs, args.repeats * 5)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
