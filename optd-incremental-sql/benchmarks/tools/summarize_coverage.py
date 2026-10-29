#!/usr/bin/env python3
"""Combine immutable qualification reports without combining their rounds.

Every --report takes a profile label, report path, and expected report SHA-256.
Relative manifest paths are resolved from --repository-root. A query receives
coverage only when one individual profile contains all three successful rounds,
a nonempty result and a nonzero signed output delta. Other failures remain in
the aggregate. Different SQL parameter/source identities are retained verbatim.
"""

import argparse
import hashlib
import json
from pathlib import Path


STAGES = ("insert_remaining_half", "delete_first_row", "restore_first_row")
STATUSES = {"pass", "fail", "verified_empty_only", "verified_unchanged_output"}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def checked_json(path, expected):
    data = path.read_bytes()
    actual = digest(data)
    if actual != expected:
        raise ValueError(f"SHA-256 mismatch for {path}: expected {expected}, got {actual}")
    return json.loads(data)


def nonnegative(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def check_record(record):
    status = record.get("status")
    if status not in STATUSES:
        raise ValueError(f"unknown query status: {status!r}")
    rounds = record.get("rounds")
    if not isinstance(rounds, list) or len(rounds) > 3:
        raise ValueError("query rounds must be a list of at most three entries")
    for index, item in enumerate(rounds):
        if item.get("stage") != STAGES[index] or item.get("status") not in {"pass", "fail"}:
            raise ValueError("round stage/status is invalid or out of order")
        if item["status"] == "fail" and index != len(rounds) - 1:
            raise ValueError("rounds continue after a failed round")
        if item["status"] == "pass" and not all(
            nonnegative(item.get(field)) for field in ("output_rows", "signed_delta_rows")
        ):
            raise ValueError("successful round has invalid row counts")
    completed = sum(item["status"] == "pass" for item in rounds)
    declared = record.get("completed_rounds")
    if declared is not None and (not nonnegative(declared) or declared != completed):
        raise ValueError("completed_rounds differs from the actual successful rounds")
    complete = completed == 3 and len(rounds) == 3
    if status != "fail":
        if not complete or declared != 3 or not nonnegative(record.get("initial_rows")):
            raise ValueError("verified query lacks bootstrap or all three rounds")
        if record.get("final_rows") != rounds[-1]["output_rows"]:
            raise ValueError("final_rows differs from the last verified round")
        if record.get("oracle") != "independent_DataFusion_full_SQL":
            raise ValueError("query does not declare the independent full SQL oracle")
        if record.get("comparison") != "typed_bag_and_signed_endpoint_delta":
            raise ValueError("query does not declare typed result and signed delta checks")
        nonempty = record["initial_rows"] > 0 or any(item["output_rows"] > 0 for item in rounds)
        changed = any(item["signed_delta_rows"] > 0 for item in rounds)
        if record.get("nonempty_result_seen") is not nonempty or record.get("output_delta_seen") is not changed:
            raise ValueError("query nonempty/change flags contradict its round counts")
        expected_status = "pass" if nonempty and changed else (
            "verified_unchanged_output" if nonempty else "verified_empty_only"
        )
        if status != expected_status:
            raise ValueError("query status contradicts its nonempty/change evidence")
    return completed, complete


def load_profile(label, report_path, expected_hash, repository):
    report_path = report_path.resolve()
    report = checked_json(report_path, expected_hash)
    if report.get("format_version") != 1 or report.get("suite") not in {"tpch", "tpcds"}:
        raise ValueError("unsupported qualification report format or suite")
    manifest_path = Path(report["manifest"])
    if not manifest_path.is_absolute():
        manifest_path = repository / manifest_path
    manifest_path = manifest_path.resolve()
    manifest = checked_json(manifest_path, report["manifest_sha256"])
    if manifest.get("format_version") != 1 or manifest.get("suite") != report["suite"]:
        raise ValueError("manifest format/suite does not match its report")
    schema_path = manifest_path.parent / manifest["schema"]
    if digest(schema_path.read_bytes()) != manifest["schema_sha256"]:
        raise ValueError("manifest schema digest mismatch")
    if report.get("schema_sha256") != manifest["schema_sha256"]:
        raise ValueError("report schema identity differs from its manifest")
    identities = {}
    query_ids = set()
    for query in manifest["queries"]:
        query_id = query["query_id"]
        if not nonnegative(query_id) or query_id == 0 or query_id in query_ids:
            raise ValueError("invalid/duplicate logical query ID in manifest")
        query_ids.add(query_id)
        for variant in query["variants"]:
            variant_id = variant["variant_id"]
            if not isinstance(variant_id, str) or not variant_id:
                raise ValueError("variant_id must be a nonempty string")
            key = (query_id, variant_id)
            if key in identities:
                raise ValueError("duplicate variant in manifest")
            sql_path = manifest_path.parent / variant["sql"]
            if digest(sql_path.read_bytes()) != variant["sha256"]:
                raise ValueError(f"query SQL digest mismatch: {sql_path}")
            identities[key] = {
                "sql": str(sql_path.resolve()), "sql_sha256": variant["sha256"],
                "source": query.get("source"), "source_sha256": query.get("source_sha256"),
                "qualification": query.get("qualification"),
                "transformations": query.get("transformations", []),
            }
    if len(query_ids) != manifest.get("logical_queries"):
        raise ValueError("manifest logical query count is inconsistent")
    records = {}
    for record in report["queries"]:
        key = (record["query_id"], record["variant_id"])
        if key not in identities or key in records:
            raise ValueError("report contains an unknown or duplicate query variant")
        completed, complete = check_record(record)
        records[key] = {**record, "actual_completed_rounds": completed, "three_rounds_verified": complete}
    checks = {
        "executed_variants": len(records),
        "passed_variants": sum(row["status"] == "pass" for row in records.values()),
        "failed_variants": sum(row["status"] == "fail" for row in records.values()),
        "verified_empty_only_variants": sum(row["status"] == "verified_empty_only" for row in records.values()),
        "verified_unchanged_output_variants": sum(row["status"] == "verified_unchanged_output" for row in records.values()),
        "three_rounds_verified_variants": sum(row["three_rounds_verified"] for row in records.values()),
        "bootstrap_verified_variants": sum(nonnegative(row.get("initial_rows")) for row in records.values()),
    }
    checks["incomplete_variants"] = len(records) - checks["passed_variants"]
    for field, actual in checks.items():
        if field in report and report[field] != actual:
            raise ValueError(f"report counter {field} contradicts query records")
    metadata = {
        "profile": label, "report": str(report_path), "report_sha256": expected_hash,
        "manifest": str(manifest_path), "manifest_sha256": report["manifest_sha256"],
        "suite": report["suite"], "schema_sha256": manifest["schema_sha256"],
        "source": manifest.get("source"), "source_ref": manifest.get("source_ref"),
        "data_source": manifest.get("data_source"), "data_source_ref": manifest.get("data_source_ref"),
        "data_status": manifest.get("data_status"), "scale_factor": manifest.get("scale_factor"),
        "fixture_qualification": manifest.get("fixture_qualification"),
        "fixture_error": report.get("fixture_error"), "command": report.get("command"),
        "host_arch": report.get("host_arch"), "host_os": report.get("host_os"),
        "session_configuration": report.get("session_configuration"), "counts": checks,
    }
    return metadata, identities, records


def summarize(specifications, repository):
    loaded = []
    labels = set()
    for label, path, expected_hash in specifications:
        if label in labels:
            raise ValueError(f"duplicate profile label: {label}")
        labels.add(label)
        loaded.append(load_profile(label, Path(path), expected_hash, repository))
    suites = {metadata["suite"] for metadata, _, _ in loaded}
    if len(suites) != 1:
        raise ValueError("all reports must belong to one suite")
    keys = sorted(set().union(*(set(identities) for _, identities, _ in loaded)))
    queries = []
    for query_id, variant_id in keys:
        key = (query_id, variant_id)
        evidence = []
        selected = None
        for metadata, identities, records in loaded:
            identity = identities.get(key)
            record = records.get(key)
            item = {"profile": metadata["profile"], "report_sha256": metadata["report_sha256"],
                    "manifest_sha256": metadata["manifest_sha256"], "identity": identity,
                    "verification": record, "status": record["status"] if record else "not_executed"}
            evidence.append(item)
            if selected is None and record and record["status"] == "pass":
                selected = {"profile": metadata["profile"], "report_sha256": metadata["report_sha256"],
                            "manifest_sha256": metadata["manifest_sha256"], "sql_sha256": identity["sql_sha256"]}
        queries.append({"query_id": query_id, "variant_id": variant_id,
                        "with_pass": selected is not None, "selected_complete_profile": selected,
                        "sql_parameter_identities_differ": len({item["identity"]["sql_sha256"] for item in evidence if item["identity"]}) > 1,
                        "profiles": evidence})
    passed = sum(query["with_pass"] for query in queries)
    return {"format_version": 1, "kind": "incremental_query_coverage_aggregate", "suite": next(iter(suites)),
            "selection_policy": "first_single_profile_with_bootstrap_three_rounds_nonempty_result_and_output_delta",
            "cross_profile_round_combination": False, "scale_or_performance_qualification": False,
            "profiles": [metadata for metadata, _, _ in loaded],
            "aggregate": {"total": len(keys), "with_pass": passed, "remaining": len(keys) - passed,
                          "profiles_with_failures": sum(metadata["counts"]["failed_variants"] > 0 for metadata, _, _ in loaded)},
            "queries": queries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--report", action="append", nargs=3, required=True,
                        metavar=("PROFILE", "REPORT_JSON", "EXPECTED_SHA256"))
    parser.add_argument("--repository-root", type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    report = summarize(args.report, args.repository_root.resolve())
    output = args.output.resolve()
    if output in {Path(path).resolve() for _, path, _ in args.report}:
        raise ValueError("aggregate output cannot overwrite an input report")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "sha256": digest(output.read_bytes()), **report["aggregate"]}))


if __name__ == "__main__":
    main()
