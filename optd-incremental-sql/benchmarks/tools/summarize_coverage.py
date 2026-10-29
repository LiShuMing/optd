#!/usr/bin/env python3
"""Combine immutable qualification reports without combining their rounds.

Every --report takes a profile label, report path, and expected report SHA-256.
Relative manifest paths are resolved from --repository-root. A query receives
coverage only when one individual profile contains all three successful rounds,
a nonempty result and a nonzero signed output delta. Other failures remain in
the aggregate. Different SQL parameter/source identities are retained verbatim.
An explicit expected backend prevents Arrow and Iceberg profiles from being
combined; only historical Arrow reports may omit the backend declaration.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re


STAGES = ("insert_remaining_half", "delete_first_row", "restore_first_row")
STATUSES = {"pass", "fail", "verified_empty_only", "verified_unchanged_output"}
ARROW_BACKEND = "typed_arrow"
ICEBERG_BACKEND = "typed_iceberg_endpoint_vector"
BACKENDS = (ARROW_BACKEND, ICEBERG_BACKEND)
ICEBERG_WORK_FIELDS = (
    "changed_sources", "endpoint_scans", "decoded_batches", "decoded_rows",
    "decoded_bytes", "signed_change_rows",
)


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


def check_backend(value, expected_backend, where):
    """Only historical Arrow evidence may omit its backend declaration."""
    if expected_backend not in BACKENDS:
        raise ValueError(f"unsupported expected backend: {expected_backend!r}")
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a JSON object with source_backend identity")
    actual = value.get("source_backend", ARROW_BACKEND)
    if actual != expected_backend:
        raise ValueError(f"{where} source_backend {actual!r} differs from expected backend {expected_backend!r}")
    return actual


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def valid_digest(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def check_source_vector(vector, expected_hash, tables):
    """Check retained identity evidence, not the existence of source files."""
    if (not isinstance(vector, dict) or type(vector.get("version")) is not int or vector["version"] != 1 or
            not isinstance(vector.get("sources"), dict) or not vector["sources"] or
            set(vector["sources"]) != set(tables)):
        raise ValueError("Iceberg source vector lacks the complete registered source set")
    if not valid_digest(expected_hash) or digest(canonical_json(vector)) != expected_hash:
        raise ValueError("Iceberg source vector SHA-256 mismatch")
    for source in vector["sources"].values():
        if not isinstance(source, dict):
            raise ValueError("Iceberg source vector has an invalid source binding")
        catalog = source.get("catalog")
        namespace = source.get("namespace")
        snapshot = source.get("snapshot")
        iceberg_schema = source.get("iceberg_schema")
        arrow_layout = source.get("arrow_layout")
        if (not isinstance(catalog, dict) or catalog.get("kind") != "local/1" or
                not isinstance(catalog.get("canonical_root"), str) or
                not Path(catalog["canonical_root"]).is_absolute() or
                not isinstance(namespace, list) or not namespace or
                any(not isinstance(part, str) or not part.strip() for part in namespace) or
                not isinstance(source.get("name"), str) or not source["name"].strip() or
                not isinstance(source.get("table_uuid"), str) or
                re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", source["table_uuid"]) is None or
                source.get("reference") != "main" or not nonnegative(source.get("schema_id")) or
                type(source.get("normalizer_version")) is not int or source["normalizer_version"] != 1 or
                not isinstance(iceberg_schema, dict) or not isinstance(iceberg_schema.get("fields"), list) or
                not isinstance(arrow_layout, dict) or not isinstance(arrow_layout.get("fields"), list)):
            raise ValueError("Iceberg source vector lacks complete UUID/schema/Arrow/normalizer identity")
        for field, domain in [("iceberg_schema", b"optd-incremental-sql/iceberg-schema/1\0"),
                              ("arrow_layout", b"optd-incremental-sql/arrow-layout/1\0")]:
            declared = source.get(field + "_sha256")
            if not valid_digest(declared) or digest(domain + canonical_json(source[field])) != declared:
                raise ValueError(f"Iceberg source vector {field} SHA-256 mismatch")
        if not isinstance(snapshot, dict) or snapshot.get("kind") not in {"empty", "snapshot"}:
            raise ValueError("Iceberg source vector lacks an explicit snapshot cursor")
        if snapshot["kind"] == "snapshot" and (
                not isinstance(snapshot.get("id"), int) or isinstance(snapshot["id"], bool) or
                not nonnegative(snapshot.get("sequence_number")) or snapshot["sequence_number"] == 0 or
                not valid_digest(snapshot.get("descriptor_sha256"))):
            raise ValueError("Iceberg source vector has an invalid snapshot cursor")
    return vector


def check_iceberg_record(record):
    successful_rounds = [item for item in record["rounds"] if item["status"] == "pass"]
    bootstrap_verified = nonnegative(record.get("initial_rows"))
    if record["status"] == "fail" and not bootstrap_verified and not successful_rounds:
        return
    tables = record.get("registered_tables")
    if (not isinstance(tables, list) or not tables or
            any(not isinstance(name, str) or not name.strip() for name in tables) or
            len(tables) != len(set(tables))):
        raise ValueError("Iceberg query lacks complete registered_tables")
    if successful_rounds and not bootstrap_verified:
        raise ValueError("Iceberg successful rounds lack verified bootstrap evidence")
    before = None
    if bootstrap_verified:
        before = check_source_vector(record.get("bootstrap_source_vector"),
                                     record.get("bootstrap_source_vector_sha256"), tables)
        if record.get("bootstrap_strict_reopen_verified") is not True:
            raise ValueError("Iceberg bootstrap lacks strict reopen verification")
    for item in successful_rounds:
        after = check_source_vector(item.get("source_vector"), item.get("source_vector_sha256"), tables)
        changed = 0
        for name in tables:
            previous, current = before["sources"][name], after["sources"][name]
            if {key: value for key, value in previous.items() if key != "snapshot"} != {
                    key: value for key, value in current.items() if key != "snapshot"}:
                raise ValueError("Iceberg source vector changes a fixed binding/schema identity")
            old, new = previous["snapshot"], current["snapshot"]
            if new == old:
                continue
            if (new["kind"] != "snapshot" or (old["kind"] == "snapshot" and
                    (new["id"] == old["id"] or new["sequence_number"] <= old["sequence_number"]))):
                raise ValueError("Iceberg source vector cursor does not advance")
            changed += 1
        work = item.get("source_work")
        if (not isinstance(work, dict) or
                not all(nonnegative(work.get(field)) for field in ICEBERG_WORK_FIELDS)):
            raise ValueError("Iceberg round lacks complete nonnegative source_work counters")
        if changed == 0 or work["changed_sources"] != changed or work["endpoint_scans"] < changed:
            raise ValueError("Iceberg source_work contradicts changed snapshot cursors")
        if item.get("strict_reopen_verified") is not True:
            raise ValueError("Iceberg round lacks strict reopen verification")
        before = after
    if record["status"] != "fail" and (
            record.get("strict_reopen_verified") is not True or
            record.get("full_output_log_replay_verified") is not True):
        raise ValueError("Iceberg query lacks strict reopen/full output log replay verification")


def check_record(record, backend=ARROW_BACKEND):
    check_backend(record, backend, "query")
    status = record.get("status")
    if status not in STATUSES:
        raise ValueError(f"unknown query status: {status!r}")
    rounds = record.get("rounds")
    if not isinstance(rounds, list) or len(rounds) > 3:
        raise ValueError("query rounds must be a list of at most three entries")
    for index, item in enumerate(rounds):
        if (not isinstance(item, dict) or item.get("stage") != STAGES[index] or
                item.get("status") not in {"pass", "fail"}):
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
        expected_oracle = ("independent_DataFusion_full_SQL_over_exact_Iceberg_endpoints"
                           if backend == ICEBERG_BACKEND else "independent_DataFusion_full_SQL")
        if record.get("oracle") != expected_oracle:
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
    if backend == ICEBERG_BACKEND:
        check_iceberg_record(record)
    return completed, complete


def load_profile(label, report_path, expected_hash, repository, expected_backend=ARROW_BACKEND):
    report_path = report_path.resolve()
    report = checked_json(report_path, expected_hash)
    if report.get("format_version") != 1 or report.get("suite") not in {"tpch", "tpcds"}:
        raise ValueError("unsupported qualification report format or suite")
    backend = check_backend(report, expected_backend, "report")
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
        completed, complete = check_record(record, backend)
        records[key] = {**record, "source_backend": backend,
                        "actual_completed_rounds": completed, "three_rounds_verified": complete}
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
        "source_backend": backend,
        "iceberg_artifact_root": report.get("iceberg_artifact_root"),
        "source": manifest.get("source"), "source_ref": manifest.get("source_ref"),
        "data_source": manifest.get("data_source"), "data_source_ref": manifest.get("data_source_ref"),
        "data_status": manifest.get("data_status"), "scale_factor": manifest.get("scale_factor"),
        "fixture_qualification": manifest.get("fixture_qualification"),
        "fixture_error": report.get("fixture_error"), "command": report.get("command"),
        "host_arch": report.get("host_arch"), "host_os": report.get("host_os"),
        "session_configuration": report.get("session_configuration"), "counts": checks,
    }
    return metadata, identities, records


def summarize(specifications, repository, expected_backend=ARROW_BACKEND):
    if expected_backend not in BACKENDS:
        raise ValueError(f"unsupported expected backend: {expected_backend!r}")
    loaded = []
    labels = set()
    for label, path, expected_hash in specifications:
        if label in labels:
            raise ValueError(f"duplicate profile label: {label}")
        labels.add(label)
        loaded.append(load_profile(label, Path(path), expected_hash, repository, expected_backend))
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
                    "source_backend": metadata["source_backend"],
                    "manifest_sha256": metadata["manifest_sha256"], "identity": identity,
                    "verification": record, "status": record["status"] if record else "not_executed"}
            evidence.append(item)
            if selected is None and record and record["status"] == "pass":
                selected = {"profile": metadata["profile"], "report_sha256": metadata["report_sha256"],
                            "source_backend": metadata["source_backend"],
                            "manifest_sha256": metadata["manifest_sha256"], "sql_sha256": identity["sql_sha256"]}
        queries.append({"query_id": query_id, "variant_id": variant_id,
                        "source_backend": expected_backend,
                        "with_pass": selected is not None, "selected_complete_profile": selected,
                        "sql_parameter_identities_differ": len({item["identity"]["sql_sha256"] for item in evidence if item["identity"]}) > 1,
                        "profiles": evidence})
    passed = sum(query["with_pass"] for query in queries)
    return {"format_version": 1, "kind": "incremental_query_coverage_aggregate", "suite": next(iter(suites)),
            "source_backend": expected_backend,
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
    parser.add_argument("--expected-backend", choices=BACKENDS, default=ARROW_BACKEND)
    args = parser.parse_args()
    report = summarize(args.report, args.repository_root.resolve(), args.expected_backend)
    output = args.output.resolve()
    if output in {Path(path).resolve() for _, path, _ in args.report}:
        raise ValueError("aggregate output cannot overwrite an input report")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "sha256": digest(output.read_bytes()),
                      "source_backend": report["source_backend"], **report["aggregate"]}))


if __name__ == "__main__":
    main()
