#!/usr/bin/env python3
"""Summarize structured maintenance coverage separately from semantic witnesses.

Inputs are explicitly named coverage aggregates and expected SHA-256 identities.
Counts concern selected complete profiles; categories overlap across queries.
Operator-local counters do not certify end-to-end delta complexity or performance.
"""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


def numeric_leaves(value, prefix, work):
    """Sum measured numeric boundaries without inventing values for absent fields."""
    if not isinstance(value, dict):
        raise ValueError("structured work must contain nonnegative integers")
    for field, count in value.items():
        name = f"{prefix}.{field}"
        if isinstance(count, dict):
            numeric_leaves(count, name, work)
        elif type(count) is int and count >= 0:
            work[name] += count
        else:
            raise ValueError("work counters must be nonnegative integers")


def node_work(node, work):
    for prefix in ["accumulator_work", "group_work", "keyed_work"]:
        values = node.get(prefix)
        if values is None:
            continue
        if not isinstance(values, dict):
            raise ValueError("structured node work must contain nonnegative integers")
        scalar = {k: v for k, v in values.items() if k != "scope" and not isinstance(v, list)}
        numeric_leaves(scalar, prefix, work)
        for field, values_ in values.items():
            if not isinstance(values_, list):
                continue
            for count in values_:
                if type(count) is not int or count < 0:
                    raise ValueError("work counters must be nonnegative integers")
            work[f"{prefix}.{field}"] += sum(values_)
        if prefix == "keyed_work":
            work["keyed_work.snapshot_input_rows"] += sum(values["snapshot_input_rows_before"]) + sum(values["snapshot_input_rows_after"])


def round_work(round_, work, domains, require_sql_work=True):
    """Database verification/reopen work has separate attribution from writes."""
    def domain(name, known):
        domains[name]["measured_rounds" if known else "unknown_rounds"] += 1

    replayed = round_.get("replayed", False)
    if type(replayed) is not bool:
        raise ValueError("replayed must be a boolean")
    report = round_.get("commit")
    if replayed:
        stats = []  # The receipt's original work is not current execution.
    elif "sql_stats" in round_:
        stats = [round_["sql_stats"]]
    elif "commit" in round_:
        if report is not None and (not isinstance(report, dict) or not isinstance(report.get("maintenance"), dict) or not isinstance(report.get("dml"), list)):
            raise ValueError("qualified report lacks structured actual SQL work")
        stats = list(report["maintenance"].values()) if report is not None else []
    elif require_sql_work:
        raise ValueError("qualified report lacks structured actual SQL work")
    else:
        stats = None
    for stat in stats or []:
        if not isinstance(stat, dict) or not isinstance(stat.get("nodes"), list):
            raise ValueError("qualified report lacks structured actual SQL work")
        for node in stat["nodes"]:
            node_work(node, work)
        if stat.get("work") is not None:
            numeric_leaves(stat["work"], "refresh_work", work)
    domain("operators", stats is not None)
    domain("refresh_orchestration", stats is not None and all(stat.get("work") is not None for stat in stats))

    dml_known = "commit" in round_
    if dml_known and not replayed and report is not None:
        for statement in report["dml"]:
            logical = {k: statement[k] for k in ["affected_rows", "source_relation_rows", "net_changed_rows"] if k in statement}
            numeric_leaves(logical, "dml.logical", work)
            if statement.get("execution") is None:
                dml_known = False
            else:
                numeric_leaves(statement["execution"], "dml.execution", work)
    domain("dml", dml_known)
    for name, field in [
        ("source_acquisition", "source_work"),
        ("storage", "storage_work"),
        ("verification_before_reopen", "verification_store_work_before_reopen"),
        ("recovery", "recovery_work"),
        ("verification_after_reopen", "verification_store_work_after_reopen"),
    ]:
        value = round_.get(field)
        domain(name, value is not None)
        if value is not None:
            numeric_leaves(value, field, work)


def empty_domains():
    return {name: {"measured_rounds": 0, "unknown_rounds": 0} for name in [
        "operators", "refresh_orchestration", "dml", "source_acquisition", "storage",
        "verification_before_reopen", "recovery", "verification_after_reopen"]}


def checked_qualification(qualification):
    """Validate a complete plan, including nodes whose compilation itself failed."""
    planned = qualification["planned_nodes"]
    blockers = qualification["complete_recompute_blockers"]
    blocked_ids = {node["id"] for node in planned
                   if node["strategy"] in (None, "OperatorRecompute")}
    if (len({node["id"] for node in planned}) != len(planned) or
            len({node["id"] for node in blockers}) != len(blockers) or
            {node["id"] for node in blockers} != blocked_ids or
            type(qualification["strict_plan_eligible"]) is not bool or
            qualification["strict_plan_eligible"] != (not blocked_ids)):
        raise ValueError("planned blockers and strict eligibility contradict each other")
    return planned, blockers


def refused_profiles(query):
    """Keep preflight evidence even when there is no successful semantic witness."""
    reports = []
    for profile in query.get("profiles", []):
        verification = profile.get("verification", {})
        if verification.get("status") != "unsupported_strict_plan":
            continue
        qualification = verification.get("plan_qualification")
        if (qualification is None or verification.get("completed_rounds") != 0 or
                verification.get("rounds") != []):
            raise ValueError("strict preflight refusal requires a plan and zero executed rounds")
        planned, blockers = checked_qualification(qualification)
        if qualification["strict_plan_eligible"]:
            raise ValueError("strict refusal contradicts eligible preflight")
        reports.append({"profile": profile["profile"], "strict_plan_eligible": False,
                        "complete_recompute_blockers": blockers, "planned_nodes": planned})
    return reports


def audit(aggregate):
    queries = []
    strategies = Counter()
    blocker_rules = Counter()
    totals = Counter()
    total_domains = empty_domains()
    for query in aggregate["queries"]:
        selected = query.get("selected_complete_profile")
        if not query.get("with_pass") or not selected:
            reports = refused_profiles(query)
            # A unique profile is unambiguous. Multiple profiles remain individually
            # inspectable; do not silently select a favorable schema/plan variant.
            refused = reports[0] if len(query.get("profiles", [])) == len(reports) == 1 else None
            if refused is not None:
                strategies.update({node["strategy"] for node in refused["planned_nodes"]
                                   if node["strategy"] is not None})
                blocker_rules.update({node["rule"]["name"]
                                      for node in refused["complete_recompute_blockers"]})
            queries.append({"query_id": query["query_id"], "semantic_pass": False,
                            "strict_plan_eligible": False if refused else None,
                            "complete_recompute_blockers": refused["complete_recompute_blockers"] if refused else None,
                            "preflight_profiles": reports,
                            "complete_recompute_required": any(node["strategy"] == "OperatorRecompute" for node in refused["planned_nodes"]) if refused else None,
                            "plan_compilation_failed": any(node["strategy"] is None for node in refused["planned_nodes"]) if refused else None})
            continue
        profiles = [p for p in query["profiles"] if p["profile"] == selected["profile"]]
        if len(profiles) != 1:
            raise ValueError("selected profile must identify exactly one verification record")
        verification = profiles[0]["verification"]
        if verification["status"] != "pass" or verification.get("completed_rounds") != 3:
            raise ValueError("selected semantic witness is not a complete passing profile")
        qualification = verification.get("plan_qualification")
        planned, blockers = checked_qualification(qualification) if qualification is not None else ([], None)
        actual_blockers = {node["id"] for node in blockers} if blockers is not None else set()
        if qualification is not None:
            if any(node["strategy"] is None for node in planned):
                raise ValueError("semantic pass cannot contain failed plan compilation")
            strategies.update({node["strategy"] for node in planned})
            blocker_rules.update({node["rule"]["name"] for node in blockers})
        if len(verification["rounds"]) != 3:
            raise ValueError("a complete witness requires three rounds")
        work = Counter()
        domains = empty_domains()
        for round_ in verification["rounds"]:
            round_work(round_, work, domains, require_sql_work=qualification is not None)
        for name, counts in domains.items():
            for field, count in counts.items():
                total_domains[name][field] += count
        totals.update(work)
        queries.append({"query_id": query["query_id"], "semantic_pass": True,
                        "strict_plan_eligible": not actual_blockers if qualification is not None else None,
                        "complete_recompute_blockers": blockers,
                        "complete_recompute_required": bool(actual_blockers) if qualification is not None else None,
                        "plan_compilation_failed": False if qualification is not None else None,
                        "strategies": sorted({n["strategy"] for n in planned}),
                        "observed_work": dict(work), "work_domains": domains})
    return {"suite": aggregate["suite"], "query_mode": aggregate.get("query_mode", "original"),
            "source_backend": aggregate["source_backend"], "queries": queries,
            "counts": {"total": len(queries),
                       "semantic_pass": sum(q["semantic_pass"] for q in queries),
                       "strict_plan_eligible": sum(q["strict_plan_eligible"] is True for q in queries),
                       "complete_recompute": sum(q["complete_recompute_required"] is True for q in queries),
                       "strict_plan_rejected": sum(q["strict_plan_eligible"] is False for q in queries),
                       "plan_compilation_failed": sum(q["plan_compilation_failed"] is True for q in queries),
                       "plan_qualification_unknown": sum(q["strict_plan_eligible"] is None for q in queries)},
            "queries_with_strategy": dict(strategies), "queries_with_blocking_rule": dict(blocker_rules),
            "observed_work": dict(totals), "work_domains": total_domains, "performance_qualified": False,
            "end_to_end_delta_work_qualified": False,
            "counter_scope": "observed_partial_sums_by_operation_boundary; source_acquisition, refresh_work, DML, transaction_storage, verification_and_recovery_separate; missing_domains_unknown; allocator_and_physical_device_IO_unmeasured"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--aggregate", nargs=3, action="append", required=True,
                        metavar=("LABEL", "PATH", "SHA256"))
    args = parser.parse_args()
    results = {}
    for label, path, expected in args.aggregate:
        if label in results:
            raise ValueError("duplicate aggregate label")
        source = Path(path)
        if source.resolve() == args.output.resolve():
            raise ValueError("output cannot overwrite an input aggregate")
        if source.stat().st_size > 128 * 1024 * 1024:
            raise ValueError("aggregate exceeds explicit 128MiB report budget")
        content = source.read_bytes()
        if hashlib.sha256(content).hexdigest() != expected:
            raise ValueError("aggregate SHA-256 mismatch")
        results[label] = {"aggregate": str(source.resolve()), "sha256": expected,
                          **audit(json.loads(content))}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"format_version": 1, "profiles": results}, indent=2) + "\n")
    print(json.dumps({label: result["counts"] for label, result in results.items()}))


if __name__ == "__main__":
    main()
