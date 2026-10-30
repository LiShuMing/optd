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


def audit(aggregate):
    queries = []
    strategies = Counter()
    blocker_rules = Counter()
    totals = Counter()
    for query in aggregate["queries"]:
        selected = query.get("selected_complete_profile")
        if not query.get("with_pass") or not selected:
            queries.append({"query_id": query["query_id"], "semantic_pass": False,
                            "strict_plan_eligible": None, "complete_recompute_blockers": None})
            continue
        profiles = [p for p in query["profiles"] if p["profile"] == selected["profile"]]
        if len(profiles) != 1:
            raise ValueError("selected profile must identify exactly one verification record")
        verification = profiles[0]["verification"]
        if verification["status"] != "pass" or verification.get("completed_rounds") != 3:
            raise ValueError("selected semantic witness is not a complete passing profile")
        qualification = verification.get("plan_qualification")
        if qualification is None:
            queries.append({"query_id": query["query_id"], "semantic_pass": True,
                            "strict_plan_eligible": None, "complete_recompute_blockers": None})
            continue
        planned = qualification["planned_nodes"]
        actual_blockers = {n["id"] for n in planned if n["strategy"] == "OperatorRecompute"}
        blockers = qualification["complete_recompute_blockers"]
        if (len({b["id"] for b in blockers}) != len(blockers) or
                {b["id"] for b in blockers} != actual_blockers or
                qualification["strict_plan_eligible"] != (not actual_blockers)):
            raise ValueError("planned blockers and strict eligibility contradict each other")
        strategies.update({node["strategy"] for node in planned})
        blocker_rules.update({node["rule"]["name"] for node in blockers})
        work = Counter()
        for round_ in verification["rounds"]:
            if "sql_stats" not in round_:
                raise ValueError("qualified report lacks structured actual SQL work")
            for node in round_["sql_stats"]["nodes"]:
                for prefix, fields in [
                    ("accumulator_work", ["delta_input_rows", "historical_input_rows"]),
                    ("group_work", ["aggregate_input_rows", "touched_groups"]),
                    ("keyed_work", ["touched_keys", "full_input_scans", "cached_before_output_rows"]),
                ]:
                    values = node.get(prefix)
                    if values:
                        for field in fields:
                            count = values[field]
                            if type(count) is not int or count < 0:
                                raise ValueError("work counters must be nonnegative integers")
                            work[f"{prefix}.{field}"] += count
                keyed = node.get("keyed_work")
                if keyed:
                    work["keyed_work.snapshot_input_rows"] += sum(keyed["snapshot_input_rows_before"]) + sum(keyed["snapshot_input_rows_after"])
            for field, count in round_.get("source_work", {}).items():
                work[f"source_work.{field}"] += count
        totals.update(work)
        queries.append({"query_id": query["query_id"], "semantic_pass": True,
                        "strict_plan_eligible": not actual_blockers,
                        "complete_recompute_blockers": blockers,
                        "strategies": sorted({n["strategy"] for n in planned}),
                        "observed_work": dict(work)})
    return {"suite": aggregate["suite"], "query_mode": aggregate.get("query_mode", "original"),
            "source_backend": aggregate["source_backend"], "queries": queries,
            "counts": {"total": len(queries),
                       "semantic_pass": sum(q["semantic_pass"] for q in queries),
                       "strict_plan_eligible": sum(q["strict_plan_eligible"] is True for q in queries),
                       "complete_recompute": sum(q["strict_plan_eligible"] is False for q in queries),
                       "plan_qualification_unknown": sum(q["strict_plan_eligible"] is None for q in queries)},
            "queries_with_strategy": dict(strategies), "queries_with_blocking_rule": dict(blocker_rules),
            "observed_work": dict(totals), "performance_qualified": False,
            "end_to_end_delta_work_qualified": False,
            "counter_scope": "actual_operator_local_work_plus_separate_source_acquisition; full cache copies/materialization/checkpoint costs excluded"}


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
