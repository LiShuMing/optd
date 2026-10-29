#!/usr/bin/env python3
"""Reproduce the TPC-H fixture from the repository's existing SQL logic tests.

No downloads, database dependencies or generated rows are needed. The ignored
SQL/schema extracts must match the committed manifest; this tool never rewrites
that manifest to accept changed input. Q15's CREATE VIEW is replaced with an
equivalent CTE and explicitly recorded. --verify-only writes nothing.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def extract(repo, destination, verify_only=False):
    slt_root = repo / "optd-sqllogictest/slt"
    expected = json.loads((destination / "manifest.json").read_text(encoding="utf-8"))
    identities = {query["query_id"]: query for query in expected["queries"]}
    queries = destination / "queries"
    if not verify_only:
        queries.mkdir(exist_ok=True)
    entries = []
    for number in range(1, 23):
        original = slt_root / f"tpch-q{number}.slt"
        disabled = not original.exists()
        if disabled:
            original = original.with_suffix(".slt.disabled")
        text = original.read_text(encoding="utf-8")
        if digest(original) != identities[number]["source_sha256"]:
            raise ValueError(f"pinned SLT source digest mismatch: {original}")
        statements = re.findall(r"(?m)^query[^\n]*\n(.*?)(?m:^----\s*$)", text, re.S)
        if len(statements) != 1:
            raise ValueError(f"{original}: expected exactly one result query")
        sql = statements[0].strip() + "\n"
        transformations = []
        if number == 15:
            view = re.search(r"(?mi)^create view\s+(\w+)\s*(\([^;]*?\))\s+as\s+(.*?);", text, re.S)
            if view is None:
                raise ValueError("Q15 CREATE VIEW fixture is missing")
            sql = f"WITH {view[1]} {view[2]} AS (\n{view[3].strip()}\n)\n{sql}"
            transformations.append("ddl_view_to_cte_preserving_parameters")
        relative = f"queries/q{number:02}.sql"
        target = destination / relative
        sql_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        if sql_hash != identities[number]["variants"][0]["sha256"]:
            raise ValueError(f"pinned query extract digest mismatch: {target}")
        if verify_only:
            if digest(target) != sql_hash:
                raise ValueError(f"existing query extract digest mismatch: {target}")
        else:
            target.write_text(sql, encoding="utf-8")
        entries.append({
            "query_id": number,
            "variants": [{"variant_id": str(number), "sql": relative, "sha256": digest(target)}],
            "source": str(original.relative_to(repo)),
            "source_sha256": digest(original),
            "old_harness_disabled": disabled,
            "transformations": transformations,
            "qualification": "repository_parameter_variant_not_tpc_certification",
            "status": "available_not_executed",
        })
    source_schema = slt_root / "_tpch_tables.slt.part"
    text = source_schema.read_text(encoding="utf-8")
    definitions = re.findall(r"(?mi)^CREATE TABLE\s+\w+\s*\(.*?\);", text, re.S)
    if len(definitions) != 8:
        raise ValueError(f"expected 8 TPC-H CREATE TABLE statements, found {len(definitions)}")
    schema = destination / "schema.sql"
    schema_bytes = ("\n\n".join(definitions) + "\n").encode("utf-8")
    if hashlib.sha256(schema_bytes).hexdigest() != expected["schema_sha256"]:
        raise ValueError("pinned schema extract digest mismatch")
    if verify_only:
        if digest(schema) != expected["schema_sha256"]:
            raise ValueError("existing schema extract digest mismatch")
    else:
        schema.write_bytes(schema_bytes)
    tables = []
    for definition in definitions:
        name = re.search(r"(?i)CREATE TABLE\s+(\w+)", definition)[1].lower()
        csv = repo / "datafusion-optd-cli/tpch-sf0_01" / f"{name}.csv"
        with csv.open(encoding="utf-8") as stream:
            rows = sum(1 for _ in stream)
        tables.append({"name": name, "csv": str(csv.relative_to(repo)), "rows": rows, "sha256": digest(csv)})
    manifest = {
        "format_version": 1,
        "suite": "tpch",
        "source": "existing_repository_sql_logic_tests",
        "logical_queries": 22,
        "schema": "schema.sql",
        "schema_sha256": digest(schema),
        "csv_path_base": "repository_root",
        "csv_delimiter": "|",
        "csv_has_header": False,
        "scale_factor": None,
        "scale_note": "Directory name sf0_01 is historical; exact observed row counts are authoritative.",
        "tables": tables,
        "queries": entries,
    }
    if manifest != expected:
        raise ValueError("extracted source/data identities differ from committed manifest")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    result = extract(root, Path(__file__).resolve().parents[1] / "tpch", args.verify_only)
    print(f"extracted {len(result['queries'])} queries and {len(result['tables'])} table declarations")
