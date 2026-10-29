#!/usr/bin/env python3
"""Preserve a query/schema profile while using separately generated table CSVs.

All SQL/schema hashes remain unchanged. The output records both manifest
identities and references the original data files, without duplicating them.
The runner's typed CSV parser checks representability against the chosen schema.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(args):
    query_path = args.query_manifest.resolve()
    data_path = args.data_manifest.resolve()
    query = json.loads(query_path.read_text(encoding="utf-8"))
    data = json.loads(data_path.read_text(encoding="utf-8"))
    if query["suite"] != data["suite"]:
        raise ValueError("query and data suites differ")
    if {table["name"] for table in query["tables"]} != {table["name"] for table in data["tables"]}:
        raise ValueError("query and data table names differ")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError("output must be empty")
    output.mkdir(parents=True, exist_ok=True)
    for relative, expected in [(query["schema"], query["schema_sha256"])]:
        source = query_path.parent / relative
        if digest(source) != expected:
            raise ValueError("query schema digest mismatch")
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    for entry in query["queries"]:
        for variant in entry["variants"]:
            source = query_path.parent / variant["sql"]
            if digest(source) != variant["sha256"]:
                raise ValueError("query SQL digest mismatch")
            target = output / variant["sql"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
    if data["csv_path_base"] != "manifest_directory":
        raise ValueError("data manifest must resolve CSVs from its own directory")
    tables = []
    for original in data["tables"]:
        table = dict(original)
        csv = (data_path.parent / table["csv"]).resolve()
        if digest(csv) != table["sha256"]:
            raise ValueError(f"data CSV digest mismatch for {table['name']}")
        table["csv"] = str(csv)
        tables.append(table)
    query.update({
        "query_manifest": str(query_path),
        "query_manifest_sha256": digest(query_path),
        "data_manifest": str(data_path),
        "data_manifest_sha256": digest(data_path),
        "data_source": data["source"],
        "data_source_ref": data.get("source_ref"),
        "data_status": data.get("data_status"),
        "duckdb_version": data.get("duckdb_version"),
        "scale_factor": data.get("scale_factor"),
        "fixture_qualification": "original_query_and_schema_profile_with_separate_public_generator_data",
        "schema_profile": "original_query_schema_typed_CSV_parser_checks_value_compatibility",
        "csv_path_base": "manifest_directory",
        "csv_null": data.get("csv_null", ""),
        "tables": tables,
    })
    path = output / "manifest.json"
    path.write_text(json.dumps(query, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"manifest": str(path), "queries": len(query["queries"]), "tables": len(tables), "query_sql_and_schema_unchanged": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query_manifest", type=Path)
    parser.add_argument("data_manifest", type=Path)
    parser.add_argument("output", type=Path)
    main(parser.parse_args())
