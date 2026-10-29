#!/usr/bin/env python3
"""Create a tiny, labelled 24-table fixture for the genuine TPC-DS SQL files.

This data tests planner/operator compatibility; it is NOT dsdgen output or
scale/performance qualification. The report must retain empty-result counts.
Use generate_duckdb_fixture.py for data produced by the public TPC generator.
"""

import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import shutil


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(args):
    source = Path(__file__).resolve().parents[1] / "tpcds"
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError("output must be empty")
    output.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source / "schema.sql", output / "schema.sql")
    shutil.copytree(source / "queries", output / "queries")
    table_dir = output / "tables"
    table_dir.mkdir()
    schema = (source / "schema.sql").read_text(encoding="utf-8")
    tables = []
    for name, body in re.findall(r"CREATE TABLE\s+(\w+)\s*\((.*?)\);", schema, re.S):
        columns = re.findall(r"(?m)^\s*(\w+)\s+(BIGINT|INTEGER|VARCHAR|DATE|DECIMAL\(\d+,\s*\d+\))", body)
        path = table_dir / (name + ".csv")
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream, delimiter="|", lineterminator="\n")
            for index in range(1, args.rows + 1):
                row = []
                for column, typ in columns:
                    if typ.startswith("DECIMAL"):
                        value = f"{index}.25"
                    elif typ == "DATE":
                        value = f"2000-08-{(index - 1) % 28 + 1:02}"
                    elif typ in ("BIGINT", "INTEGER"):
                        if column == "d_year":
                            value = 1998 + (index - 1) % 5
                        elif column in ("d_moy", "d_qoy", "d_dom"):
                            value = {"d_moy": 8, "d_qoy": 3, "d_dom": index % 28 + 1}[column]
                        elif column == "d_month_seq":
                            value = 1200 + index
                        else:
                            value = index
                    else:
                        if column.endswith("_state"):
                            value = ["TN", "CA", "GA", "TX"][index % 4]
                        elif column.endswith("_country"):
                            value = "United States"
                        elif column.endswith("_gender"):
                            value = "M" if index % 2 else "F"
                        elif column.endswith("_marital_status"):
                            value = "S" if index % 2 else "M"
                        elif column.endswith("_education_status"):
                            value = "College"
                        elif "channel" in column:
                            value = "N"
                        elif column == "i_category":
                            value = ["Books", "Electronics", "Sports", "Home"][index % 4]
                        else:
                            value = f"{column}_{index}"
                    row.append(value)
                writer.writerow(row)
        tables.append({"name": name, "csv": str(path.relative_to(output)), "rows": args.rows, "sha256": digest(path)})
    if len(tables) != 24:
        raise ValueError("incomplete table set")
    manifest.update({
        "data_status": "synthetic_schema_fixture_not_dsdgen",
        "fixture_qualification": "operator_compatibility_only_not_standard_data_or_performance",
        "csv_path_base": "manifest_directory",
        "csv_null": "",
        "tables": tables,
        "synthetic_rows_per_table": args.rows,
    })
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"manifest": str(output / "manifest.json"), "tables": len(tables), "rows_per_table": args.rows, "qualification": manifest["fixture_qualification"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--rows", type=int, default=12)
    args = parser.parse_args()
    if not 2 <= args.rows <= 1000:
        parser.error("rows must be in 2..1000; this is a small compatibility fixture")
    main(args)
