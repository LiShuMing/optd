#!/usr/bin/env python3
"""Fetch and derive TPC-DS SQL/schema from DuckDB's fixed public v1.4.3 tree.

The committed manifest is the expected identity, never regenerated from a
download. Upstream SQL, header and derived schema stay ignored local files;
they are not redistributed under this repository's MIT license. SQL is not
rewritten; statements become explicitly enumerated variants. No rows or
database packages are downloaded. --verify-only performs no network requests.
"""

from concurrent.futures import ThreadPoolExecutor
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

from generate_duckdb_fixture import split_queries


SOURCE = "https://github.com/duckdb/duckdb/tree/v1.4.3/extension/tpcds/dsdgen"
MIRROR = "https://cdn.jsdelivr.net/gh/duckdb/duckdb@v1.4.3/extension/tpcds/dsdgen"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def derive_schema(text):
    tables = []
    for name, table in re.findall(r'struct (\w+)\s*\{\s*static constexpr char \*Name = "(\w+)";', text):
        column_match = re.search(rf"const char \*{name}::Columns\[\]\s*=\s*\{{(.*?)\}};", text, re.S)
        type_match = re.search(rf"const LogicalType {name}::Types\[\]\s*=\s*\{{(.*?)\}};", text, re.S)
        if not column_match or not type_match:
            raise ValueError(f"missing column/type declaration for {table}")
        columns = re.findall(r'"([^"]+)"', column_match[1])
        types = re.findall(r"LogicalType::(DECIMAL\(\d+,\s*\d+\)|[A-Z]+)", type_match[1])
        if len(columns) != len(types):
            raise ValueError(f"column/type count mismatch for {table}")
        unsupported = set(re.sub(r"\(.*", "", typ) for typ in types) - {"BIGINT", "INTEGER", "VARCHAR", "DATE", "DECIMAL"}
        if unsupported:
            raise ValueError(f"unknown upstream logical types: {unsupported}")
        ddl = "CREATE TABLE " + table + " (\n" + ",\n".join(f"  {column} {typ}" for column, typ in zip(columns, types)) + "\n);"
        tables.append({"name": table, "columns": len(columns), "ddl": ddl})
    if len(tables) != 24:
        raise ValueError(f"expected 24 tables, found {len(tables)}")
    return tables


def fetch(relative, target):
    if target.is_file() and target.stat().st_size > 0:
        return None
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".download")
    result = subprocess.run([
        "curl", "--fail", "--location", "--connect-timeout", "6", "--max-time", "20",
        "--silent", "--show-error", MIRROR + "/" + relative, "--output", str(temporary),
    ], check=False, capture_output=True, text=True, timeout=25)
    if result.returncode:
        temporary.unlink(missing_ok=True)
        return f"{relative}: curl exit {result.returncode}: {result.stderr.strip()}"
    if temporary.stat().st_size > 1_000_000:
        temporary.unlink()
        raise ValueError(f"unexpectedly large source file: {relative}")
    temporary.replace(target)
    return None


def verify_bytes(data, expected, identity):
    if hashlib.sha256(data).hexdigest() != expected:
        raise ValueError(f"source/derived digest mismatch: {identity}")


def main(args):
    destination = Path(__file__).resolve().parents[1] / "tpcds"
    manifest_path = destination / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (manifest.get("format_version"), manifest.get("suite"), manifest.get("source_ref")) != (1, "tpcds", "v1.4.3"):
        raise ValueError("expected committed TPC-DS v1.4.3 manifest")
    if len(manifest["queries"]) != 99 or manifest["logical_queries"] != 99:
        raise ValueError("expected all 99 committed query identities")
    source_dir = destination / "sources"
    pairs = [(f"queries/{query['query_id']:02}.sql", source_dir / f"q{query['query_id']:02}.sql") for query in manifest["queries"]]
    pairs.append(("include/dsdgen_schema.hpp", source_dir / "dsdgen_schema.hpp"))
    if not args.verify_only:
        with ThreadPoolExecutor(max_workers=3) as pool:
            failures = [failure for failure in pool.map(lambda pair: fetch(*pair), pairs) if failure]
        if failures:
            raise RuntimeError("source files remain missing; rerun to resume:\n" + "\n".join(failures))
    verify_bytes((source_dir / "dsdgen_schema.hpp").read_bytes(), manifest["schema_source_sha256"], "dsdgen_schema.hpp")
    for query in manifest["queries"]:
        original = source_dir / f"q{query['query_id']:02}.sql"
        verify_bytes(original.read_bytes(), query["source_sha256"], str(original))
    tables = derive_schema((source_dir / "dsdgen_schema.hpp").read_text(encoding="utf-8"))
    schema = destination / "schema.sql"
    schema_bytes = ("\n\n".join(table["ddl"] for table in tables) + "\n").encode("utf-8")
    verify_bytes(schema_bytes, manifest["schema_sha256"], "derived schema.sql")
    if not args.verify_only:
        schema.write_bytes(schema_bytes)
    else:
        verify_bytes(schema.read_bytes(), manifest["schema_sha256"], str(schema))
    variants = 0
    for query in manifest["queries"]:
        number = query["query_id"]
        original = source_dir / f"q{number:02}.sql"
        statements = split_queries(original.read_text(encoding="utf-8"))
        if len(statements) != len(query["variants"]):
            raise ValueError(f"query {number} statement count differs from manifest")
        for statement, variant in zip(statements, query["variants"]):
            path = destination / variant["sql"]
            data = statement.encode("utf-8")
            verify_bytes(data, variant["sha256"], str(path))
            if not args.verify_only:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
            else:
                verify_bytes(path.read_bytes(), variant["sha256"], str(path))
            variants += 1
    print(json.dumps({"manifest": str(manifest_path), "manifest_sha256": digest(manifest_path),
                      "logical_queries": 99, "variants": variants, "tables": len(tables),
                      "upstream_and_derived_hashes_verified": True, "network_permitted": not args.verify_only}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-only", action="store_true")
    main(parser.parse_args())
