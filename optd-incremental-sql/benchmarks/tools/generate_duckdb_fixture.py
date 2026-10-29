#!/usr/bin/env python3
"""Export genuine TPC fixtures with the pinned public DuckDB generator.

This optional fixture tool is deliberately outside the Rust dependency graph.
Install the explicitly selected, pinned DuckDB in a separate environment. The
default is 1.4.3; 1.2.2 is an explicit glibc 2.17 data-generation alternative.
Query SQL is exported
verbatim from the named extension; multi-statement queries retain every variant.
Data and the manifest are generated artifacts and should stay outside git.
"""

import argparse
import hashlib
import json
from pathlib import Path
import sys


VERSION = "1.4.3"
CSV_NULL = "\\N"
EXPECTED = {"tpch": (22, 8), "tpcds": (99, 24)}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def split_queries(sql):
    """Split statements without splitting quoted text or SQL comments."""
    start, i, quote, block, line = 0, 0, None, False, False
    statements = []
    while i < len(sql):
        char = sql[i]
        pair = sql[i:i + 2]
        if line:
            if char == "\n":
                line = False
        elif block:
            if pair == "*/":
                block = False
                i += 1
        elif quote:
            if char == quote:
                if i + 1 < len(sql) and sql[i + 1] == quote:
                    i += 1
                else:
                    quote = None
        elif pair == "--":
            line = True
            i += 1
        elif pair == "/*":
            block = True
            i += 1
        elif char in "'\"":
            quote = char
        elif char == ";":
            if sql[start:i].strip():
                statements.append(sql[start:i + 1].strip() + "\n")
            start = i + 1
        i += 1
    if quote or block:
        raise ValueError("unterminated SQL quote or comment")
    if sql[start:].strip():
        statements.append(sql[start:].strip() + "\n")
    return statements


def generate(args):
    import duckdb  # Optional: only needed when explicitly generating a fixture.

    if duckdb.__version__ != args.duckdb_version:
        raise ValueError(f"expected duckdb {args.duckdb_version}; found {duckdb.__version__}")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError("output must be empty; refusing to overwrite an existing fixture")
    output.mkdir(parents=True, exist_ok=True)
    queries_dir = output / "queries"
    queries_dir.mkdir()
    table_dir = output / "tables"
    table_dir.mkdir()
    connection = duckdb.connect()
    connection.execute("SET threads = 1")
    extension_dir = output / ".extensions"
    connection.execute("SET extension_directory = " + literal(extension_dir))
    if args.install_extension:
        connection.execute(f"INSTALL {args.suite} FROM 'https://extensions.duckdb.org'")
    if args.extension_file:
        connection.execute("LOAD " + literal(args.extension_file.resolve()))
    else:
        connection.execute(f"LOAD {args.suite}")
    generator = "dbgen" if args.suite == "tpch" else "dsdgen"
    connection.execute(f"CALL {generator}(sf={args.scale_factor})")
    query_rows = connection.execute(f"SELECT query_nr, query FROM {args.suite}_queries() ORDER BY query_nr").fetchall()
    expected_queries, expected_tables = EXPECTED[args.suite]
    if [int(row[0]) for row in query_rows] != list(range(1, expected_queries + 1)):
        raise ValueError("generator did not return the complete numbered query set")
    entries = []
    for number, sql in query_rows:
        original = queries_dir / f"q{number:02}.source.sql"
        original.write_text(sql, encoding="utf-8")
        statements = split_queries(sql)
        variants = []
        for index, statement in enumerate(statements):
            suffix = chr(ord("a") + index) if len(statements) > 1 else ""
            variant_id = f"{number}{suffix}"
            path = queries_dir / f"q{number:02}{suffix}.sql"
            path.write_text(statement, encoding="utf-8")
            variants.append({"variant_id": variant_id, "sql": str(path.relative_to(output)), "sha256": digest(path)})
        if not variants:
            raise ValueError(f"empty generated query {number}")
        entries.append({
            "query_id": int(number),
            "variants": variants,
            "source": f"queries/q{number:02}.source.sql",
            "source_sha256": digest(original),
            "transformations": ["split_statements_without_semantic_rewrite"],
            "qualification": "duckdb_generator_parameters_not_tpc_certification",
            "status": "available_not_executed",
        })
    table_rows = connection.execute("SELECT table_name, sql FROM duckdb_tables() WHERE NOT internal ORDER BY table_name").fetchall()
    if len(table_rows) != expected_tables:
        raise ValueError(f"expected {expected_tables} tables, found {len(table_rows)}")
    schema = output / "schema.sql"
    schema.write_text("\n\n".join(row[1] for row in table_rows) + "\n", encoding="utf-8")
    tables = []
    for name, _ in table_rows:
        csv = table_dir / f"{name}.csv"
        connection.execute(f"COPY {identifier(name)} TO {literal(csv)} (FORMAT CSV, HEADER FALSE, DELIMITER '|', NULL {literal(CSV_NULL)})")
        count = connection.execute(f"SELECT count(*) FROM {identifier(name)}").fetchone()[0]
        tables.append({"name": name, "csv": str(csv.relative_to(output)), "rows": count, "sha256": digest(csv)})
    extension = connection.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = ?", [args.suite])
    extension_metadata = [dict(zip([column[0] for column in extension.description], row)) for row in extension.fetchall()]
    manifest = {
        "format_version": 1,
        "suite": args.suite,
        "source": f"https://github.com/duckdb/duckdb/tree/v{args.duckdb_version}/extension/" + args.suite,
        "source_ref": f"v{args.duckdb_version}",
        "duckdb_version": duckdb.__version__,
        "duckdb_build": connection.execute("SELECT * FROM pragma_version()").fetchall(),
        "generator_threads": 1,
        "extension_file_sha256": digest(args.extension_file) if args.extension_file else None,
        "extension_metadata": extension_metadata,
        "logical_queries": expected_queries,
        "schema": "schema.sql",
        "schema_sha256": digest(schema),
        "csv_path_base": "manifest_directory",
        "csv_delimiter": "|",
        "csv_has_header": False,
        "csv_null": CSV_NULL,
        "scale_factor": args.scale_factor,
        "data_status": "public_duckdb_dbgen" if args.suite == "tpch" else "public_duckdb_dsdgen",
        "fixture_qualification": "public_generator_data_not_tpc_certification_or_performance",
        "tables": tables,
        "queries": entries,
    }
    path = output / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"manifest": str(path), "logical_queries": expected_queries, "variants": sum(len(q["variants"]) for q in entries), "tables": len(tables)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=sorted(EXPECTED))
    parser.add_argument("output", type=Path)
    parser.add_argument("--scale-factor", type=float, default=0.01)
    parser.add_argument("--duckdb-version", choices=["1.4.3", "1.2.2"], default=VERSION)
    parser.add_argument("--install-extension", action="store_true", help="explicitly permit the named DuckDB extension download")
    parser.add_argument("--extension-file", type=Path, help="load a locally downloaded official signed extension without another download")
    options = parser.parse_args()
    if options.scale_factor <= 0:
        parser.error("scale factor must be positive")
    if options.install_extension and options.extension_file:
        parser.error("choose either --install-extension or --extension-file")
    try:
        generate(options)
    except (ImportError, ValueError) as error:
        print(f"fixture generation failed: {error}", file=sys.stderr)
        sys.exit(1)
