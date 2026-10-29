"""Negative and reproducibility checks for directed witness input preparation."""

import json
from datetime import date
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import generate_query_witness_fixture as witness


class QueryWitnessFixture(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir="/tmp", prefix="optd-witness-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.source = Path(witness.__file__).resolve().parents[1] / "tpcds/manifest.json"

    def test_reproducible_rows_original_identities_and_explicit_nulls(self):
        first = witness.generate(self.source, self.root / "first", 3)
        second = witness.generate(self.source, self.root / "second", 3)
        self.assertEqual(first["manifest_sha256"], second["manifest_sha256"])
        one = json.loads(Path(first["manifest"]).read_text())
        original = json.loads(self.source.read_text())
        self.assertEqual(one["schema_sha256"], original["schema_sha256"])
        self.assertEqual(len(one["queries"]), 99)
        for before, after in zip(original["queries"], one["queries"]):
            self.assertEqual(before["variants"][0]["sha256"], after["variants"][0]["sha256"])
        self.assertEqual(first["input_rows"], 3)
        self.assertEqual(one["csv_null"], "\\N")
        self.assertIn("\\N", (self.root / "first/tables/item.csv").read_text())
        self.assertEqual((self.root / "first/tables/web_sales.csv").stat().st_size, 0)

    def test_typed_values_reject_rounding_float_and_invalid_dates(self):
        for value, typ in [(1.2, "DECIMAL(7,2)"), ("1.001", "DECIMAL(7,2)"),
                           ("100000.00", "DECIMAL(7,2)"), ("NaN", "DECIMAL(7,2)"),
                           (True, "BIGINT"), (2 ** 31, "INTEGER"), ("2001-02-29", "DATE")]:
            with self.subTest(value=value, typ=typ), self.assertRaises(ValueError):
                witness.typed_value(value, typ)

    def test_output_escape_nonempty_and_unknown_columns_are_rejected_before_writes(self):
        with self.assertRaisesRegex(ValueError, "child directory of /tmp"):
            witness.generate(self.source, Path(witness.__file__).resolve().parent / "forbidden", 3)
        occupied = self.root / "occupied"
        occupied.mkdir()
        (occupied / "keep.txt").write_text("preserve")
        with self.assertRaisesRegex(ValueError, "empty directory"):
            witness.generate(self.source, occupied, 3)
        with mock.patch.object(witness, "recipe", return_value=({"item": [{"wrong_column": 1}]}, "invalid")):
            with self.assertRaisesRegex(ValueError, "unknown columns"):
                witness.generate(self.source, self.root / "bad_columns", 3)
        self.assertFalse((self.root / "bad_columns").exists())
        self.assertEqual((occupied / "keep.txt").read_text(), "preserve")

    def test_changed_source_manifest_is_rejected(self):
        changed = self.root / "changed.json"
        changed.write_bytes(self.source.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "manifest digest mismatch"):
            witness.generate(changed, self.root / "bad_digest", 3)
        self.assertFalse((self.root / "bad_digest").exists())

    def test_all99_recipes_preserve_original_column_types_and_calendar_names(self):
        _, schemas = witness.checked_source(self.source)
        for query_id in range(1, 100):
            with self.subTest(query_id=query_id):
                rows, description = witness.recipe(query_id)
                self.assertTrue(description)
                self.assertTrue(rows)
                self.assertTrue(set(rows).issubset(schemas))
                for name, values in rows.items():
                    columns = dict(schemas[name])
                    for row in values:
                        self.assertTrue(set(row).issubset(columns), (name, row))
                        for column, value in row.items():
                            witness.typed_value(value, columns[column])
                        if name == "date_dim" and "d_day_name" in row:
                            self.assertEqual(date.fromisoformat(row["d_date"]).strftime("%A"), row["d_day_name"])


if __name__ == "__main__":
    unittest.main()
