"""Evidence-integrity regressions for the aggregate qualification report."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from summarize_coverage import STAGES, digest, summarize


class CoverageIntegrity(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        (self.directory / "schema.sql").write_text("CREATE TABLE t (id INTEGER);\n")
        (self.directory / "query.sql").write_text("SELECT * FROM t;\n")
        manifest = {
            "format_version": 1, "suite": "tpch", "logical_queries": 1,
            "schema": "schema.sql", "schema_sha256": digest((self.directory / "schema.sql").read_bytes()),
            "queries": [{"query_id": 1, "source": "explicit_test_variant", "variants": [{
                "variant_id": "1", "sql": "query.sql", "sha256": digest((self.directory / "query.sql").read_bytes())}]}],
        }
        self.manifest = self.directory / "manifest.json"
        self.manifest.write_text(json.dumps(manifest))
        self.record = {
            "query_id": 1, "variant_id": "1", "status": "pass", "completed_rounds": 3,
            "initial_rows": 1, "final_rows": 2, "nonempty_result_seen": True, "output_delta_seen": True,
            "oracle": "independent_DataFusion_full_SQL", "comparison": "typed_bag_and_signed_endpoint_delta",
            "rounds": [{"stage": stage, "status": "pass", "output_rows": 2, "signed_delta_rows": 1} for stage in STAGES],
        }

    def write_report(self, label, record):
        report = {
            "format_version": 1, "suite": "tpch", "manifest": str(self.manifest),
            "manifest_sha256": digest(self.manifest.read_bytes()),
            "schema_sha256": json.loads(self.manifest.read_text())["schema_sha256"], "queries": [record],
        }
        path = self.directory / f"{label}.json"
        path.write_text(json.dumps(report))
        return label, str(path), digest(path.read_bytes())

    def test_single_complete_profile_is_selected_and_other_failure_is_retained(self):
        failed = {"query_id": 1, "variant_id": "1", "status": "fail", "reason": "planner failure", "rounds": []}
        aggregate = summarize([self.write_report("failed", failed), self.write_report("good", self.record)], self.directory)
        self.assertEqual(aggregate["aggregate"], {"total": 1, "with_pass": 1, "remaining": 0, "profiles_with_failures": 1})
        query = aggregate["queries"][0]
        self.assertEqual(query["selected_complete_profile"]["profile"], "good")
        self.assertEqual(query["profiles"][0]["verification"]["reason"], "planner failure")

    def test_incomplete_profiles_never_combine_rounds(self):
        partial = copy.deepcopy(self.record)
        partial.update(status="fail", completed_rounds=2, rounds=partial["rounds"][:2])
        aggregate = summarize([self.write_report("one", partial), self.write_report("two", partial)], self.directory)
        self.assertEqual(aggregate["aggregate"]["with_pass"], 0)
        partial["status"] = "pass"
        with self.assertRaisesRegex(ValueError, "all three rounds"):
            summarize([self.write_report("false_pass", partial)], self.directory)

    def test_changed_report_and_sql_are_rejected(self):
        specification = self.write_report("good", self.record)
        path = Path(specification[1])
        original = path.read_bytes()
        path.write_bytes(original + b"\n")
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            summarize([specification], self.directory)
        path.write_bytes(original)
        (self.directory / "query.sql").write_text("SELECT 0;\n")
        with self.assertRaisesRegex(ValueError, "query SQL digest mismatch"):
            summarize([specification], self.directory)

    def test_unchanged_output_is_retained_without_pass(self):
        unchanged = copy.deepcopy(self.record)
        unchanged.update(status="verified_unchanged_output", output_delta_seen=False)
        for item in unchanged["rounds"]:
            item["signed_delta_rows"] = 0
        aggregate = summarize([self.write_report("unchanged", unchanged)], self.directory)
        self.assertEqual(aggregate["aggregate"]["remaining"], 1)
        self.assertIsNone(aggregate["queries"][0]["selected_complete_profile"])


if __name__ == "__main__":
    unittest.main()
