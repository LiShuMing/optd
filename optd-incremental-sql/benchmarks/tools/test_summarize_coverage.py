"""Evidence-integrity regressions for the aggregate qualification report."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from summarize_coverage import ARROW_BACKEND, ICEBERG_BACKEND, STAGES, canonical_json, digest, summarize


def iceberg_query_record(record, root="/tmp/optd-test-catalog"):
    """Build complete report-identity evidence without claiming an IO run."""
    record = copy.deepcopy(record)
    iceberg_schema = {"type": "struct", "schema-id": 0,
                      "fields": [{"id": 1, "name": "id", "required": False, "type": "int"}]}
    arrow_layout = {"fields": [{"name": "id", "nullable": True, "data_type": "Int32", "metadata": {}}],
                    "metadata": {}}

    def vector(sequence):
        return {"version": 1, "sources": {"t": {
            "catalog": {"kind": "local/1", "canonical_root": root},
            "namespace": ["qualification"], "name": "t",
            "table_uuid": "11111111-2222-3333-4444-555555555555", "reference": "main",
            "snapshot": {"kind": "snapshot", "id": 100 + sequence,
                         "sequence_number": sequence, "descriptor_sha256": "a" * 64},
            "schema_id": 0, "iceberg_schema": iceberg_schema,
            "iceberg_schema_sha256": digest(b"optd-incremental-sql/iceberg-schema/1\0" + canonical_json(iceberg_schema)),
            "arrow_layout": arrow_layout,
            "arrow_layout_sha256": digest(b"optd-incremental-sql/arrow-layout/1\0" + canonical_json(arrow_layout)),
            "normalizer_version": 1,
        }}}

    record.update(source_backend=ICEBERG_BACKEND, registered_tables=["t"],
                  oracle="independent_DataFusion_full_SQL_over_exact_Iceberg_endpoints",
                  bootstrap_source_vector=vector(1), bootstrap_strict_reopen_verified=True,
                  strict_reopen_verified=True, full_output_log_replay_verified=True)
    record["bootstrap_source_vector_sha256"] = digest(canonical_json(record["bootstrap_source_vector"]))
    for index, item in enumerate(record["rounds"], 2):
        item.update(source_vector=vector(index), strict_reopen_verified=True,
                    source_work={"changed_sources": 1, "endpoint_scans": 2, "decoded_batches": 2,
                                 "decoded_rows": 3, "decoded_bytes": 16, "signed_change_rows": 1})
        item["source_vector_sha256"] = digest(canonical_json(item["source_vector"]))
    return record


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

    def write_report(self, label, record, backend=None):
        report = {
            "format_version": 1, "suite": "tpch", "manifest": str(self.manifest),
            "manifest_sha256": digest(self.manifest.read_bytes()),
            "schema_sha256": json.loads(self.manifest.read_text())["schema_sha256"], "queries": [record],
        }
        if backend is not None:
            report["source_backend"] = backend
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

    def test_historical_arrow_backend_is_preserved_at_every_aggregate_level(self):
        aggregate = summarize([self.write_report("legacy", self.record)], self.directory)
        self.assertEqual(aggregate["source_backend"], ARROW_BACKEND)
        self.assertEqual(aggregate["profiles"][0]["source_backend"], ARROW_BACKEND)
        query = aggregate["queries"][0]
        self.assertEqual(query["source_backend"], ARROW_BACKEND)
        self.assertEqual(query["selected_complete_profile"]["source_backend"], ARROW_BACKEND)
        self.assertEqual(query["profiles"][0]["verification"]["source_backend"], ARROW_BACKEND)

    def test_iceberg_requires_explicit_report_and_query_backend(self):
        record = iceberg_query_record(self.record)
        with self.assertRaisesRegex(ValueError, "report source_backend"):
            summarize([self.write_report("missing-report", record)], self.directory, ICEBERG_BACKEND)
        del record["source_backend"]
        with self.assertRaisesRegex(ValueError, "query source_backend"):
            summarize([self.write_report("missing-query", record, ICEBERG_BACKEND)], self.directory, ICEBERG_BACKEND)

    def test_mixed_backends_cannot_supply_one_qualification(self):
        specifications = [self.write_report("arrow", self.record),
                          self.write_report("iceberg", iceberg_query_record(self.record), ICEBERG_BACKEND)]
        for backend in [ARROW_BACKEND, ICEBERG_BACKEND]:
            with self.subTest(backend=backend), self.assertRaisesRegex(ValueError, "source_backend"):
                summarize(specifications, self.directory, backend)

    def test_iceberg_pass_retains_backend_and_early_failed_profile(self):
        failed = {"query_id": 1, "variant_id": "1", "source_backend": ICEBERG_BACKEND,
                  "status": "fail", "reason": "source scan rejected", "rounds": []}
        aggregate = summarize([self.write_report("failed", failed, ICEBERG_BACKEND),
                               self.write_report("good", iceberg_query_record(self.record), ICEBERG_BACKEND)],
                              self.directory, ICEBERG_BACKEND)
        self.assertEqual(aggregate["source_backend"], ICEBERG_BACKEND)
        self.assertEqual(aggregate["aggregate"]["with_pass"], 1)
        self.assertEqual(aggregate["aggregate"]["profiles_with_failures"], 1)
        self.assertEqual(aggregate["queries"][0]["profiles"][0]["verification"]["reason"], "source scan rejected")

    def test_false_iceberg_pass_cannot_omit_cursor_work_reopen_or_replay(self):
        top_fields = ["bootstrap_source_vector", "bootstrap_source_vector_sha256",
                      "bootstrap_strict_reopen_verified", "strict_reopen_verified",
                      "full_output_log_replay_verified"]
        round_fields = ["source_vector", "source_vector_sha256", "source_work", "strict_reopen_verified"]
        for location, field in [("query", key) for key in top_fields] + [("round", key) for key in round_fields]:
            with self.subTest(location=location, field=field):
                record = iceberg_query_record(self.record)
                del (record if location == "query" else record["rounds"][0])[field]
                with self.assertRaisesRegex(ValueError, "Iceberg"):
                    summarize([self.write_report("false-pass", record, ICEBERG_BACKEND)], self.directory, ICEBERG_BACKEND)

    def test_iceberg_rejects_bad_vector_digest_substituted_binding_or_cursor_rollback(self):
        for mutation in ["digest", "uuid", "rollback", "work"]:
            with self.subTest(mutation=mutation):
                record = iceberg_query_record(self.record)
                item = record["rounds"][0]
                if mutation == "digest":
                    item["source_vector_sha256"] = "0" * 64
                elif mutation == "uuid":
                    item["source_vector"]["sources"]["t"]["table_uuid"] = "22222222-2222-3333-4444-555555555555"
                elif mutation == "rollback":
                    item["source_vector"]["sources"]["t"]["snapshot"]["sequence_number"] = 1
                else:
                    item["source_work"]["changed_sources"] = True
                if mutation in {"uuid", "rollback"}:
                    item["source_vector_sha256"] = digest(canonical_json(item["source_vector"]))
                with self.assertRaisesRegex(ValueError, "Iceberg"):
                    summarize([self.write_report("invalid", record, ICEBERG_BACKEND)], self.directory, ICEBERG_BACKEND)

    def test_iceberg_rejects_non_endpoint_oracle(self):
        record = iceberg_query_record(self.record)
        record["oracle"] = "independent_DataFusion_full_SQL"
        with self.assertRaisesRegex(ValueError, "independent full SQL oracle"):
            summarize([self.write_report("wrong-oracle", record, ICEBERG_BACKEND)], self.directory, ICEBERG_BACKEND)

    def test_malformed_iceberg_round_is_a_rejection_instead_of_an_uncaught_error(self):
        record = iceberg_query_record(self.record)
        record["rounds"][0] = None
        with self.assertRaisesRegex(ValueError, "round stage/status"):
            summarize([self.write_report("bad-round", record, ICEBERG_BACKEND)], self.directory, ICEBERG_BACKEND)


if __name__ == "__main__":
    unittest.main()
