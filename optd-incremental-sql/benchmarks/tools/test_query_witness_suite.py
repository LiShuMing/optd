"""Exercise real process timeouts and rejection boundaries of the suite runner."""

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import run_query_witness_suite as suite
from test_summarize_coverage import iceberg_query_record
from summarize_coverage import ARROW_BACKEND, ICEBERG_BACKEND, STAGES


class QueryWitnessSuite(unittest.TestCase):
    def setUp(self):
        environment = mock.patch.dict(suite.os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        suite.os.environ.pop(suite.ICEBERG_ROOT_ENV, None)
        self.directory = tempfile.TemporaryDirectory(dir="/tmp", prefix="optd-witness-suite-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.binary = self.root / "fake-benchmark"
        self.binary.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(5)\n")
        self.binary.chmod(0o700)
        self.sha = suite.file_digest(self.binary)

    def reporting_binary(self, backend=ARROW_BACKEND, iceberg_root=None, omit_proof=False):
        record = {
            "status": "pass", "completed_rounds": 3, "initial_rows": 1, "final_rows": 2,
            "nonempty_result_seen": True, "output_delta_seen": True,
            "oracle": "independent_DataFusion_full_SQL", "comparison": "typed_bag_and_signed_endpoint_delta",
            "rounds": [{"stage": stage, "status": "pass", "output_rows": 2, "signed_delta_rows": 1} for stage in STAGES],
        }
        if backend == ICEBERG_BACKEND:
            record = iceberg_query_record(record, str(iceberg_root))
            if omit_proof:
                del record["bootstrap_source_vector"]
        self.binary.write_text(f'''#!{sys.executable}
import hashlib,json,os,pathlib,sys
manifest_path = pathlib.Path(sys.argv[1])
manifest = json.loads(manifest_path.read_text())
query = next(query for query in manifest["queries"] if query["query_id"] == int(sys.argv[3]))
record = {record!r}
record.update(query_id=query["query_id"],variant_id=query["variants"][0]["variant_id"])
report = {{"format_version":1,"suite":manifest["suite"],"manifest":str(manifest_path),
 "manifest_sha256":hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
 "schema_sha256":manifest["schema_sha256"],"source_backend":{backend!r},"queries":[record],
 "iceberg_artifact_root":os.environ.get({suite.ICEBERG_ROOT_ENV!r})}}
pathlib.Path(sys.argv[2]).write_text(json.dumps(report))
''')
        self.sha = suite.file_digest(self.binary)

    def test_query_selection_rejects_duplicate_missing_and_out_of_range_ids(self):
        self.assertEqual(suite.selected_queries(None), list(range(1, 100)))
        for values in [[], [0], [100], [1, 1], [True], ["3"]]:
            with self.subTest(values=values), self.assertRaises(ValueError):
                suite.selected_queries(values)

    def test_identity_timeout_and_output_parameters_fail_before_writes(self):
        output = self.root / "not-created"
        for sha in ["invalid", "0" * 64]:
            with self.subTest(sha=sha), self.assertRaises(ValueError):
                suite.run_suite(self.binary, sha, output, [3])
        for timeout in [0, -1, float("inf"), float("nan"), 3601]:
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                suite.run_suite(self.binary, self.sha, output, [3], timeout)
        with self.assertRaisesRegex(ValueError, "child directory of /tmp"):
            suite.run_suite(self.binary, self.sha, Path(suite.__file__).parent / "forbidden", [3])
        occupied = self.root / "occupied"
        occupied.mkdir()
        with self.assertRaisesRegex(ValueError, "already exists"):
            suite.run_suite(self.binary, self.sha, occupied, [3])
        self.assertFalse(output.exists())

    def test_actual_timeout_is_retained_and_following_query_still_runs(self):
        with redirect_stdout(io.StringIO()):
            progress = suite.run_suite(self.binary, self.sha, self.root / "timeouts", [3, 8], 0.05)
        self.assertEqual(progress["status"], "fail")
        self.assertTrue(progress["complete"])
        self.assertFalse(progress["full99_coverage"])
        self.assertEqual([row["status"] for row in progress["records"]], ["hard_timeout", "hard_timeout"])
        self.assertTrue(all(row["hard_timeout"] for row in progress["records"]))
        self.assertTrue(all(Path(row["log"]).exists() for row in progress["records"]))
        saved = json.loads((self.root / "timeouts/progress.json").read_text())
        self.assertEqual(saved, progress)
        self.assertIn("no completed reports", progress["aggregation_error"])

    def test_changed_executable_stops_and_preserves_previous_query_failure(self):
        def changed(*args, **kwargs):
            self.binary.write_text("replaced during execution\n")
            return mock.Mock(returncode=1)

        with mock.patch.object(suite.subprocess, "run", side_effect=changed), redirect_stdout(io.StringIO()):
            progress = suite.run_suite(self.binary, self.sha, self.root / "changed", [3, 8])
        self.assertEqual(len(progress["records"]), 1)
        self.assertFalse(progress["complete"])
        self.assertEqual(progress["status"], "fail")
        self.assertIn("identity mismatch", progress["error"])

    def test_fixture_failure_is_retained_and_next_query_still_runs(self):
        original = suite.generate

        def failed(source, output, query_id):
            if query_id == 3:
                raise ValueError("deliberately rejected recipe")
            return original(source, output, query_id)

        with mock.patch.object(suite, "generate", side_effect=failed), redirect_stdout(io.StringIO()):
            progress = suite.run_suite(self.binary, self.sha, self.root / "bad-fixture", [3, 8], 0.05)
        self.assertEqual([row["status"] for row in progress["records"]], ["fixture_error", "hard_timeout"])
        self.assertTrue(progress["complete"])
        self.assertEqual(progress["status"], "fail")
        self.assertIn("rejected recipe", progress["records"][0]["error"])

    def test_backend_and_root_conflicts_fail_before_any_output_is_created(self):
        output, root = self.root / "not-created", self.root / "iceberg-not-created"
        parameters = [
            {"expected_backend": "unknown"},
            {"expected_backend": ICEBERG_BACKEND},
            {"iceberg_root": root},
            {"expected_backend": ICEBERG_BACKEND, "iceberg_root": Path("/tmp")},
            {"expected_backend": ICEBERG_BACKEND, "iceberg_root": Path(suite.__file__).parent / "forbidden"},
            {"expected_backend": ICEBERG_BACKEND, "iceberg_root": self.root},
            {"expected_backend": ICEBERG_BACKEND, "iceberg_root": output},
            {"expected_backend": ICEBERG_BACKEND, "iceberg_root": output / "nested"},
        ]
        for values in parameters:
            with self.subTest(values=values), self.assertRaises(ValueError):
                suite.run_suite(self.binary, self.sha, output, [3], **values)
        self.assertFalse(output.exists())
        self.assertFalse(root.exists())

    def test_inherited_source_environment_cannot_change_default_or_explicit_backend(self):
        output, root = self.root / "not-created", self.root / "iceberg-not-created"
        for inherited in ["", "/tmp/unrelated-iceberg-root"]:
            with self.subTest(inherited=inherited), mock.patch.dict(suite.os.environ, {suite.ICEBERG_ROOT_ENV: inherited}):
                with self.assertRaisesRegex(ValueError, "inherited"):
                    suite.run_suite(self.binary, self.sha, output, [3])
                with self.assertRaisesRegex(ValueError, "inherited"):
                    suite.run_suite(self.binary, self.sha, output, [3],
                                    expected_backend=ICEBERG_BACKEND, iceberg_root=root)
        self.assertFalse(output.exists())
        self.assertFalse(root.exists())

    def test_real_process_gets_explicit_iceberg_environment_and_audited_command(self):
        root = self.root / "real-process-source-root"
        capture = self.root / "captured-environment.json"
        self.binary.write_text(f'''#!{sys.executable}
import json,os,pathlib,sys
pathlib.Path({str(capture)!r}).write_text(json.dumps({{"root":os.environ.get({suite.ICEBERG_ROOT_ENV!r}),"argv":sys.argv}}))
sys.exit(1)
''')
        self.sha = suite.file_digest(self.binary)
        with redirect_stdout(io.StringIO()):
            progress = suite.run_suite(self.binary, self.sha, self.root / "real-process-output", [3],
                                       expected_backend=ICEBERG_BACKEND, iceberg_root=root)
        observed = json.loads(capture.read_text())
        self.assertEqual(observed["root"], str(root))
        self.assertEqual(observed["argv"], progress["records"][0]["command"])
        self.assertEqual(progress["identity"]["source_backend"], ICEBERG_BACKEND)
        self.assertEqual(progress["identity"]["environment"], {suite.ICEBERG_ROOT_ENV: str(root)})
        self.assertEqual(progress["records"][0]["command_audit"]["environment"], progress["identity"]["environment"])
        self.assertEqual(progress["records"][0]["status"], "process_failed")
        self.assertTrue(root.is_dir())

    def test_real_report_process_preserves_backend_and_subset_cannot_claim_full99(self):
        for backend in [ARROW_BACKEND, ICEBERG_BACKEND]:
            with self.subTest(backend=backend):
                root = self.root / f"source-{backend}" if backend == ICEBERG_BACKEND else None
                self.reporting_binary(backend, root)
                with redirect_stdout(io.StringIO()):
                    progress = suite.run_suite(self.binary, self.sha, self.root / f"reports-{backend}", [3],
                                               expected_backend=backend, iceberg_root=root)
                self.assertEqual(progress["status"], "pass")
                self.assertFalse(progress["full99_coverage"])
                self.assertEqual(progress["aggregate"]["source_backend"], backend)
                aggregate = json.loads(Path(progress["aggregate"]["path"]).read_text())
                self.assertEqual(aggregate["source_backend"], backend)
                self.assertEqual(aggregate["aggregate"]["with_pass"], 1)

    def test_real_process_cannot_claim_iceberg_using_arrow_or_missing_proof(self):
        for omit_proof in [False, True]:
            with self.subTest(omit_proof=omit_proof):
                root = self.root / f"bad-source-{omit_proof}"
                self.reporting_binary(ICEBERG_BACKEND if omit_proof else ARROW_BACKEND, root, omit_proof)
                with redirect_stdout(io.StringIO()):
                    progress = suite.run_suite(self.binary, self.sha, self.root / f"bad-output-{omit_proof}", [3],
                                               expected_backend=ICEBERG_BACKEND, iceberg_root=root)
                self.assertEqual(progress["status"], "fail")
                self.assertEqual(progress["records"][0]["status"], "invalid_report")
                self.assertEqual(progress["records"][0]["exit_code"], 0)
                self.assertIn("aggregation_error", progress)

    def test_real_report_cannot_substitute_an_unselected_iceberg_root(self):
        root = self.root / "selected-source-root"
        self.reporting_binary(ICEBERG_BACKEND, root)
        original = self.binary.read_text()
        self.binary.write_text(original.replace(
            f"os.environ.get({suite.ICEBERG_ROOT_ENV!r})", repr("/tmp/unselected-source-root")))
        self.sha = suite.file_digest(self.binary)
        with redirect_stdout(io.StringIO()):
            progress = suite.run_suite(self.binary, self.sha, self.root / "wrong-root-report", [3],
                                       expected_backend=ICEBERG_BACKEND, iceberg_root=root)
        self.assertEqual(progress["status"], "fail")
        self.assertEqual(progress["records"][0]["status"], "invalid_report")
        self.assertIn("artifact root", progress["records"][0]["error"])


if __name__ == "__main__":
    unittest.main()
