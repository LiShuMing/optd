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


class QueryWitnessSuite(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir="/tmp", prefix="optd-witness-suite-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.binary = self.root / "fake-benchmark"
        self.binary.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(5)\n")
        self.binary.chmod(0o700)
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


if __name__ == "__main__":
    unittest.main()
