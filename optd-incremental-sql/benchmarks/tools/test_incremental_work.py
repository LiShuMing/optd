"""Keep semantic pass, planned capability and actual observed work independent."""
import copy
import unittest
from summarize_incremental_work import audit


class IncrementalWork(unittest.TestCase):
    def fixture(self):
        return {"suite":"tpch", "source_backend":"typed_arrow", "query_mode":"relational_core",
                "queries":[{"query_id":1,"with_pass":True,"selected_complete_profile":{"profile":"p"},
                            "profiles":[{"profile":"p","verification":{
                                "status":"pass","completed_rounds":3,
                                "plan_qualification":{"strict_plan_eligible":False,
                                    "planned_nodes":[{"id":0,"strategy":"AccumulatorDelta"},
                                                     {"id":1,"strategy":"OperatorRecompute"}],
                                    "complete_recompute_blockers":[{"id":1,"rule":{"name":"sort"},"reason":"ordered"}]},
                                "rounds":[{"sql_stats":{"nodes":[{"strategy":"AccumulatorDelta","accumulator_work":{
                                    "delta_input_rows":2,"historical_input_rows":0}}]}} for _ in range(3)]}}]}]}

    def test_value_index_work_sums_actual_counts_and_legacy_absence_stays_unknown(self):
        source = self.fixture()
        rounds = source["queries"][0]["profiles"][0]["verification"]["rounds"]
        for round_ in rounds:
            round_["sql_stats"]["nodes"][0]["accumulator_work"]["value_index_work"] = {
                "input_rows": 4, "value_updates": 2,
                "directory_entries_copied": 3, "extremum_lookups": 2}
        work = audit(source)["observed_work"]
        for name, expected in [("input_rows", 12), ("value_updates", 6),
                               ("directory_entries_copied", 9), ("extremum_lookups", 6)]:
            self.assertEqual(work["accumulator_work.value_index_work." + name], expected)
        for round_ in rounds:
            del round_["sql_stats"]["nodes"][0]["accumulator_work"]["value_index_work"]
        self.assertFalse(any("value_index_work" in key for key in audit(source)["observed_work"]))

    def test_distinct_membership_work_preserves_zero_crossings_and_legacy_absence(self):
        source = self.fixture()
        rounds = source["queries"][0]["profiles"][0]["verification"]["rounds"]
        for round_ in rounds:
            round_["sql_stats"]["nodes"][0]["accumulator_work"]["distinct_work"] = {
                "input_rows": 4, "value_updates": 2,
                "directory_entries_copied": 3, "entering_values": 1, "leaving_values": 0}
        work = audit(source)["observed_work"]
        for name, expected in [("input_rows", 12), ("value_updates", 6),
                               ("directory_entries_copied", 9), ("entering_values", 3),
                               ("leaving_values", 0)]:
            self.assertEqual(work["accumulator_work.distinct_work." + name], expected)
        for round_ in rounds:
            del round_["sql_stats"]["nodes"][0]["accumulator_work"]["distinct_work"]
        self.assertFalse(any("distinct_work" in key for key in audit(source)["observed_work"]))

    def test_match_count_work_retains_measured_zeros_and_legacy_unknown(self):
        source = self.fixture()
        rounds = source["queries"][0]["profiles"][0]["verification"]["rounds"]
        for round_ in rounds:
            round_["sql_stats"]["nodes"] = [{"strategy": "MatchCountDelta", "keyed_work": {
                "scope": "JoinKeys", "snapshot_input_rows_before": [0, 0],
                "snapshot_input_rows_after": [0, 0], "match_join_work": {
                    "count_probes": 1, "peer_entries_visited": 0,
                    "matched_pairs": 0, "bucket_entries_copied": 300}}}]
        work = audit(source)["observed_work"]
        self.assertEqual(work["keyed_work.match_join_work.count_probes"], 3)
        self.assertEqual(work["keyed_work.match_join_work.peer_entries_visited"], 0)
        self.assertEqual(work["keyed_work.match_join_work.bucket_entries_copied"], 900)
        for round_ in rounds:
            del round_["sql_stats"]["nodes"][0]["keyed_work"]["match_join_work"]
        self.assertFalse(any("match_join_work" in key for key in audit(source)["observed_work"]))

    def test_residual_pair_and_count_copy_work_remains_separate_and_legacy_unknown(self):
        source = self.fixture()
        rounds = source["queries"][0]["profiles"][0]["verification"]["rounds"]
        for round_ in rounds:
            round_["sql_stats"]["nodes"] = [{"strategy": "MatchCountDelta", "keyed_work": {
                "scope": "JoinKeys", "snapshot_input_rows_before": [0, 0],
                "snapshot_input_rows_after": [0, 0], "match_join_work": {
                    "residual_candidate_pairs": 6, "residual_predicate_rows": 6,
                    "match_count_updates": 3, "count_map_entries_copied": 9,
                    "count_directory_entries_copied": 2}}}]
        work = audit(source)["observed_work"]
        for name, total in [("residual_candidate_pairs", 18), ("residual_predicate_rows", 18),
                            ("match_count_updates", 9), ("count_map_entries_copied", 27),
                            ("count_directory_entries_copied", 6)]:
            self.assertEqual(work["keyed_work.match_join_work." + name], total)
        for round_ in rounds:
            del round_["sql_stats"]["nodes"][0]["keyed_work"]["match_join_work"]["residual_predicate_rows"]
        self.assertNotIn("keyed_work.match_join_work.residual_predicate_rows", audit(source)["observed_work"])

    def test_unexecuted_planned_blocker_prevents_strict_coverage_claim(self):
        result=audit(self.fixture())
        self.assertEqual(result["counts"],{"total":1,"semantic_pass":1,"strict_plan_eligible":0,
                                            "complete_recompute":1,"strict_plan_rejected":1,
                                            "plan_compilation_failed":0,"plan_qualification_unknown":0})
        self.assertEqual(result["observed_work"]["accumulator_work.delta_input_rows"],6)
        self.assertFalse(result["end_to_end_delta_work_qualified"])
        self.assertEqual(result["queries_with_blocking_rule"],{"sort":1})

    def test_unknown_legacy_plan_and_failed_semantics_are_not_strict_passes(self):
        source=self.fixture(); del source["queries"][0]["profiles"][0]["verification"]["plan_qualification"]
        self.assertEqual(audit(source)["counts"]["plan_qualification_unknown"],1)
        source["queries"][0]["with_pass"]=False
        self.assertEqual(audit(source)["counts"]["semantic_pass"],0)

    def test_contradictory_blockers_and_missing_actual_work_reject_report(self):
        source=self.fixture(); bad=copy.deepcopy(source)
        bad["queries"][0]["profiles"][0]["verification"]["plan_qualification"]["strict_plan_eligible"]=True
        with self.assertRaisesRegex(ValueError,"contradict"):audit(bad)
        del source["queries"][0]["profiles"][0]["verification"]["rounds"][0]["sql_stats"]
        with self.assertRaisesRegex(ValueError,"structured"):audit(source)

    def refusal(self):
        source = self.fixture()
        source["queries"][0]["with_pass"] = False
        report = source["queries"][0]["profiles"][0]["verification"]
        report.update(status="unsupported_strict_plan", completed_rounds=0, rounds=[])
        return source

    def test_known_preflight_refusal_is_never_erased_by_failed_semantics(self):
        source = self.refusal()
        result = audit(source)
        self.assertEqual(result["counts"]["semantic_pass"], 0)
        self.assertEqual(result["counts"]["plan_qualification_unknown"], 0)
        self.assertEqual(result["counts"]["complete_recompute"], 1)
        self.assertFalse(result["queries"][0]["strict_plan_eligible"])
        self.assertEqual(result["queries_with_blocking_rule"], {"sort": 1})
        self.assertEqual(result["observed_work"], {})
        self.assertEqual(result["queries"][0]["preflight_profiles"][0]["profile"], "p")

    def test_refusal_rejects_execution_or_contradictory_plan_and_keeps_profile_ambiguity(self):
        for field, value in [("completed_rounds", 1), ("rounds", [{}]), ("plan_qualification", None)]:
            source = self.refusal()
            source["queries"][0]["profiles"][0]["verification"][field] = value
            with self.assertRaisesRegex(ValueError, "zero executed rounds"):
                audit(source)
        source = self.refusal()
        source["queries"][0]["profiles"].append({"profile": "unknown", "verification": {}})
        query = audit(source)["queries"][0]
        self.assertIsNone(query["strict_plan_eligible"])
        self.assertEqual(len(query["preflight_profiles"]), 1)

    def test_compilation_failure_is_a_blocker_but_never_a_semantic_pass(self):
        source = self.refusal()
        report = source["queries"][0]["profiles"][0]["verification"]
        report["plan_qualification"]["planned_nodes"][1]["strategy"] = None
        result = audit(source)
        self.assertFalse(result["queries"][0]["strict_plan_eligible"])
        self.assertEqual(result["counts"]["complete_recompute"], 0)
        self.assertEqual(result["counts"]["plan_compilation_failed"], 1)
        self.assertEqual(result["counts"]["strict_plan_rejected"], 1)
        source["queries"][0]["with_pass"] = True
        report.update(status="pass", completed_rounds=3, rounds=[{}] * 3)
        with self.assertRaisesRegex(ValueError, "failed plan compilation"):
            audit(source)


class UnifiedWork(unittest.TestCase):
    fixture = IncrementalWork.fixture
    def verification(self, source):
        return source["queries"][0]["profiles"][0]["verification"]

    def test_lazy_table_work_keeps_validation_scan_export_and_legacy_absence_separate(self):
        source = self.fixture()
        rounds = self.verification(source)["rounds"]
        for round_ in rounds:
            round_["commit"] = {"maintenance": {}, "dml": [{"execution": {
                "selector": {"table_reads": {"materialization": {"materialized_rows": 1},
                    "ordering_rows": 256}},
                "target": {"leaf_validation": {"rows": 257},
                    "delta_update": {"materialized_rows": 0}}}}]}
            round_["storage_work"] = {"table_leaf_demand": {"materialized_rows": 257},
                "table_leaf_validation": {"rows": 0}}
        work = audit(source)["observed_work"]
        self.assertEqual(work["dml.execution.selector.table_reads.materialization.materialized_rows"], 3)
        self.assertEqual(work["dml.execution.target.leaf_validation.rows"], 771)
        self.assertEqual(work["dml.execution.target.delta_update.materialized_rows"], 0)
        self.assertEqual(work["storage_work.table_leaf_demand.materialized_rows"], 771)
        for round_ in rounds:
            del round_["commit"]["dml"][0]["execution"]["selector"]["table_reads"]
            del round_["commit"]["dml"][0]["execution"]["target"]["leaf_validation"]
            del round_["storage_work"]["table_leaf_demand"]
            del round_["storage_work"]["table_leaf_validation"]
        work = audit(source)["observed_work"]
        self.assertFalse(any("table_reads" in key or "leaf_validation" in key or "leaf_demand" in key for key in work))

    def test_sql_database_work_keeps_transaction_verification_and_recovery_separate(self):
        source = self.fixture()
        verification = self.verification(source)
        verification["plan_qualification"] = {"strict_plan_eligible": True,
            "planned_nodes": [{"id": 0, "strategy": "AccumulatorDelta"}],
            "complete_recompute_blockers": []}
        verification["rounds"] = [{"commit": {"dml": [{"affected_rows": 1,
            "source_relation_rows": 100, "net_changed_rows": 2,
            "execution": {"selector": {"source_rows": 100},
                          "target": {"collected_rows": 100}}}],
            "maintenance": {"mv": {"nodes": [], "work": {
                "sources": {"old_row_entries_copied": 100},
                "state_copies": {"keyed_input_entries": 80}}}}},
            "storage_work": {"tables": {"written": {"files": 1, "rows": 100, "bytes": 900}}},
            "verification_store_work_before_reopen": {"tables": {"read": {"rows": 1000}}},
            "recovery_work": {"dependency_rows": 100, "store": {"tables": {"read": {"rows": 100}}}},
            "verification_store_work_after_reopen": {"tables": {"read": {"rows": 1000}}}}
            for _ in range(3)]
        result = audit(source)
        work = result["observed_work"]
        self.assertEqual(work["refresh_work.sources.old_row_entries_copied"], 300)
        self.assertEqual(work["refresh_work.state_copies.keyed_input_entries"], 240)
        self.assertEqual(work["dml.execution.selector.source_rows"], 300)
        self.assertEqual(work["storage_work.tables.written.rows"], 300)
        self.assertEqual(work["recovery_work.dependency_rows"], 300)
        self.assertEqual(work["verification_store_work_before_reopen.tables.read.rows"], 3000)
        self.assertEqual(result["queries"][0]["work_domains"]["storage"],
                         {"measured_rounds": 3, "unknown_rounds": 0})
        self.assertFalse(result["performance_qualified"])
        self.assertFalse(result["end_to_end_delta_work_qualified"])

    def test_legacy_missing_and_partial_fields_are_unknown_instead_of_zero(self):
        source = self.fixture()
        rounds = self.verification(source)["rounds"]
        rounds[0]["sql_stats"]["work"] = {"sources": {"map_rebuilds": 0}}
        result = audit(source)
        domains = result["queries"][0]["work_domains"]
        self.assertEqual(domains["refresh_orchestration"], {"measured_rounds": 1, "unknown_rounds": 2})
        self.assertEqual(domains["storage"], {"measured_rounds": 0, "unknown_rounds": 3})
        self.assertNotIn("storage_work.tables.written.rows", result["observed_work"])
        self.assertEqual(result["observed_work"]["refresh_work.sources.map_rebuilds"], 0)

    def test_invalid_numeric_work_is_rejected_in_all_new_domains(self):
        for field in ["source_work", "storage_work", "recovery_work",
                      "verification_store_work_before_reopen", "verification_store_work_after_reopen"]:
            for invalid in [True, -1, 1.5, "1"]:
                source = self.fixture()
                self.verification(source)["rounds"][0][field] = {"rows": invalid}
                with self.assertRaisesRegex(ValueError, "nonnegative integers"):
                    audit(source)

    def test_missing_original_dml_execution_is_unknown_but_empty_current_commit_is_measured(self):
        source = self.fixture()
        rounds = self.verification(source)["rounds"]
        rounds[0] = {"commit": {"dml": [{"affected_rows": 1}], "maintenance": {}}}
        rounds[1] = {"commit": None}
        rounds[2] = {"commit": {"dml": [], "maintenance": {}}}
        result = audit(source)
        self.assertEqual(result["queries"][0]["work_domains"]["dml"],
                         {"measured_rounds": 2, "unknown_rounds": 1})
        self.assertEqual(result["queries"][0]["work_domains"]["operators"],
                         {"measured_rounds": 3, "unknown_rounds": 0})

    def test_replayed_receipt_is_not_added_as_current_execution(self):
        source = self.fixture()
        self.verification(source)["rounds"] = [{"replayed": True, "commit": {
            "dml": [{"execution": {"selector": {"source_rows": 900}}}],
            "maintenance": {"mv": {"nodes": [], "work": {"sources": {"map_rebuilds": 1}}}}},
            "storage_work": {"manifest_reads": {"files": 8}}} for _ in range(3)]
        work = audit(source)["observed_work"]
        self.assertEqual(work["storage_work.manifest_reads.files"], 24)
        self.assertNotIn("dml.execution.selector.source_rows", work)
        self.assertNotIn("refresh_work.sources.map_rebuilds", work)

    def test_work_does_not_require_plan_qualification_and_incomplete_round_lists_reject(self):
        source = self.fixture()
        verification = self.verification(source)
        del verification["plan_qualification"]
        verification["rounds"][0]["storage_work"] = {"manifest_reads": {"files": 8}}
        result = audit(source)
        self.assertEqual(result["observed_work"]["storage_work.manifest_reads.files"], 8)
        self.assertIsNone(result["queries"][0]["strict_plan_eligible"])
        verification["rounds"].pop()
        with self.assertRaisesRegex(ValueError, "three rounds"):
            audit(source)
