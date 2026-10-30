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

    def test_unexecuted_planned_blocker_prevents_strict_coverage_claim(self):
        result=audit(self.fixture())
        self.assertEqual(result["counts"],{"total":1,"semantic_pass":1,"strict_plan_eligible":0,
                                            "complete_recompute":1,"plan_qualification_unknown":0})
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
