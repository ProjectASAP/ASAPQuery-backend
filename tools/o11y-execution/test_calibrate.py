import copy
import unittest
from calibrate import calibrate


class CalibrationTests(unittest.TestCase):
    def setUp(self):
        self.manifest = {"plan_id": 1, "horizon_seconds": 60,
                         "workload": {"q": {}}, "components": {
                             "source:x": {"unit": "horizon", "multiplicity": 1},
                             "state:x:update": {"unit": "horizon", "multiplicity": 1},
                             "query:q:0": {"unit": "query_evaluation", "multiplicity": 6},
                             "result:q": {"unit": "query_evaluation", "multiplicity": 6}}}
        self.candidates = {"candidates": [{"manifest": self.manifest}]}
        self.measurements = {"units": "cpu_ns", "data_snapshot_id": "d", "candidates": [{
            "plan_id": 1, "manifest": copy.deepcopy(self.manifest), "executable": True,
            "horizon_seconds": 60,
            "horizon_phases": {key: {"cpu_ns": 10, "raw_measurement_file": key + ".json"}
                               for key in ("install", "ingest_and_build", "residency", "retirement")},
            "queries": {"q": {"cpu_ns": 100, "evaluations": 10, "classification": "warm",
                              "correct": True, "raw_measurement_file": "q.json"}}}]}

    def run_provider(self):
        return calibrate(self.candidates, self.measurements, "d", 1000, 1000)

    def test_inclusive_cpu_is_counted_once(self):
        # Horizon and per-evaluation totals preserve measured CPU without double counting.
        evidence, audit = self.run_provider()
        costs = evidence["quotes"][0]["unit_costs"]
        self.assertEqual(sum(costs[k] * v["multiplicity"] for k, v in self.manifest["components"].items()), 100)
        self.assertEqual(len(audit["attribution"][0]["allocation"]), 2)

    def test_unknown_candidate_is_not_given_free_quote(self):
        # Unmeasured alternatives remain unavailable rather than getting default zeros.
        self.measurements["candidates"] = []
        evidence, audit = self.run_provider()
        self.assertEqual(evidence["quotes"], [])
        self.assertEqual(audit["unavailable"][0]["reason"], "not measured")

    def test_binding_without_valid_execution_is_rejected(self):
        # A binding or failed/incorrect response cannot authorize a cost quote.
        self.measurements["candidates"][0]["queries"]["q"]["classification"] = "bound"
        with self.assertRaises(ValueError):
            self.run_provider()

    def test_missing_retirement_is_rejected(self):
        # Complete horizon accounting must not silently omit a lifecycle phase.
        del self.measurements["candidates"][0]["horizon_phases"]["retirement"]
        with self.assertRaises(ValueError):
            self.run_provider()

    def test_changed_manifest_cannot_reuse_measurements(self):
        # Any change in calibrated candidate identity invalidates the quote.
        self.measurements["candidates"][0]["manifest"]["horizon_seconds"] = 30
        with self.assertRaises(ValueError):
            self.run_provider()

    def test_shared_profile_preserves_measured_cpu_units(self):
        # Inclusive measured phases determine the coarse model; input count only normalizes updates.
        from update_global_profile import update
        self.measurements["candidates"][0]["resources"] = {"peak_memory_bytes": 123, "storage_bytes": 456}
        snapshot = {"implementation": {"horizon_seconds": 60, "implementation_cost": {}},
                    "workload_cost_evidence": {"old": True}}
        result, audit = update(snapshot, self.measurements, 20)
        self.assertEqual(result["implementation"]["lifecycle_costs"]["maintenance_per_update"], 0.5)
        self.assertEqual(result["implementation"]["implementation_cost"]["cpu_cost"], 40)
        self.assertNotIn("workload_cost_evidence", result)
        self.assertIn("not measured zero", " ".join(audit["limitations"]))


if __name__ == "__main__":
    unittest.main()
