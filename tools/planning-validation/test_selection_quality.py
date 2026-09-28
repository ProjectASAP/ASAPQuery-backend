"""Synthetic audit fixtures test the gate; they are not a real-evidence evaluation."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from selection_quality import evaluate


class SelectionQualityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.counter = 0
        trace = self.save({"test_only": "synthetic stand-in for the artifact contract"})
        digest = trace["sha256"]
        self.experiment = dict(schema_version=1, workload_kind="real_trace", scope="single_query",
            objective="total_cpu_seconds", evaluation_interval_ms=[1000, 2000],
            calibration_end_ms=1000, horizon_seconds=1,
            limits=dict(p95_latency_ms=10, peak_memory_bytes=100), trace=trace,
            environment=dict(machine="test", backend_revision="test", planner_revision="test"))
        self.experiment["evidence"] = {name: dict(self.save(dict(test_only=name, model_version="test-measured", data_snapshot_id="test-generation", objective="total_cpu_seconds", backend_revision="test", planner_revision="test")),
            origin="measured", workload_sha256=digest, observed_at_ms=0, valid_for_ms=2000)
            for name in ["statistics", "accuracy", "resources"]}
        self.experiment["plan"] = self.save({"cost_comparison": {"model_version":"test-measured", "data_snapshot_id":"test-generation", "candidate_evaluations": [
            dict(physical_candidate_id="a", status="selected", total_cost=1),
            dict(physical_candidate_id="b", status="unselected", total_cost=2),
            dict(candidate_id="unsupported", status="bind_failed", unavailable_reason="unsupported input")
        ]}})
        self.experiment["candidates"] = []
        for name, cpu in [("a", 1), ("b", 2)]:
            correctness = self.save(dict(candidate_id=name, workload_sha256=digest, passed=True))
            measured = dict(candidate_id=name, environment=self.experiment["environment"],
                workload_sha256=digest, horizon_seconds=1,
                trials=[dict(total_cpu_seconds=cpu, p95_latency_ms=5,
                             peak_memory_bytes=50, correctness=correctness) for _ in range(3)])
            self.experiment["candidates"].append(dict(candidate_id=name, measurements=self.save(measured)))

    def save(self, value):
        path = self.root / f"artifact-{self.counter}.json"
        self.counter += 1
        raw = json.dumps(value).encode()
        path.write_bytes(raw)
        return dict(path=path.name, sha256=hashlib.sha256(raw).hexdigest())

    def measurement(self, index, mutate):
        run = self.experiment["candidates"][index]
        value = json.loads((self.root / run["measurements"]["path"]).read_text())
        mutate(value)
        run["measurements"] = self.save(value)

    def test_single_and_full_workload_are_reported_separately(self):
        for scope in ["single_query", "full_workload"]:
            self.experiment["scope"] = scope
            result = evaluate(self.root, self.experiment)
            self.assertTrue(result["passed"])
            self.assertEqual(result["scope"], scope)
            self.assertEqual(result["selection_regret_cpu_seconds"], 0)

    def test_predicted_minimum_is_not_enough_when_actual_selection_is_worse(self):
        self.measurement(0, lambda m: [t.update(total_cpu_seconds=3) for t in m["trials"]])
        result = evaluate(self.root, self.experiment)
        self.assertFalse(result["passed"])
        self.assertEqual(result["selection_regret_cpu_seconds"], 1)

    def test_missing_candidate_or_evidence_cannot_pass(self):
        for key in ["statistics", "accuracy", "resources"]:
            experiment = copy.deepcopy(self.experiment)
            del experiment["evidence"][key]
            with self.assertRaises(KeyError):
                evaluate(self.root, experiment)
        self.experiment["candidates"].pop()
        with self.assertRaisesRegex(ValueError, "incomplete candidate"):
            evaluate(self.root, self.experiment)

    def test_stale_synthetic_mismatched_and_overlapping_evidence_rejected(self):
        for field, value in [("origin", "synthetic"), ("valid_for_ms", 500),
                             ("workload_sha256", "wrong"), ("observed_at_ms", 1500)]:
            experiment = copy.deepcopy(self.experiment)
            experiment["evidence"]["resources"][field] = value
            with self.assertRaises(ValueError):
                evaluate(self.root, experiment)
        self.experiment["calibration_end_ms"] = 1500
        with self.assertRaisesRegex(ValueError, "overlaps"):
            evaluate(self.root, self.experiment)

    def test_constraints_and_wrong_results_fail_even_when_cpu_is_best(self):
        for field, value in [("peak_memory_bytes", 101), ("p95_latency_ms", 11)]:
            original = copy.deepcopy(self.experiment)
            self.measurement(0, lambda m: m["trials"][0].update({field: value}))
            self.assertFalse(evaluate(self.root, self.experiment)["passed"])
            self.experiment = original
        wrong = self.save(dict(candidate_id="a", workload_sha256=self.experiment["trace"]["sha256"], passed=False))
        self.measurement(0, lambda m: m["trials"][0].update(correctness=wrong))
        self.assertFalse(evaluate(self.root, self.experiment)["passed"])

    def test_wrong_horizon_environment_and_unrepeated_measurements_rejected(self):
        for field, value in [("horizon_seconds", 2), ("environment", {}), ("trials", [])]:
            original = copy.deepcopy(self.experiment)
            self.measurement(0, lambda m: m.update({field: value}))
            with self.assertRaises(ValueError):
                evaluate(self.root, self.experiment)
            self.experiment = original

    def test_measurements_must_be_the_inputs_used_for_selection(self):
        ref = self.experiment["evidence"]["resources"]
        contents = json.loads((self.root / ref["path"]).read_text())
        contents["model_version"] = "different-cost-model"
        ref.update(self.save(contents))
        with self.assertRaisesRegex(ValueError, "priced resource model mismatch"):
            evaluate(self.root, self.experiment)

    def test_tampered_artifact_is_rejected(self):
        ref = self.experiment["evidence"]["resources"]
        (self.root / ref["path"]).write_text("tampered")
        with self.assertRaisesRegex(ValueError, "checksum"):
            evaluate(self.root, self.experiment)


if __name__ == "__main__":
    unittest.main()
