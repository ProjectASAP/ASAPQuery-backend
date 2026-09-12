"""Preflight and readiness contracts for production-planned PromQL acceptance."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import accuracy_suite
import dataset
import load_dataset  # Makes the existing replay module available.
import replay
from planned_run import prepare


class PlannedTests(unittest.TestCase):
    def test_complete_window_and_costed_registration_are_required(self):
        """Reject unpriced plans and repetitions whose first window predates the data."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset.write(root / "data", dataset.synthetic(1, 1, 0, 120000), 2402, {"dataset": "synthetic"})
            manifest = accuracy_suite.corpus("synthetic")
            snapshot = {"snapshot_version": 2, "query_workload": {"repeating_queries": [{"query": "sum_over_time(fake_metric[1m])"}]}}
            qid = "synthetic/1m/False/temporal_sum"
            with self.assertRaisesRegex(ValueError, "cost evidence"):
                prepare(root / "data", manifest, qid, snapshot, None, 2)
            snapshot["workload_cost_evidence"] = {"quotes": [{"test_placeholder": True}]}
            _, corpus = prepare(root / "data", manifest, qid, snapshot, None, 2)
            self.assertEqual(corpus["queries"][0]["eval_timestamp_ms"], 120000)
            with self.assertRaisesRegex(ValueError, "full temporal history"):
                prepare(root / "data", manifest, qid, snapshot, None, 3)

    def test_every_window_needs_observed_summary_readout(self):
        """A warm label without a real summary read is insufficient readiness evidence."""
        body = {"status": "success", "infos": ["data_source: asap_query"],
                "data": {"resultType": "vector", "result": [{"metric": {}, "value": [120, "8"]}]}}
        headers = {"x-asap-summary-readout-evaluations": "1", "x-asap-exact-subquery-rpcs": "0"}
        answer = {"http_status": 200, "response": body, "headers": headers}
        query = [{"id": "q", "query": "sum_over_time(fake_metric[1m])", "eval_timestamp_ms": 120000}]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(replay, "request", return_value=answer) as calls:
                replay.verify_summary_ready(query, "http://asap", root, 2, 60000)
            self.assertEqual(calls.call_count, 2)
            report = json.loads((root / "summary-readiness.json").read_text())
            self.assertTrue(report["complete"])
            self.assertEqual([p["evaluation_ms"] for p in report["probes"]], [60000, 120000])
            with patch.object(replay, "request", return_value={**answer, "headers": {}}), self.assertRaisesRegex(RuntimeError, "not ready"):
                replay.verify_summary_ready(query, "http://asap", root, 2, 60000)

    def test_planned_replay_journals_asap_before_waiting_for_native(self):
        """The production replay also preserves ASAP evidence before a native failure."""
        answer = {"http_status": 200, "response": {"status": "success", "infos": ["data_source: asap_query"],
                  "data": {"resultType": "vector", "result": [{"metric": {}, "value": [120, "8"]}]}},
                  "headers": {}, "elapsed_ns": 100}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def request(url):
                if url.startswith("http://native"):
                    completed = [json.loads(line) for line in (root / "endpoint-requests.jsonl").read_text().splitlines()]
                    self.assertEqual(completed[0]["engine"], "asap")
                    raise OSError("service disconnected")
                return answer
            queries = [{"id": "q", "query": "sum_over_time(fake_metric[1m])", "eval_timestamp_ms": 120000}]
            with patch.object(replay, "request", side_effect=request):
                rows = replay.replay(queries, "http://asap", root, 1, exact_url="http://native", backend_first=True)
            self.assertEqual(rows[0]["execution"], "warm")
            self.assertEqual(rows[0]["exact"]["response"]["status"], "error")
            self.assertFalse(rows[0]["comparison"]["equal"])


if __name__ == "__main__":
    unittest.main()
