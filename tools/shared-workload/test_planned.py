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
import planned_run
from planned_run import prepare


class PlannedTests(unittest.TestCase):
    def test_series_ordered_trace_reaches_production_replay(self):
        """Globally interleaved trace times remain unchanged and replay in time order."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            records = [("google_cluster_cpu_rate", {"service": "job-a", "task": task, "host": "h"}, ts, value)
                       for task, samples in [("a", [(0, 1.0), (120000, 2.0)]),
                                             ("b", [(0, 3.0), (120000, 4.0)])]
                       for ts, value in samples]
            dataset.write(root / "data", records, 4, {"dataset": "google"})
            original = (root / "data/samples.openmetrics").read_bytes()
            manifest = accuracy_suite.corpus("google")
            (root / "queries.json").write_text(json.dumps(manifest))
            (root / "snapshot.json").write_text(json.dumps({
                "snapshot_version": 2,
                "query_workload": {"repeating_queries": [{"query": "sum_over_time(google_cluster_cpu_rate[1m])"}]},
                "workload_cost_evidence": {"quotes": [{"test_placeholder": True}]}}))
            argv = ["planned_run.py", "--data", str(root / "data"), "--manifest", str(root / "queries.json"),
                    "--snapshot", str(root / "snapshot.json"), "--query-id", "google/1m/False/temporal_sum",
                    "--compiler", "compiler", "--data-plane", "backend", "--prometheus", "prometheus",
                    "--cpu-affinity", "0", "--output", str(root / "run")]
            def replay_command(command):
                path = Path(command[command.index("--metrics") + 1])
                with path.open() as source:
                    samples = list(replay.iter_samples(source))
                self.assertEqual([row[2] for row in samples], [0, 0, 120000, 120000])
                actual = {(labels["task"], timestamp, value) for labels, value, timestamp in samples}
                self.assertEqual(actual, {(labels["task"], timestamp, value)
                                          for _, labels, timestamp, value in records})
                return type("Completed", (), {"returncode": 1})()
            with patch("sys.argv", argv), patch.object(planned_run.subprocess, "run", side_effect=replay_command):
                self.assertEqual(planned_run.main(), 1)
            self.assertEqual((root / "data/samples.openmetrics").read_bytes(), original)

    def test_ordered_metrics_use_original_file(self):
        """Ordered synthetic data needs no copy or timestamp changes."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "samples.openmetrics"
            source.write_text('x{task="a"} 1 0\nx{task="b"} 2 0\nx{task="a"} 3 0.1\n# EOF\n')
            self.assertEqual(planned_run.prepare_metrics(source, root), source)
            self.assertEqual(list(root.iterdir()), [source])

    def test_sort_does_not_hide_invalid_per_series_order(self):
        """Sorting must not repair duplicate or backwards samples within a series."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "samples.openmetrics"
            for timestamp in ["0", "0.1"]:
                source.write_text(f'x{{task="a"}} 1 0.1\nx{{task="b"}} 2 0\nx{{task="a"}} 3 {timestamp}\n')
                with self.assertRaisesRegex(ValueError, "out-of-order"):
                    planned_run.prepare_metrics(source, root)
                self.assertEqual(list(root.iterdir()), [source])

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
