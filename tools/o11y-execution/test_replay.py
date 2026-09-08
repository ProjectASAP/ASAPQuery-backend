"""Behavioral tests for the real-workload replay boundary (not speedup evidence)."""
import unittest
from unittest.mock import patch
from tempfile import TemporaryDirectory
from pathlib import Path

from replay import execution_provenance, classify, validate_workload, encode_write, parse_samples, replay


class ReplayTests(unittest.TestCase):
    def test_binding_or_unattributed_success_is_not_execution(self):
        """Only an actual successful, attributed response establishes a route."""
        self.assertEqual(classify({"status": "bound"}), "failed")
        self.assertEqual(classify({"status": "success"}), "failed")
        for source, expected in [("asap_query", "warm"), ("exact_fallback", "exact_fallback")]:
            response = {"status": "success", "infos": [f"data_source: {source}"]}
            self.assertEqual(classify(response), expected)
            response["status"] = "error"
            self.assertEqual(classify(response), "failed")
        self.assertEqual(classify({"status": "success", "infos": ["data_source: asap_query"]},
                                  {"x-asap-execution": "exact_fallback"}), "exact_fallback")

    def test_hybrid_is_not_counted_as_pure_summary_acceleration(self):
        response = {"status": "success", "infos": ["data_source: asap_query"]}
        headers = {"x-asap-execution": "exact_fallback", "x-asap-execution-detail": "hybrid",
                   "x-asap-raw-scan-evaluations": "2", "x-asap-summary-readout-evaluations": "1"}
        self.assertEqual(classify(response, headers), "exact_fallback")
        self.assertEqual(execution_provenance(response, headers)["summary_readout_evaluations"], 1)
        self.assertEqual(classify(response, {"x-asap-execution": "failed"}), "failed")

    def test_corpus_occurrences_preserved_but_all_unique_queries_registered(self):
        """The harness cannot quietly replace or omit upstream queries."""
        corpus = {"upstream_revision": "abc", "queries": [
            {"id": "a", "query": "up", "eval_timestamp_ms": 1000},
            {"id": "b", "query": "up", "eval_timestamp_ms": 1000}]}
        snapshot = {"query_workload": {"repeating_queries": [{"query": "up"}]}}
        self.assertEqual(len(validate_workload(snapshot, corpus)), 2)
        snapshot["query_workload"]["repeating_queries"] = [{"query": "synthetic"}]
        with self.assertRaises(ValueError):
            validate_workload(snapshot, corpus)

    def test_timestamped_metrics_keep_labels_values_and_time(self):
        """A data adapter must not shift timestamps or lose histogram labels."""
        rows = parse_samples(['# TYPE x histogram', 'x_bucket{le="1",job="a"} 2 1.234'])
        self.assertEqual(rows, [({"__name__": "x_bucket", "le": "1", "job": "a"}, 2.0, 1234)])
        self.assertTrue(encode_write(rows))

    def test_odd_corpus_alternates_pair_order_across_repetitions(self):
        """An odd corpus must not fix every occurrence to the same pair order."""
        response = {"http_status": 200, "headers": {}, "elapsed_ns": 1,
                    "response": {"status": "success", "infos": ["data_source: asap_query"],
                                 "data": {"resultType": "vector", "result": []}}}
        with TemporaryDirectory() as directory, patch("replay.request", return_value=response):
            rows = replay([{"id": "q", "query": "up", "eval_timestamp_ms": 0}],
                          "http://backend", Path(directory), 2, "http://exact")
        self.assertEqual([r["pair_order"] for r in rows], ["exact_first", "backend_first"])

    def test_bad_input_fails_before_any_ingest(self):
        """No silent sample drops, duplicate samples, or time reordering."""
        for lines in [["x NaN 1"], ["x 1"], ["x 1 2", "x 2 1"], ["x 1 1", "x 2 1"]]:
            with self.assertRaises(ValueError):
                parse_samples(lines)


if __name__ == "__main__":
    unittest.main()
