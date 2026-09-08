"""Behavioral tests for the real-workload replay boundary (not speedup evidence)."""
import unittest

from replay import classify, validate_workload, encode_write, parse_samples


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

    def test_bad_input_fails_before_any_ingest(self):
        """No silent sample drops, duplicate samples, or time reordering."""
        for lines in [["x NaN 1"], ["x 1"], ["x 1 2", "x 2 1"], ["x 1 1", "x 2 1"]]:
            with self.assertRaises(ValueError):
                parse_samples(lines)


if __name__ == "__main__":
    unittest.main()
