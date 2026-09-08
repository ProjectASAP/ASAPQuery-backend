"""Correctness gates must survive missing rows, failed responses and zero baselines."""
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlparse, parse_qs
from compare import compare_results, summarize, process_delta


def vector(*values):
    return {"status": "success", "data": {"resultType": "vector", "result": [
        {"metric": {"job": key}, "value": [123, str(value)]} for key, value in values]}}


class ComparisonTests(unittest.TestCase):
    def test_group_matching_is_not_row_position(self):
        """Permuting a group-by result does not change correctness."""
        result = compare_results(vector(("b", 2), ("a", 1)), vector(("a", 1), ("b", 2)))
        self.assertTrue(result["equal"])
        self.assertEqual(result["completeness"], 1)

    def test_missing_rows_and_zero_baseline_cannot_hide_errors(self):
        """Report structural loss separately; zero denominators are not epsilon-clamped."""
        result = compare_results(vector(("a", 2)), vector(("a", 0), ("b", 3)))
        self.assertFalse(result["equal"])
        self.assertEqual(result["missing_series"], 1)
        self.assertEqual(result["max_absolute_error"], 2)
        self.assertEqual(result["zero_baseline_mismatches"], 1)
        self.assertIsNone(result["max_relative_error"])

    def test_duplicates_and_errors_are_not_equal(self):
        """Malformed or failed responses cannot become a successful empty comparison."""
        for response in [{"status": "error"}, vector(("a", 1), ("a", 1))]:
            self.assertFalse(compare_results(response, vector(("a", 1)))["comparable"])

    def test_failed_or_missing_baseline_prevents_benefit_claim(self):
        """Failures remain in the workload denominator; no successful-subset speedup."""
        row = {"execution": "warm", "phase": "repeat", "elapsed_ns": 10,
               "response": vector(("a", 1)), "exact": {"http_status": 500,
               "elapsed_ns": 100, "response": {"status": "error"}}}
        report = summarize([row])
        self.assertEqual(report["occurrences"], 1)
        self.assertIsNone(report["matched_query_latency_ratio"])
        self.assertIsNone(report["end_to_end_benefit"])

    def test_matrix_requires_the_same_timestamps_and_empty_series(self):
        """Series and time coverage are part of correctness even without numeric samples."""
        empty = {"status": "success", "data": {"resultType": "matrix", "result": []}}
        series = {"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"job": "a"}, "values": []}]}}
        self.assertFalse(compare_results(empty, series)["equal"])

    def test_pid_reuse_and_unreadable_processes_are_not_free_cpu(self):
        """A missing counter or a different process lifetime must remain unknown."""
        self.assertIsNone(process_delta(None, None))
        self.assertIsNone(process_delta({"pid": 1, "start_ticks": 1}, {"pid": 1, "start_ticks": 2}))

    def test_paired_replay_preserves_occurrences_time_and_alternates_order(self):
        """Harness conformance only: stub responses are never benchmark evidence."""
        from replay import replay
        calls = []
        def respond(url):
            calls.append(url)
            response = vector(("a", 1))
            response["infos"] = ["data_source: asap_query"]
            return {"response": response, "http_status": 200, "headers": {}, "elapsed_ns": 10}
        queries = [{"id": x, "query": "sum(up{job=\"a\"})", "eval_timestamp_ms": 1234567} for x in ["a", "b"]]
        with tempfile.TemporaryDirectory() as folder, patch("replay.request", side_effect=respond):
            rows = replay(queries, "http://backend", Path(folder), 2, "http://exact")
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(r["comparison"]["equal"] for r in rows))
        self.assertEqual([urlparse(u).netloc for u in calls[:4]], ["exact", "backend", "backend", "exact"])
        for url in calls:
            self.assertEqual(parse_qs(urlparse(url).query), {"query": [queries[0]["query"]], "time": ["1234.567"]})


if __name__ == "__main__":
    unittest.main()
