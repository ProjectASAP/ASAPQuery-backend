"""Contracts for dataset binding, matched HTTP replay and accounting boundaries."""
import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import accuracy_suite as suite
import dataset
import resources
import load_dataset
from summarize_accuracy import summarize


@contextmanager
def endpoints(route="warm", wrong=False):
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            query = parse_qs(urlparse(self.path).query)
            calls.append((self.path, query))
            value = "9" if wrong and self.path.startswith("/asap") else "8"
            body = {"status": "success", "data": {"resultType": "vector", "result": [
                {"metric": {"label_0": "g000000"}, "value": [float(query["time"][0]), value]}]}}
            self.send_response(200)
            self.send_header("x-asap-execution", route)
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            calls.append((self.path, body))
            self.send_response(200)
            self.send_header("x-asap-execution", route)
            self.end_headers()
            self.wfile.write(b'{"labels":{"label_0":"g000000"},"value":8}\n')
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class AccuracyTests(unittest.TestCase):
    def test_profiles_and_families(self):
        """Every requested window/filter is bound, while gauge counters are N/A."""
        for profile, (metric, group, _, counter) in suite.PROFILES.items():
            manifest = suite.corpus(profile)
            queries = manifest["queries"]
            self.assertEqual(len({q["id"] for q in queries}), len(queries))
            self.assertEqual({q["window"] for q in queries}, set(suite.WINDOWS))
            self.assertEqual(manifest["cardinalities"], [10, 100, 1000, 10000, 100000, 1000000])
            for q in queries:
                expected = counter or metric if q["name"].startswith(("rate", "increase")) else metric
                self.assertIn(expected, q["promql"])
                self.assertIn(expected, q["clickhouse_sql"])
                self.assertNotIn("'data'", q["clickhouse_sql"])
                if profile != "synthetic":
                    self.assertNotIn("label_0", q["promql"])
                    if q["name"].startswith(("rate", "increase")):
                        self.assertEqual(q["status"], "not_applicable")
            for name in ("spatial_sum", "spatial_topk", "spatial_quantile_0.99", "temporal_sum",
                         "temporal_quantile_0.75", "rate", "rate_spatial_sum", "temporal_sum_spatial_sum",
                         "rate_spatial_topk", "quantile_ratio", "spatial_count", "increase", "nested_spatial_sum"):
                self.assertTrue(any(q["name"] == name for q in queries), name)
        with self.assertRaises(ValueError):
            suite.corpus("google", "x' OR 1=1")

    def test_client_data_and_counter_resets(self):
        """The client exposition and SQL rows encode identical 100ms samples."""
        from prometheus_client.openmetrics.parser import text_string_to_metric_families
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "data"
            meta = dataset.write(root, dataset.synthetic(2, 4, 1000000, 100), 32)
            rows = [json.loads(line) for line in (root / "samples.jsonl").read_text().splitlines()]
            # A historical import stream repeats families; unlike one scrape,
            # parse its individual client-encoded sample lines independently.
            samples = [s for line in (root / "samples.openmetrics").read_text().splitlines() if not line.startswith("#")
                       for family in text_string_to_metric_families(line + "\n# EOF\n") for s in family.samples]
            self.assertEqual(meta["samples"], 32)
            self.assertEqual(meta["series"], 16)
            self.assertEqual(len(samples), len(rows))
            for sample, row in zip(samples, rows):
                self.assertEqual((sample.name, sample.labels, sample.value, float(sample.timestamp) * 1000),
                                 (row["metric"], row["labels"], row["value"], row["ts_ms"]))
        values = [v for m, _, _, v in dataset.synthetic(1, 1, 0, 99800) if m.endswith("_total")]
        self.assertGreater(values[996], values[997])

    def test_trace_mapping_and_duplicates(self):
        """Native timestamps and identities survive normalization; duplicates fail."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            google = root / "google.jsonl"
            google.write_text(json.dumps({"metric": "google_cluster_cpu_rate", "timestamp_ms": 1234,
                                         "value": 0.25, "attributes": {"service": "job-1", "host": "host-2", "task": "3"}}) + "\n")
            self.assertEqual(list(dataset.trace(google, "google"))[0][2:], (1234, 0.25))
            alibaba = root / "container_usage.csv"
            alibaba.write_text("c_1,m_1,12.5,25,30,0,0,0,0,0,0\n")
            row = list(dataset.trace(alibaba, "alibaba"))[0]
            self.assertEqual(row, ("alibaba_container_cpu_util", {"machine_id": "m_1", "container_id": "c_1"}, 12500, 25.0))
            with self.assertRaisesRegex(ValueError, "duplicate"):
                dataset.write(root / "bad", [row, row], 10)
            self.assertFalse((root / "bad/data-manifest.json").exists())

    def run_fixture(self, root, url, **changes):
        manifest = suite.corpus("synthetic")
        manifest["queries"] = [q for q in manifest["queries"] if q["name"] == "spatial_sum" and not q["filtered"]]
        (root / "queries.json").write_text(json.dumps(manifest))
        (root / "loaded.json").write_text(json.dumps({"data": {"start_ms": 1000000, "end_ms": 1002000,
                                     "provenance": {"dataset": "synthetic"}, "sha256": {"samples.jsonl": "fixture"}}}))
        args = argparse.Namespace(manifest=root / "queries.json", loaded_data=root / "loaded.json", output=root / "results.jsonl",
                                  prometheus=url + "/prom", victoriametrics=url + "/vm", clickhouse=url + "/ch",
                                  asap_prometheus=url + "/asap", asap_clickhouse=url + "/asap-sql",
                                  start_ms=1000000, end_ms=1002000, query_name=[], rtol=1e-9, atol=1e-12,
                                  require_warm=True, components=None, required_pairs=["promql", "sql"])
        for key, value in changes.items():
            setattr(args, key, value)
        code = suite.run(args)
        return code, [json.loads(line) for line in args.output.read_text().splitlines()]

    def test_http_e2e_schedule_and_sql_rendering(self):
        """All five HTTP adapters execute matched timestamps and record measurements."""
        with tempfile.TemporaryDirectory() as tmp, endpoints() as (url, calls):
            code, rows = self.run_fixture(Path(tmp), url)
            self.assertEqual(code, 0)
            self.assertEqual(len(calls), 15)
            self.assertEqual([r["evaluation_ms"] for r in rows], [1000000, 1001000, 1002000])
            self.assertEqual(len(rows[0]["measurements"]), 5)
            sql = next(body for path, body in calls if path == "/ch")
            self.assertNotIn(b"{eval_ms}", sql)
            self.assertTrue(sql.endswith(b"FORMAT JSONEachRow"))
            summary = next(iter(summarize(rows)["queries"].values()))
            self.assertEqual(summary["warm_occurrences"], 3)
            self.assertEqual(summary["latency"]["prometheus"]["count"], 3)

    def test_wrong_results_and_fallback_fail(self):
        """Equality cannot disguise fallback and warm evidence cannot disguise error."""
        for route, wrong in (("exact_fallback", False), ("unknown", False), ("warm", True)):
            with tempfile.TemporaryDirectory() as tmp, endpoints(route, wrong) as (url, _):
                code, rows = self.run_fixture(Path(tmp), url)
                self.assertEqual(code, 1)
                self.assertFalse(any(row["passed"] for row in rows))

    def test_native_timeout_does_not_skip_other_endpoints(self):
        """A failed VM call cannot erase successful responses or skip ASAP calls."""
        original = suite.request
        for failed in ("/prom", "/ch", "/vm"):
            with tempfile.TemporaryDirectory() as tmp, endpoints() as (url, calls):
                def probe(endpoint, *args, **kwargs):
                    if endpoint == url + failed:
                        raise TimeoutError("native timed out")
                    return original(endpoint, *args, **kwargs)
                with patch.object(suite, "request", side_effect=probe):
                    _, rows = self.run_fixture(Path(tmp), url)
                self.assertEqual(sum(path.startswith("/asap") for path, _ in calls), 6)
                self.assertTrue(all("asap_promql" in row["responses"] for row in rows))
                engine = {"/prom": "prometheus", "/ch": "clickhouse", "/vm": "victoriametrics"}[failed]
                self.assertTrue(all(row["endpoints"][engine]["error"]["kind"] == "timeout" for row in rows))

    def test_vm_mismatch_cannot_establish_vm_benefit(self):
        """Native VM semantics and a missing ASAP MetricsQL pair are explicit."""
        original = suite.request
        with tempfile.TemporaryDirectory() as tmp, endpoints() as (url, _):
            def probe(endpoint, *args, **kwargs):
                body, headers = original(endpoint, *args, **kwargs)
                if endpoint == url + "/vm":
                    body["data"]["result"][0]["value"][1] = "999"
                return body, headers
            with patch.object(suite, "request", side_effect=probe):
                code, rows = self.run_fixture(Path(tmp), url, asap_metricsql=url + "/asap-vm",
                                              required_pairs=["promql", "sql", "metricsql"])
            self.assertEqual(code, 1)
            self.assertTrue(all(not row["pairs"]["metricsql"]["eligible_for_query_comparison"] for row in rows))

    def test_missing_metricsql_pair_is_not_full_acceptance(self):
        """Without an ASAP VM endpoint the default three-pair acceptance fails."""
        with tempfile.TemporaryDirectory() as tmp, endpoints() as (url, _):
            code, rows = self.run_fixture(Path(tmp), url, required_pairs=None)
            self.assertEqual(code, 1)
            self.assertTrue(all(not row["pairs"]["metricsql"]["correctness"]["comparable"] for row in rows))

    def test_asap_first_without_client_deadline_and_vm_pair(self):
        """ASAP runs first without a deadline; MetricsQL has its own exact pair."""
        original = suite.request
        with tempfile.TemporaryDirectory() as tmp, endpoints() as (url, calls):
            def probe(endpoint, *args, **kwargs):
                self.assertIsNone(kwargs["timeout"])
                return original(endpoint, *args, **kwargs)
            with patch.object(suite, "request", side_effect=probe):
                code, rows = self.run_fixture(Path(tmp), url, asap_metricsql=url + "/asap-vm",
                                              required_pairs=["promql", "sql", "metricsql"])
            self.assertEqual(code, 0)
            self.assertTrue(all(row["pairs"]["metricsql"]["eligible_for_query_comparison"] for row in rows))
            self.assertTrue(all(not row["pairs"]["metricsql"]["eligible_for_benefit_conclusion"] for row in rows))
            self.assertEqual(rows[0]["request_order"], ["asap_promql", "asap_sql", "asap_metricsql", "prometheus", "clickhouse", "victoriametrics"])
            journal = Path(tmp) / "results.jsonl.endpoints.jsonl"
            self.assertEqual(len(journal.read_text().splitlines()), 18)

    def test_empty_oracle_fails(self):
        """An empty result pair is not positive accuracy evidence."""
        empty = {"status": "success", "data": {"resultType": "vector", "result": []}}
        self.assertFalse(suite.evaluate(empty, empty, 0, 0)["equal"])

    def test_resources_no_double_counting(self):
        """Overlapping cgroups are rejected and absent accounting stays null."""
        with self.assertRaises(ValueError):
            resources.validate({"a": {"cgroup": "/sys/fs/cgroup/test"}, "b": {"cgroup": "/sys/fs/cgroup/test/child"}})
        self.assertIsNone(resources.delta({}, {})["total"]["cpu_usec"])
        sample = {"x": {"cgroup_identity": 1, "cpu_usec": 10, "disk_read_bytes": 2,
                        "disk_write_bytes": 4, "memory_bytes": 100, "network": None}}
        after = {"x": {**sample["x"], "cpu_usec": 20, "memory_bytes": 150}}
        result = resources.delta(sample, after)
        self.assertEqual(result["total"]["cpu_usec"], 10)
        self.assertEqual(result["total"]["memory_after_bytes"], 150)

    def test_loader_http_receipt_and_hash_guard(self):
        """One source feeds Remote Write and SQL; tampering fails before writes."""
        with tempfile.TemporaryDirectory() as tmp, endpoints() as (url, calls):
            root = Path(tmp)
            dataset.write(root / "data", dataset.synthetic(1, 1, 1000000, 100), 4,
                          {"dataset": "synthetic"})
            argv = ["load_dataset.py", "--data", str(root / "data"), "--remote-write", url + "/write",
                    "--clickhouse", url + "/ch", "--output", str(root / "receipt.json")]
            with patch("sys.argv", argv):
                load_dataset.main()
            self.assertEqual(len(calls), 2)
            self.assertIsInstance(calls[0][1], bytes)
            self.assertTrue(calls[1][1].startswith(b"INSERT INTO raw_samples FORMAT JSONEachRow\n"))
            self.assertEqual(json.loads((root / "receipt.json").read_text())["data"]["samples"], 4)
            (root / "data/samples.jsonl").write_text("tampered\n")
            argv[-1] = str(root / "second-receipt.json")
            with patch("sys.argv", argv), self.assertRaisesRegex(ValueError, "hash"):
                load_dataset.main()
            self.assertEqual(len(calls), 2)

    def test_phase_wrapper_preserves_exit_and_totals(self):
        """Whole-command measurement reports disk/CPU and preserves failure status."""
        import sys
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cgroup = root / "cgroup"
            cgroup.mkdir()
            (cgroup / "cpu.stat").write_text("usage_usec 100\n")
            (cgroup / "memory.current").write_text("500\n")
            (cgroup / "io.stat").write_text("8:0 rbytes=10 wbytes=20\n")
            config = root / "components.json"
            config.write_text(json.dumps({"asap_promql": {"backend": {"cgroup": str(cgroup), "data_directory": str(cgroup)}}}))
            output = root / "phase.json"
            with patch("sys.argv", ["resources.py", "--components", str(config), "--engine", "asap_promql",
                                    "--output", str(output), "--", sys.executable, "-c", "raise SystemExit(3)"]):
                self.assertEqual(resources.main(), 3)
            report = json.loads(output.read_text())
            self.assertEqual(report["total"]["cpu_usec"], 0)
            self.assertEqual(report["sampled_total_memory_peak_bytes"], 500)
            self.assertGreater(report["total_disk_allocated_after_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
