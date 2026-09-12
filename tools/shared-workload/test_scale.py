"""Scale planning never allocates the planned billion-sample datasets in tests."""
import argparse
import json
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import accuracy_suite
import dataset
from scale_plan import main as plan_main, plan, query_manifest
from test_accuracy import endpoints


class ScaleTests(unittest.TestCase):
    def test_benefit_is_large_and_has_complete_repeated_windows(self):
        """The default benefit cell has billions of samples, not smoke-size input."""
        scale = plan()
        self.assertEqual(scale["total_samples"], 1728032000)
        self.assertEqual(scale["total_series"], 32000)
        self.assertEqual(scale["unfiltered_temporal_samples_per_query"], 576000000)
        self.assertEqual(scale["temporal_occurrences_per_query"], 31)
        self.assertEqual(scale["spatial_occurrences_per_query"], 1801)
        self.assertEqual(scale["evaluation_start_ms"] - scale["start_ms"], 3600000)
        self.assertEqual(scale["evaluation_end_ms"] - scale["evaluation_start_ms"], 1800000)
        self.assertIsNone(scale["estimated_store_bytes"])
        self.assertTrue(all(q["interval_ms"] == 1000 or q["window"] == "1h"
                            for q in query_manifest(scale)["queries"]))

    def test_members_and_cardinality_are_distinct_scale_axes(self):
        """Increasing members increases scan work without changing sum output groups."""
        small, large = plan(members=4), plan(members=64)
        self.assertEqual(small["spatial_sum_output_groups"], large["spatial_sum_output_groups"])
        self.assertEqual(large["unfiltered_temporal_samples_per_query"], 16 * small["unfiltered_temporal_samples_per_query"])
        self.assertEqual(plan("cardinality")["total_series"], 8000000)
        self.assertEqual(plan(measured_bytes_per_sample=20)["estimated_store_bytes"], 1728032000 * 20)

    def test_scale_generation_requires_explicit_budget(self):
        """A large plan fails before creating output unless its sample budget is allowed."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "scale.json").write_text(json.dumps(plan()))
            with patch("sys.argv", ["dataset.py", "--dataset", "synthetic", "--scale-plan", str(root / "scale.json"),
                                    "--output", str(root / "data")]), patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
                dataset.main()
            self.assertFalse((root / "data").exists())

    def test_plan_command_emits_matrix_without_allocating_samples(self):
        """All requested cardinality/window cells are planned without generating data."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "plan"
            with patch("sys.argv", ["scale_plan.py", "--output", str(root)]), patch("builtins.print"):
                plan_main()
            self.assertEqual({p.name for p in root.iterdir()}, {"scale.json", "queries.json", "matrix.json"})
            matrix = json.loads((root / "matrix.json").read_text())
            self.assertEqual(len(matrix), 30)
            self.assertEqual({c["groups"] for c in matrix}, {10, 100, 1000, 10000, 100000, 1000000})

    def test_runner_uses_planned_timestamps(self):
        """A matching scale receipt supplies the full interval to the five HTTP adapters."""
        with tempfile.TemporaryDirectory() as tmp, endpoints() as (url, calls):
            root = Path(tmp)
            scale = plan("smoke", groups=1, members=1)
            (root / "queries.json").write_text(json.dumps(query_manifest(scale)))
            meta = {"provenance": {"dataset": "synthetic", "scale_plan": scale},
                    "samples": scale["total_samples"], "series": scale["total_series"],
                    "start_ms": scale["start_ms"], "end_ms": scale["evaluation_end_ms"], "sha256": {}}
            (root / "receipt.json").write_text(json.dumps({"data": meta}))
            args = argparse.Namespace(manifest=root / "queries.json", loaded_data=root / "receipt.json", output=root / "results.jsonl",
                                      start_ms=None, end_ms=None, query_name=["temporal_sum"], components=None,
                                      prometheus=url + "/prom", victoriametrics=url + "/vm", clickhouse=url + "/ch",
                                      asap_prometheus=url + "/asap", asap_clickhouse=url + "/asap-sql",
                                      rtol=1e-9, atol=1e-12, require_warm=True)
            self.assertEqual(accuracy_suite.run(args), 0)
            rows = [json.loads(line) for line in args.output.read_text().splitlines()]
            self.assertEqual(len(calls), 20)
            self.assertEqual({r["evaluation_ms"] for r in rows}, {scale["evaluation_start_ms"], scale["evaluation_end_ms"]})
            self.assertTrue(all(r["scale"] == scale for r in rows))

    def test_smoke_plan_drives_actual_generation(self):
        """Plan dimensions override CLI defaults and survive in the data receipt."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scale = plan("smoke", groups=1, members=1)
            (root / "scale.json").write_text(json.dumps(scale))
            with patch("sys.argv", ["dataset.py", "--dataset", "synthetic", "--scale-plan", str(root / "scale.json"),
                                    "--output", str(root / "data")]), patch("builtins.print"):
                dataset.main()
            meta = json.loads((root / "data/data-manifest.json").read_text())
            self.assertEqual(meta["samples"], scale["total_samples"])
            self.assertEqual(meta["series"], scale["total_series"])
            self.assertEqual(meta["provenance"]["scale_plan"], scale)

    def test_runner_rejects_undersized_data_and_shortened_repetition(self):
        """A run cannot advertise a large scale while loading less or replaying less."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scale = plan()
            manifest = root / "queries.json"
            manifest.write_text(json.dumps(query_manifest(scale)))
            receipt = root / "receipt.json"
            meta = {"provenance": {"scale_plan": scale}, "samples": 100, "series": 2}
            receipt.write_text(json.dumps({"data": meta}))
            args = argparse.Namespace(manifest=manifest, loaded_data=receipt, start_ms=None, end_ms=None)
            with self.assertRaisesRegex(ValueError, "requested scale"):
                accuracy_suite.run(args)
            meta.update(samples=scale["total_samples"], series=scale["total_series"])
            receipt.write_text(json.dumps({"data": meta}))
            args.end_ms = scale["evaluation_start_ms"]
            with self.assertRaisesRegex(ValueError, "repetition interval"):
                accuracy_suite.run(args)


if __name__ == "__main__":
    unittest.main()
