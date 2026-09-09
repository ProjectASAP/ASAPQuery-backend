"""CPU scope checks for repetition amortization reports."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from query_sweep import trial_summary


class SweepTests(unittest.TestCase):
    def test_setup_charges_fallback_and_planning_but_keeps_query_batch_separate(self):
        # Background/setup work cannot disappear when a warm query avoids fallback.
        values = lambda a, b, c: {"backend": {"cpu_ns": a}, "fallback_service": {"cpu_ns": b}, "exact_service": {"cpu_ns": c}}
        with TemporaryDirectory() as directory:
            folder = Path(directory)
            (folder / "replay").mkdir()
            report = {"all_requests": {}, "by_execution_detail": {}, "estimated_cost": {},
                      "planning_resources": {"cpu_ns": 7}, "process_phases": {
                          "after_ingest_and_drain": values(10, 20, 15), "after_queries": values(13, 20, 19)}}
            batch = {"resources": values(3, 0, 4), "first_pass_resources": values(1, 0, 2),
                     "repeat_resources": values(2, 0, 2), "cpu_tick_ns": 1}
            for name, content in (("comparison", report), ("query-batch-resources", batch)):
                (folder / f"replay/{name}.json").write_text(json.dumps(content))
            sides = trial_summary(folder)["sides"]
            self.assertEqual(sides["backend_plus_fallback"]["setup_and_all_updates_cpu_ns"], 37)
            self.assertEqual(sides["backend_plus_fallback"]["setup_update_query_cpu_ns"], 40)
            self.assertEqual(sides["backend_plus_fallback"]["query_batch_cpu_ns"], 3)
            self.assertTrue(sides["backend_plus_fallback"]["query_cpu_censored"])
            self.assertEqual(sides["prometheus"]["setup_update_query_cpu_ns"], 19)


if __name__ == "__main__":
    unittest.main()
