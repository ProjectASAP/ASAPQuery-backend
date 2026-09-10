import unittest
from summarize import summarize


class SummaryTests(unittest.TestCase):
    def test_failures_and_infeasible_are_separate(self):
        def outcome(method, selected, passed):
            return {"method": method, "selected": {} if selected else None,
                    "held_out_pass": passed,
                    "held_out": {"payload_bytes": 512,
                                 "error": 0.02} if selected else None}
        report = {"schema_version": 1, "debug_assertions": True,
                  "args": {"backend_revision": "test", "events": 100,
                           "cardinality": 10, "zipf": 0, "epsilon": 0.01},
                  "runs": [{"search": {"visited": [1]}, "calibration_wall_seconds": 1,
                            "outcomes": [outcome("autosketch_adapted", False, None),
                                         outcome("asapplanner_erp_selector", True, False),
                                         outcome("grid_oracle", True, False)]}]}
        rows = summarize(report)["outcomes"]
        self.assertEqual(rows[0]["no_feasible_configuration"], 1)
        self.assertEqual(rows[0]["held_out_fail"], 0)
        self.assertIsNone(rows[0]["mean_selected_payload_bytes"])
        self.assertEqual(rows[1]["held_out_fail"], 1)
        self.assertEqual(rows[1]["held_out_pass"], 0)

    def test_empty_report_rejected(self):
        with self.assertRaises(ValueError):
            summarize({"schema_version": 1, "runs": []})


if __name__ == "__main__":
    unittest.main()
