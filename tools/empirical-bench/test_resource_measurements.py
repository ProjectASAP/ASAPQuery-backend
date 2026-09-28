"""Resource normalization contracts; values are synthetic, never calibration data."""
import math
import unittest
from run import cpu_batch, cpu_per_op


class ResourceMeasurementTests(unittest.TestCase):
    def test_batch_rejects_unpaired_cpu_samples(self):
        for user, system in [([1, 2], [3]), ([1], [2, 3]), ([], [1])]:
            with self.subTest(user=user, system=system):
                with self.assertRaises(ValueError):
                    cpu_batch({"query_cpu_time_ms": {
                        "user_ms": {"samples": user}, "sys_ms": {"samples": system}}}, "query")

    def test_batch_and_per_op_reject_invalid_raw_measurements(self):
        for invalid in [-1, math.inf, math.nan]:
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    cpu_batch({"query_cpu_time_ms": {
                        "user_ms": {"samples": [invalid]}, "sys_ms": {"samples": [10]}}}, "query")
        for field in ["rate", "elapsed", "user", "system"]:
            for invalid in [-1, math.inf, math.nan]:
                values = dict(rate=100, elapsed=1000, user=1, system=1)
                values[field] = invalid
                row = {"query_throughput_items_per_sec": {"samples": [values["rate"]]},
                       "query_wall_time_ms": {"samples": [values["elapsed"]]},
                       "query_cpu_time_ms": {"user_ms": {"samples": [values["user"]]},
                                             "sys_ms": {"samples": [values["system"]]}}}
                with self.subTest(field=field, invalid=invalid):
                    with self.assertRaises(ValueError):
                        cpu_per_op(row, "query")

    def test_cpu_conversion_preserves_repetitions_and_units(self):
        row = {"query_cpu_time_ms": {"user_ms": {"samples": [2, 4]},
                                     "sys_ms": {"samples": [1, 2]}}}
        batch = cpu_batch(row, "query")
        self.assertEqual(batch["value"], 4_500_000)
        self.assertEqual(batch["samples"], 2)
        row.update(query_throughput_items_per_sec={"samples": [300, 300]},
                   query_wall_time_ms={"samples": [1000, 2000]})
        per_op = cpu_per_op(row, "query")
        self.assertEqual(per_op["value"], 10_000)
        self.assertEqual(per_op["stddev"], 0)
        self.assertEqual(per_op["samples"], 2)

    def test_missing_resource_dimension_stays_unknown(self):
        self.assertIsNone(cpu_batch({}, "query"))
        self.assertIsNone(cpu_per_op({}, "query"))


if __name__ == "__main__":
    unittest.main()
