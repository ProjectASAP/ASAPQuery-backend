"""Semantic guards for the evaluation generator and differential reader."""
import tempfile
import unittest
from pathlib import Path

from differential import compare_topk, unique_vector
from generate import manifest, write_data


class WorkloadTest(unittest.TestCase):
    def test_duplicate_outputs_are_rejected(self):
        """A malformed engine response must not silently lose duplicate rows."""
        with self.assertRaises(ValueError):
            unique_vector([({"x": "a"}, 1), ({"x": "a"}, 2)])

    def test_topk_allows_only_valid_cutoff_ties(self):
        """Tied winners are valid, but missing a strictly higher value is not."""
        population = unique_vector([({"member": str(i)}, value) for i, value in enumerate([5, 4, 4, 1])])
        valid = unique_vector([({"member": "0"}, 5), ({"member": "2"}, 4)])
        invalid = unique_vector([({"member": "1"}, 4), ({"member": "2"}, 4)])
        self.assertTrue(compare_topk(population, valid, [], 2)["equal"])
        self.assertFalse(compare_topk(population, invalid, [], 2)["equal"])

    def test_groups_and_members_count_distinct_series(self):
        """Scale counts groups separately from the series within each group."""
        data = manifest(10, 4, 60_000, 100)
        self.assertEqual(data["total_series"], 40)
        self.assertEqual(data["total_samples"], 24_040)

    def test_formats_preserve_the_same_millisecond_timestamps(self):
        """Prometheus OpenMetrics uses seconds; VM text imports use milliseconds."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_data(root, 1, 1, 100, 100, 1_700_000_000_123, 97)
            self.assertIn("1700000000123", (root / "samples.prom").read_text())
            self.assertIn("1700000000.123", (root / "samples.openmetrics").read_text())
            self.assertTrue((root / "samples.openmetrics").read_text().endswith("# EOF\n"))
            with self.assertRaises(FileExistsError):
                write_data(root, 1, 1, 100, 100, 1_700_000_000_123, 97)


if __name__ == "__main__":
    unittest.main()
