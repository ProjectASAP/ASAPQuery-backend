"""Regression checks for failures that could otherwise turn a mismatch into a pass."""
import copy
import json
from pathlib import Path
import unittest

from run import check_coverage, compare_vector, expected_samples

HERE = Path(__file__).resolve().parent


def vector(*samples):
    return {"status": "success", "data": {"resultType": "vector", "result": [
        {"metric": labels, "value": [1788825840, str(value)]}
        for labels, value in samples
    ]}}


class ComparatorTests(unittest.TestCase):
    def test_nonfinite_values(self):
        for value in ("NaN", "+Inf", "-Inf"):
            compare_vector(vector(({}, value)), [{"labels": {}, "value": value}], 1788825840)
        for actual, wanted in (("NaN", 0), (0, "NaN"), ("-Inf", "+Inf"), (0, "+Inf")):
            with self.subTest(actual=actual, wanted=wanted), self.assertRaises(ValueError):
                compare_vector(vector(({}, actual)), [{"labels": {}, "value": wanted}], 1788825840)

    def test_empty_is_not_zero(self):
        compare_vector(vector(), [], 1788825840)
        with self.assertRaises(ValueError):
            compare_vector(vector(({}, 0)), [], 1788825840)

    def test_checks_every_series_and_full_labels(self):
        expected = [{"labels": {"job": "a"}, "value": 5}, {"labels": {"job": "b"}, "value": 50}]
        bad_results = [
            vector(({"job": "a"}, 5), ({"job": "b"}, 51)),
            vector(({"job": "a"}, 5), ({"job": "a"}, 50)),
            vector(({"job": "a"}, 5), ({"job": "b", "__name__": "wrong"}, 50)),
        ]
        for body in bad_results:
            with self.subTest(body=body), self.assertRaises(ValueError):
                compare_vector(body, expected, 1788825840)

    def test_timestamp_value_uses_epoch_but_sample_time_is_evaluation(self):
        query = {"value_is_timestamp": True, "expected": [{"labels": {"job": "a"}, "value": 120}]}
        expected = expected_samples(query, 1788825600)
        self.assertEqual(expected[0]["value"], 1788825720)
        compare_vector(vector(({"job": "a"}, 1788825720)), expected, 1788825840)
        with self.assertRaises(ValueError):
            compare_vector(vector(({"job": "a"}, 120)), expected, 1788825840)
        wrong_time = vector(({"job": "a"}, 1788825720))
        wrong_time["data"]["result"][0]["value"][0] -= 60
        with self.assertRaises(ValueError):
            compare_vector(wrong_time, expected, 1788825840)

    def test_topk_order_is_only_checked_when_requested(self):
        expected = [{"labels": {"job": "b"}, "value": 50}, {"labels": {"job": "a"}, "value": 5}]
        reverse = vector(({"job": "a"}, 5), ({"job": "b"}, 50))
        compare_vector(reverse, expected, 1788825840)
        with self.assertRaises(ValueError):
            compare_vector(reverse, expected, 1788825840, ordered=True)

    def test_error_or_wrong_type_does_not_pass_as_empty(self):
        for body in ({"status": "error", "error": "unsupported"},
                     {"status": "success", "data": {"resultType": "scalar", "result": [0, "0"]}}):
            with self.subTest(body=body), self.assertRaises(ValueError):
                compare_vector(body, [], 1788825840)


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.cases = json.loads((HERE / "cases.json").read_text())
        self.catalog = json.loads((HERE / "catalog.json").read_text())

    def test_missing_function_cannot_be_reported_as_complete(self):
        self.cases["queries"] = [q for q in self.cases["queries"] if q["function"] != "rate"]
        with self.assertRaisesRegex(ValueError, "Missing rollup coverage"):
            check_coverage(self.cases, self.catalog)

    def test_experimental_flag_and_duplicate_ids_are_checked(self):
        bad = copy.deepcopy(self.cases)
        next(q for q in bad["queries"] if q["function"] == "limitk")["experimental"] = False
        with self.assertRaisesRegex(ValueError, "experimental flag"):
            check_coverage(bad, self.catalog)
        self.cases["queries"].append(self.cases["queries"][0])
        with self.assertRaisesRegex(ValueError, "Duplicate case IDs"):
            check_coverage(self.cases, self.catalog)


if __name__ == "__main__":
    unittest.main()
