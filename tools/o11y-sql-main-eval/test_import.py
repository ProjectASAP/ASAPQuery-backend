import unittest
from import_openmetrics import parse_series


class LabelTransportTests(unittest.TestCase):
    def test_escaped_labels_remain_distinct_and_ordered(self):
        metric, labels = parse_series(r'm{job="a,b",member="x\"y",path="a\\b",line="a\nb"}')
        self.assertEqual(metric, 'm')
        self.assertEqual(list(labels), ['job', 'member', 'path', 'line'])
        self.assertEqual(labels, {'job': 'a,b', 'member': 'x"y', 'path': 'a\\b', 'line': 'a\nb'})

    def test_empty_label_set(self):
        self.assertEqual(parse_series('m{}'), ('m', {}))
        self.assertEqual(parse_series('m'), ('m', {}))

    def test_invalid_or_duplicate_labels_are_rejected(self):
        for token in ['m{x="a",x="b"}', 'm{x="a",garbage}', 'm{x="a"', 'm{garbage}']:
            with self.assertRaises(ValueError):
                parse_series(token)


if __name__ == '__main__':
    unittest.main()
