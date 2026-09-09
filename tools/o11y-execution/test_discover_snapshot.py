import json
from pathlib import Path
import subprocess
import tempfile
import unittest


class DiscoverSnapshotTests(unittest.TestCase):
    def test_historical_input_does_not_backdate_plan_activation(self):
        script = Path(__file__).with_name("discover_snapshot.py")
        template = Path(__file__).parents[2] / "docs/examples/asapquery-planning-snapshot.json"
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            corpus = root / "queries.json"
            metrics = root / "metrics.prom"
            output = root / "snapshot.json"
            corpus.write_text(json.dumps({
                "upstream_revision": "test",
                "queries": [{"id": "q", "query": "max_over_time(m[1m])", "eval_timestamp_ms": 120_000}],
            }))
            metrics.write_text("m{job=\"test\"} 1 60\nm{job=\"test\"} 2 120\n")
            subprocess.run([
                "python3", str(script), "--corpus", str(corpus), "--metrics", str(metrics),
                "--template", str(template), "--output", str(output), "--repetitions", "1",
            ], check=True)
            snapshot = json.loads(output.read_text())
            environment = snapshot["environment"]
            self.assertEqual(environment["activation_unix_ms"], environment["observed_at_unix_ms"])
            provenance = json.loads(output.with_suffix(".provenance.json").read_text())
            self.assertEqual(provenance["first_timestamp_ms"], 60_000)


if __name__ == "__main__":
    unittest.main()
