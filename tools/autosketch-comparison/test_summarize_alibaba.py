"""Final reports must reject incomplete, inconsistent or smoke-sized evidence."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from summarize_alibaba import summarize, validate_run


class ValidationTests(unittest.TestCase):
    def fixture(self):
        plan = {'planning_seconds': 0.1}
        samples = [{'end_minute': end, 'panel_id': panel,
                    'panel': {'window': [1, 10, 60][panel // 2]},
                    'merge_group': panel // 2, 'readout_seconds': 1., 'readout_cpu_seconds': .9,
                    'normalized_loss': 0., 'window_keys': 3, 'window_events': 30}
                   for end in range(61, 64) for panel in range(6)]
        dashboards = [{'end_minute': end, 'timed_query_operations_seconds': 9.,
                       'merge_groups': [{'id': i, 'panels': [i*2, i*2+1], 'seconds': 1., 'cpu_seconds': .9} for i in range(3)]}
                      for end in range(61, 64)]
        return {'status': 'complete', 'workload': 'Service', 'deployment': plan,
                'events': 100, 'samples': samples, 'dashboard_samples': dashboards,
                'timing': {'merge_seconds': 9., 'readout_seconds': 18.}, 'violations': 0}, plan

    def test_valid_primitive_measurements_are_accepted(self):
        row, plan = self.fixture()
        validate_run(row, plan, 'service', 100, 20, 21)

    def test_missing_samples_wrong_counts_and_shared_double_charges_fail(self):
        row, plan = self.fixture()
        for mutate in [lambda r: r['samples'].pop(), lambda r: r.update(events=99),
                       lambda r: r['dashboard_samples'][0].update(timed_query_operations_seconds=12.),
                       lambda r: r.update(violations=1), lambda r: r['timing'].update(merge_seconds=float('nan'))]:
            changed = copy.deepcopy(row)
            mutate(changed)
            with self.assertRaises(ValueError):
                validate_run(changed, plan, 'service', 100, 20, 21)

    def test_smoke_geometry_cannot_generate_final_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'manifest.json').write_text(json.dumps({'arguments': {
                'calibration_files': 20, 'total_files': 21, 'trials': 3,
                'profile_trials': 3, 'calibration_events': 100}}))
            with self.assertRaisesRegex(ValueError, 'full real-data'):
                summarize(root)
            self.assertFalse((root/'report.md').exists())


if __name__ == '__main__':
    unittest.main()
