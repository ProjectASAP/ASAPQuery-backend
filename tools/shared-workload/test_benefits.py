"""Endpoint failures and fallback must not manufacture a successful benchmark."""
import unittest
from unittest.mock import patch
import benefits


def response(value=1, route='warm'):
    return {'status': 200, 'headers': {'x-asap-execution': route}, 'elapsed_ns': 10,
            'body': {'status': 'success', 'data': {'resultType': 'vector', 'result':
                     [{'metric': {}, 'value': [1, str(value)]}]}}}


class ComparisonTests(unittest.TestCase):
    def test_native_timeout_still_runs_all_six_endpoints(self):
        calls = []
        def request(engine, endpoint, query, evaluation_ms, timeout):
            calls.append((engine, endpoint))
            if engine == 'prometheus' and endpoint == 'native':
                raise TimeoutError('native timed out')
            return response()
        with patch.object(benefits, 'query_endpoint', side_effect=request):
            report = benefits.compare_round({e: {'native': 'native', 'asap': 'asap'} for e in benefits.ENGINES},
                                            {'promql': 'm', 'metricsql': 'm', 'clickhouse_sql': 'SELECT 1'}, 1000, 1)
        self.assertEqual(len(calls), 6)
        self.assertFalse(report['prometheus']['passed'])
        self.assertEqual(report['prometheus']['asap']['status'], 200)

    def test_vm_mismatch_fails_even_when_other_engines_match(self):
        def request(engine, endpoint, *_):
            return response(99 if engine == 'victoriametrics' and endpoint == 'asap' else 1)
        with patch.object(benefits, 'query_endpoint', side_effect=request):
            report = benefits.compare_round({e: {'native': 'native', 'asap': 'asap'} for e in benefits.ENGINES},
                                            {}, 1000, 1)
        self.assertFalse(report['victoriametrics']['passed'])

    def test_fallback_and_missing_provenance_are_not_acceleration(self):
        for route in ['exact_fallback', 'unknown', 'hybrid']:
            result = benefits.assess('prometheus', response(), response(route=route))
            self.assertTrue(result['passed'])
            self.assertFalse(result['accelerated'])

    def test_numeric_tolerance_is_explicit(self):
        self.assertFalse(benefits.assess('prometheus', response(100), response(101))['passed'])
        accepted = benefits.assess('prometheus', response(100), response(101), relative=.02)
        self.assertTrue(accepted['passed'])
        self.assertEqual(accepted['comparison']['max_absolute_error'], 1)

    def test_sql_metadata_and_values_are_checked(self):
        native = dict(response(), body={'meta': [{'name': 'v', 'type': 'Float64'}], 'data': [{'v': 1}]})
        asap = dict(native, body={'meta': [{'name': 'v', 'type': 'UInt64'}], 'data': [{'v': 1}]})
        self.assertFalse(benefits.assess('clickhouse', native, asap)['passed'])



class CostTests(unittest.TestCase):
    def test_maintenance_can_eliminate_query_savings(self):
        from costs import break_even
        self.assertIsNone(break_even(100, 10, 5, 6)['refreshes'])
        self.assertEqual(break_even(100, 10, 5, 1)['refreshes'], 25)

    def test_missing_cpu_is_not_zero(self):
        from costs import complete_sum, break_even
        self.assertIsNone(complete_sum([1, None]))
        self.assertIsNone(break_even(100, None, 5, 0)['refreshes'])

class WorkflowTests(unittest.TestCase):
    def test_backfill_splits_range_at_installed_pane_width(self):
        import experiment
        install = {'summary_catalog': {'materializations': {
            '7': {'window_layout': {'pane_secs': 4}, 'pane_origin_ms': 1000}}}}
        with patch.object(experiment, 'post') as post, patch.object(benefits, 'http', return_value={
                'status': 200, 'body': {'jobs': [{'status': 'complete'}]}}):
            experiment.backfill('http://backend', install, 'db', 'samples', 1000, 9000)
        self.assertEqual(post.call_args.args[1]['windows_total'], 2)

    def test_backfill_rejects_partial_pane_without_enqueuing(self):
        import experiment
        install = {'summary_catalog': {'materializations': {
            '7': {'window_layout': {'pane_secs': 4}, 'pane_origin_ms': 1000}}}}
        with patch.object(experiment, 'post') as post:
            with self.assertRaises(ValueError):
                experiment.backfill('http://backend', install, 'db', 'samples', 1000, 8000)
            post.assert_not_called()

    def test_native_and_fallback_cannot_be_the_same_instance(self):
        import experiment
        config = {'table': 'samples', 'batches': [{}], 'engines': {
            e: {arm: {'url': 'http://same'} for arm in ('native', 'fallback')} for e in benefits.ENGINES}}
        with self.assertRaises(ValueError):
            experiment.validate(config)

class RefreshAccountingTests(unittest.TestCase):
    def test_break_even_counts_dashboard_refreshes_not_individual_queries(self):
        from costs import summarize_costs
        phases = []
        for engine in benefits.ENGINES:
            for arm, phase, cpu in [('native', 'build', 10), ('asap', 'build', 110), ('asap', 'maintenance', 5)]:
                phases.append({'engine': engine, 'arm': arm, 'phase': phase, 'cpu_ns': cpu, 'wall_ns': cpu, 'complete': True})
        records = [{'repeat': 0, 'engines': {engine: {
            'passed': True, 'native': {'cpu_ns': 10, 'elapsed_ns': 10},
            'asap': {'cpu_ns': 5, 'elapsed_ns': 5}} for engine in benefits.ENGINES}} for _ in range(2)]
        result = summarize_costs(phases, records)
        self.assertEqual(result['prometheus']['observed_refreshes'], 1)
        self.assertEqual(result['prometheus']['cpu_break_even']['refreshes'], 20)
        records[0]['engines']['victoriametrics']['passed'] = False
        failed = summarize_costs(phases, records)['victoriametrics']
        self.assertIsNone(failed['cpu_break_even']['refreshes'])
        self.assertIsNone(failed['native_over_asap_phase_cpu_ratio'])


if __name__ == '__main__':
    unittest.main()
