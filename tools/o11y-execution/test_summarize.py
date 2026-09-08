import copy
import unittest
from summarize import query_cost_comparison, MODEL


class CostScopeTests(unittest.TestCase):
    def setUp(self):
        self.plan = {'cost_comparison': {'model_version': MODEL, 'data_snapshot_id': 'sha256:abc', 'selected_plan_id': 1,
                    'selected_manifest': {'plan_id': 1, 'workload': {'q': {'query': 'sum(up)'}}, 'components': {
                        'query:q:0': {'multiplicity': 2, 'unit': 'query_evaluation'},
                        'result:q': {'multiplicity': 2, 'unit': 'query_evaluation'}}},
                    'component_costs': {'query:q:0': 100, 'result:q': 0, 'source:x': 999}}}
        self.report = {'all_requests': {'backend_plus_fallback_cpu_ns': 200}}
        self.queries = [{'query': 'sum(up)'}, {'query': 'sum(up)'}]
        self.run = {'inputs': {'data': 'abc'}}

    def result(self):
        return query_cost_comparison(self.plan, self.report, self.queries, self.run)

    def test_query_only_already_weighted(self):
        self.assertEqual(self.result()['estimated_over_measured_ratio'], .5)
        self.assertEqual(self.result()['estimated_query_cpu_ns'], 100)

    def test_wrong_demand(self):
        self.queries.pop()
        self.assertFalse(self.result()['available'])

    def test_wrong_units(self):
        self.plan['cost_comparison']['model_version'] = 'analytical'
        self.assertFalse(self.result()['available'])

    def test_wrong_input(self):
        self.run['inputs']['data'] = 'other'
        self.assertFalse(self.result()['available'])

    def test_missing_cpu(self):
        self.report['all_requests']['backend_plus_fallback_cpu_ns'] = None
        self.assertFalse(self.result()['available'])

    def test_missing_component(self):
        del self.plan['cost_comparison']['component_costs']['result:q']
        self.assertFalse(self.result()['available'])

    def test_verified_snapshot_maps_original_text_by_compiler_id(self):
        cost = self.plan['cost_comparison']
        manifest = cost['selected_manifest']
        manifest['workload']['compat-query-0'] = manifest['workload'].pop('q')
        for mapping in [manifest['components'], cost['component_costs']]:
            for old in list(mapping):
                if ':q' in old:
                    mapping[old.replace(':q', ':compat-query-0')] = mapping.pop(old)
        self.queries = [{'query': 'sum (up)'}, {'query': 'sum (up)'}]
        snapshot = {'query_workload': {'repeating_queries': [{'query': 'sum (up)'}]},
                    'workload_cost_evidence': {'model_version': MODEL, 'quotes': [{'manifest': copy.deepcopy(manifest)}]}}
        self.assertTrue(query_cost_comparison(self.plan, self.report, self.queries, self.run, snapshot)['available'])
        snapshot['workload_cost_evidence']['quotes'][0]['manifest']['plan_id'] = 999
        self.assertFalse(query_cost_comparison(self.plan, self.report, self.queries, self.run, snapshot)['available'])
