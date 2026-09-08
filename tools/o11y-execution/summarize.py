#!/usr/bin/env python3
"""Derive a compact report without modifying immutable replay evidence."""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path

MODEL = 'measured-inclusive-cpu-ns-v1'
STALE = 'No common conversion from provider cost units to measured resource units'


def query_cost_comparison(planning, comparison, queries, run, snapshot=None):
    cost = planning['cost_comparison']
    unavailable = {'available': False, 'estimated_over_measured_ratio': None}
    def reject(reason):
        return dict(unavailable, reason=reason)
    if cost.get('model_version') != MODEL:
        return reject('Provider model has no verified CPU-nanosecond interpretation')
    if cost.get('data_snapshot_id', '').removeprefix('sha256:') not in run.get('inputs', {}).values():
        return reject('Measured input hash does not match the cost data snapshot')
    manifest = cost['selected_manifest']
    if manifest['plan_id'] != cost['selected_plan_id']:
        return reject('Selected manifest does not match selected plan')
    original_by_id = {}
    if snapshot is not None:
        evidence = snapshot.get('workload_cost_evidence', {})
        if evidence.get('model_version') != MODEL or not any(q.get('manifest') == manifest for q in evidence.get('quotes', [])):
            return reject('Snapshot does not contain the selected calibration manifest')
        original_by_id = {f'compat-query-{i}': q['query'] for i, q in enumerate(snapshot['query_workload']['repeating_queries'])}
    actual = Counter(q['query'] for q in queries)
    expected = {}
    for qid, query in manifest['workload'].items():
        keys = [k for k in manifest['components'] if k.startswith(f'query:{qid}:') or k == f'result:{qid}']
        counts = {manifest['components'][k]['multiplicity'] for k in keys}
        if len(counts) != 1 or any(manifest['components'][k]['unit'] != 'query_evaluation' for k in keys):
            return reject('Missing or inconsistent query evaluation multiplicities')
        count = counts.pop()
        text = original_by_id.get(qid, query['query'])
        if text in expected:
            return reject('Ambiguous duplicate query in manifest')
        expected[text] = count
    if actual != expected:
        return reject('Measured query multiset differs from priced evaluation demand')
    keys = [k for k in manifest['components'] if k.startswith(('query:', 'result:'))]
    values = [cost['component_costs'].get(k) for k in keys]
    if any(not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for v in values):
        return reject('Missing or invalid query cost')
    measured = comparison['all_requests'].get('backend_plus_fallback_cpu_ns')
    if not isinstance(measured, (int, float)) or not math.isfinite(measured) or measured <= 0:
        return reject('Query CPU is unavailable or below measurement resolution')
    estimated = sum(values)
    return {'available': True, 'model_version': MODEL, 'estimated_query_cpu_ns': estimated,
            'measured_query_cpu_ns': measured, 'estimated_over_measured_ratio': estimated / measured,
            'scope': 'Same input hash and query evaluation multiset; backend plus fallback query CPU only. '
                     'Component costs already include multiplicity. Setup, residency and retirement are excluded. '
                     'Calibration and replay cache/background timing can differ; /proc CPU is tick-quantized.'}


def summarize(directory):
    def read(name):
        return json.loads((directory / name).read_text())
    comparison, planning, queries, run = [read(n) for n in ['comparison.json', 'planning.json', 'queries.json', 'run.json']]
    snapshot_path = Path(run.get('configuration', {}).get('snapshot', '/missing'))
    snapshot = None
    if snapshot_path.is_file() and hashlib.sha256(snapshot_path.read_bytes()).hexdigest() == run['inputs'].get(str(snapshot_path.resolve())):
        snapshot = json.loads(snapshot_path.read_text())
    aligned = query_cost_comparison(planning, comparison, queries, run, snapshot)
    aggregate = {k: v for k, v in comparison['all_requests'].items() if k != 'comparisons'}
    limitations = [x for x in comparison['limitations'] if not (aligned['available'] and x == STALE)]
    limitations += ['Full lifecycle estimated/measured ratio is unavailable: isolated residency, retirement and service startup scopes are not aligned.',
                    'First pass is not a guaranteed cold-cache trial. Sequential service rate is not concurrent throughput.',
                    'Summed process lifetime memory peaks are conservative and need not occur simultaneously.']
    return {'schema_version': 1, 'inputs': run['inputs'], 'samples': run['samples'],
            'query_occurrences': run['query_occurrences'], 'aggregate': aggregate,
            'by_phase': {k: {a: b for a, b in v.items() if a != 'comparisons'} for k, v in comparison['by_phase'].items()},
            'selected_plan_id': planning['cost_comparison']['selected_plan_id'],
            'candidate_costs': planning['cost_comparison']['alternatives'],
            'query_cost_comparison': aligned, 'phase_resources': comparison['phase_resources'],
            'storage': comparison['storage'], 'limitations': limitations, 'acceptance_complete': False,
            'evidence_sha256': {n: hashlib.sha256((directory / n).read_bytes()).hexdigest() for n in
                                ['comparison.json', 'planning.json', 'queries.json', 'run.json']}}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('trial', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(summarize(args.trial), indent=2) + '\n')
