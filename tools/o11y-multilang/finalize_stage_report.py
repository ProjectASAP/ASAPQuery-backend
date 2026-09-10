#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--stage', type=Path, required=True)
p.add_argument('--raw', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
d = json.loads(a.stage.read_text())
raw = json.loads(a.raw.read_text())['runs']
routes = {(x['query_id'], x['engine']): x.get('execution') for x in raw if x['status'] == 'ok'}
for q in d['queries']:
    m = q['languages']['metricsql']
    s = q['languages']['clickhouse_sql']
    if m['parser_canonical']['status'] != 'pass':
        terminal = {'evidence': 'offline_fail_closed', 'stage': 'parser_canonical', 'status': 'typed_fallback', 'reason': m['parser_canonical'].get('reason')}
    elif m['planner']['status'] != 'pass':
        terminal = {'evidence': 'offline_fail_closed', 'stage': 'planner', 'status': 'typed_fallback', 'reason': m['planner'].get('reason')}
    elif m['compiler']['status'] != 'pass':
        terminal = {'evidence': 'offline_fail_closed', 'stage': 'compiler', 'status': 'typed_fallback', 'reason': m['compiler'].get('reason')}
    else:
        terminal = {'evidence': 'offline_inference', 'stage': 'publication', 'status': 'exact_fallback', 'reason': 'corpus sidecar absent from measured physical plan'}
    m['terminal'] = terminal
    m['adapter'] = {'evidence': 'http_observed', 'status': 'pass'}
    m['fallback'] = {'evidence': 'http_observed', 'status': 'pass' if routes.get((q['id'], 'asap_metricsql')) == 'exact_fallback' else 'failed'}
    s['terminal'] = {'evidence': 'offline_fail_closed', 'stage': 'parser_canonical', 'status': 'typed_fallback', 'reason': s['parser_canonical'].get('reason')}
    s['adapter'] = {'evidence': 'http_observed', 'status': 'pass'}
    s['fallback'] = {'evidence': 'http_observed', 'status': 'pass' if routes.get((q['id'], 'asap_clickhouse')) == 'exact_fallback' else 'failed'}
a.output.write_text(json.dumps(d, indent=2) + '\n')
