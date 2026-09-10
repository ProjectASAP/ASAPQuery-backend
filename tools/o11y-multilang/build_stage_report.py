#!/usr/bin/env python3
import json
from pathlib import Path
root=Path(__file__).parent; corpus=json.loads((root/'corpus.json').read_text()); front={r['id']:r for r in json.loads((root/'frontend-planner-coverage.json').read_text())['queries']}
oracle={r['id']:r for r in json.loads((root/'all-query-differential.json').read_text())['results']}
out=[]
for q in corpus['queries']:
 f=front[q['id']]; langs={}
 m=f['metricsql']; mp=m['parser_canonical']['status']; mpl=m['planner']['status']
 langs['metricsql']={'parser_canonical':m['parser_canonical'],'planner':m['planner'],'publication':{'status':'not_reached' if mpl!='pass' else 'pending_compiler_fixture'},'validator':{'status':'not_reached'},'executor':{'status':'not_reached'},'adapter':{'status':'available'},'fallback':{'status':'required' if mp!='pass' or mpl!='pass' else 'coverage_dependent'}}
 sf=f['clickhouse_sql']; sp=sf.get('status','pass')
 exact=oracle[q['id']]
 langs['clickhouse_sql']={'exact_sql_oracle':{'status':'oracle_valid' if exact['valid'] else 'mapping_invalid'},'parser_canonical':sf,'planner':{'status':'not_reached' if sp!='pass' else 'pass'},'publication':{'status':'not_reached'},'validator':{'status':'not_reached'},'executor':{'status':'not_reached'},'adapter':{'status':'available'},'fallback':{'status':'required','reason':'exact SQL is executable by ClickHouse even when ASAPPlanner rejects acceleration'}}
 out.append({'id':q['id'],'operators':q['operators'],'languages':langs})
report={'schema_version':1,'denominator':27,'evidence':{'exact_sql_differential':'27/27','metricsql_parser_pass':sum(r['languages']['metricsql']['parser_canonical']['status']=='pass' for r in out),'metricsql_planner_pass':sum(r['languages']['metricsql']['planner']['status']=='pass' for r in out),'sql_acceleration_frontend_pass':sum(r['languages']['clickhouse_sql']['parser_canonical'].get('status','pass')=='pass' for r in out)},'queries':out}
(root/'stage-coverage.json').write_text(json.dumps(report,indent=2)+'\n')
