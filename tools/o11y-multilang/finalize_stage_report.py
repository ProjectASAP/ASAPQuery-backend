#!/usr/bin/env python3
import argparse,json
from pathlib import Path
def main():
 p=argparse.ArgumentParser();p.add_argument('--stage',type=Path,required=True);p.add_argument('--raw',type=Path,required=True);a=p.parse_args();d=json.loads(a.stage.read_text());raw=json.loads(a.raw.read_text())['runs']
 routes={(x['query_id'],x['engine']):x.get('execution') for x in raw if x['status']=='ok'}
 for q in d['queries']:
  m=q['languages']['metricsql'];s=q['languages']['clickhouse_sql']
  if m['parser_canonical']['status']!='pass':mt={'stage':'parser_canonical','status':'typed_fallback','reason':m['parser_canonical'].get('reason')}
  elif m['planner']['status']!='pass':mt={'stage':'planner','status':'typed_fallback','reason':m['planner'].get('reason')}
  elif m.get('compiler',{}).get('status')!='pass':mt={'stage':'compiler','status':'typed_fallback','reason':m.get('compiler',{}).get('reason')}
  else:mt={'stage':'publication','status':'exact_fallback','reason':'the benchmark physical plan does not install corpus MetricsQL sidecars'}
  m['terminal']=mt;m['adapter']={'status':'pass'};m['fallback']={'status':'pass' if routes.get((q['id'],'asap_metricsql'))=='exact_fallback' else 'failed'}
  sf=s['parser_canonical'];s['terminal']={'stage':'parser_canonical','status':'typed_fallback','reason':sf.get('reason')};s['adapter']={'status':'pass'};s['fallback']={'status':'pass' if routes.get((q['id'],'asap_clickhouse'))=='exact_fallback' else 'failed'}
 a.stage.write_text(json.dumps(d,indent=2)+'\n')
if __name__=='__main__':main()
