#!/usr/bin/env python3
import argparse,json,re,subprocess
from pathlib import Path
STAGES=('parser','canonical','planner','publication','validator','executor','adapter','fallback')
def main():
 p=argparse.ArgumentParser(); p.add_argument('--corpus',type=Path,default=Path(__file__).with_name('corpus.json')); p.add_argument('--output',type=Path,required=True); p.add_argument('--dry-run-command',action='append',default=[]); a=p.parse_args()
 corpus=json.loads(a.corpus.read_text()); out={'schema_version':1,'denominator':corpus['count'],'queries':[],'commands':[]}
 for cmd in a.dry_run_command:
  r=subprocess.run(cmd,shell=True,text=True,capture_output=True); out['commands'].append({'command':cmd,'exit_code':r.returncode,'stdout':r.stdout,'stderr':r.stderr})
 for row in corpus['queries']:
  unsupported=[]
  if any(x in row['operators'] for x in ('histogram_quantile','subquery','scalar')): unsupported.append('runtime_operator')
  # Backend explicitly rejects nested rate/increase MetricsQL until equivalence is proven.
  if ('rate' in row['operators'] or 'increase' in row['operators']) and 'sum' in row['operators']: unsupported.append('metricsql_shape_guard')
  stages={s:{'status':'not_run','reason':'dry-run evidence absent'} for s in STAGES}
  stages['fallback']={'status':'expected' if unsupported else 'available','reason':','.join(unsupported) or 'no statically known blocker'}
  out['queries'].append({'id':row['id'],'operators':row['operators'],'stages':stages,'unsupported':unsupported})
 out['counts']={'total':len(out['queries']),'statically_accelerable':sum(not x['unsupported'] for x in out['queries']),'typed_fallback':sum(bool(x['unsupported']) for x in out['queries'])}
 a.output.write_text(json.dumps(out,indent=2)+'\n')
if __name__=='__main__': main()
