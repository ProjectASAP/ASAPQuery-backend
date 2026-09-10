#!/usr/bin/env python3
import argparse,json,urllib.parse,urllib.request
from pathlib import Path
def get(url): return json.load(urllib.request.urlopen(url,timeout=60))
def norm(rows):
 out=[]
 for labels,value in rows:
  out.append((tuple(sorted(labels.items())),round(float(value),12)))
 return sorted(out)
def main():
 p=argparse.ArgumentParser();p.add_argument('--corpus',type=Path,default=Path(__file__).with_name('corpus.json'));p.add_argument('--prometheus',default='http://127.0.0.1:19090');p.add_argument('--clickhouse',default='http://127.0.0.1:18123');p.add_argument('--eval-ms',type=int,default=1788891296000);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 corpus=json.loads(a.corpus.read_text()); evidence=[]
 for row in corpus['queries']:
  if row.get('sql_mapping_status')!='oracle_pending':continue
  pu=a.prometheus+'/api/v1/query?'+urllib.parse.urlencode({'query':row['promql'],'time':a.eval_ms/1000})
  pres=get(pu); prows=[(x['metric'],x['value'][1]) for x in pres['data']['result']]
  sql=row['clickhouse_sql'].format(eval_ms=a.eval_ms)+' FORMAT JSON'
  cu=a.clickhouse+'/?'+urllib.parse.urlencode({'user':'bench','password':'bench','query':sql})
  cres=get(cu); crows=[(x['labels'],x['value']) for x in cres['data']]
  pn,cn=norm(prows),norm(crows); ok=pn==cn
  evidence.append({'id':row['id'],'valid':ok,'prometheus':pn,'clickhouse':cn,'sql':sql})
  row['sql_mapping_status']='oracle_valid' if ok else 'mapping_invalid'
 a.corpus.write_text(json.dumps(corpus,indent=2)+'\n');a.output.write_text(json.dumps({'eval_ms':a.eval_ms,'results':evidence},indent=2)+'\n')
 if not all(x['valid'] for x in evidence):raise SystemExit(1)
if __name__=='__main__':main()
