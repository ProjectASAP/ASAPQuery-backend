#!/usr/bin/env python3
import argparse,json,statistics
from pathlib import Path
def pct(xs,p):
 xs=sorted(xs); return xs[min(len(xs)-1,int((len(xs)-1)*p))]/1e6
def norm_prom(body):
 d=json.loads(body); return sorted((tuple(sorted(x['metric'].items())),round(float(x['value'][1]),10)) for x in d['data']['result'])
def main():
 p=argparse.ArgumentParser();p.add_argument('raw',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args();d=json.loads(a.raw.read_text());runs=d['runs'];out={}
 for e in sorted({x['engine'] for x in runs}):
  rs=[x for x in runs if x['engine']==e];ns=[x['elapsed_ns'] for x in rs if x['status']=='ok']
  out[e]={'attempted':len(rs),'ok':len(ns),'p50_ms':pct(ns,.5),'p95_ms':pct(ns,.95),'mean_ms':statistics.mean(ns)/1e6}
 # Prometheus and VM use the same response shape; compare every paired trial.
 by={(x['query_id'],x['repetition'],x['engine']):x for x in runs}
 if 'victoriametrics' in out:
  pairs=[]
  for q in {x['query_id'] for x in runs}:
   for r in range(d['repetitions']):
    x,y=by[q,r,'prometheus'],by[q,r,'victoriametrics']
    pairs.append(x['status']=='ok' and y['status']=='ok' and norm_prom(x['body'])==norm_prom(y['body']))
  out['victoriametrics']['prometheus_differential']={'valid':sum(pairs),'total':len(pairs)}
 a.output.write_text(json.dumps({'source':str(a.raw),'engines':out},indent=2)+'\n');print(json.dumps(out,indent=2))
if __name__=='__main__':main()
