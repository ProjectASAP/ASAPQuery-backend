#!/usr/bin/env python3
"""Run every corpus query against exact engines and optional ASAP proxies.

Every attempt is retained in the denominator.  The alternating order avoids
always giving one engine the warm cache.  This runner deliberately records raw
responses and typed transport failures; summarization is a separate step.
"""
import argparse, json, time, urllib.parse, urllib.request
from pathlib import Path

def request(url, params):
    started=time.perf_counter_ns()
    try:
        with urllib.request.urlopen(url+'?'+urllib.parse.urlencode(params),timeout=120) as r:
            body=r.read().decode(); headers=dict(r.headers); code=r.status
        return {'status':'ok','http_status':code,'elapsed_ns':time.perf_counter_ns()-started,
                'execution':headers.get('x-asap-execution'),'body':body}
    except Exception as e:
        return {'status':'error','elapsed_ns':time.perf_counter_ns()-started,
                'reason':f'{type(e).__name__}: {e}'}

def main():
    p=argparse.ArgumentParser(); p.add_argument('--corpus',type=Path,default=Path(__file__).with_name('corpus.json'))
    p.add_argument('--eval-ms',type=int,default=1788891296000); p.add_argument('--repetitions',type=int,default=5)
    p.add_argument('--prometheus',default='http://127.0.0.1:19090/api/v1/query')
    p.add_argument('--victoriametrics'); p.add_argument('--clickhouse',default='http://127.0.0.1:18123/')
    p.add_argument('--asap-metricsql'); p.add_argument('--asap-clickhouse'); p.add_argument('--output',type=Path,required=True)
    a=p.parse_args(); rows=json.loads(a.corpus.read_text())['queries']; engines=['prometheus','clickhouse']
    if a.victoriametrics: engines.append('victoriametrics')
    if a.asap_metricsql: engines.append('asap_metricsql')
    if a.asap_clickhouse: engines.append('asap_clickhouse')
    runs=[]
    for rep in range(a.repetitions):
      for q in rows:
       for engine in (engines if rep%2==0 else list(reversed(engines))):
        if engine=='prometheus': url=a.prometheus; params={'query':q['promql'],'time':a.eval_ms/1000}
        elif engine=='victoriametrics': url=a.victoriametrics; params={'query':q['metricsql'],'time':a.eval_ms/1000}
        elif engine=='asap_metricsql': url=a.asap_metricsql; params={'query':q['metricsql'],'time':a.eval_ms/1000}
        else:
          url=a.clickhouse if engine=='clickhouse' else a.asap_clickhouse
          params={'user':'bench','password':'bench','query':q['clickhouse_sql'].format(eval_ms=a.eval_ms)+' FORMAT JSON'}
        result=request(url,params); result.update({'query_id':q['id'],'engine':engine,'repetition':rep})
        runs.append(result)
    a.output.write_text(json.dumps({'schema_version':1,'eval_ms':a.eval_ms,'repetitions':a.repetitions,
      'denominator_per_engine':len(rows)*a.repetitions,'runs':runs},indent=2)+'\n')
    print(json.dumps({e:{'attempted':sum(x['engine']==e for x in runs),'ok':sum(x['engine']==e and x['status']=='ok' for x in runs)} for e in engines},indent=2))

if __name__=='__main__': main()
