#!/usr/bin/env python3
import argparse,json
from pathlib import Path
def prom(body):
 d=json.loads(body);data=d.get('data',{});rows=[]
 for x in data.get('result',[]):
  v=x.get('value');rows.append({'labels':dict(sorted(x.get('metric',{}).items())),'timestamp':float(v[0]),'value':round(float(v[1]),10)})
 return {'resultType':data.get('resultType'),'rows':sorted(rows,key=lambda x:json.dumps(x,sort_keys=True)),'warnings':d.get('warnings',[])}
def ch(body):
 d=json.loads(body);rows=[{'labels':dict(sorted(x.get('labels',{}).items())),'timestamp':x.get('timestamp'),'value':round(float(x['value']),10)} for x in d.get('data',[])]
 return {'resultType':'table','rows':sorted(rows,key=lambda x:json.dumps(x,sort_keys=True)),'warnings':d.get('warnings',[])}
def main():
 p=argparse.ArgumentParser();p.add_argument('raw',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args();d=json.loads(a.raw.read_text());by={(x['query_id'],x['repetition'],x['engine']):x for x in d['runs']};out=[]
 pairs=[('clickhouse','asap_clickhouse',ch),('victoriametrics','asap_metricsql',prom),('prometheus','victoriametrics',prom)]
 for q in sorted({x['query_id'] for x in d['runs']}):
  for rep in range(d['repetitions']):
   for left,right,parse in pairs:
    l,r=by[q,rep,left],by[q,rep,right];record={'query_id':q,'repetition':rep,'left':left,'right':right}
    try:
     ls,rs=parse(l['body']),parse(r['body']);fields={k:ls[k]==rs[k] for k in ('resultType','rows','warnings')};record.update({'status':'match' if all(fields.values()) else 'mismatch','fields':fields,'left_structured':ls,'right_structured':rs})
    except Exception as e:record.update({'status':'error','reason':f'{type(e).__name__}: {e}'})
    out.append(record)
   # Cross-protocol SQL oracle compares label/value rows; instant Prometheus
   # timestamps and ClickHouse's table result type are intentionally reported
   # but are not semantically equivalent protocol fields.
   l,r=by[q,rep,'prometheus'],by[q,rep,'clickhouse'];ls,rs=prom(l['body']),ch(r['body'])
   lp=[{'labels':x['labels'],'value':x['value']} for x in ls['rows']];rp=[{'labels':x['labels'],'value':x['value']} for x in rs['rows']]
   out.append({'query_id':q,'repetition':rep,'left':'prometheus','right':'clickhouse','status':'match' if lp==rp else 'mismatch','fields':{'labels_and_values':lp==rp,'timestamp':'not_comparable','resultType':'not_comparable','warnings':ls['warnings']==rs['warnings']},'left_structured':ls,'right_structured':rs})
 a.output.write_text(json.dumps({'schema_version':1,'comparisons':out},indent=2)+'\n');print(json.dumps({'match':sum(x['status']=='match' for x in out),'mismatch':sum(x['status']=='mismatch' for x in out),'error':sum(x['status']=='error' for x in out)},indent=2))
if __name__=='__main__':main()
