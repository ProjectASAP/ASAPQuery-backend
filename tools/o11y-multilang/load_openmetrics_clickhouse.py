#!/usr/bin/env python3
"""Stream strict OpenMetrics samples into ClickHouse raw_samples as JSONEachRow."""
import argparse,json,re,urllib.request,urllib.parse
LINE=re.compile(r'^(?P<metric>[A-Za-z_:][A-Za-z0-9_:]*)(?:\{(?P<labels>.*)\})?\s+(?P<value>\S+)\s+(?P<ts>\S+)$')
def labels(raw):
 out={}
 if not raw:return out
 for part in re.findall(r'(?:[^,"\\]|\\.|"[^"]*")+',raw):
  if not part.strip():continue
  k,v=part.split('=',1);out[k]=json.loads(v)
 return out
def main():
 p=argparse.ArgumentParser();p.add_argument('--metrics',required=True);p.add_argument('--url',required=True);p.add_argument('--batch',type=int,default=20000);a=p.parse_args()
 ddl="CREATE TABLE IF NOT EXISTS raw_samples(metric LowCardinality(String),labels Map(String,String),ts_ms UInt64,value Float64) ENGINE=MergeTree ORDER BY (metric,cityHash64(labels),ts_ms)"
 urllib.request.urlopen(urllib.request.Request(a.url+'/?user=bench&password=bench&query='+urllib.parse.quote(ddl),method='POST')).read()
 batch=[]
 def send():
  if not batch:return
  req=urllib.request.Request(a.url+'/?user=bench&password=bench&query='+urllib.parse.quote('INSERT INTO raw_samples FORMAT JSONEachRow'),data=('\n'.join(batch)+'\n').encode(),method='POST');urllib.request.urlopen(req,timeout=60).read();batch.clear()
 with open(a.metrics) as f:
  for line in f:
   if line.startswith('#') or not line.strip():continue
   m=LINE.match(line.strip())
   if not m: raise ValueError(line[:200])
   value=float(m['value']); batch.append(json.dumps({'metric':m['metric'],'labels':labels(m['labels']),'ts_ms':round(float(m['ts'])*1000),'value':value},separators=(',',':')))
   if len(batch)>=a.batch:send()
 send()
if __name__=='__main__':main()
