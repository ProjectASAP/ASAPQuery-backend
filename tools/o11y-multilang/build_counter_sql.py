#!/usr/bin/env python3
import json
from pathlib import Path
E='{eval_ms}'
def series(metric,window,offset=0,rate=False):
 end=f'({E}-{offset})'; start=f'({end}-{window})'
 divisor=f'/({window}/1000)' if rate else ''
 return f'''SELECT labels, corrected*(sampled+least(start_extra,if(corrected>0,sampled*(first_value/corrected),start_extra))+end_extra)/sampled{divisor} AS value FROM (SELECT labels,samples,length(samples) n,samples[1].1 first_ts,samples[n].1 last_ts,samples[1].2 first_value,samples[n].2 last_value,(last_ts-first_ts)/1000 sampled,(last_value-first_value)+arraySum(i -> if(samples[i].2<samples[i-1].2,samples[i-1].2,0.),range(2,n+1)) corrected,if((first_ts-{start})/1000<sampled/(n-1)*1.1,(first_ts-{start})/1000,sampled/(n-1)/2) start_extra,if(({end}-last_ts)/1000<sampled/(n-1)*1.1,({end}-last_ts)/1000,sampled/(n-1)/2) end_extra FROM (SELECT labels,arraySort(x -> x.1,groupArray((ts_ms,value))) samples FROM raw_samples WHERE metric='{metric}' AND ts_ms>={start} AND ts_ms<={end} GROUP BY labels) WHERE n>=2)'''
def agg(src,by=None):
 if by:return f"SELECT map('{by}',job) labels,sum(value) value FROM (SELECT labels['{by}'] job,value FROM ({src})) GROUP BY job"
 return f"SELECT map() labels,sum(value) value FROM ({src})"
def ratio(a,b):return f"SELECT a.labels,a.value/b.value value FROM ({a}) a INNER JOIN ({b}) b ON a.labels=b.labels"
def top(src,k):return f"SELECT * FROM ({src}) ORDER BY value DESC,labels LIMIT {k}"
def filt(src):return f"SELECT * FROM ({src}) WHERE value>0"
inc=lambda m,w,o=0:series(m,w,o,False); rate=lambda m,w,o=0:series(m,w,o,True)
job=lambda x:agg(x,'job'); glob=lambda x:agg(x)
ratio6=ratio(job(inc('backend_http_5xx_total',21600000)),job(inc('backend_http_requests_total',21600000)))
M={
'q01':f'SELECT * FROM ({ratio6}) ORDER BY value DESC,labels','q02':top(ratio6,1),
'q03':ratio(glob(inc('payment_service_http_5xx_total',3600000)),glob(inc('payment_service_http_requests_total',3600000))),
'q04':ratio(glob(inc('payment_service_http_5xx_total',3600000,21600000)),glob(inc('payment_service_http_requests_total',3600000,21600000))),
'q08':glob(rate('backend_process_cpu_seconds_total',3600000)),
'q11':top(job(rate('backend_process_cpu_seconds_total',3600000)),2),
'q14':glob(rate('backend_http_requests_total',300000)),'q15':job(rate('backend_http_requests_total',300000)),
'q16':ratio(glob(inc('backend_http_5xx_total',3600000)),glob(inc('backend_http_requests_total',3600000))),
'q17':top(ratio6,1),'q18':filt(job(inc('backend_http_5xx_total',86400000))),
'q19':glob(rate('order_service_http_requests_total',300000)),'q20':glob(rate('order_service_http_requests_total',300000,3600000)),
'q22':glob(rate('order_service_http_requests_total',300000)),
'q25':top(f"SELECT a.labels,a.value/b.value value FROM ({job(inc('backend_http_5xx_total',86400000))}) a CROSS JOIN ({glob(inc('backend_http_5xx_total',86400000))}) b",1),
'q26':top(job(rate('backend_process_cpu_seconds_total',21600000)),1),
}
p=Path(__file__).with_name('corpus.json');x=json.loads(p.read_text())
for r in x['queries']:
 if r['id'] in M:r['clickhouse_sql']=M[r['id']];r['sql_mapping_status']='oracle_pending'
p.write_text(json.dumps(x,indent=2)+'\n')
