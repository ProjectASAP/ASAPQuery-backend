#!/usr/bin/env python3
import json
from pathlib import Path
E='{eval_ms}'
M={
'q10':f"""SELECT map() labels,max(value) value FROM (SELECT ts_ms,sum(value) value FROM raw_samples WHERE metric='backend_process_resident_memory_bytes' AND ts_ms>={E}-21600000 AND ts_ms<={E} AND modulo({E}-ts_ms,60000)=0 GROUP BY ts_ms)""",
'q27':f"""SELECT map('job',job) labels,avg(value) value FROM (SELECT labels['job'] job,ts_ms,sum(value) value FROM raw_samples WHERE metric='backend_process_resident_memory_bytes' AND ts_ms+4000>={E}-21600000 AND ts_ms+4000<={E} AND modulo(ts_ms+4000,60000)=0 GROUP BY job,ts_ms) GROUP BY job ORDER BY value DESC,job LIMIT 3""",
}

def grid_rate(metric):
 start=f"toUInt64(intDiv({E}-21600000+59999,60000))"; end=f"toUInt64(intDiv({E},60000)+1)"
 return f"""SELECT grid_ms,labels,corrected*(sampled+least(start_extra,if(corrected>0,sampled*(first_value/corrected),start_extra))+end_extra)/sampled/300 value FROM (SELECT grid_ms,labels,samples,length(samples) n,samples[1].1 first_ts,samples[n].1 last_ts,samples[1].2 first_value,samples[n].2 last_value,(last_ts-first_ts)/1000 sampled,(last_value-first_value)+arraySum(i -> if(samples[i].2<samples[i-1].2,samples[i-1].2,0.),range(2,n+1)) corrected,if((first_ts-(grid_ms-300000))/1000<sampled/(n-1)*1.1,(first_ts-(grid_ms-300000))/1000,sampled/(n-1)/2) start_extra,if((grid_ms-last_ts)/1000<sampled/(n-1)*1.1,(grid_ms-last_ts)/1000,sampled/(n-1)/2) end_extra FROM (SELECT grid_ms,labels,arraySort(x->x.1,groupArray((ts_ms,value))) samples FROM raw_samples CROSS JOIN (SELECT arrayJoin(arrayMap(x->x*60000,range({start},{end}))) grid_ms) grids WHERE metric='{metric}' AND ts_ms>=grid_ms-300000 AND ts_ms<=grid_ms GROUP BY grid_ms,labels) WHERE n>=2)"""
def grid_sum_rate(metric): return f"SELECT grid_ms,sum(value) value FROM ({grid_rate(metric)}) GROUP BY grid_ms"
M['q13']=f"SELECT map() labels,max(value) value FROM ({grid_sum_rate('backend_process_cpu_seconds_total')})"
M['q24']=f"SELECT map() labels,max(a.value/b.value) value FROM ({grid_sum_rate('order_service_http_5xx_total')}) a INNER JOIN ({grid_sum_rate('order_service_http_requests_total')}) b ON a.grid_ms=b.grid_ms"
p=Path(__file__).with_name('corpus.json');x=json.loads(p.read_text())
for r in x['queries']:
 if r['id'] in M:r['clickhouse_sql']=M[r['id']];r['sql_mapping_status']='oracle_pending'
p.write_text(json.dumps(x,indent=2)+'\n')
