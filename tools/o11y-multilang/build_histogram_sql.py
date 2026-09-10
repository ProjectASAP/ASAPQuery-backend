#!/usr/bin/env python3
import json
from pathlib import Path
# Reuse the reviewed counter template without importing side-effect script.
ns={}; exec(Path(__file__).with_name('build_counter_sql.py').read_text().split("p=Path(__file__)")[0],ns)
src=ns['rate']('order_service_http_request_duration_seconds_bucket',300000)
sql=f'''SELECT map() labels,if(idx=length(buckets),buckets[idx-1].1,if(idx=1 AND buckets[idx].1<=0,buckets[idx].1,(if(idx=1,0.,buckets[idx-1].1)+(buckets[idx].1-if(idx=1,0.,buckets[idx-1].1))*(rank-if(idx=1,0.,buckets[idx-1].2))/(buckets[idx].2-if(idx=1,0.,buckets[idx-1].2))))) value FROM (SELECT buckets,0.95*buckets[length(buckets)].2 rank,arrayFirstIndex(x->x.2>=rank,buckets) idx FROM (SELECT arrayMap(i->(raw[i].1,arrayMax(arrayMap(x->x.2,arraySlice(raw,1,i)))),range(1,length(raw)+1)) buckets FROM (SELECT arraySort(x->x.1,groupArray((if(le='+Inf',inf,toFloat64(le)),value))) raw FROM (SELECT labels['le'] le,sum(value) value FROM ({src}) GROUP BY le)))) WHERE idx>0'''
p=Path(__file__).with_name('corpus.json');x=json.loads(p.read_text())
for r in x['queries']:
 if r['id']=='q21':r['clickhouse_sql']=sql;r['sql_mapping_status']='oracle_pending'
p.write_text(json.dumps(x,indent=2)+'\n')
