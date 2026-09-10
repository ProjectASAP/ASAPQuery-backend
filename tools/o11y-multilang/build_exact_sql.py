#!/usr/bin/env python3
"""Attach native ClickHouse SQL for the first exact-semantics pattern batch."""
import json
from pathlib import Path
E='{eval_ms}'
SQL={
'q05': f"""SELECT labels, max(value) AS value FROM raw_samples WHERE metric='cache_refresh_lag_seconds' AND ts_ms>{E}-43200000 AND ts_ms<={E} GROUP BY labels ORDER BY labels""",
'q06': f"""SELECT labels, max(value) AS value FROM raw_samples WHERE metric='user_service_cache_refresh_lag_seconds' AND ts_ms>{E}-43200000 AND ts_ms<={E} GROUP BY labels ORDER BY labels""",
'q07': f"""SELECT mapConcat(labels,map('__name__','user_service_cache_refresh_lag_seconds')) AS labels, argMax(value,ts_ms) AS value FROM raw_samples WHERE metric='user_service_cache_refresh_lag_seconds' AND ts_ms>{E}-300000 AND ts_ms<={E} GROUP BY labels ORDER BY labels""",
'q09': f"""SELECT map() AS labels, sum(value) AS value FROM (SELECT labels,argMax(value,ts_ms) AS value FROM raw_samples WHERE metric='backend_process_resident_memory_bytes' AND ts_ms>{E}-300000 AND ts_ms<={E} GROUP BY labels)""",
'q12': f"""SELECT map('job',job) AS labels,sum(value) AS value FROM (SELECT labels['job'] AS job,labels,argMax(value,ts_ms) AS value FROM raw_samples WHERE metric='backend_process_resident_memory_bytes' AND ts_ms>{E}-300000 AND ts_ms<={E} GROUP BY job,labels) GROUP BY job ORDER BY value DESC,job LIMIT 2""",
'q23': f"""SELECT labels,max(value) AS value FROM raw_samples WHERE metric='backend_retry_backlog_depth' AND ts_ms>{E}-21600000 AND ts_ms<={E} GROUP BY labels ORDER BY value DESC,labels LIMIT 2""",
}
p=Path(__file__).with_name('corpus.json'); corpus=json.loads(p.read_text())
for row in corpus['queries']:
 row['clickhouse_sql']=SQL.get(row['id'])
 row['sql_mapping_status']='oracle_pending' if row['id'] in SQL else 'later_pattern_batch'
p.write_text(json.dumps(corpus,indent=2)+'\n')
