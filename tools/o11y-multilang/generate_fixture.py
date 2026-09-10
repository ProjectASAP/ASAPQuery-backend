#!/usr/bin/env python3
"""Generate the complete deterministic OpenMetrics fixture used by this benchmark."""
import argparse,math
from pathlib import Path
EVAL_MS=1788891296000
COUNTERS=['backend_http_5xx_total','backend_http_requests_total','payment_service_http_5xx_total','payment_service_http_requests_total','backend_process_cpu_seconds_total','order_service_http_requests_total','order_service_http_5xx_total']
GAUGES=['cache_refresh_lag_seconds','user_service_cache_refresh_lag_seconds','backend_process_resident_memory_bytes','backend_retry_backlog_depth']
JOBS=[('api-gateway','api-gateway:8081'),('user-service','user-service:8082'),('order-service','order-service:8083'),('payment-service','payment-service:8084')]
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args();start=EVAL_MS-26*3600_000;lines=[]
 for mi,m in enumerate(COUNTERS):
  for ji,(job,instance) in enumerate(JOBS):
   value=0.
   for n,ts in enumerate(range(start,EVAL_MS+1,240_000)):
    if n==180 and ji==1:value=2. # deterministic reset
    value+=(mi+1)*(ji+2)+(n%5)
    lines.append(f'{m}{{job="{job}",instance="{instance}"}} {value:.9f} {ts/1000:.3f}')
 for mi,m in enumerate(GAUGES):
  for ji,(job,instance) in enumerate(JOBS):
   for n,ts in enumerate(range(start,EVAL_MS+1,240_000)):
    value=(mi+1)*10+ji*3+(n%17)+math.sin(n/7)
    lines.append(f'{m}{{job="{job}",instance="{instance}"}} {value:.9f} {ts/1000:.3f}')
 # Classic cumulative buckets; +Inf is mandatory for histogram_quantile.
 for ji,(job,instance) in enumerate([JOBS[2]]):
  counts={.1:0.,.5:0.,1.:0.,5.:0.,float('inf'):0.}
  for n,ts in enumerate(range(start,EVAL_MS+1,240_000)):
   inc=[1+n%2,2+n%3,3+n%4,5+n%5,7+n%6]
   for le,delta in zip(counts,inc):counts[le]+=delta
   for le,v in counts.items():
    lev='+Inf' if math.isinf(le) else str(le)
    lines.append(f'order_service_http_request_duration_seconds_bucket{{job="{job}",instance="{instance}",le="{lev}"}} {v:.9f} {ts/1000:.3f}')
 lines.append('# EOF');a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text('\n'.join(lines)+'\n')
if __name__=='__main__':main()
