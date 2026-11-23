#!/bin/bash
# Host-side live run on backend 815640bd (tiered views, partial windows), detached: both engines
# under the same limits, the 8-user query load from second 0 (demo plan section 8), screenshots of
# the cumulative dashboard after the warm-up (2 h) and at 4 h and 6 h.
cd $ASAPQ_BENCH
export BIN_DIR=$BENCH_ROOT/bin-live-8156 SERIES=10000 WORKLOAD=asap-demo FINAL_QUERY_RESULTS_CACHED=1 \
       DATA_PLANE_ARGS="--precompute-threads 12 --view-cache-max-bytes 0" LIMITS=1 START_LOAD_USERS=8 \
       AUX_CPUS=48-55
./run_live_demo.sh reset
REPLAN=1 ./run_live_demo.sh start
start_ms=$(cat live/stream_start_ms)
for h in 2 4 6; do
  until [ "$(date +%s)" -gt $(( start_ms / 1000 + h * 3600 )) ]; do sleep 60; done
  monitoring/screenshot.sh asap-demo "$start_ms" now "live/screenshots/8156-${h}h" 2>&1 | tail -1
  echo "== $(date -u +%T) screenshots after ${h} h"
done
echo CHAIN_DONE
