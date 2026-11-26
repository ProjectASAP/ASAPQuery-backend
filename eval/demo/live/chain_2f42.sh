#!/bin/bash
# Host-side chain on backend 2f428878 (detached): unlimited run, 2 h warm-up (the result
# cache is not persisted; a 1 h range panel needs about one span after a restart), 3 states; then
# the same under LIMITS=1.
: "${ASAPQ_BENCH:?set ASAPQ_BENCH}" "${BENCH_ROOT:?set BENCH_ROOT}"
cd $ASAPQ_BENCH
export BIN_DIR=$BENCH_ROOT/bin-live-2f42 SERIES=10000 WORKLOAD=asap-demo FINAL_QUERY_RESULTS_CACHED=1 \
       DATA_PLANE_ARGS="--precompute-threads 12"
demo/run_live_demo.sh reset
REPLAN=1 demo/run_live_demo.sh start
bash demo/live/demo_sequence_tagged.sh "$(( $(cat demo/live/stream_start_ms) / 1000 + 7320 ))" 8 v3-nolimits > demo/live/logs/sequence-v3-nolimits.log 2>&1
demo/run_live_demo.sh reset
LIMITS=1 demo/run_live_demo.sh start
bash demo/live/demo_sequence_tagged.sh "$(( $(cat demo/live/stream_start_ms) / 1000 + 7320 ))" 8 v3-limits > demo/live/logs/sequence-v3-limits.log 2>&1
