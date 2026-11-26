#!/bin/bash
# Host-side chain (detached): the 3-state sequence on the running unlimited run (tag
# "nolimits"), then restart with LIMITS=1 and, after the 2 h warm-up, the 3 states again ("limits").
: "${ASAPQ_BENCH:?set ASAPQ_BENCH}" "${BENCH_ROOT:?set BENCH_ROOT}"
cd $ASAPQ_BENCH
export BIN_DIR=$BENCH_ROOT/bin-live SERIES=10000 WORKLOAD=asap-demo
bash demo/live/demo_sequence_tagged.sh "$(date +%s)" 8 nolimits > demo/live/logs/sequence-nolimits.log 2>&1
demo/run_live_demo.sh reset
LIMITS=1 demo/run_live_demo.sh start
start_s=$(( $(cat demo/live/stream_start_ms) / 1000 + 7320 ))
bash demo/live/demo_sequence_tagged.sh "$start_s" 8 limits > demo/live/logs/sequence-limits.log 2>&1
