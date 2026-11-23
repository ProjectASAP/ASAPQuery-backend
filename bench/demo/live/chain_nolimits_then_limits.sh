#!/bin/bash
# Host-side chain (no session needed): the 3-state sequence on the running unlimited run (tag
# "nolimits"), then restart with LIMITS=1 and, after the 2 h warm-up, the 3 states again ("limits").
cd $ASAPQ_BENCH
export BIN_DIR=$BENCH_ROOT/bin-live SERIES=10000 WORKLOAD=asap-demo
bash live/demo_sequence_tagged.sh "$(date +%s)" 8 nolimits > live/logs/sequence-nolimits.log 2>&1
./run_live_demo.sh reset
LIMITS=1 ./run_live_demo.sh start
start_s=$(( $(cat live/stream_start_ms) / 1000 + 7320 ))
bash live/demo_sequence_tagged.sh "$start_s" 8 limits > live/logs/sequence-limits.log 2>&1
