#!/bin/bash
# After the unlimited 3-state sequence: restart the live run with both engines under the same
# enforced limits (LIMITS=1), then, after the 2 h warm-up, run the 3-state sequence again (tag "limits").
cd $ASAPQ_BENCH
export BIN_DIR=$BENCH_ROOT/bin-live SERIES=10000 WORKLOAD=asap-demo
until grep -q SEQUENCE_DONE live/logs/demo_sequence_v2.log 2>/dev/null; do sleep 60; done
./run_live_demo.sh reset
LIMITS=1 ./run_live_demo.sh start
start_s=$(( $(cat live/stream_start_ms) / 1000 + 7320 ))
exec bash live/demo_sequence_tagged.sh "$start_s" 8 limits
