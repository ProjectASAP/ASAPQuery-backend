#!/bin/bash
# Host-side live run on backend 24342eab (tiered views; Planner pin 90a6e86), detached: the same
# workload, limits and load as chain_8156.sh (both engines under the same limits, the 8-user query
# load from second 0), screenshots of the cumulative dashboard after 2 h, 4 h and 6 h.
# Unlike `run_live_demo.sh reset`, the previous run's data is moved aside, not deleted.
set -u
cd $ASAPQ_BENCH || exit 1
[ -x $BENCH_ROOT/bin-live-2434/data_plane ] || { echo "no $BENCH_ROOT/bin-live-2434/data_plane"; exit 1; }
export BIN_DIR=$BENCH_ROOT/bin-live-2434 SERIES=10000 WORKLOAD=asap-demo FINAL_QUERY_RESULTS_CACHED=1 \
       DATA_PLANE_ARGS="--precompute-threads 12 --view-cache-max-bytes 0" LIMITS=1 START_LOAD_USERS=8 \
       AUX_CPUS=48-55
LIVE=live
old=${OLD_TAG:-8156}
aside=$LIVE/aside-$old-$(date -u +%Y%m%dT%H%M%SZ)
./run_live_demo.sh stop
mkdir -p "$aside"
for e in asap victoriametrics; do
  if mountpoint -q "$LIVE/mnt/$e"; then sudo -n umount "$LIVE/mnt/$e"; fi
  if [ -f "$LIVE/disk/$e.img" ]; then mv "$LIVE/disk/$e.img" "$aside/$e.img"; fi
  harness/cgroup_limits.sh remove "$e" 2>/dev/null || true   # fresh cgroup counters (cpu, memory.peak)
done
if [ -d "$LIVE/data" ]; then mv "$LIVE/data" "$aside/data"; fi
cp -a "$LIVE/prep-$WORKLOAD-s$SERIES" "$aside/"
for f in stream_start_ms reference_start_ms; do cp -p "$LIVE/$f" "$aside/" 2>/dev/null || true; done
rm -f "$LIVE/limits.env"
echo "== $(date -u +%T) previous run moved to $aside"
REPLAN=1 ./run_live_demo.sh start
start_ms=$(cat live/stream_start_ms)
for h in 2 4 6; do
  until [ "$(date +%s)" -gt $(( start_ms / 1000 + h * 3600 )) ]; do sleep 60; done
  monitoring/screenshot.sh asap-demo "$start_ms" now "live/screenshots/2434-${h}h" 2>&1 | tail -1
  echo "== $(date -u +%T) screenshots after ${h} h"
done
echo CHAIN_DONE
