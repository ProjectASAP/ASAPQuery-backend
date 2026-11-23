#!/bin/bash
# Host-side live run of the final query workload (demo48 at 2,800 series: 21 queries, 19 planned and
# 2 VictoriaMetrics-only; 4 feeder shards; 4 query users), as the final campaign prepared it (its plan,
# feeder tables and manifest, unchanged), on backend 24342eab, with the same engine limits as the
# previous live runs (12 cores, 64 GiB with page cache, a 200 GiB filesystem per engine).
# The previous run's data is moved aside, not deleted. Screenshots after 10 min, 1 h and 3 h
# (the longest window, so every accuracy check has started).
: "${ASAPQ_BENCH:?set ASAPQ_BENCH}" "${BENCH_ROOT:?set BENCH_ROOT}"
set -u
cd $ASAPQ_BENCH || exit 1
[ -x $BENCH_ROOT/bin-live-2434-d48/data_plane ] || { echo "no $BENCH_ROOT/bin-live-2434-d48/data_plane"; exit 1; }
[ -f live/prep-demo48-s2800/backend/plan.json ] || { echo "no live/prep-demo48-s2800"; exit 1; }
export BIN_DIR=$BENCH_ROOT/bin-live-2434-d48 SERIES=2800 WORKLOAD=demo48 \
       DATA_PLANE_ARGS="--precompute-threads 12 --view-cache-max-bytes 0" LIMITS=1 START_LOAD_USERS=4 \
       AUX_CPUS=48-55 INGEST_GAUGE=data INGEST_SERIES_QUERY='max_over_time(data[30m])' INGEST_COUNT_QUERY=
LIVE=live
old=${OLD_TAG:-2434}
aside=$LIVE/aside-$old-$(date -u +%Y%m%dT%H%M%SZ)
./run_live_demo.sh stop
mkdir -p "$aside"
for e in asap victoriametrics; do
  if mountpoint -q "$LIVE/mnt/$e"; then sudo -n umount "$LIVE/mnt/$e"; fi
  if [ -f "$LIVE/disk/$e.img" ]; then mv "$LIVE/disk/$e.img" "$aside/$e.img"; fi
  harness/cgroup_limits.sh remove "$e" 2>/dev/null || true   # fresh cgroup counters (cpu, io, memory.peak)
done
if [ -d "$LIVE/data" ]; then mv "$LIVE/data" "$aside/data"; fi
for f in stream_start_ms reference_start_ms; do cp -p "$LIVE/$f" "$aside/" 2>/dev/null || true; done
cp -p monitoring/textfile/asap_demo.prom "$aside/" 2>/dev/null && rm -f monitoring/textfile/asap_demo.prom
rm -f "$LIVE/limits.env" "$LIVE"/cmds/feeder-*
echo "== $(date -u +%T) previous run moved to $aside"
./run_live_demo.sh start   # the plan exists: kept (no REPLAN)
start_ms=$(cat live/stream_start_ms)
for m in 10 60 180; do
  until [ "$(date +%s)" -gt $(( start_ms / 1000 + m * 60 )) ]; do sleep 30; done
  monitoring/screenshot.sh asap-demo "$start_ms" now "live/screenshots/d48-${m}min" 2>&1 | tail -1
  echo "== $(date -u +%T) screenshots after ${m} min"
done
echo CHAIN_DONE
