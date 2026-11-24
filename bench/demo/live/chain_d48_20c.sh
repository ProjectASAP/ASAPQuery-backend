#!/bin/bash
# Host-side live run of the final query workload (demo48 at 2,800 series: 21 queries, 19 planned and
# 2 VictoriaMetrics-only; 4 feeder shards; 4 query users), as the final campaign prepared it (its plan,
# feeder tables and manifest, unchanged), on backend 24342eab, with equal and larger engine limits
# than chain_d48.sh (where VictoriaMetrics could not keep up at 12 cores):
#   ASAP             cpuset 0-9,28-37   (20 vCPUs, SMT siblings together), cpu.max 20 cores
#   VictoriaMetrics  cpuset 10-19,38-47 (20 vCPUs),                        cpu.max 20 cores
#   both             memory.max 96 GiB (page cache included), a 200 GiB data filesystem
#   reference VM     cpuset 20-23,48-51; feeders, proxies, query load, exporters and the monitoring
#                    containers 24-27,52-55
# ASAP precomputes with 20 threads. The run has no fixed end. The previous run's data is moved
# aside, not deleted. Screenshots after 10 min, 1 h, 3 h (every accuracy check has started) and 6 h.
: "${ASAPQ_BENCH:?set ASAPQ_BENCH}" "${BENCH_ROOT:?set BENCH_ROOT}"
set -u
cd $ASAPQ_BENCH || exit 1
[ -x $BENCH_ROOT/bin-live-2434-d48/data_plane ] || { echo "no $BENCH_ROOT/bin-live-2434-d48/data_plane"; exit 1; }
[ -f live/prep-demo48-s2800/backend/plan.json ] || { echo "no live/prep-demo48-s2800"; exit 1; }
export BIN_DIR=$BENCH_ROOT/bin-live-2434-d48 SERIES=2800 WORKLOAD=demo48 \
       DATA_PLANE_ARGS="--precompute-threads 20 --view-cache-max-bytes 0" LIMITS=1 START_LOAD_USERS=4 \
       ASAP_CPUS=0-9,28-37 VM_CPUS=10-19,38-47 REF_CPUS=20-23,48-51 AUX_CPUS=24-27,52-55 \
       LIMIT_CORES=20 LIMIT_MEMORY_BYTES=$((96 * 1024 * 1024 * 1024)) LIMIT_DISK_BYTES=214748364800 \
       INGEST_GAUGE=data INGEST_SERIES_QUERY='max_over_time(data[30m])' INGEST_COUNT_QUERY=
LIVE=live
old=${OLD_TAG:-d48}
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
# The monitoring containers (Prometheus, Grafana, exporters) onto the aux CPUs, off the engines' cpusets.
for c in $(sudo -n docker ps --format '{{.Names}}' | grep '^asap-vs-vm-monitoring-'); do
  sudo -n docker update --cpuset-cpus "$AUX_CPUS" "$c" >/dev/null && echo "== $c on cpus $AUX_CPUS"
done
start_ms=$(cat live/stream_start_ms)
for m in 10 60 180 360; do
  until [ "$(date +%s)" -gt $(( start_ms / 1000 + m * 60 )) ]; do sleep 30; done
  monitoring/screenshot.sh asap-demo "$start_ms" now "live/screenshots/d48-${m}min" 2>&1 | tail -1
  echo "== $(date -u +%T) screenshots after ${m} min"
done
echo CHAIN_DONE
