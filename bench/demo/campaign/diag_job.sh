#!/bin/bash
# diag_job.sh <commit> <tag> <SERIES> <MODES> <WORKLOADS> : a diagnostic run with the unified harness,
# arms SEQUENTIAL (victoriametrics, asap: each alone in its cgroup), VictoriaMetrics v1.156.0,
# query delay 6 s, KEEP_VM_DATA=1, Zipf data with declared ranges. Caller's env passes
# QUERY_USERS, ENGINE_LIMITS, DATA_PLANE_EXTRA_ARGS, PRECOMPUTE_THREADS (default 12).
set -u
commit=$1 tag=$2 series=$3 modes=$4; shift 4; workloads="$*"
source $BENCH_ENV
B=$ASAPQ_BENCH
export BIN_DIR=$BENCH_ROOT/campaign-bin-$commit
[ -x "$BIN_DIR/data_plane" ] || { echo "missing $BIN_DIR"; exit 2; }
export VICTORIAMETRICS_VERSION=v1.156.0
[ -x "$BIN_DIR/victoria-metrics-v1.156.0" ] || cp -a $BIN_DIR/victoria-metrics-v1.156.0 "$BIN_DIR/"
export PRECOMPUTE_THREADS=${PRECOMPUTE_THREADS:-12}
export FEEDER_VALUES=${FEEDER_VALUES:-zipf} ZIPF_S=${ZIPF_S:-1.1} ZIPF_N=${ZIPF_N:-1000000}
export FEEDER_BIN=$BIN_DIR/paced_feeder_realistic
export ARMS=${ARMS:-victoriametrics,asap} CONCURRENT=0 QUERY_DELAY_S=6 KEEP_VM_DATA=1 ARM_CORES=12 \
       QUERY_PHASE_S=${QUERY_PHASE_S:-600} MONITORING=none PROFILE=0
export RUN_TAG=$tag SERIES=$series MODES=$modes WORKLOADS=$workloads
rm -rf "$B"/runs/$RUN_TAG.*
meta=$B/runs/$RUN_TAG.meta
{ echo "diag tag=$tag host=$(hostname) backend_commit=$(cat $BIN_DIR/BACKEND_COMMIT) start=$(date -u +%FT%TZ)"
  env | grep -E "^(SERIES|MODES|WORKLOADS|ARMS|CONCURRENT|QUERY_DELAY_S|QUERY_USERS|QUERY_PHASE_S|ENGINE_LIMITS|DATA_PLANE_EXTRA_ARGS|PRECOMPUTE_THREADS|FEEDER_VALUES|ZIPF_S|ZIPF_N|FEEDER_BIN|ARM_CORES|KEEP_VM_DATA)=" | sort
  echo "harness_sha256_16=$(cd $B && cat harness/*.py run_vm_demo.sh fetch_binaries.sh | sha256sum | cut -c1-16)"
  sha256sum $BIN_DIR/data_plane $BIN_DIR/control_plane $FEEDER_BIN; } > "$meta"
(cd $B && ./run_vm_demo.sh) > "$B/runs/$RUN_TAG.campaign.log" 2>&1
for d in "$B"/runs/$RUN_TAG.*/; do
  [ -d "$d" ] || continue
  p=$B/prep/$(basename "$d" | sed "s/^$RUN_TAG\.//")
  echo "column_magnitudes: $(python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1]))['data_facts'].get('column_magnitudes')))" "$p/manifest.json" 2>/dev/null)" >> "$meta"
done
echo "end=$(date -u +%FT%TZ)" >> "$meta"
grep -q VM_DEMO_DONE "$B/runs/$RUN_TAG.campaign.log"
