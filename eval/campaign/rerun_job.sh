#!/bin/bash
# rerun_job.sh <commit> <cfg> <round> <SERIES> <MODES> <RANGE or -> <WORKLOADS...>
# One queue job: one round of one config with the unified harness (run_vm_demo.sh does
# prepare, run, accuracy_vs_vm.py post-run reference, report.py, demo_table.py), then
# host_meta.json into each run dir. Arms SEQUENTIAL (victoriametrics, asap), VictoriaMetrics
# v1.156.0, query delay 6 s, KEEP_VM_DATA=1, ARM_CORES=12, QUERY_PHASE_S=600.
# QUERY_USERS (if the harness supports it) is passed through from the queue line's env.
# Idempotent: removes this tag's earlier (half-finished) run dirs first.
set -u
commit=$1 cfg=$2 round=$3 series=$4 modes=$5 range=$6; shift 6; workloads="$*"
{ [ -n "${BENCH_ENV:-}" ] && source "$BENCH_ENV"; }
B=$ASAPQ_BENCH
export BIN_DIR=$BENCH_ROOT/campaign-bin-$commit
[ -x "$BIN_DIR/data_plane" ] || { echo "missing $BIN_DIR (build it on the build host: build_backend.sh $commit)"; exit 2; }
[ "$(cut -c1-8 "$BIN_DIR/BACKEND_COMMIT")" = "${commit:0:8}" ] || { echo "BIN_DIR is not $commit"; exit 2; }
export VICTORIAMETRICS_VERSION=v1.156.0
vm=$BIN_DIR/victoria-metrics-$VICTORIAMETRICS_VERSION
[ -x "$vm" ] || cp -a $BIN_DIR/victoria-metrics-$VICTORIAMETRICS_VERSION "$vm"
export PRECOMPUTE_THREADS=${PRECOMPUTE_THREADS:-12}
# Zipf data for the final rerun (same for both engines); FEEDER_VALUES=realistic|realistic-smooth|original are options.
export FEEDER_VALUES=${FEEDER_VALUES:-zipf} ZIPF_S=${ZIPF_S:-1.1} ZIPF_N=${ZIPF_N:-1000000}
[ "$FEEDER_VALUES" = original ] && unset FEEDER_VALUES || export FEEDER_BIN=$BIN_DIR/paced_feeder_realistic
export ARMS=victoriametrics,asap CONCURRENT=0 QUERY_DELAY_S=6 KEEP_VM_DATA=1 ARM_CORES=12 \
       QUERY_PHASE_S=${QUERY_PHASE_S:-600} MONITORING=none PROFILE=0
export RUN_TAG=rerun-$cfg-r$round SERIES=$series MODES=$modes WORKLOADS=$workloads
# memory-budget configs: cost inputs per config (cost model 3 + memory limit), if present.
case $cfg in membudget-*)
  export PLANNER_COST_INPUTS=$CAMPAIGN_DIR/cost-inputs/$cfg.json
  [ -f "$PLANNER_COST_INPUTS" ] || { echo "missing $PLANNER_COST_INPUTS"; exit 2; } ;;
esac
if [ "$range" != "-" ]; then export RANGE=$range; else unset RANGE; fi
rm -rf "$B"/runs/$RUN_TAG.* "$B"/runs/$RUN_TAG.done
meta=$B/runs/$RUN_TAG.meta
{ echo "cfg=$cfg round=$round host=$(hostname) backend_commit=$(cat $BIN_DIR/BACKEND_COMMIT) start=$(date -u +%FT%TZ)"
  echo "SERIES=$SERIES MODES=$MODES RANGE=${RANGE:-} WORKLOADS=$WORKLOADS ARMS=$ARMS CONCURRENT=$CONCURRENT QUERY_DELAY_S=$QUERY_DELAY_S KEEP_VM_DATA=$KEEP_VM_DATA ARM_CORES=$ARM_CORES QUERY_PHASE_S=$QUERY_PHASE_S QUERY_USERS=${QUERY_USERS:-} PLANNER_COST_INPUTS=${PLANNER_COST_INPUTS:-} PRECOMPUTE_THREADS=$PRECOMPUTE_THREADS FEEDER_VALUES=${FEEDER_VALUES:-original} ZIPF_S=$ZIPF_S ZIPF_N=$ZIPF_N FEEDER_BIN=${FEEDER_BIN:-}"
  echo "harness_sha256_16=$(cd $B && cat harness/*.py run_vm_demo.sh fetch_binaries.sh | sha256sum | cut -c1-16)"
  echo "victoriametrics: $("$vm" -version 2>&1 | head -1)"
  sha256sum $BIN_DIR/data_plane $BIN_DIR/control_plane "$vm"; } > "$meta"
(cd $B && ./run_vm_demo.sh) > "$B/runs/$RUN_TAG.campaign.log" 2>&1
rc=$?
for d in "$B"/runs/$RUN_TAG.*/; do
  [ -d "$d" ] || continue
  (cd $B && BIN_DIR=$BIN_DIR report_demo/collect_meta.sh "$d/host_meta.json" >/dev/null)
done
# The value ranges declared to the planner (column_magnitudes), per prepared workload.
for d in "$B"/runs/$RUN_TAG.*/; do
  [ -d "$d" ] || continue
  p=$B/prep/$(basename "$d" | sed "s/^$RUN_TAG\.//")
  echo "column_magnitudes $(basename "$p"): $(python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1]))['data_facts'].get('column_magnitudes')))" "$p/manifest.json" 2>/dev/null)" >> "$meta"
done
echo "end=$(date -u +%FT%TZ) rc=$rc" >> "$meta"
grep -q VM_DEMO_DONE "$B/runs/$RUN_TAG.campaign.log" || exit 1
touch "$B/runs/$RUN_TAG.done"
