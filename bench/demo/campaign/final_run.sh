#!/bin/bash
# final_run.sh <backend-commit> <jobfile>: the final rounds on this host, one job after another.
# jobfile lines:  cfg|round|SERIES|MODES|RANGE|WORKLOADS   (RANGE empty for instant; # comments)
# Each job: run_vm_demo.sh (unified harness) with arms SEQUENTIAL (victoriametrics, then asap),
# VictoriaMetrics v1.156.0, query delay 6 s, KEEP_VM_DATA=1, RUN_TAG=final1121-<cfg>-r<round>;
# run_vm_demo.sh runs accuracy_vs_vm.py (post-run VM ground truth), report.py, demo_table.py;
# then host_meta.json (report_demo/collect_meta.sh) into each run directory.
commit=$1; jobs=$2
{ [ -n "${BENCH_ENV:-}" ] && source "$BENCH_ENV"; }
export BIN_DIR=$BENCH_ROOT/campaign-bin-$commit
[ "$(cut -c1-8 "$BIN_DIR/BACKEND_COMMIT")" = "${commit:0:8}" ] || { echo "BIN_DIR $BIN_DIR is not $commit" >&2; exit 2; }
export VICTORIAMETRICS_VERSION=v1.156.0
vm=$BIN_DIR/victoria-metrics-$VICTORIAMETRICS_VERSION
[ -x "$vm" ] || cp -a $BIN_DIR/victoria-metrics-$VICTORIAMETRICS_VERSION "$vm"
[ -x "$vm" ] || { echo "missing $vm" >&2; exit 3; }
export ARMS=victoriametrics,asap CONCURRENT=0 QUERY_DELAY_S=6 KEEP_VM_DATA=1 ARM_CORES=12 \
       QUERY_PHASE_S=${FINAL_QUERY_PHASE_S:-600} MONITORING=none PROFILE=0
B=$ASAPQ_BENCH
grep -v '^#' "$jobs" | grep . | while IFS='|' read -r cfg round series modes range workloads; do
  export RUN_TAG=final1121-$cfg-r$round SERIES=$series MODES=$modes WORKLOADS=$workloads
  if [ -n "$range" ]; then export RANGE=$range; else unset RANGE; fi
  [ -f "$B/runs/$RUN_TAG.done" ] && continue
  meta=$B/runs/$RUN_TAG.meta
  { echo "cfg=$cfg round=$round host=$(hostname) backend_commit=$(cat $BIN_DIR/BACKEND_COMMIT) start=$(date -u +%FT%TZ)"
    echo "SERIES=$SERIES MODES=$MODES RANGE=${RANGE:-} WORKLOADS=$WORKLOADS ARMS=$ARMS CONCURRENT=$CONCURRENT QUERY_DELAY_S=$QUERY_DELAY_S KEEP_VM_DATA=$KEEP_VM_DATA ARM_CORES=$ARM_CORES QUERY_PHASE_S=$QUERY_PHASE_S"
    echo "harness_sha256_16=$(cd $B && cat harness/*.py run_vm_demo.sh fetch_binaries.sh | sha256sum | cut -c1-16)"
    echo "victoriametrics: $("$vm" -version 2>&1 | head -1)"
    sha256sum $BIN_DIR/data_plane $BIN_DIR/control_plane "$vm"; } > "$meta"
  (cd $B && ./run_vm_demo.sh) > "$B/runs/$RUN_TAG.campaign.log" 2>&1
  for d in "$B"/runs/$RUN_TAG.*/; do
    [ -d "$d" ] && (cd $B && BIN_DIR=$BIN_DIR report_demo/collect_meta.sh "$d/host_meta.json" >/dev/null) && \
      python3 -c "import json,sys; p=sys.argv[1]; m=json.load(open(p)); m['victoriametrics_version']=sys.argv[2]; json.dump(m,open(p,'w'),indent=1)" \
        "$d/host_meta.json" "$VICTORIAMETRICS_VERSION"  # collect_meta.sh picks the first VM binary in BIN_DIR
  done
  echo "end=$(date -u +%FT%TZ)" >> "$meta"
  touch "$B/runs/$RUN_TAG.done"
done
echo "FINAL_HOST_DONE $(date -u +%FT%TZ)"
