#!/bin/bash
# launch.sh <cfg>: run_vm_demo.sh for one campaign config; SERIES/MODES/WORKLOADS/RANGE/BIN_DIR from the env.
# VictoriaMetrics: VICTORIAMETRICS_VERSION (default v1.156.0) must be in BIN_DIR (copied from $BIN_DIR if missing).
cfg=$1
source $BENCH_ENV
export BIN_DIR=${BIN_DIR:?set BIN_DIR}
export ARMS=${ARMS:-victoriametrics,asap}
export QUERY_DELAY_S=${QUERY_DELAY_S:-6}
export VICTORIAMETRICS_VERSION=${VICTORIAMETRICS_VERSION:-v1.156.0}
vm=$BIN_DIR/victoria-metrics-$VICTORIAMETRICS_VERSION
[ -x "$vm" ] || cp -a $BIN_DIR/victoria-metrics-$VICTORIAMETRICS_VERSION "$vm" 2>/dev/null
[ -x "$vm" ] || { echo "missing $vm; not launching $cfg" >&2; exit 3; }
cd $ASAPQ_BENCH
export RUN_TAG=camp1120-$cfg ARM_CORES=${ARM_CORES:-12} CONCURRENT=1 QUERY_PHASE_S=600 MONITORING=none PROFILE=0
{ echo "cfg=$cfg host=$(hostname) backend_commit=$(cat $BIN_DIR/BACKEND_COMMIT) start=$(date -u +%FT%TZ)";
  echo "SERIES=$SERIES MODES=$MODES WORKLOADS=$WORKLOADS RANGE=${RANGE:-} ARM_CORES=$ARM_CORES ARMS=$ARMS CONCURRENT=1 QUERY_PHASE_S=600 ASAP_PERSIST_FLAG=${ASAP_PERSIST_FLAG:-default} QUERY_DELAY_S=${QUERY_DELAY_S:-3}";
  echo "harness_sha256_16=$(cat harness/*.py run_vm_demo.sh fetch_binaries.sh | sha256sum | cut -c1-16)";
  echo "victoriametrics: $("$vm" -version 2>&1 | head -1)";
  sha256sum $BIN_DIR/data_plane $BIN_DIR/control_plane "$vm"; } > $CAMPAIGN_DIR/$cfg.meta
nohup setsid ./run_vm_demo.sh > $CAMPAIGN_DIR/$cfg.log 2>&1 < /dev/null &
echo "launched $cfg pid $!"
