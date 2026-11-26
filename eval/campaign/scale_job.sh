#!/bin/bash
# scale_job.sh <tag> <commit> <step S1|S4> <workload> <query_phase_s> [arms]
# One scaling-campaign job, run on the DRIVER host (feeders, harness, query client,
# post-run reference); the engines run on the ENGINE host (ENGINE_SSH, private link),
# one arm after another on the same cpuset so the hardware is identical:
# asap, victoriametrics (single-node), vmcluster (same total budget).
# Step budgets (each arm): S1 12 vCPU / 3 GiB / 50 GB disk, 10k series per metric, 1 user;
#                          S4 48 vCPU / 12 GiB / 200 GB disk, 40k series per metric, 4 users.
# VictoriaMetrics: -search.maxQueryDuration=600s (single-node and vmselect), client timeout
# 600 s, concurrency limits at defaults; everything recorded in <tag>.meta and vm_flags.json.
set -u
tag=$1 commit=$2 step=$3 workload=$4 qphase=$5 arms=${6:-asap,victoriametrics,vmcluster}
{ [ -n "${BENCH_ENV:-}" ] && source "$BENCH_ENV"; }
B=$ASAPQ_BENCH
export BIN_DIR=$BENCH_ROOT/campaign-bin-$commit
[ -x "$BIN_DIR/data_plane" ] || { echo "missing $BIN_DIR"; exit 2; }
export ENGINE_SSH=${ENGINE_SSH:?set ENGINE_SSH} ENGINE_ADDR=${ENGINE_ADDR:?set ENGINE_ADDR} NET_IFACE=${NET_IFACE:?set NET_IFACE}
export ENGINE_SSH_OPTS=${ENGINE_SSH_OPTS:-"-o BatchMode=yes"}
export VICTORIAMETRICS_VERSION=v1.156.0 VM_MAX_QUERY_DURATION=600s QUERY_TIMEOUT_S=600
export FEEDER_VALUES=zipf ZIPF_S=1.1 ZIPF_N=1000000 FEEDER_BIN=$BIN_DIR/paced_feeder_realistic
export QUERY_DELAY_S=6 KEEP_VM_DATA=1 CONCURRENT=0 MONITORING=none PROFILE=0 MODES=instant
case $step in
  S1) export SERIES=${SERIES_OVERRIDE:-10000} QUERY_USERS=1 PRECOMPUTE_THREADS=12 \
        ENGINE_LIMITS='{"memory_bytes": 3221225472, "cpu_cores": 12, "disk_bytes": 53687091200}' \
        VMCLUSTER_LAYOUT=vmstorage:1:6,vmselect:1:4,vminsert:1:2
      # 6 physical cores of socket 0 with their hyperthreads.
      cpus=0,2,4,6,8,10,28,30,32,34,36,38 ;;
  S4) export SERIES=40000 QUERY_USERS=4 PRECOMPUTE_THREADS=48 \
        ENGINE_LIMITS='{"memory_bytes": 12884901888, "cpu_cores": 48, "disk_bytes": 214748364800}' \
        VMCLUSTER_LAYOUT=vmstorage:2:16,vmselect:1:12,vminsert:1:4
      # 24 physical cores (12 per socket) with their hyperthreads; 4 cores left for the agent.
      cpus=0-23,28-51 ;;
  *) echo "unknown step $step"; exit 2 ;;
esac
export ARM_CPUS="asap=$cpus;victoriametrics=$cpus;vmcluster=$cpus"
export ARMS=$arms QUERY_PHASE_S=$qphase WORKLOADS=$workload RUN_TAG=$tag
export DATA_PLANE_EXTRA_ARGS=${DATA_PLANE_EXTRA_ARGS:-}
rm -rf "$B"/runs/$tag.* 
meta=$B/runs/$tag.meta
{ echo "tag=$tag step=$step driver=$(hostname) engine=$ENGINE_SSH ($ENGINE_ADDR) backend_commit=$(cat $BIN_DIR/BACKEND_COMMIT) start=$(date -u +%FT%TZ)"
  env | grep -E '^(SERIES|QUERY_|ENGINE_LIMITS|ENGINE_ADDR|VMCLUSTER_LAYOUT|ARM_CPUS|ARMS|VM_MAX_QUERY_DURATION|PRECOMPUTE_THREADS|FEEDER_|ZIPF_|KEEP_VM_DATA|CONCURRENT|MODES|WORKLOADS|DATA_PLANE_EXTRA_ARGS|NET_IFACE|VICTORIAMETRICS_VERSION)=' | sort
  echo "vm_flags: single-node and vmselect -search.maxQueryDuration=$VM_MAX_QUERY_DURATION -search.latencyOffset=${QUERY_DELAY_S}s; concurrency limits default; client timeout ${QUERY_TIMEOUT_S}s"
  echo "harness_sha256_16=$(cd $B && cat harness/*.py scripts/run_vm_demo.sh scripts/fetch_binaries.sh | sha256sum | cut -c1-16)"
  for c in victoria-metrics vmstorage vmselect vminsert; do echo "$c: $($BIN_DIR/$c-$VICTORIAMETRICS_VERSION -version 2>&1 | head -1)"; done
  sha256sum $BIN_DIR/data_plane $BIN_DIR/control_plane $BIN_DIR/*-v1.156.0 $FEEDER_BIN
  echo "engine host: $(ssh $ENGINE_SSH_OPTS $ENGINE_SSH 'hostname; lscpu | grep "Model name"; lsblk -d -o NAME,ROTA,MODEL | tr "\n" " "; df -h / | tail -1')"
} > "$meta"
(cd $B && ./scripts/run_vm_demo.sh) > "$B/runs/$tag.campaign.log" 2>&1
rc=$?
echo "end=$(date -u +%FT%TZ) rc=$rc" >> "$meta"
grep -q VM_DEMO_DONE "$B/runs/$tag.campaign.log" || exit 1
touch "$B/runs/$tag.done"
