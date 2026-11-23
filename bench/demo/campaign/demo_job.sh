#!/bin/bash
# demo_job.sh <tag> <commit> <series> <users> <run_s> [arms] [workload]
# One 48-core demo job (demo plan sections 8-9) on the DRIVER host: feeders (FEEDER_SHARDS
# processes), harness, query client and post-run reference here; the engines on the ENGINE
# host (ENGINE_SSH, private 10 GbE link), each in a 48 vCPU (24 cores + hyperthreads) /
# 100 GiB cgroup with a 400 GB data filesystem, one arm after another on the same cpuset.
# Run model: from empty, ingest at real time and every query from the start for run_s
# seconds (QUERIES_FROM_START=1, no backfill); totals counted from second 0.
# VictoriaMetrics v1.156.0: defaults except -search.latencyOffset=6s, -retentionPeriod=1d,
# -search.maxQueryDuration=600s; client timeout 600 s. Data: integer Zipf, declared ranges.
set -u
tag=$1 commit=$2 series=$3 users=$4 run_s=$5 arms=${6:-victoriametrics} workload=${7:-demo/demo48}
source $BENCH_ENV
B=$ASAPQ_BENCH
export BIN_DIR=$BENCH_ROOT/campaign-bin-$commit
[ -x "$BIN_DIR/data_plane" ] || { echo "missing $BIN_DIR"; exit 2; }
export ENGINE_SSH=${ENGINE_SSH:?set ENGINE_SSH} ENGINE_ADDR=${ENGINE_ADDR:?set ENGINE_ADDR} NET_IFACE=${NET_IFACE:-eno2}
export ENGINE_SSH_OPTS=${ENGINE_SSH_OPTS:-"-o BatchMode=yes"}
export VICTORIAMETRICS_VERSION=v1.156.0 VM_MAX_QUERY_DURATION=600s QUERY_TIMEOUT_S=600
export FEEDER_VALUES=zipf ZIPF_S=1.1 ZIPF_N=1000000 FEEDER_BIN=$BIN_DIR/paced_feeder_realistic
export FEEDER_SHARDS=${FEEDER_SHARDS:-4} QUERIES_FROM_START=1
export QUERY_DELAY_S=6 KEEP_VM_DATA=${KEEP_VM_DATA:-1} CONCURRENT=0 MONITORING=none PROFILE=0 MODES=instant
export SERIES=$series QUERY_USERS=$users PRECOMPUTE_THREADS=${PRECOMPUTE_THREADS:-48}
export ENGINE_LIMITS='{"memory_bytes": 107374182400, "cpu_cores": 48, "disk_bytes": 400000000000}'
cpus=0-23,28-51
export ARM_CPUS="asap=$cpus;victoriametrics=$cpus;vmcluster=$cpus"
export ARMS=$arms QUERY_PHASE_S=$run_s WORKLOADS=$workload RUN_TAG=$tag
export DATA_PLANE_EXTRA_ARGS=${DATA_PLANE_EXTRA_ARGS:-}
# Never clobber a run of the same tag: refuse when its meta exists (running or finished);
# a rerun needs a new tag (2026-11-23: a stale duplicate launch deleted a running run).
if ls "$B"/runs/$tag.meta >/dev/null 2>&1; then echo "refusing: $B/runs/$tag.meta exists (use a new tag)"; exit 3; fi
rm -rf "$B"/runs/$tag.*
meta=$B/runs/$tag.meta
{ echo "tag=$tag driver=$(hostname) engine=$ENGINE_SSH ($ENGINE_ADDR) backend_commit=$(cat $BIN_DIR/BACKEND_COMMIT) start=$(date -u +%FT%TZ)"
  env | grep -E '^(SERIES|QUERY_[A-Z_]*|QUERIES_FROM_START|ENGINE_LIMITS|ENGINE_ADDR|ARM_CPUS|ARMS|VM_MAX_QUERY_DURATION|PRECOMPUTE_THREADS|FEEDER_[A-Z_]*|ZIPF_[A-Z]*|ANSWER_RESULTS_EVERY|KEEP_VM_DATA|CONCURRENT|MODES|WORKLOADS|DATA_PLANE_EXTRA_ARGS|NET_IFACE|VICTORIAMETRICS_VERSION)=' | sort
  echo "harness_sha256_16=$(cd $B && cat harness/*.py run_vm_demo.sh fetch_binaries.sh | sha256sum | cut -c1-16)"
  echo "victoria-metrics: $($BIN_DIR/victoria-metrics-$VICTORIAMETRICS_VERSION -version 2>&1 | head -1)"
  sha256sum $BIN_DIR/data_plane $BIN_DIR/control_plane $BIN_DIR/victoria-metrics-$VICTORIAMETRICS_VERSION $FEEDER_BIN
  echo "engine host: $(ssh $ENGINE_SSH_OPTS $ENGINE_SSH 'hostname; lscpu | grep "Model name"; lsblk -d -o NAME,ROTA,MODEL | tr "\n" " "; df -h / | tail -1')"
} > "$meta"
(cd $B && ./run_vm_demo.sh) > "$B/runs/$tag.campaign.log" 2>&1
rc=$?
echo "end=$(date -u +%FT%TZ) rc=$rc" >> "$meta"
grep -q VM_DEMO_DONE "$B/runs/$tag.campaign.log" || exit 1
touch "$B/runs/$tag.done"
