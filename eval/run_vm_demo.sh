#!/bin/bash
# ASAP data plane vs VictoriaMetrics demo, one command:
#
#   SERIES=1000 QUERY_PHASE_S=600 WORKLOADS="demo/dashboard-heavy" ./run_vm_demo.sh
#
# For each workload: prepare (scaled manifest, feeder table, ASAPPlanner's
# plan through the backend's control plane, timed as a one-time offline
# cost), run the arms one after another on the same feed, then report.py
# and demo_table.py (the absolute-numbers table, also exported to Grafana).
#
# Environment (defaults in brackets):
#   ASAPQ_BENCH    base directory: this repository's eval/ (required)
#   BIN_DIR        binaries; pinned releases are downloaded here when missing (required)
#   SERIES         series per metric, one run per value, each a multiple of 200
#                  (4 regions x 5 services x 10 jobs x label_0) ["1000 10000"]
#   WORKLOADS      set/workload list [seed-862/workload-01 seed-862/workload-02 seed-862/workload-03 demo/dashboard-heavy]
#   ARMS           [victoriametrics,asap]  (accuracy reference: VictoriaMetrics, rate/increase via
#                  rate_prometheus/increase_prometheus; add prometheus to run that arm too)
#   QUERY_PHASE_S  seconds of queries after the ingest phase (the longest window) [600]
#   ARM_CORES      cores per arm: each arm's engine gets its own cpuset of this many cores, in ARMS
#                  order from cpu 0; feeders, the harness and monitoring get the rest; 0 = no pinning [16]
#   CONCURRENT     1: all arms at once on one feed start (shorter, but memory bandwidth is shared) [0]
#   MONITORING     docker | binaries | none [docker when `sudo -n docker` works, else binaries]
#   PROFILE        1: CPU-profile VM and the data plane in each query phase [0]; PROFILE_SECONDS [60]
#   BACKEND_DIR    ASAPQuery-backend checkout: builds data_plane/control_plane into BIN_DIR when
#                  missing, and its benchmarks/scripts/profile.sh profiles the data plane [$ASAPQ_BENCH/backend]
#   RUN_TAG        prefix of run directories [vmdemo]
#   SCREENSHOTS    1: render the asap-vs-vm dashboard (whole page and every panel) over each run's
#                  time range into <run>/screenshots (../deployment/monitoring/screenshot.sh) [0]
#   MODES          measurement groups, run and reported apart ["instant range"]:
#                  instant = /api/v1/query at each evaluation time; range = /api/v1/query_range
#   RANGE          range group's STEP_S:SPAN_S[:REFRESH_S], as a Grafana panel [15:3600 = 1h span,
#                  15 s step, refreshed every 15 s]; ASAP plans it with range_evaluation {step, span}.
#                  The ingest phase grows by the span (the first request reads span + window).
#   ASAP_PERSIST_FLAG  data_plane flag naming its materialized-view directory [detected from --help];
#                  the directory is in the arm's run directory, the same disk as VictoriaMetrics' data
#   QUERY_DELAY_S  seconds after an evaluation time its query is sent, every arm alike [6]
#   IGNORE_ACCURACY_GUARANTEES  1: plan on cost alone (earlier MVP runs) [unset: guarantees enforced]
#   REMOTE_WRITE_SHAPE  feeder's --remote-write-shape: per-sample (as Prometheus sends) | per-series [per-sample]
#   ARM_CPUS       explicit cpusets 'arm=cpus;arm2=cpus' (run.py --arm-cpus) instead of ARM_CORES; with
#                  ENGINE_SSH (engines on another host, see run.py) they are the engine host's cpus
set -euo pipefail
[ -n "${BENCH_ENV:-}" ] && [ -f "$BENCH_ENV" ] && source "$BENCH_ENV"
export ASAPQ_BENCH=${ASAPQ_BENCH:?set ASAPQ_BENCH}
export BIN_DIR=${BIN_DIR:?set BIN_DIR}
MON=$ASAPQ_BENCH/../deployment/monitoring   # this repository's deployment/monitoring
mkdir -p "$MON/grafana/dashboards" "$MON/textfile"
BASE=$ASAPQ_BENCH
SERIES=${SERIES:-1000 10000}
ARM_CORES=${ARM_CORES:-16}
WORKLOADS=${WORKLOADS:-seed-862/workload-01 seed-862/workload-02 seed-862/workload-03 demo/dashboard-heavy}
ARMS=${ARMS:-victoriametrics,asap}
QUERY_PHASE_S=${QUERY_PHASE_S:-600}
CONCURRENT=${CONCURRENT:-0}
PROFILE=${PROFILE:-0}
PROFILE_SECONDS=${PROFILE_SECONDS:-60}
export BACKEND_DIR=${BACKEND_DIR:-$BASE/backend}
RUN_TAG=${RUN_TAG:-vmdemo}
MODES=${MODES:-instant range}
RANGE=${RANGE:-15:3600}
HARNESS_METRICS_PORT=19093

log() { echo "$(date -u +%FT%TZ) $*"; }

for n in $SERIES; do (( n % 200 == 0 )) || { echo "SERIES values must be multiples of 200" >&2; exit 2; }; done
# Cores left after the arms' cpusets (run.py --arm-cores assigns the same blocks).
NARMS=$(echo "$ARMS" | tr ',' '\n' | grep -c .)
NCPU=$(nproc)
if [ -n "${ARM_CPUS:-}" ]; then
  ARM_CORES=0; AUX_CPUS=""
elif (( ARM_CORES > 0 )); then
  (( ARM_CORES * NARMS < NCPU )) || { echo "ARM_CORES=$ARM_CORES x $NARMS arms needs more than $NCPU cores" >&2; exit 2; }
  AUX_CPUS="$(( ARM_CORES * NARMS ))-$(( NCPU - 1 ))"
else
  AUX_CPUS=""
fi

# 1. Binaries: pinned releases, and the Rust binaries built when missing.
"$BASE/fetch_binaries.sh" victoriametrics prometheus flamegraph
build() {  # dir, cargo args..., then copies the named binaries
  local dir=$1; shift
  local bins=$1; shift
  local target=${CARGO_TARGET_DIR:-$BASE/target}
  log "building $bins in $dir"
  (cd "$dir" && CARGO_TARGET_DIR=$target cargo build --release --locked "$@")
  for b in $bins; do install -m 0755 "$target/release/$b" "$BIN_DIR/$b"; done
}
[ -x "$BIN_DIR/paced_feeder" ] || build "$BASE/ASAPQuery/asap-tools/data-sources/paced-feeder" paced_feeder
if [[ ",$ARMS," == *",asap,"* ]]; then
  if [ ! -x "$BIN_DIR/data_plane" ] || [ ! -x "$BIN_DIR/control_plane" ]; then
    build "$BACKEND_DIR" "data_plane control_plane" -p data_plane -p control_plane
  fi
fi

# 2. Monitoring.
python3 "$MON/grafana/make_dashboard.py" >/dev/null
MONITORING=${MONITORING:-}
if [ -z "$MONITORING" ]; then
  if sudo -n docker info >/dev/null 2>&1; then MONITORING=docker; else MONITORING=binaries; fi
fi
case $MONITORING in
  docker) sudo -n -E docker compose -f "$MON/docker-compose.yml" up -d
          # Monitoring on the auxiliary cores, off the arms' cpusets.
          [ -n "$AUX_CPUS" ] && sudo -n docker update --cpuset-cpus "$AUX_CPUS" \
            $(sudo -n docker compose -f "$MON/docker-compose.yml" ps -q) >/dev/null ;;
  binaries) "$MON/run_binaries.sh" start
          if [ -n "$AUX_CPUS" ]; then
            for f in "${MON_DATA:-$MON/data}"/pids/*; do
              taskset -apc "$AUX_CPUS" "$(cat "$f")" >/dev/null 2>&1 || sudo -n taskset -apc "$AUX_CPUS" "$(cat "$f")" >/dev/null 2>&1 || true
            done
          fi ;;
  none) ;;
esac
log "monitoring: $MONITORING (Grafana http://127.0.0.1:${GRAFANA_PORT:-13000}/d/asap-vs-vm)"

# 3. Each workload: prepare, run, report.
cd "$BASE/harness"
for SERIES_N in $SERIES; do
LABEL0=$(( SERIES_N / 200 ))
for MODE in $MODES; do
for w in $WORKLOADS; do
  set_name=${w%%/*}; workload=${w#*/}
  suffix=""; range_arg=()
  if [ "$MODE" = range ]; then suffix=".range-${RANGE//:/-}"; range_arg=(--range "$RANGE"); fi
  P=$BASE/prep/$set_name.$workload.s$SERIES_N$suffix
  R=$BASE/runs/$RUN_TAG.$set_name.$workload.s$SERIES_N$suffix
  mkdir -p "$(dirname "$R")"
  log "prepare $w ($SERIES_N series)"
  skip_plan=(); [[ ",$ARMS," == *",asap,"* ]] || skip_plan=(--skip-plan)
  python3 prepare.py --set "$set_name" --workload "$workload" --label0 "$LABEL0" --out "$P" "${skip_plan[@]}" "${range_arg[@]}" \
    > "$R.prepare.log" 2>&1 || { log "prepare failed for $w: see $R.prepare.log"; continue; }
  if [[ ",$ARMS," == *",asap,"* ]] && [ ! -f "$P/backend/plan.json" ]; then
    log "no joint plan for $w (see $P/backend/plan_joint_summary.json); skipping"; continue
  fi
  if [ "$PROFILE" = 1 ]; then
    ${AUX_CPUS:+taskset -c "$AUX_CPUS"} "$BASE/profile/profile_query_phase.sh" "$R" "$PROFILE_SECONDS" "victoriametrics asap" > "$R.profile.log" 2>&1 &
    PROFILER=$!
  fi
  log "run $w ($MODE): arms $ARMS, query phase ${QUERY_PHASE_S}s"
  conc=(); [ "$CONCURRENT" = 1 ] && conc=(--concurrent)
  run_from_ms=$(( $(date +%s) * 1000 ))
  python3 run.py --prep "$P" --out "$R" --arms "$ARMS" --query-phase-s "$QUERY_PHASE_S" "${conc[@]}" \
    --arm-cores "$ARM_CORES" ${ARM_CPUS:+--arm-cpus "$ARM_CPUS"} \
    --metrics-port "$HARNESS_METRICS_PORT" --metrics-linger-s 15 > "$R.log" 2>&1 || log "run.py failed: see $R.log"
  [ "$PROFILE" = 1 ] && { kill "$PROFILER" 2>/dev/null || true; }
  if [[ ",$ARMS," == *",asap,"* && ",$ARMS," == *",victoriametrics,"* ]]; then
    # Ground truth from VictoriaMetrics: its answers, and for rate/increase its
    # untimed rate_prometheus/increase_prometheus on the kept data (report.py reads it).
    python3 accuracy_vs_vm.py "$R" "$P" "${VICTORIAMETRICS_BIN:-$BIN_DIR/victoria-metrics-${VICTORIAMETRICS_VERSION:-v1.156.0}}" \
      > "$R.accuracy_vs_vm.txt" 2>&1 || log "accuracy_vs_vm.py failed: see $R.accuracy_vs_vm.txt"
  fi
  # Compression (bits per sample): VictoriaMetrics' disk bytes per sample and a Gorilla
  # reference codec over the generated data (its kept data); before the unmount below.
  python3 compression.py "$R" "${VICTORIAMETRICS_BIN:-$BIN_DIR/victoria-metrics-${VICTORIAMETRICS_VERSION:-v1.156.0}}" \
    > "$R.compression.txt" 2>&1 || log "compression.py failed: see $R.compression.txt"
  python3 report.py --prep "$P" --run "$R" > "$R.summary.txt" 2>&1 || log "report.py failed: see $R.summary.txt"
  python3 demo_table.py --prep "$P" --run "$R" --textfile-dir "$MON/textfile" \
    | tee "$R.demo_table.txt" || true
  # Disk limits (run.py: each engine's data dir is its own loop-mounted filesystem of the
  # planner's disk limit): unmount them now that the reference and reports are done; the
  # images stay in the run directory (<data dir>.img).
  for m in $(findmnt -rn -o TARGET | grep "^$R/" || true); do sudo -n umount "$m" || log "umount $m failed"; done
  if [ "${SCREENSHOTS:-0}" = 1 ] && [ "$MONITORING" != none ]; then
    # Every panel of the dashboard (and the whole page) over this run's time range.
    AUX_CPUS=$AUX_CPUS "$MON/screenshot.sh" asap-vs-vm "$run_from_ms" "$(( $(date +%s) * 1000 ))" \
      "$R/screenshots" "run=$(basename "$R")" > "$R.screenshots.log" 2>&1 || log "screenshots failed: see $R.screenshots.log"
  fi
  log "done $w ($MODE): $R"
done
done
done
log VM_DEMO_DONE
