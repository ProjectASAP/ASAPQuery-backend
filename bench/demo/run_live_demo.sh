#!/bin/bash
# Live Grafana demo: a paced feeder (real time, continuous) sends the same
# samples by Remote Write to the ASAP data plane (`data_plane --no-forward`,
# installing ASAPPlanner's plan of the live dashboard workload, planned with
# range_evaluation), VictoriaMetrics single-node (no stream aggregation) and
# Prometheus 3.5 (the exact reference). Each runs on its own cpuset. Grafana
# queries each through a logging pass-through (live/proxy.py) that records
# every request's start/end/step and status.
#
#   ./run_live_demo.sh start      prepare (plan once), start everything, wire up monitoring
#   ./run_live_demo.sh stop       stop the live processes (data and views kept)
#   ./run_live_demo.sh status     processes, ingestion, and Grafana requests answered/refused
#   ./run_live_demo.sh verify     per data source and query: requests, statuses, start/end/step seen
#   ./run_live_demo.sh compare    ASAP and VictoriaMetrics vs Prometheus on the last full hour's grid
#   ./run_live_demo.sh browse [--minutes N]  headless browser keeps DASHBOARD [asap-demo] open; screenshots in live/screenshots
#   ./run_live_demo.sh restart-vm      restart VictoriaMetrics (VM_VERSION), same flags and data; resumes its feeder
#   ./run_live_demo.sh resume-feeders  restart any feeder that stopped (leaves a gap, annotated)
#   ./run_live_demo.sh load on [N]|off  start/stop the query load: N simulated users [LOAD_USERS or 8],
#                                   each sending every query at its interval (staggered) to every engine
#                                   that runs it, no throttling; annotated on the dashboards
#   ./run_live_demo.sh watchdog   start the detached watchdog (start does it): every 30 s, restarts any
#                                 component whose process died (feeders from the next second, annotated)
#   ./run_live_demo.sh pin        re-apply the cpusets below to the running processes
#   ./run_live_demo.sh reset      stop and delete the live data (TSDBs, views)
#
# Environment (defaults in brackets):
#   ASAPQ_BENCH  BIN_DIR  (required; see bench/demo/README.md)
#   SERIES       input series per metric, a multiple of 200 [10000]
#   WORKLOAD     demo workload under workloads/ [dashboard-live]; asap-demo for the presentation
#                dashboard (uid asap-demo; metrics lat and reqs)
#   RANGE        STEP_S:SPAN_S:REFRESH_S the plan's range_evaluation and the panels' step/span [15:3600:15]
#   ASAP_CPUS VM_CPUS PROM_CPUS AUX_CPUS   cpusets [24-35, 36-47, 48-51, 48-55]: 12 cores per engine
#                (the feeders and the proxies run on AUX_CPUS)
#   LIMITS=1     both engines under identical enforced limits (cgroup cpu.max + cpuset, memory.max
#                with page cache, fixed-size filesystem): LIMIT_CORES [12], LIMIT_MEMORY_BYTES and
#                LIMIT_DISK_BYTES [the planner's targets in live/planner/cost_inputs.json]
#   EXACT_STEP_S step of the Prometheus (exact) panels when not the plan's, e.g. 300 at 10k series,
#                where Prometheus cannot evaluate every 15 s step of a 1 h range within its 2 min timeout
#                [300 from 10000 series up, else the plan step]
#   REPLAN=1     plan again
#   REPORT_DATA  the final experiment's report_data.json (report_demo/build.py) shown in the
#                presentation dashboard's Resources and Scale rows [$ASAPQ_BENCH/report_demo/final/report_data.json] even if the live prep directory has a plan
# Ports: data plane 19711, VictoriaMetrics 19712, Prometheus 19713; their
# proxies (Grafana's data sources) 19721, 19722, 19723.
set -euo pipefail
[ -f $BENCH_ENV ] && source $BENCH_ENV
export ASAPQ_BENCH=${ASAPQ_BENCH:?set ASAPQ_BENCH}
export BIN_DIR=${BIN_DIR:?set BIN_DIR}
BASE=$ASAPQ_BENCH
LIVE=$BASE/live
SERIES=${SERIES:-10000}
WORKLOAD=${WORKLOAD:-dashboard-live}
RANGE=${RANGE:-15:3600:15}
QUERY_DELAY_S=${QUERY_DELAY_S:-6}   # panels query up to now - this (and VictoriaMetrics' latency offset is 1 s less)
VM_VERSION=${VM_VERSION:-1.156.0}   # VictoriaMetrics single-node release (fetch_binaries.sh pins it)
EXACT=${EXACT:-0}   # 1: also run Prometheus 3.5 (the exact column of asap-vs-vm-live); off by default
# LIMITS=1: both engines under the same enforced limits (harness/cgroup_limits.sh): cpu.max of
# LIMIT_CORES cores plus the engine's cpuset, memory.max (page cache included), and a fixed-size
# filesystem of LIMIT_DISK_BYTES for its data. Defaults: the planner's targets (live/planner).
# (|| true: under set -e/pipefail a missing limits.env must not end the script.)
LIMITS=${LIMITS:-$(sed -n 's/^LIMITS=//p' "$LIVE/limits.env" 2>/dev/null || true)}
LIMITS=${LIMITS:-0}
LIMIT_CORES=${LIMIT_CORES:-12}
LIMIT_MEMORY_BYTES=${LIMIT_MEMORY_BYTES:-$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["cost_model"]["cluster_capacity"]["memory_bytes"])' "$LIVE/planner/cost_inputs.json" 2>/dev/null || echo 68719476736)}
LIMIT_DISK_BYTES=${LIMIT_DISK_BYTES:-$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["cost_model"]["cluster_capacity"]["disk_bytes"])' "$LIVE/planner/cost_inputs.json" 2>/dev/null || echo 214748364800)}
CGL="$BASE/harness/cgroup_limits.sh"
# The ingestion exporter's probes (live/ingest_exporter.py): a gauge, a planned per-series query, and a
# planned sample count over 5 min (empty: no visible-fraction probe, e.g. a workload with no such query).
INGEST_GAUGE=${INGEST_GAUGE:-lat}
INGEST_SERIES_QUERY=${INGEST_SERIES_QUERY-max_over_time(lat[30m])}
INGEST_COUNT_QUERY=${INGEST_COUNT_QUERY-sum(count_over_time(lat[5m]))}
DATA_PLANE_ARGS=${DATA_PLANE_ARGS:-}   # extra data_plane flags, e.g. "--precompute-threads 12"
REPORT_DATA=${REPORT_DATA:-$BASE/report_demo/final/report_data.json}
ASAP_CPUS=${ASAP_CPUS:-24-35}
VM_CPUS=${VM_CPUS:-36-47}
PROM_CPUS=${PROM_CPUS:-48-51}
AUX_CPUS=${AUX_CPUS:-48-55}
P_ASAP=19711; P_VM=19712; P_PROM=19713; P_REF=19714
REF_CPUS=${REF_CPUS:-16-23}   # the reference VictoriaMetrics (accuracy truth; no query load)
X_ASAP=19721; X_VM=19722; X_PROM=19723
PREP=$LIVE/prep-$WORKLOAD-s$SERIES
DATA=$LIVE/data
if [ "$LIMITS" = 1 ]; then
  ASAP_VIEW_DIR=$LIVE/mnt/asap/views; VM_DATA_DIR=$LIVE/mnt/victoriametrics/data
else
  ASAP_VIEW_DIR=$DATA/asap-views; VM_DATA_DIR=$DATA/victoria-metrics
fi
LOGS=$LIVE/logs
PIDS=$LIVE/pids
COMPOSE="$BASE/monitoring/docker-compose.yml"
mkdir -p "$DATA" "$LOGS" "$PIDS"

log() { echo "$(date -u +%FT%TZ) $*"; }

# Feeder tables: the prep's shards feeder_table.<i>.json (prepare.py FEEDER_SHARDS; one feeder per
# shard per sink, as in the campaign's harness), else the whole feeder_table.json.
feeder_tables() { ls "$PREP"/feeder_table.[0-9]*.json 2>/dev/null | sort -t. -k2 -n || true; }
feeder_roles() {  # sink -> "role table" lines: feeder-<sink> (one table) or feeder-<sink>-<i> (shards)
  local sink=$1 t
  if [ -n "$(feeder_tables)" ]; then
    for t in $(feeder_tables); do
      echo "feeder-$sink-$(basename "$t" .json | cut -d. -f2) $t"
    done
  else
    echo "feeder-$sink $PREP/feeder_table.json"
  fi
}
start_feeders() {  # sink port, start ms: every feeder of that sink (same seed and start: identical samples)
  local sink=$1 start_ms=$2 role table
  while read -r role table; do
    spawn "$role" "$BIN_DIR/paced_feeder" --sink remote-write --url "http://127.0.0.1:$sink/api/v1/write" \
      --workload-table "$table" --seed 862 --duration-secs 2592000 --start-ms "$start_ms"
  done < <(feeder_roles "$sink")
}

cpus_of() {  # role -> cpuset
  case $1 in
    data_plane) echo "$ASAP_CPUS" ;; victoria-metrics) echo "$VM_CPUS" ;; prometheus) echo "$PROM_CPUS" ;;
    reference-vm) echo "$REF_CPUS" ;;
    *) echo "$AUX_CPUS" ;;
  esac
}

spawn() {  # role, command...
  local role=$1; shift
  if [ -f "$PIDS/$role" ] && kill -0 "$(cat "$PIDS/$role")" 2>/dev/null; then log "$role already running"; return; fi
  # The command (with its environment and cpuset) is kept, so the watchdog can start it again.
  mkdir -p "$LIVE/cmds"
  { printf 'RUST_LOG=%q ' "${RUST_LOG:-warn}"; printf '%q ' taskset -c "$(cpus_of "$role")" "$@"; echo; } > "$LIVE/cmds/$role"
  setsid nohup env RUST_LOG="${RUST_LOG:-warn}" taskset -c "$(cpus_of "$role")" "$@" >>"$LOGS/$role.log" 2>&1 < /dev/null &
  echo $! > "$PIDS/$role"
  place_limits "$role" $!
  log "started $role (pid $!, cpus $(cpus_of "$role"))"
}

place_limits() {  # role pid: put an engine into its limits cgroup when LIMITS=1
  [ "$LIMITS" = 1 ] || return 0
  case $1 in
    data_plane) "$CGL" add asap "$2" ;;
    victoria-metrics) "$CGL" add victoriametrics "$2" ;;
  esac
}

setup_limits() {
  [ "$LIMITS" = 1 ] || return 0
  echo "LIMITS=1" > "$LIVE/limits.env"
  "$CGL" create asap "$ASAP_CPUS" "$LIMIT_CORES" "$LIMIT_MEMORY_BYTES"
  "$CGL" create victoriametrics "$VM_CPUS" "$LIMIT_CORES" "$LIMIT_MEMORY_BYTES"
  "$CGL" disk asap "$LIVE/disk/asap.img" "$LIVE/mnt/asap" "$LIMIT_DISK_BYTES"
  "$CGL" disk victoriametrics "$LIVE/disk/victoriametrics.img" "$LIVE/mnt/victoriametrics" "$LIMIT_DISK_BYTES"
  mkdir -p "$ASAP_VIEW_DIR" "$VM_DATA_DIR"
}

respawn() {  # role: start a dead component again from its kept command. A feeder starts from the
  # next whole second (the paced feeder cannot backfill), leaving an annotated gap.
  local role=$1 cmd
  cmd=$(cat "$LIVE/cmds/$role" 2>/dev/null) || return
  if [[ $role == feeder-* ]]; then
    local next_ms=$(( ($(date +%s) + 2) * 1000 ))
    cmd=$(sed -E "s/--start-ms [0-9]+/--start-ms $next_ms/" <<<"$cmd")
    echo "$cmd" > "$LIVE/cmds/$role"
  fi
  setsid nohup bash -c "exec env $cmd" >>"$LOGS/$role.log" 2>&1 < /dev/null &
  echo $! > "$PIDS/$role"
  place_limits "$role" $!
  log "watchdog: restarted $role (pid $!)"
  annotate "watchdog restarted $role$([[ $role == feeder-* ]] && echo ' (samples missing until now)')"
}

watchdog_loop() {  # every 30 s, restart any component whose pid file names a dead process
  while true; do
    for f in "$PIDS"/*; do
      [ -f "$f" ] || continue
      kill -0 "$(cat "$f")" 2>/dev/null || respawn "$(basename "$f")"
    done
    sleep 30
  done
}

wait_http() {  # url
  for _ in $(seq 240); do curl -sf -o /dev/null "$1" && return 0; sleep 0.5; done
  log "not ready: $1"; return 1
}

stop_all() {
  if [ -f "$LIVE/watchdog.pid" ]; then kill "$(cat "$LIVE/watchdog.pid")" 2>/dev/null || true; rm -f "$LIVE/watchdog.pid"; fi
  for f in "$PIDS"/*; do
    [ -f "$f" ] || continue
    pid=$(cat "$f")
    if kill -0 "$pid" 2>/dev/null; then kill "$pid"; log "stopping $(basename "$f") ($pid)"; fi
  done
  # Wait for each to exit (VictoriaMetrics and Prometheus flush on SIGTERM).
  for f in "$PIDS"/*; do
    [ -f "$f" ] || continue
    pid=$(cat "$f")
    for _ in $(seq 60); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid"
    rm -f "$f"
  done
}

annotate() {  # text: a Grafana annotation (all dashboards) at now, e.g. a restart
  curl -sf -u "${GRAFANA_AUTH:-admin:admin}" -H 'Content-Type: application/json' \
    -X POST "http://127.0.0.1:${GRAFANA_PORT:-13000}/api/annotations" \
    -d "$(python3 -c 'import json,sys,time; print(json.dumps({"time": int(time.time()*1000), "tags": ["live"], "text": sys.argv[1]}))' "$1")" \
    >/dev/null || log "annotation not posted: $1"
}

start_reference() {  # The reference VictoriaMetrics and its own feeder (identical samples; from the next
  # whole second when added to a running run, so it has the history from then on).
  local start_ms=${1:-$(( ($(date +%s) + 2) * 1000 ))}
  echo "$start_ms" > "$LIVE/reference_start_ms"
  spawn reference-vm "$BIN_DIR/victoria-metrics-v$VM_VERSION" -storageDataPath="$DATA/reference-vm" \
    -httpListenAddr=127.0.0.1:$P_REF -retentionPeriod=2d -search.latencyOffset=${QUERY_DELAY_S}s \
    -search.maxQueryDuration=600s -search.maxSamplesPerQuery=100000000000 \
    -search.maxSamplesPerSeries=1000000000 -loggerLevel=WARN
  wait_http http://127.0.0.1:$P_REF/health
  start_feeders $P_REF "$start_ms"
}

start_accuracy() {
  spawn accuracy python3 "$LIVE/accuracy.py" --asap http://127.0.0.1:$P_ASAP --vm http://127.0.0.1:$P_VM \
    --reference http://127.0.0.1:$P_REF --reference-start-ms "$(cat "$LIVE/reference_start_ms" 2>/dev/null || echo 0)" \
    --queries "$PREP/queries.json" --behind-s "$QUERY_DELAY_S" \
    --textfile "$BASE/monitoring/textfile/asap_demo.prom" --report-data "$REPORT_DATA"
}

resume_feeder() {  # sink port: restart its feeders that stopped (a feeder stops when every request
  # of a tick fails, e.g. while its engine restarts). The paced feeder sends tick i at its start
  # plus i seconds of wall clock, so it cannot backfill: it restarts at the next whole second, and
  # the engine keeps a gap from the failed tick to then (annotated). Counters restart from the
  # generator's first values there, which the engines read as a counter reset.
  local sink=$1 role table f start_ms tick next_ms
  while read -r role table; do
    f="$PIDS/$role"
    [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null && continue
    [ -f "$LOGS/$role.log" ] || continue
    start_ms=$(cat "$LIVE/stream_start_ms")
    tick=$(grep -oE "^tick [0-9]+: every request failed" "$LOGS/$role.log" | tail -1 | grep -oE "[0-9]+" || true)
    [ -n "$tick" ] || continue
    rm -f "$f"
    next_ms=$(( ($(date +%s) + 2) * 1000 ))
    spawn "$role" "$BIN_DIR/paced_feeder" --sink remote-write --url "http://127.0.0.1:$sink/api/v1/write" \
      --workload-table "$table" --seed 862 --duration-secs 2592000 --start-ms "$next_ms"
    annotate "$role (port $sink) restarted: no samples from $(date -u -d @$(( (start_ms / 1000) + tick )) +%T) to $(date -u -d @$((next_ms / 1000)) +%T) UTC"
  done < <(feeder_roles "$sink")
}

prepare() {
  if [ -f "$PREP/backend/plan.json" ] && [ "${REPLAN:-0}" != 1 ]; then
    log "plan: $PREP/backend/plan.json (kept; REPLAN=1 to plan again)"; return
  fi
  (( SERIES % 200 == 0 )) || { echo "SERIES must be a multiple of 200" >&2; exit 2; }
  log "planning $WORKLOAD at $SERIES series with range_evaluation $RANGE"
  (cd "$BASE/harness" && python3 prepare.py --set demo --workload "$WORKLOAD" --label0 $((SERIES / 200)) \
     --range "$RANGE" --out "$PREP") > "$LOGS/prepare.log" 2>&1
  [ -f "$PREP/backend/plan.json" ] || { log "planning failed: $LOGS/prepare.log"; exit 1; }
  python3 -c 'import json,sys; j=json.load(open(sys.argv[1])); print("planning (offline, one-time): %.2f s wall, %.2f core-s, peak RSS %.0f MiB" % (j["planning_seconds"], j["planning_cpu_seconds"], j["planning_peak_rss_bytes"]/2**20))' \
    "$PREP/backend/plan_joint_summary.json"
}

monitoring() {
  # Live scrape jobs and data sources are in the shared monitoring config;
  # reload the monitoring Prometheus, and restart Grafana only when its
  # provisioned live data sources are not there yet.
  if ! sudo -n docker compose -f "$COMPOSE" ps -q prometheus >/dev/null 2>&1; then
    log "monitoring compose not running: start it with sudo -n -E docker compose -f $COMPOSE up -d"; return
  fi
  # The config is a single-file bind mount: a replaced file is only seen
  # after a restart, so restart when the live jobs are not scraped yet.
  if ! curl -s http://127.0.0.1:19090/api/v1/targets | grep -q '"job":"live_limits"'; then
    sudo -n docker compose -f "$COMPOSE" restart prometheus >/dev/null 2>&1
  fi
  if ! curl -sf -u "${GRAFANA_AUTH:-admin:admin}" http://127.0.0.1:${GRAFANA_PORT:-13000}/api/datasources/uid/live-asap >/dev/null; then
    log "restarting Grafana to provision the live data sources"
    sudo -n docker compose -f "$COMPOSE" restart grafana >/dev/null
  fi
  # process-exporter groups the live processes apart from harness runs.
  if ! curl -s http://127.0.0.1:19092/metrics | grep -q 'groupname="live-data_plane"'; then
    sudo -n docker compose -f "$COMPOSE" restart process-exporter >/dev/null
  fi
}

start() {
  prepare
  python3 "$BASE/monitoring/grafana/make_live_dashboard.py" --queries "$PREP/queries.json" --shift-s "$QUERY_DELAY_S" \
    --exact-step-s "${EXACT_STEP_S:-$( (( SERIES >= 10000 )) && echo 300 || echo 0)}" \
    $( [ "$EXACT" = 1 ] || echo --no-exact ) >/dev/null
  # The presentation dashboard (ASAP vs VictoriaMetrics only).
  local run_start_ms
  run_start_ms=$(( ($(date +%s) / 60 + 1) * 60 * 1000 ))
  python3 "$BASE/monitoring/grafana/make_demo_dashboard.py" --prep "$PREP" --shift-s "$QUERY_DELAY_S" \
    --planner-dir "$LIVE/planner" --run-start-ms "$run_start_ms" --cores "$LIMIT_CORES" \
    $( [ "$LIMITS" = 1 ] && echo --memory-bytes "$LIMIT_MEMORY_BYTES" --disk-bytes "$LIMIT_DISK_BYTES" ) >/dev/null
  local start_ms=$run_start_ms
  echo "$start_ms" > "$LIVE/stream_start_ms"
  printf 'global:\n  scrape_interval: 1h\n' > "$LIVE/prometheus.yml"
  setup_limits
  sinks="$P_ASAP $P_VM"
  start_reference "$start_ms"
  if [ "$EXACT" = 1 ]; then
  sinks="$sinks $P_PROM"
  spawn prometheus "$BIN_DIR/prometheus-3.5.0" --config.file="$LIVE/prometheus.yml" \
    --storage.tsdb.path="$DATA/prometheus" --storage.tsdb.retention.time=2d \
    --web.listen-address=127.0.0.1:$P_PROM --web.enable-remote-write-receiver \
    --query.max-samples=10000000000 --query.timeout=2m --log.level=warn
  fi
  spawn victoria-metrics "$BIN_DIR/victoria-metrics-v$VM_VERSION" -storageDataPath="$VM_DATA_DIR" \
    -httpListenAddr=127.0.0.1:$P_VM -retentionPeriod=2d -search.latencyOffset=${QUERY_DELAY_S}s \
    -search.maxQueryDuration=600s -search.maxSamplesPerQuery=100000000000 \
    -search.maxSamplesPerSeries=1000000000 -loggerLevel=WARN
  RUST_LOG=warn spawn data_plane "$BIN_DIR/data_plane" --plan "$PREP/backend/plan.json" \
    --prometheus-url http://127.0.0.1:9 --listen 127.0.0.1:$P_ASAP --no-forward \
    --allowed-lateness-ms 2000 --stream-start-ms "$start_ms" --view-dir "$ASAP_VIEW_DIR" $DATA_PLANE_ARGS
  [ "$EXACT" = 1 ] && wait_http http://127.0.0.1:$P_PROM/-/ready
  wait_http http://127.0.0.1:$P_VM/health
  wait_http http://127.0.0.1:$P_ASAP/-/ready
  spawn proxy-asap python3 "$LIVE/proxy.py" --name ASAP --listen 127.0.0.1:$X_ASAP \
    --upstream http://127.0.0.1:$P_ASAP --log "$LOGS/requests-asap.jsonl"
  spawn proxy-vm python3 "$LIVE/proxy.py" --name VictoriaMetrics --listen 127.0.0.1:$X_VM \
    --upstream http://127.0.0.1:$P_VM --log "$LOGS/requests-vm.jsonl"
  [ "$EXACT" = 1 ] && spawn proxy-prometheus python3 "$LIVE/proxy.py" --name "Prometheus (exact)" \
    --listen 127.0.0.1:$X_PROM --upstream http://127.0.0.1:$P_PROM --log "$LOGS/requests-prometheus.jsonl"
  # One feeder per sink (per shard), the same seed and start: identical samples, real time.
  for sink in $sinks; do start_feeders "$sink" "$start_ms"; done
  # Accuracy in the background against the reference VictoriaMetrics, and
  # the final experiment's numbers, for the presentation dashboard.
  start_accuracy
  # Live ingestion metrics: feeder lag and overruns, answer freshness, one series from both engines.
  spawn ingest-exporter python3 "$LIVE/ingest_exporter.py" --listen 127.0.0.1:19724 \
    $(feeder_roles $P_ASAP | while read -r r _; do echo --feeder-log "asap=$LOGS/$r.log"; done) \
    $(feeder_roles $P_VM | while read -r r _; do echo --feeder-log "victoriametrics=$LOGS/$r.log"; done) \
    --gauge "$INGEST_GAUGE" --series-query "$INGEST_SERIES_QUERY" --count-query "$INGEST_COUNT_QUERY" \
    --asap http://127.0.0.1:$P_ASAP --vm http://127.0.0.1:$P_VM --delay-s "$QUERY_DELAY_S" \
    --expected "$( [ -n "$INGEST_COUNT_QUERY" ] && python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print(m["data_facts"]["series"]["leaf_count"] * m["samples_per_sec_per_series"] * 300)' "$PREP/manifest.json" || echo 0)"
  monitoring
  if [ "$LIMITS" = 1 ]; then
    spawn limits-exporter python3 "$LIVE/limits_exporter.py" --listen 127.0.0.1:19726 \
      --engine "asap=/sys/fs/cgroup/asap-limits/asap,$LIVE/mnt/asap,$LOGS/data_plane.log" \
      --engine "victoriametrics=/sys/fs/cgroup/asap-limits/victoriametrics,$LIVE/mnt/victoriametrics,$LOGS/victoria-metrics.log"
  fi
  "$0" watchdog
  # START_LOAD_USERS=N: the query load runs from the start (queries from second 0, as in the plan).
  if [ -n "${START_LOAD_USERS:-}" ]; then "$0" load on "$START_LOAD_USERS"; fi
  annotate "live run started: $WORKLOAD, $SERIES series, VictoriaMetrics v$VM_VERSION, data_plane $(sha256sum "$BIN_DIR/data_plane" | cut -c1-12)"
  log "feed starts at $(date -u -d @$((start_ms / 1000)) +%T) UTC; ASAP panels answer once 1 h span + longest window of data exist"
  log "Grafana: ssh -N -L 13000:127.0.0.1:13000 <run host>, then http://127.0.0.1:13000/d/asap-demo (presentation) or /d/asap-vs-vm-live"
}

status() {
  if [ -f "$LIVE/watchdog.pid" ] && kill -0 "$(cat "$LIVE/watchdog.pid")" 2>/dev/null; then
    echo "watchdog up (pid $(cat "$LIVE/watchdog.pid"))"; else echo "watchdog DOWN"; fi
  for f in "$LIVE"/logs/demo_sequence*.log; do [ -f "$f" ] && echo "$(basename "$f"): $(tail -1 "$f")"; done
  for f in "$PIDS"/*; do
    [ -f "$f" ] || continue
    pid=$(cat "$f"); r=down; kill -0 "$pid" 2>/dev/null && r="up (cpus $(taskset -pc "$pid" | awk '{print $NF}'))"
    echo "$(basename "$f") $pid $r"
  done
  curl -s http://127.0.0.1:$P_ASAP/metrics | grep -E '^asap_(samples_total|precompute_backlog_windows|watermark_lag_seconds|samples_queued)' | head -8 || true
  curl -s http://127.0.0.1:$P_VM/metrics | grep -E '^vm_rows_inserted_total\{type="promremotewrite"\}' || true
  python3 "$LIVE/verify.py" --logs "$LOGS" --since-minutes 10 --brief
}

case "${1:-status}" in
  start) start ;;
  stop) stop_all ;;
  status) status ;;
  verify) python3 "$LIVE/verify.py" --logs "$LOGS" "${@:2}" ;;
  compare) python3 "$LIVE/compare.py" --asap http://127.0.0.1:$P_ASAP --vm http://127.0.0.1:$P_VM \
             --exact http://127.0.0.1:$P_PROM --queries "$PREP/queries.json" "${@:2}" ;;
  browse) # A headless browser holding the dashboard open (auto-refresh), with screenshots.
          [ -d "$LIVE/.pylib/playwright" ] || sudo -n docker run --rm -v "$LIVE:/live" \
            mcr.microsoft.com/playwright/python:v1.49.0-jammy@sha256:70c06141ac9041eedef4a68387ea978ceb090c37bd5b66583353b7fe17b9f3d7 \
            pip install -q --target /live/.pylib playwright==1.49.0
          sudo -n docker run --rm --network host --cpuset-cpus "$AUX_CPUS" -v "$LIVE:/live" -e PYTHONPATH=/live/.pylib \
            mcr.microsoft.com/playwright/python:v1.49.0-jammy@sha256:70c06141ac9041eedef4a68387ea978ceb090c37bd5b66583353b7fe17b9f3d7 \
            python3 /live/browse.py --url "http://127.0.0.1:${GRAFANA_PORT:-13000}/d/${DASHBOARD:-asap-demo}" --out /live/screenshots "${@:2}"
          sudo -n chown -R "$(id -u):$(id -g)" "$LIVE/screenshots" ;;
  restart-vm) # VictoriaMetrics alone (e.g. another release: VM_VERSION=...), same flags and data dir.
          if [ -f "$PIDS/victoria-metrics" ]; then kill "$(cat "$PIDS/victoria-metrics")" 2>/dev/null || true
            while kill -0 "$(cat "$PIDS/victoria-metrics")" 2>/dev/null; do sleep 0.5; done; rm -f "$PIDS/victoria-metrics"; fi
          spawn victoria-metrics "$BIN_DIR/victoria-metrics-v$VM_VERSION" -storageDataPath="$VM_DATA_DIR" \
            -httpListenAddr=127.0.0.1:$P_VM -retentionPeriod=2d -search.latencyOffset=${QUERY_DELAY_S}s \
            -search.maxQueryDuration=600s -search.maxSamplesPerQuery=100000000000 \
            -search.maxSamplesPerSeries=1000000000 -loggerLevel=WARN
          wait_http http://127.0.0.1:$P_VM/health
          resume_feeder $P_VM
          annotate "VictoriaMetrics restarted as v$VM_VERSION (same flags and data directory)" ;;
  resume-feeders) for sink in $P_ASAP $P_VM $P_PROM; do resume_feeder $sink; done ;;
  load) # The query load (the planned queries at their intervals, both engines alike).
          case "${2:-}" in
            on) users=${3:-${LOAD_USERS:-8}}
                spawn query-load python3 "$LIVE/query_load.py" --queries "$PREP/queries.json" \
                  --engine "asap=http://127.0.0.1:$P_ASAP" --engine "victoriametrics=http://127.0.0.1:$P_VM" \
                  --users "$users" --delay-s "$QUERY_DELAY_S" --timeout-s 600 --listen 127.0.0.1:19725 \
                  --log "$LOGS/query-load.jsonl" --accuracy-textfile "$BASE/monitoring/textfile/asap_demo.prom" \
                  --ingest-metrics http://127.0.0.1:19724/metrics
                annotate "query load ON: $users simulated users, every query at its own interval, to every engine that runs it" ;;
            off) if [ -f "$PIDS/query-load" ]; then kill "$(cat "$PIDS/query-load")" 2>/dev/null || true
                   rm -f "$PIDS/query-load"; fi
                 annotate "query load OFF: ingestion only (plus any open dashboards)" ;;
            *) [ -f "$PIDS/query-load" ] && kill -0 "$(cat "$PIDS/query-load")" 2>/dev/null && echo on || echo off ;;
          esac ;;
  restart-accuracy) if [ -f "$PIDS/accuracy" ]; then kill "$(cat "$PIDS/accuracy")" 2>/dev/null || true; rm -f "$PIDS/accuracy"; fi
          start_accuracy ;;
  add-reference) # Add the reference VictoriaMetrics to a running run (history from now on).
          start_reference
          if [ -f "$PIDS/accuracy" ]; then kill "$(cat "$PIDS/accuracy")" 2>/dev/null || true; rm -f "$PIDS/accuracy"; fi
          start_accuracy
          annotate "reference VictoriaMetrics added (accuracy truth; its history starts now)" ;;
  watchdog) # Start the detached watchdog (restarts dead components every 30 s); it survives the session.
          if [ -f "$LIVE/watchdog.pid" ] && kill -0 "$(cat "$LIVE/watchdog.pid")" 2>/dev/null; then
            echo "watchdog running (pid $(cat "$LIVE/watchdog.pid"))"
          else
            setsid nohup "$0" watchdog-loop >>"$LOGS/watchdog.log" 2>&1 < /dev/null &
            echo $! > "$LIVE/watchdog.pid"; echo "watchdog started (pid $!)"
          fi ;;
  watchdog-loop) watchdog_loop ;;
  adopt) # Record the commands of components started before commands were kept (from /proc).
          mkdir -p "$LIVE/cmds"
          for f in "$PIDS"/*; do
            role=$(basename "$f"); pid=$(cat "$f")
            [ -f "$LIVE/cmds/$role" ] || [ ! -r "/proc/$pid/cmdline" ] && continue
            mapfile -d '' argv < "/proc/$pid/cmdline"
            { printf 'RUST_LOG=warn '; printf '%q ' taskset -c "$(cpus_of "$role")" "${argv[@]}"; echo; } > "$LIVE/cmds/$role"
            echo "adopted $role"
          done ;;
  pin) for f in "$PIDS"/*; do role=$(basename "$f"); taskset -apc "$(cpus_of "$role")" "$(cat "$f")" >/dev/null && echo "$role -> $(cpus_of "$role")"; done ;;
  reset) stop_all; rm -rf "$DATA"
         for e in asap victoriametrics; do
           if mountpoint -q "$LIVE/mnt/$e"; then sudo -n umount "$LIVE/mnt/$e"; fi
           rm -f "$LIVE/disk/$e.img"; "$CGL" remove "$e" 2>/dev/null || true
         done
         rm -f "$LIVE/limits.env"; log "live data removed" ;;
  *) echo "usage: $0 start|stop|status|verify|compare|pin|reset" >&2; exit 2 ;;
esac
