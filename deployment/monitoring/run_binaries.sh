#!/bin/bash
# The monitoring stack of docker-compose.yml without docker: the same
# configs, run from the pinned release binaries in $BIN_DIR (fetched by
# eval/scripts/fetch_binaries.sh when missing). State lives in $MON_DATA.
#
#   deployment/monitoring/run_binaries.sh start|stop|status
#
# process-exporter reads every process' /proc; run as a user who can (root
# via sudo -n when available, else the processes of this user only).
set -euo pipefail
BASE=${ASAPQ_BENCH:?set ASAPQ_BENCH}
BIN_DIR=${BIN_DIR:?set BIN_DIR}
MON=$BASE/../deployment/monitoring   # this repository's deployment/monitoring
MON_DATA=${MON_DATA:-$MON/data}
GRAFANA_PORT=${GRAFANA_PORT:-13000}
PIDS=$MON_DATA/pids
mkdir -p "$MON_DATA" "$PIDS" "$MON/textfile"

start_one() {  # name, command...
  local name=$1; shift
  if [ -f "$PIDS/$name" ] && kill -0 "$(cat "$PIDS/$name")" 2>/dev/null; then
    echo "$name already running"; return
  fi
  nohup "$@" > "$MON_DATA/$name.log" 2>&1 &
  echo $! > "$PIDS/$name"
  echo "started $name (pid $!)"
}

case "${1:-start}" in
start)
  BIN_DIR=$BIN_DIR "$BASE/scripts/fetch_binaries.sh" prometheus node_exporter process_exporter grafana
  start_one prometheus "$BIN_DIR/prometheus-3.5.0" --config.file="$MON/prometheus/prometheus.yml" \
    --storage.tsdb.path="$MON_DATA/prometheus" --storage.tsdb.retention.time=30d \
    --web.listen-address=127.0.0.1:19090
  start_one node_exporter "$BIN_DIR/node_exporter-1.12.1" --web.listen-address=127.0.0.1:19091 \
    --collector.textfile.directory="$MON/textfile"
  SUDO=""; if sudo -n true 2>/dev/null; then SUDO="sudo -n"; fi
  start_one process_exporter $SUDO "$BIN_DIR/process-exporter-0.8.7" --config.path="$MON/process-exporter/config.yml" \
    --web.listen-address=127.0.0.1:19092 --threads=false
  G=$BIN_DIR/grafana-12.4.14
  mkdir -p "$MON_DATA/grafana"
  start_one grafana env GF_SERVER_HTTP_PORT="$GRAFANA_PORT" GF_AUTH_ANONYMOUS_ENABLED=true \
    GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer GF_PATHS_DATA="$MON_DATA/grafana" GF_PATHS_LOGS="$MON_DATA/grafana" \
    GF_PATHS_PROVISIONING="$MON/grafana/provisioning" ASAP_DASHBOARDS_DIR="$MON/grafana/dashboards" \
    GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH="$MON/grafana/dashboards/asap-vs-vm.json" \
    "$G/bin/grafana" server --homepath "$G"
  echo "Grafana: http://127.0.0.1:$GRAFANA_PORT/d/asap-vs-vm"
  ;;
stop)
  for f in "$PIDS"/*; do
    [ -f "$f" ] || continue
    pid=$(cat "$f")
    kill "$pid" 2>/dev/null || sudo -n kill "$pid" 2>/dev/null || true
    rm -f "$f"; echo "stopped $(basename "$f")"
  done
  ;;
status)
  for f in "$PIDS"/*; do
    [ -f "$f" ] || continue
    if kill -0 "$(cat "$f")" 2>/dev/null || sudo -n kill -0 "$(cat "$f")" 2>/dev/null; then s=up; else s=down; fi
    echo "$(basename "$f") $s"
  done
  ;;
*) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
