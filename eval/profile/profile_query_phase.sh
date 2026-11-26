#!/bin/bash
# CPU-profile the demo arms during their query phase.
#
#   profile/profile_query_phase.sh RUN_DIR [SECONDS] [ARMS]
#
# Start it next to run.py (run_vm_demo.sh does with PROFILE=1). For each arm
# in ARMS (default "victoriametrics asap") it waits for RUN_DIR/<arm>/state.json
# to say the query phase started (run.py writes it), waits PROFILE_OFFSET_S
# (default 10), then profiles the arm's engine for SECONDS (default 60):
#
#   victoriametrics  Go pprof from /debug/pprof/profile?seconds=N (top via
#                    `go tool pprof` when Go is installed, else the raw profile
#                    only), and perf record (frame pointers) -> flamegraph
#   asap             the backend's benchmarks/scripts/profile.sh when $BACKEND_DIR has it
#                    (ASAP_PROFILE_CMD: its command line, with {pid} {seconds}
#                    {out}), else perf record (DWARF call graphs) -> flamegraph
#
# Output: RUN_DIR/<arm>/profile/{top.txt,flamegraph.svg,perf.data,cpu.pprof,...}
set -uo pipefail
RUN_DIR=${1:?usage: $0 RUN_DIR [SECONDS] [ARMS]}
SECS=${2:-60}
ARMS=${3:-victoriametrics asap}
OFFSET=${PROFILE_OFFSET_S:-10}
BASE=${ASAPQ_BENCH:?set ASAPQ_BENCH}
BIN_DIR=${BIN_DIR:?set BIN_DIR}
BACKEND_DIR=${BACKEND_DIR:-}
ASAP_PROFILE_CMD=${ASAP_PROFILE_CMD:-'{script} --pid {pid} --seconds {seconds} --out {out}'}

FG=${FLAMEGRAPH_DIR:-}
for d in "${FG:-}" "${FLAMEGRAPH_DIR:-}"; do
  [ -n "$d" ] && [ -x "$d/flamegraph.pl" ] && { FG=$d; break; }
done
if [ ! -x "$FG/flamegraph.pl" ]; then
  BIN_DIR=$BIN_DIR "$BASE/fetch_binaries.sh" flamegraph >/dev/null && FG=$BIN_DIR/FlameGraph-41fee1f
fi
GO=${GO:-$(command -v go || true)}
for g in "${GO_BIN:-}" "$(command -v go || true)"; do [ -z "$GO" ] && [ -x "$g" ] && GO=$g; done

PERF=perf
if [ "$(cat /proc/sys/kernel/perf_event_paranoid)" -gt 1 ] && sudo -n true 2>/dev/null; then PERF="sudo -n perf"; fi

state() {  # arm key -> value from state.json
  python3 -c 'import json,sys; s=json.load(open(sys.argv[1])); v=s; [v:=v[k] for k in sys.argv[2:]]; print(v)' \
    "$RUN_DIR/$1/state.json" "${@:2}" 2>/dev/null
}

wait_query_phase() {
  local arm=$1
  echo "$(date -u +%T) $arm: waiting for its query phase"
  until [ "$(state "$arm" phase)" = query ]; do
    [ "$(state "$arm" phase)" = done ] && return 1
    sleep 2
  done
  sleep "$OFFSET"
}

perf_flame() {  # pid out callgraph
  local pid=$1 out=$2 cg=$3
  $PERF record -F 199 --call-graph "$cg" -p "$pid" -o "$out/perf.data" -- sleep "$SECS" >"$out/perf-record.log" 2>&1
  $PERF report -i "$out/perf.data" --stdio --no-children --percent-limit 0.5 >"$out/top.txt" 2>/dev/null
  if [ -x "$FG/stackcollapse-perf.pl" ]; then
    $PERF script -i "$out/perf.data" 2>/dev/null | "$FG/stackcollapse-perf.pl" >"$out/stacks.folded"
    "$FG/flamegraph.pl" --title "$(basename "$(dirname "$out")") query phase, ${SECS}s" \
      "$out/stacks.folded" >"$out/flamegraph.svg"
  fi
  [ "$PERF" = perf ] || sudo -n chown -R "$(id -u):$(id -g)" "$out"
}

profile_vm() {
  local out=$RUN_DIR/victoriametrics/profile pid url
  mkdir -p "$out"
  pid=$(state victoriametrics pids victoria-metrics); url=$(state victoriametrics query_url)
  echo "$(date -u +%T) victoriametrics: pprof + perf for ${SECS}s (pid $pid)"
  curl -sf "$url/debug/pprof/profile?seconds=$SECS" -o "$out/cpu.pprof" &
  local curl_pid=$!
  perf_flame "$pid" "$out" fp
  wait $curl_pid
  if [ -n "$GO" ] && [ -s "$out/cpu.pprof" ]; then
    "$GO" tool pprof -top -nodecount=60 "$out/cpu.pprof" >"$out/pprof-top.txt" 2>&1
    "$GO" tool pprof -top -cum -nodecount=60 "$out/cpu.pprof" >"$out/pprof-top-cum.txt" 2>&1
    "$GO" tool pprof -raw "$out/cpu.pprof" >"$out/pprof-raw.txt" 2>&1
  else
    echo "no Go toolchain: cpu.pprof kept raw (go tool pprof -top cpu.pprof)" >"$out/pprof-top.txt"
  fi
}

profile_asap() {
  local out=$RUN_DIR/asap/profile pid script
  mkdir -p "$out"
  pid=$(state asap pids data_plane)
  script=${BACKEND_DIR:+$BACKEND_DIR/benchmarks/scripts/profile.sh}
  if [ -n "$script" ] && [ -x "$script" ]; then
    local cmd=${ASAP_PROFILE_CMD//\{script\}/$script}
    cmd=${cmd//\{pid\}/$pid}; cmd=${cmd//\{seconds\}/$SECS}; cmd=${cmd//\{out\}/$out}
    echo "$(date -u +%T) asap: $cmd"
    bash -c "$cmd" >"$out/profile.sh.log" 2>&1
  else
    echo "$(date -u +%T) asap: perf (DWARF) for ${SECS}s (pid $pid)"
    perf_flame "$pid" "$out" dwarf,16384
  fi
}

for arm in $ARMS; do
  wait_query_phase "$arm" || { echo "$arm finished before its query phase was seen"; continue; }
  case $arm in
    victoriametrics) profile_vm ;;
    asap) profile_asap ;;
    *) pid=$(python3 -c 'import json,sys; print(next(iter(json.load(open(sys.argv[1]))["pids"].values())))' "$RUN_DIR/$arm/state.json")
       mkdir -p "$RUN_DIR/$arm/profile"; perf_flame "$pid" "$RUN_DIR/$arm/profile" dwarf,16384 ;;
  esac
  echo "$(date -u +%T) $arm: profile in $RUN_DIR/$arm/profile"
done
