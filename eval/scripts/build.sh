#!/bin/bash
# Build the paced feeder (with campaign/paced-feeder-realistic.diff applied, for
# FEEDER_VALUES=realistic), this repository's data_plane and control_plane, and
# optionally ASAPQuery's query engine, and install the binaries into $BIN_DIR.
#   ASAPQ_BENCH, BIN_DIR   required (see eval/README.md)
#   ASAP_PLANNER_DIR       optional: also build the Planner's random-workload example
#   ASAPQUERY_ENGINE=1     also build ASAPQuery's query_engine_rust (the harness's asapquery arm)
set -euo pipefail
: "${ASAPQ_BENCH:?set ASAPQ_BENCH}" "${BIN_DIR:?set BIN_DIR}"
[ -n "${BENCH_ENV:-}" ] && [ -f "$BENCH_ENV" ] && source "$BENCH_ENV"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$ASAPQ_BENCH/target}
mkdir -p "$BIN_DIR"
set -x
FEEDER=$ASAPQ_BENCH/ASAPQuery/asap-tools/data-sources
patch -p0 -N -s -d "$FEEDER" < "$ASAPQ_BENCH/campaign/paced-feeder-realistic.diff" || true   # already applied: skipped
(cd "$FEEDER/paced-feeder" && cargo build --release)
install -m 0755 "$CARGO_TARGET_DIR/release/paced_feeder" "$BIN_DIR/"
(cd "$ASAPQ_BENCH/backend" && cargo build --release -p control_plane -p data_plane)
install -m 0755 "$CARGO_TARGET_DIR/release/data_plane" "$CARGO_TARGET_DIR/release/control_plane" "$BIN_DIR/"
if [ "${ASAPQUERY_ENGINE:-0}" = 1 ]; then
  (cd "$ASAPQ_BENCH/ASAPQuery" && cargo build --release -p query_engine_rust --bin query_engine_rust)
  install -m 0755 "$CARGO_TARGET_DIR/release/query_engine_rust" "$BIN_DIR/"
fi
if [ -n "${ASAP_PLANNER_DIR:-}" ]; then
  (cd "$ASAP_PLANNER_DIR" && cargo build --release -p asap-planner --example asapquery_random_workload)
fi
echo BUILD_OK
