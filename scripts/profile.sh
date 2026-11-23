#!/usr/bin/env bash
# Records a CPU profile of a running data_plane and writes a flamegraph.
#
#   scripts/profile.sh [SECONDS] [OUTPUT.svg]
#
# Build with symbols first, so frames have names and stacks are whole:
#   CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release -p data_plane
# Needs `perf` (and perf_event_paranoid <= 1, or root). The flamegraph is
# drawn with inferno (`inferno-collapse-perf`, `inferno-flamegraph`); without
# it, with the FlameGraph scripts (`stackcollapse-perf.pl`, `flamegraph.pl`)
# in $FLAMEGRAPH_DIR or on the PATH.
# `perf report --sort comm` splits the profile by thread: ingest,
# precompute, query-N and the HTTP workers.
set -euo pipefail

seconds="${1:-30}"
output="${2:-data_plane-flamegraph.svg}"

command -v perf >/dev/null || {
  echo "perf not found" >&2
  exit 1
}
if command -v inferno-collapse-perf >/dev/null && command -v inferno-flamegraph >/dev/null; then
  collapse=(inferno-collapse-perf)
  draw=(inferno-flamegraph)
elif [[ -n "${FLAMEGRAPH_DIR:-}" && -x "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" && -x "$FLAMEGRAPH_DIR/flamegraph.pl" ]]; then
  collapse=("$FLAMEGRAPH_DIR/stackcollapse-perf.pl")
  draw=("$FLAMEGRAPH_DIR/flamegraph.pl")
elif command -v stackcollapse-perf.pl >/dev/null && command -v flamegraph.pl >/dev/null; then
  collapse=(stackcollapse-perf.pl)
  draw=(flamegraph.pl)
else
  missing=()
  command -v inferno-collapse-perf >/dev/null || missing+=(inferno-collapse-perf)
  command -v inferno-flamegraph >/dev/null || missing+=(inferno-flamegraph)
  echo "missing ${missing[*]} (cargo install inferno), and no stackcollapse-perf.pl and flamegraph.pl in \$FLAMEGRAPH_DIR or on the PATH" >&2
  exit 1
fi
pid="$(pgrep -n -x data_plane)" || {
  echo "no running data_plane process" >&2
  exit 1
}

data="$(mktemp --suffix=.perf.data)"
trap 'rm -f "$data"' EXIT
perf record -F 499 -g --call-graph dwarf -p "$pid" -o "$data" -- sleep "$seconds"
perf script -i "$data" | "${collapse[@]}" | "${draw[@]}" >"$output"
echo "wrote $output"
