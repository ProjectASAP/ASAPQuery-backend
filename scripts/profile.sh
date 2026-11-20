#!/usr/bin/env bash
# Records a CPU profile of a running data_plane and writes a flamegraph.
#
#   scripts/profile.sh [SECONDS] [OUTPUT.svg]
#
# Build with symbols first, so frames have names:
#   CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release -p data_plane
# Needs `perf` (and perf_event_paranoid <= 1, or root) and the inferno
# tools (`cargo install inferno`). `perf report --sort comm` splits the
# profile by thread: ingest, precompute, query-N and the HTTP workers.
set -euo pipefail

seconds="${1:-30}"
output="${2:-data_plane-flamegraph.svg}"
pid="$(pgrep -n -x data_plane)" || {
  echo "no running data_plane process" >&2
  exit 1
}
for tool in perf inferno-collapse-perf inferno-flamegraph; do
  command -v "$tool" >/dev/null || {
    echo "$tool not found" >&2
    exit 1
  }
done

data="$(mktemp --suffix=.perf.data)"
trap 'rm -f "$data"' EXIT
perf record -F 499 -g --call-graph dwarf -p "$pid" -o "$data" -- sleep "$seconds"
perf script -i "$data" | inferno-collapse-perf | inferno-flamegraph >"$output"
echo "wrote $output"
