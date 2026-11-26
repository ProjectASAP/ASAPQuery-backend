#!/bin/bash
# V15 of docs/design/tiered-view-storage.md: the dashboard plan at SERIES series
# per metric, 1 sample/s, MINUTES of samples, run twice under the same cgroup v2
# memory.max with swap off:
#   control   every view window kept in memory (VIEW_CACHE_MAX_BYTES=off)
#   tiered    the default window cache budget (derived from memory.max)
# Expected: the control is OOM-killed (memory.events oom_kill > 0) and the tiered
# run answers every query with oom_kill 0. Needs sudo -n to create the cgroup.
# WORKLOAD, COLD and VIEW_READ are passed to the bench (see benchmarks/micro/tier_bench.rs).
#
#   benchmarks/scripts/tiered_views_cgroup.sh [MEMORY_MAX_BYTES] [SERIES] [MINUTES] [WORK_DIR]
set -uo pipefail
MEM=${1:-1610612736}
SERIES=${2:-10000}
MINUTES=${3:-70}
WORK=${4:-$(mktemp -d)}
ROOT=/sys/fs/cgroup/asap-tiering
BENCH=${BENCH:-target/release/examples/tier_bench}
[ -x "$BENCH" ] || { echo "build first: cargo build --release -p data_plane --example tier_bench" >&2; exit 2; }
sudo -n mkdir -p "$ROOT"
echo "+memory" | sudo -n tee "$ROOT/cgroup.subtree_control" >/dev/null
status=0
for arm in control tiered; do
  cg=$ROOT/$arm-$$
  sudo -n mkdir -p "$cg"
  echo "$MEM" | sudo -n tee "$cg/memory.max" >/dev/null
  echo 0 | sudo -n tee "$cg/memory.swap.max" >/dev/null 2>&1 || true
  budget=$([ "$arm" = control ] && echo off || echo "")
  log=$WORK/$arm.log
  # The shell moves itself into the cgroup, then runs the bench there.
  sudo -n -E bash -c "echo \$\$ > $cg/cgroup.procs && exec sudo -n -u $(id -un) env VIEW_CACHE_MAX_BYTES=$budget WORKLOAD=${WORKLOAD:-} COLD=${COLD:-0} VIEW_READ=${VIEW_READ:-mmap} \
    $BENCH $SERIES $MINUTES $WORK/$arm-views 3" >"$log" 2>&1
  code=$?
  kills=$(awk '/^oom_kill /{print $2}' "$cg/memory.events")
  answered=$(grep -c ' 200 OK ' "$log")
  echo "$arm: exit $code, oom_kill $kills, answered $answered; log $log"
  if [ "$arm" = tiered ] && { [ "$kills" != 0 ] || [ "$code" != 0 ]; }; then status=1; fi
  if [ "$arm" = control ] && [ "$kills" = 0 ]; then echo "control was not OOM-killed: memory.max is too large to show the difference"; fi
  rm -rf "$WORK/$arm-views"
  sudo -n rmdir "$cg" 2>/dev/null || true
done
exit $status
