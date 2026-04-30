#!/usr/bin/env bash
set -euo pipefail

# run_full_eval.sh — orchestrator for the three-way query benchmark.
#
# Brings up the merged asap-quickstart + benchmarks docker-compose stack,
# waits for services to be healthy and ingest to populate, runs all 4
# runners (W1+W2 against ASAP, Prom, VM) plus the concurrency sweep,
# then renders the three-way comparison report.
#
# Usage:
#   ./benchmarks/run_full_eval.sh             # leave stack up at end
#   ./benchmarks/run_full_eval.sh --down      # tear down at end
#   ./benchmarks/run_full_eval.sh --skip-sweep  # skip 60s/concurrency sweep
#
# Env knobs:
#   ASAP_IMAGE_TAG   — pin asap images (default: v0.2.0)
#   SWEEP_DURATION   — seconds per concurrency level (default: 60)
#   SWEEP_LEVELS     — comma list (default: 1,4,16,64)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="$REPO_ROOT/benchmarks"
SCRIPTS_DIR="$BENCH_DIR/scripts"

DOWN_AT_END=0
SKIP_SWEEP=0
for arg in "$@"; do
  case "$arg" in
    --down)        DOWN_AT_END=1 ;;
    --skip-sweep)  SKIP_SWEEP=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

SWEEP_DURATION="${SWEEP_DURATION:-60}"
SWEEP_LEVELS="${SWEEP_LEVELS:-1,4,16,64}"

cd "$REPO_ROOT"

COMPOSE_ARGS=(
  --project-directory "$REPO_ROOT"
  -f "$REPO_ROOT/asap-quickstart/docker-compose.yml"
  -f "$BENCH_DIR/docker-compose.yml"
)

echo "[run_full_eval] === bringing up stack ==="
docker compose "${COMPOSE_ARGS[@]}" up -d --remove-orphans

echo "[run_full_eval] === waiting for ASAP/Prom/Arroyo healthy ==="
"$SCRIPTS_DIR/wait_for_stack.sh"

echo "[run_full_eval] === waiting for VictoriaMetrics ==="
elapsed=0
until curl -sf --max-time 5 "http://localhost:8428/health" > /dev/null 2>&1; do
  if [ "$elapsed" -ge 180 ]; then
    echo "[run_full_eval] ERROR: VictoriaMetrics not healthy in 180s" >&2
    exit 1
  fi
  sleep 5
  elapsed=$((elapsed + 5))
done
echo "[run_full_eval] VictoriaMetrics healthy"

echo "[run_full_eval] === waiting for ingest to populate sketches ==="
"$SCRIPTS_DIR/ingest_wait.sh"

echo "[run_full_eval] === seeding cold-store for W2 ad-hoc queries ==="
python3 "$SCRIPTS_DIR/seed_cold_store.py" --hours 2

echo "[run_full_eval] === running W1+W2 against ASAP ==="
python3 "$SCRIPTS_DIR/run_asap_workloads.py"

echo "[run_full_eval] === running W1+W2 against Prometheus ==="
python3 "$SCRIPTS_DIR/run_prom.py"

echo "[run_full_eval] === running W1+W2 against VictoriaMetrics ==="
python3 "$SCRIPTS_DIR/run_vm.py"

if [ "$SKIP_SWEEP" -eq 0 ]; then
  echo "[run_full_eval] === concurrency sweep (duration=${SWEEP_DURATION}s, levels=${SWEEP_LEVELS}) ==="
  python3 "$SCRIPTS_DIR/run_concurrency_sweep.py" \
    --duration "$SWEEP_DURATION" \
    --concurrency "$SWEEP_LEVELS"
else
  echo "[run_full_eval] skipping concurrency sweep (--skip-sweep)"
fi

echo "[run_full_eval] === rendering three-way report ==="
python3 "$SCRIPTS_DIR/compare_three_way.py"

echo "[run_full_eval] === DONE ==="
echo "Reports:    $BENCH_DIR/reports/"
echo "Top-level:  $BENCH_DIR/reports/three_way_eval.md"

if [ "$DOWN_AT_END" -eq 1 ]; then
  echo "[run_full_eval] tearing down stack (--down)"
  docker compose "${COMPOSE_ARGS[@]}" down -v
else
  echo "[run_full_eval] leaving stack up. Tear down with:"
  echo "  docker compose ${COMPOSE_ARGS[*]} down -v"
fi
