#!/bin/bash
# Render a Grafana dashboard (whole page) and each of its panels to PNG for
# one time range, with headless Chromium in the pinned Playwright image.
#
#   deployment/monitoring/screenshot.sh <dashboard uid> <from> <to> <out dir> [NAME=VALUE ...]
#
# <from>/<to>: epoch milliseconds (sent to Grafana as ISO-8601 UTC) or relative times (now-1h, now).
# NAME=VALUE sets a dashboard variable (e.g. run=vmdemo.demo.smoke.s1000).
# Grafana stays on 127.0.0.1 (host network); GRAFANA_PORT [13000].
set -euo pipefail
BASE=${ASAPQ_BENCH:?set ASAPQ_BENCH}
UID_=${1:?dashboard uid}; FROM=${2:?from}; TO=${3:?to}; OUT=${4:?out dir}; shift 4
IMAGE=mcr.microsoft.com/playwright/python:v1.49.0-jammy@sha256:70c06141ac9041eedef4a68387ea978ceb090c37bd5b66583353b7fe17b9f3d7
MON=$BASE/../deployment/monitoring   # this repository's deployment/monitoring
LIB=$MON/.pylib
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
if [ ! -d "$LIB/playwright" ]; then
  sudo -n docker run --rm -v "$MON:/m" "$IMAGE" pip install -q --target /m/.pylib playwright==1.49.0 >/dev/null 2>&1
fi
vars=(); for v in "$@"; do vars+=(--vars "$v"); done
sudo -n docker run --rm --network host ${AUX_CPUS:+--cpuset-cpus "$AUX_CPUS"} -e PYTHONPATH=/m/.pylib \
  -v "$MON:/m:ro" -v "$OUT:/out" "$IMAGE" \
  python3 /m/screenshot.py --grafana "http://127.0.0.1:${GRAFANA_PORT:-13000}" --uid "$UID_" \
  --from "$FROM" --to "$TO" --out /out "${vars[@]}"
sudo -n chown -R "$(id -u):$(id -g)" "$OUT"
