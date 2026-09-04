#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROM_IMAGE="${ASAPQUERY_PROMETHEUS_IMAGE:-prom/prometheus:v2.55.1}"
PUSH_IMAGE="${ASAPQUERY_PUSHGATEWAY_IMAGE:-prom/pushgateway:v1.9.0}"
RUN_ID="asapquery-demo-$$"
PROM_CONTAINER="${RUN_ID}-prometheus"
PUSH_CONTAINER="${RUN_ID}-pushgateway"
EVIDENCE_DIR="${ASAPQUERY_DEMO_EVIDENCE_DIR:-${REPO_DIR}/target/asapquery-demo-evidence}"
BACKEND_PID=""

cleanup() {
    if [[ -n "${BACKEND_PID}" ]]; then
        kill "${BACKEND_PID}" 2>/dev/null || true
        wait "${BACKEND_PID}" 2>/dev/null || true
    fi
    docker rm -f "${PROM_CONTAINER}" "${PUSH_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for command in cargo curl docker python3; do
    command -v "${command}" >/dev/null 2>&1 || {
        echo "missing required command: ${command}" >&2
        exit 1
    }
done
docker info >/dev/null
mkdir -p "${EVIDENCE_DIR}"

echo "Building the production backend..."
cargo build --locked -p data_plane --bin data_plane

docker run -d --rm --name "${PUSH_CONTAINER}" --network host \
    "${PUSH_IMAGE}" --web.listen-address=:19092 >/dev/null
docker run -d --rm --name "${PROM_CONTAINER}" --network host \
    -v "${SCRIPT_DIR}/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
    "${PROM_IMAGE}" \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/prometheus \
    --web.listen-address=:19090 >/dev/null

for _ in $(seq 1 120); do
    if curl -fsS http://127.0.0.1:19090/-/healthy >/dev/null; then
        break
    fi
    sleep 0.25
done
curl -fsS http://127.0.0.1:19090/-/healthy >/dev/null

"${REPO_DIR}/target/debug/data_plane" \
    --profile asapquery \
    --planning-snapshot "${REPO_DIR}/docs/examples/asapquery-compatibility-demo-snapshot.json" \
    --prometheus-server http://127.0.0.1:19090 \
    --forward-unsupported-queries \
    --http-port 19091 \
    --precompute-allowed-lateness-ms 0 \
    --precompute-flush-interval-ms 100 \
    --output-dir "${EVIDENCE_DIR}/backend" &
BACKEND_PID=$!

for _ in $(seq 1 120); do
    if curl -fsS http://127.0.0.1:19091/api/v1/health >/dev/null; then
        break
    fi
    sleep 0.25
done
curl -fsS http://127.0.0.1:19091/api/v1/health >/dev/null

echo "Publishing raw metrics through Prometheus for complete 5s windows..."
counter=10
for sample in $(seq 1 18); do
    if [[ "${sample}" == "7" ]]; then
        counter=2
    else
        counter=$((counter + sample))
    fi
    latency=$((10 + (sample % 6) * 10))
    curl -fsS --data-binary @- \
        http://127.0.0.1:19092/metrics/job/asapquery-demo <<EOF >/dev/null
# TYPE asap_demo_counter_total counter
asap_demo_counter_total ${counter}
# TYPE asap_demo_gauge gauge
asap_demo_gauge ${sample}
# TYPE asap_demo_latency_ms gauge
asap_demo_latency_ms ${latency}
EOF
    sleep 1
done
sleep 2

query_backend() {
    local query=$1
    local output=$2
    shift 2
    curl -fsS --get http://127.0.0.1:19091/api/v1/query \
        --data-urlencode "query=${query}" "$@" >"${output}"
}

assert_warm_response() {
    python3 - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    response = json.load(source)
assert response.get("status") == "success", response
assert response.get("data", {}).get("result"), response
assert "data_source: asap_query" in response.get("infos", []), response
PY
}

end=$(( $(date +%s) / 5 * 5 - 5 ))
start=$((end - 5))
for query in \
    'rate(asap_demo_counter_total[5s])' \
    'increase(asap_demo_counter_total[5s])' \
    'sum_over_time(asap_demo_gauge[5s])' \
    'quantile_over_time(0.5, asap_demo_latency_ms[5s])'; do
    slug="$(printf '%s' "${query}" | tr -cs '[:alnum:]' '_')"
    query_backend "${query}" "${EVIDENCE_DIR}/${slug}-instant.json" \
        --data-urlencode "time=${end}"
    assert_warm_response "${EVIDENCE_DIR}/${slug}-instant.json"

    curl -fsS --get http://127.0.0.1:19091/api/v1/query_range \
        --data-urlencode "query=${query}" \
        --data-urlencode "start=${start}" \
        --data-urlencode "end=${end}" \
        --data-urlencode 'step=5' >"${EVIDENCE_DIR}/${slug}-range.json"
    assert_warm_response "${EVIDENCE_DIR}/${slug}-range.json"
done

fallback_query='max(asap_demo_gauge)'
query_backend "${fallback_query}" "${EVIDENCE_DIR}/fallback-backend.json" \
    --data-urlencode "time=${end}" --data-urlencode 'timeout=7s'
curl -fsS --get http://127.0.0.1:19090/api/v1/query \
    --data-urlencode "query=${fallback_query}" \
    --data-urlencode "time=${end}" --data-urlencode 'timeout=7s' \
    >"${EVIDENCE_DIR}/fallback-direct.json"
python3 - "${EVIDENCE_DIR}/fallback-backend.json" \
    "${EVIDENCE_DIR}/fallback-direct.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    backend = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    direct = json.load(source)
assert backend.get("data") == direct.get("data"), (backend, direct)
PY

curl -fsS http://127.0.0.1:19091/api/v1/physical-plan/status \
    >"${EVIDENCE_DIR}/physical-plan-status.json"
curl -fsS http://127.0.0.1:19091/metrics >"${EVIDENCE_DIR}/backend.metrics"
python3 - "${EVIDENCE_DIR}/physical-plan-status.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    status = json.load(source)
assert status.get("status") == "success", status
materializations = status.get("materializations", [])
assert len(materializations) == 3, materializations
assert all(item.get("phase") == "serving" for item in materializations), materializations
PY
grep -q '^asap_remote_write_samples_total [1-9]' "${EVIDENCE_DIR}/backend.metrics"

echo "PASS: Prometheus Remote Write -> PrecomputePlan -> QueryPlan DAG -> warm result"
echo "Evidence: ${EVIDENCE_DIR}"
