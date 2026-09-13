#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
FIXTURE_DIR="${SCRIPT_DIR}/fixtures"
WORK_DIR="$(mktemp -d)"
PORT_BASE="${CDE_TEST_PORT_BASE:-19400}"
CURRENT_PID=""

cleanup() {
    if [[ -n "${CURRENT_PID}" ]]; then
        kill "${CURRENT_PID}" 2>/dev/null || true
        wait "${CURRENT_PID}" 2>/dev/null || true
    fi
    rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

require() {
    command -v "$1" >/dev/null || {
        echo "missing required command: $1" >&2
        exit 1
    }
}

for command in cargo curl gzip; do
    require "${command}"
done

cargo build --manifest-path "${TOOL_DIR}/Cargo.toml"
EXPORTER="${TOOL_DIR}/target/debug/cluster_data_exporter"

start_exporter() {
    local port="$1"
    local input_directory="$2"
    shift 2
    "${EXPORTER}" -i "${input_directory}" -p "${port}" --exit-after-eof-ms=2000 "$@" &
    CURRENT_PID="$!"
    for _ in $(seq 1 80); do
        if curl -fsS "http://127.0.0.1:${port}/metrics" >/dev/null 2>&1; then
            return
        fi
        sleep 0.05
    done
    echo "exporter did not become ready on port ${port}" >&2
    exit 1
}

assert_metric() {
    local port="$1"
    local metric="$2"
    local label_fragment="$3"
    curl -fsS "http://127.0.0.1:${port}/metrics" |
        grep -F "${metric}" |
        grep -Fq "${label_fragment}"
}

stop_exporter() {
    kill "${CURRENT_PID}"
    wait "${CURRENT_PID}" 2>/dev/null || true
    CURRENT_PID=""
}

google_dir="${WORK_DIR}/google"
mkdir -p "${google_dir}"
# Verifies Google task-usage CSV replay and metric labels.
gzip -c "${FIXTURE_DIR}/google/part-00000-of-00500.csv" > "${google_dir}/part-00000-of-00500.csv.gz"
start_exporter "${PORT_BASE}" "${google_dir}" google --metrics=mean-cpu-usage-rate --part-index=0
assert_metric "${PORT_BASE}" "google_mean_cpu_usage_rate_0" 'job_id="job-a"'
stop_exporter

for year in 2021 2022; do
    node_dir="${WORK_DIR}/node-${year}"
    mkdir -p "${node_dir}"
    # Verifies both Alibaba Node filename conventions and metric output.
    node_name="Node_0.csv.gz"
    if [[ "${year}" == 2022 ]]; then
        node_name="NodeMetrics_0.csv.gz"
    fi
    gzip -c "${FIXTURE_DIR}/alibaba/node.csv" > "${node_dir}/${node_name}"
    port=$((PORT_BASE + year - 2020))
    start_exporter "${port}" "${node_dir}" alibaba --data-type=node --data-year="${year}" --part-index=0 --speedup=1
    assert_metric "${port}" "alibaba_node_cpu_usage" 'node_id="node-a"'
    stop_exporter
done

for year in 2021 2022; do
    ms_dir="${WORK_DIR}/msresource-${year}"
    mkdir -p "${ms_dir}"
    # Verifies both Alibaba MSResource filename conventions and metric output.
    ms_name="MSResource_0.csv.gz"
    if [[ "${year}" == 2022 ]]; then
        ms_name="MSMetrics_0.csv.gz"
    fi
    gzip -c "${FIXTURE_DIR}/alibaba/msresource.csv" > "${ms_dir}/${ms_name}"
    port=$((PORT_BASE + year - 2018))
    start_exporter "${port}" "${ms_dir}" alibaba --data-type=ms-resource --data-year="${year}" --part-index=0 --speedup=1
    assert_metric "${port}" "alibaba_microservice_cpu_usage" 'ms_name="frontend"'
    stop_exporter
done

echo "cluster-data-exporter smoke checks passed"
