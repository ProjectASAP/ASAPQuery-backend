#!/usr/bin/env bash
# Run an already-built probe with matching CPU and memory budgets.
set -euo pipefail

trial=${1:?pass a unique trial suffix}
: "${CLICKHOUSE_USER:?set the evaluation account}"
: "${CLICKHOUSE_PASSWORD:?set the evaluation password}"
: "${TEST_BINARY:?set the executable path printed by cargo test --release --no-run}"
: "${CARGO_TARGET_DIR:?set the target directory used for TEST_BINARY}"
: "${CLICKHOUSE_BENCH_INPUT:?set the extracted JSONEachRow file}"
: "${CLICKHOUSE_BENCH_END_MS:?set the exclusive evaluation endpoint}"
: "${CLICKHOUSE_BENCH_METRIC:?set the original metric name}"
: "${RESULT_DIR:?set an existing absolute result directory}"

repo_root=$(git rev-parse --show-toplevel)
image=clickhouse/clickhouse-server@sha256:fa394da808cc53f76d0344429421d6c422a6ee85fe7450135c0e3cff4df9bcbb
server=${CLICKHOUSE_CONTAINER:-asap-clickhouse-current-eval}
backend="asap-clickhouse-probe-trial${trial}"
output="$RESULT_DIR/fine-1s-matched-trial${trial}"

if ! docker inspect "$server" >/dev/null 2>&1; then
    docker run -d --name "$server" --cpuset-cpus=60,61 --cpus=2 --memory=4g \
        -p 127.0.0.1:28123:8123 -e CLICKHOUSE_USER -e CLICKHOUSE_PASSWORD "$image"
else
    expected_image=$(docker image inspect "$image" --format '{{.Id}}')
    [[ $(docker inspect "$server" --format '{{.Image}}') == "$expected_image" ]]
    [[ $(docker inspect "$server" --format '{{.HostConfig.CpusetCpus}}/{{.HostConfig.NanoCpus}}/{{.HostConfig.Memory}}') == '60,61/2000000000/4294967296' ]]
    [[ $(docker inspect "$server" --format '{{with index .HostConfig.PortBindings "8123/tcp"}}{{(index . 0).HostIp}}:{{(index . 0).HostPort}}{{end}}') == '127.0.0.1:28123' ]]
    docker restart "$server"
fi
budget=$(docker inspect "$server" --format '{{.HostConfig.CpusetCpus}}/{{.HostConfig.NanoCpus}}/{{.HostConfig.Memory}}')
[[ "$budget" == '60,61/2000000000/4294967296' ]]
for _ in $(seq 1 100); do
    if curl --silent --fail http://127.0.0.1:28123/ping >/dev/null; then
        break
    fi
    sleep 0.1
done
curl --silent --fail http://127.0.0.1:28123/ping >/dev/null
server_pid=$(docker inspect "$server" --format '{{.State.Pid}}')
settings=$(curl --silent --show-error --fail --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
    --data-binary "SELECT name,value FROM system.settings WHERE name IN ('max_threads','use_query_cache') ORDER BY name FORMAT TSV" \
    http://127.0.0.1:28123/)
[[ "$settings" == $'max_threads\tauto(2)\nuse_query_cache\t0' ]]
printf '%s\n' "$settings" > "$output.clickhouse-settings.tsv"

docker run --name "$backend" --user "$(id -u):$(id -g)" \
    --network host --pid host --cpuset-cpus=60,61 --cpus=2 --memory=4g \
    -v "$CARGO_TARGET_DIR:$CARGO_TARGET_DIR:ro" \
    -v "$repo_root:$repo_root:ro" \
    -v "$RESULT_DIR:$RESULT_DIR" \
    -v "$CLICKHOUSE_BENCH_INPUT:$CLICKHOUSE_BENCH_INPUT:ro" \
    -w "$repo_root" -e CLICKHOUSE_URL=http://127.0.0.1:28123 \
    -e CLICKHOUSE_USER -e CLICKHOUSE_PASSWORD -e CLICKHOUSE_PID="$server_pid" \
    -e CLICKHOUSE_BENCH_INPUT -e CLICKHOUSE_BENCH_METRIC -e CLICKHOUSE_BENCH_END_MS \
    -e CLICKHOUSE_BENCH_AGGREGATE="${CLICKHOUSE_BENCH_AGGREGATE:-max}" \
    -e CLICKHOUSE_BENCH_REPETITIONS="${CLICKHOUSE_BENCH_REPETITIONS:-1000}" \
    -e CLICKHOUSE_BENCH_OUTPUT="$output.json" \
    --entrypoint "$TEST_BINARY" "$image" --nocapture

format='{"cpu_affinity":"{{.HostConfig.CpusetCpus}}","nano_cpus":{{.HostConfig.NanoCpus}},"memory_limit_bytes":{{.HostConfig.Memory}},"image":"{{.Image}}"}'
docker inspect "$backend" --format "$format" > "$output.backend-runtime.json"
docker inspect "$server" --format "$format" > "$output.clickhouse-runtime.json"
sha256sum "$TEST_BINARY" "$CARGO_TARGET_DIR/release/data_plane" > "$output.executables.sha256"
git rev-parse HEAD > "$output.source-checkout.txt"
python3 "$repo_root/tools/clickhouse-benefit/summarize.py" "$output.json" > "$output-summary.json"
