#!/usr/bin/env bash
set -euo pipefail
: "${CLICKHOUSE_USER:?set the evaluation account}"
: "${CLICKHOUSE_PASSWORD:?set the evaluation password}"
: "${MATRIX:?set the automatic publication matrix}"
: "${BACKEND_BIN:?set the frozen release backend executable}"
: "${RESULT_ROOT:?set a new result root}"
: "${RUNTIME_SOURCE_COMMIT:?set the commit used to build BACKEND_BIN}"
image=clickhouse/clickhouse-server@sha256:fa394da808cc53f76d0344429421d6c422a6ee85fe7450135c0e3cff4df9bcbb
server=asap-clickhouse-current-eval
repo_root=$(git rev-parse --show-toplevel)
mkdir "$RESULT_ROOT"
sha256sum "$BACKEND_BIN" "$MATRIX" > "$RESULT_ROOT/inputs.sha256"
git rev-parse HEAD > "$RESULT_ROOT/driver-checkout.txt"
printf '%s\n' "$RUNTIME_SOURCE_COMMIT" > "$RESULT_ROOT/runtime-source-commit.txt"
for trial in 1 2 3; do
    for query in q05 q06 q23; do
        [[ $(docker inspect "$server" --format '{{.Image}}') == $(docker image inspect "$image" --format '{{.Id}}') ]]
        [[ $(docker inspect "$server" --format '{{.HostConfig.CpusetCpus}}/{{.HostConfig.NanoCpus}}/{{.HostConfig.Memory}}') == '60,61/2000000000/4294967296' ]]
        docker restart "$server" >/dev/null
        for _ in $(seq 1 100); do
            if curl --silent --fail http://127.0.0.1:28123/ping >/dev/null; then break; fi
            sleep 0.1
        done
        curl --silent --fail http://127.0.0.1:28123/ping >/dev/null
        server_pid=$(docker inspect "$server" --format '{{.State.Pid}}')
        python3 -B "$repo_root/tools/o11y-sql-main-eval/run_automatic.py" "$MATRIX" "$BACKEND_BIN" "$RESULT_ROOT/trial-$trial-$query" --ids "$query" --repetitions 1000 --container-image "$image" --clickhouse-pid "$server_pid"
        docker inspect "$server" --format '{"image":"{{.Image}}","cpu_affinity":"{{.HostConfig.CpusetCpus}}","nano_cpus":{{.HostConfig.NanoCpus}},"memory_limit_bytes":{{.HostConfig.Memory}}}' > "$RESULT_ROOT/trial-$trial-$query/clickhouse-runtime.json"
    done
done
