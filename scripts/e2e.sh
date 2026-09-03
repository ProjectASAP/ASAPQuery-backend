#!/usr/bin/env bash
# Manual end-to-end test runner for ASAPQuery-backend.
#
# The default "all" target stays local to this repository.  "system" is an
# explicit opt-in because it delegates to ASAPCollector's multi-node harness
# and may build images, start containers, and use SSH.

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET="${1:-all}"
CURRENT_STAGE="startup"
STARTED_AT="$(date +%s)"

export CARGO_TARGET_DIR="${ASAP_E2E_CARGO_TARGET_DIR:-${REPO_DIR}/target/e2e}"
export GOCACHE="${ASAP_E2E_GO_CACHE:-${REPO_DIR}/target/e2e-go-cache}"
export CARGO_INCREMENTAL="${ASAP_E2E_CARGO_INCREMENTAL:-0}"

usage() {
    cat <<'EOF'
Usage: ./scripts/e2e.sh [target]

Targets:
  all             Run every local component suite, then the repository E2E
  contracts       Shared Rust type/protobuf wire contracts
  control-plane   Planner HTTP, OpAMP, publication, and runtime feedback
  data-plane      Query, routing, storage, ingest adapter, and lifecycle tests
  differential    Production backend PromQL vs deterministic raw-value oracle
  monitor         Real monitor gRPC transport tests
  gorilla-merger  Gorilla HTTP/WAL/block/StoreAPI/compaction/shipper tests
  whole           Controller plan -> backend install -> OTLP -> store -> PromQL
  whole-matrix    All sketch families and query shapes (diagnostic)
  differential-all Raw oracle test plus the all-sketch/query matrix
  system          Delegate to ASAPCollector's real multi-node system harness
  list            Print the suites and audit Rust E2E ignore markers

Useful environment variables:
  ASAP_COLLECTOR_DIR          Sibling ASAPCollector checkout (system target)
  ASAP_E2E_CARGO_TARGET_DIR   Rust build directory
  ASAP_E2E_GO_CACHE           Go build cache directory
  ASAP_E2E_CARGO_INCREMENTAL  Set to 1 to retain Rust incremental artifacts
  ASAP_E2E_NOCAPTURE=1        Pass --nocapture to Rust test binaries
EOF
}

say() {
    printf '\n==> %s\n' "$*"
}

die() {
    printf '\nERROR [%s]: %s\n' "${CURRENT_STAGE}" "$*" >&2
    exit 1
}

on_error() {
    local code=$?
    printf '\nFAILED [%s] (exit %s)\n' "${CURRENT_STAGE}" "${code}" >&2
    exit "${code}"
}
trap on_error ERR

need() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

rust_test() {
    local package=$1
    shift
    local test_args=(--test-threads=1)
    if [[ "${ASAP_E2E_NOCAPTURE:-0}" == "1" ]]; then
        test_args+=(--nocapture)
    fi
    cargo test --locked -p "${package}" "$@" -- "${test_args[@]}"
}

contracts() {
    CURRENT_STAGE="contracts/asap_types"
    say "contracts: shared policy and routing types"
    rust_test asap_types

    CURRENT_STAGE="contracts/asap_otel_proto"
    say "contracts: modified OTLP and monitor protobuf compatibility"
    rust_test asap_otel_proto --tests
}

control_plane() {
    CURRENT_STAGE="control-plane"
    say "control-plane: HTTP planning, OpAMP, publication, and feedback"
    rust_test control_plane
}

data_plane() {
    CURRENT_STAGE="data-plane/library"
    say "data-plane: HTTP query/routing, storage, lifecycle, and fallback"
    rust_test data_plane --lib

    CURRENT_STAGE="data-plane/edge-runtime-wire"
    say "data-plane: edge runtime sketch envelope -> backend accumulator"
    rust_test data_plane --test edge_runtime_consumes_precompute_rs

    CURRENT_STAGE="data-plane/production-process"
    say "data-plane: production binary -> modified OTLP -> SketchStore -> PromQL"
    rust_test data_plane --test component_process_e2e

    differential
}

differential() {
    CURRENT_STAGE="data-plane/promql-differential"
    say "data-plane: production backend PromQL -> raw-value oracle + instant/range parity"
    rust_test data_plane --test promql_differential_process_e2e
}

monitor() {
    CURRENT_STAGE="monitor-grpc"
    say "monitor: real bidirectional gRPC server/client"
    rust_test data_plane --test monitor_grpc

    CURRENT_STAGE="monitor-production-process"
    say "monitor: production coordinator process -> two edge streams -> sampling grants"
    rust_test data_plane --test monitor_process_e2e
}

gorilla_merger() {
    CURRENT_STAGE="gorilla-merger"
    say "gorilla-merger: HTTP ingest, WAL, blocks, StoreAPI, compaction, shipping"
    need go
    mkdir -p "${CARGO_TARGET_DIR}"
    (
        cd "${REPO_DIR}/gorilla-merger"
        GOPRIVATE="${GOPRIVATE:-github.com/ProjectASAP/*}" \
            go build -o "${CARGO_TARGET_DIR}/gorilla-merger-e2e" ./cmd/gorilla-merger
        GORILLA_MERGER_E2E_BIN="${CARGO_TARGET_DIR}/gorilla-merger-e2e" \
            GOPRIVATE="${GOPRIVATE:-github.com/ProjectASAP/*}" \
            go test -count=1 ./...
    )
}

whole() {
    CURRENT_STAGE="whole/controller-to-query"
    say "whole repository: production controller -> production backend -> OTLP -> PromQL"
    cargo build --locked -p control_plane --bin control_plane -p data_plane --bin data_plane
    ASAP_E2E_CONTROL_PLANE_BIN="${CARGO_TARGET_DIR}/debug/control_plane" \
        rust_test data_plane --test backend_process_e2e
}

whole_matrix() {
    CURRENT_STAGE="whole/sketch-query-matrix"
    say "whole repository diagnostic: every sketch and query scenario"
    rust_test data_plane --test e2e_controller_plans_and_backend_serves
}

differential_all() {
    differential
    whole_matrix
}

list_suites() {
    usage
    printf '\nRust E2E tests marked #[ignore] (expected: none):\n'
    rg -n '^[[:space:]]*#\[ignore' \
        "${REPO_DIR}/data_plane/tests" \
        "${REPO_DIR}/data_plane/src/tests" \
        "${REPO_DIR}/crates" \
        -g '*.rs' || true
}

system_e2e() {
    CURRENT_STAGE="external-system"
    need docker
    local collector_dir="${ASAP_COLLECTOR_DIR:-${REPO_DIR}/../ASAPCollector}"
    local runner="${collector_dir}/deploy/mvp-multinode/scripts/run_demo.sh"
    [[ -f "${runner}" ]] || die "ASAPCollector system runner not found: ${runner}"
    say "external system: delegating to ASAPCollector multi-node harness"
    printf 'This target may build images, start remote containers, and use SSH.\n'
    BACKEND="${REPO_DIR}" bash "${runner}" all
}

main() {
    cd "${REPO_DIR}"
    case "${TARGET}" in
        all)
            need cargo
            need rg
            contracts
            control_plane
            data_plane
            monitor
            gorilla_merger
            whole
            ;;
        contracts) need cargo; contracts ;;
        control-plane) need cargo; control_plane ;;
        data-plane) need cargo; data_plane ;;
        differential) need cargo; differential ;;
        monitor) need cargo; monitor ;;
        gorilla-merger) gorilla_merger ;;
        whole) need cargo; whole ;;
        whole-matrix) need cargo; whole_matrix ;;
        differential-all) need cargo; differential_all ;;
        system) system_e2e ;;
        list) list_suites; exit 0 ;;
        -h|--help|help) usage; exit 0 ;;
        *) usage >&2; die "unknown target: ${TARGET}" ;;
    esac

    local elapsed=$(( $(date +%s) - STARTED_AT ))
    CURRENT_STAGE="complete"
    printf '\nPASS: %s E2E target completed in %ss\n' "${TARGET}" "${elapsed}"
}

main "$@"
