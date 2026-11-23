#!/bin/bash
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-target} OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include LIBCLANG_PATH=${LIBCLANG_PATH:-}
set -x
(cd $ASAPQ_BENCH/ASAPQuery/asap-tools/data-sources/paced-feeder && cargo build --release) &&
(cd $ASAPQ_BENCH/ASAPQuery && cargo build --release -p query_engine_rust --bin query_engine_rust) &&
(cd $ASAPQ_BENCH/backend && cargo build --release -p control_plane -p data_plane) && echo BUILD_OK
df -h "$ASAPQ_BENCH" | tail -1
(cd $BENCH_ROOT/ASAPPlanner-asapq && cargo build --release -p asap-planner --example asapquery_random_workload) && echo PLANNER_OK
