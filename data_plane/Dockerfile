# Builds the data_plane (backend) binary → asap/data-plane:dev.
#
# Repo-ownership (2026-05): each ASAPQuery-backend crate owns its own image
# Dockerfile here — companion: control_plane/Dockerfile
# Dockerfile. ASAPCollector only orchestrates + references images by name; the
# old cross-repo combined builder (ASAPCollector deploy/docker/Dockerfile.backend)
# is retired.
#
# Cross-repo build-contexts: the data_plane crate path-deps `control_plane`
# (in-repo, copied with the repo) and, transitively through asap-precompute-rs,
# the sibling crates asap-precompute-rs / asap-gorilla-rust (in the ASAPCollector
# repo) and asap_sketchlib (separate repo). They are supplied as BuildKit
# build-contexts that recreate the canonical sibling layout under /src so the
# `../../ASAPCollector/...` / `../../asap_sketchlib` path-deps resolve.
#
#   DOCKER_BUILDKIT=1 docker build -f data_plane/Dockerfile \
#     --build-context asap-precompute-rs=/path/ASAPCollector/asap-precompute-rs \
#     --build-context asap-sketchlib=/path/asap_sketchlib \
#     --build-context asap-gorilla-rust=/path/ASAPCollector/asap-gorilla-rust \
#     -t asap/data-plane:dev .
FROM rust:1.90-bookworm AS build
WORKDIR /src

# protoc for the backend's prost-build usages; git for any git-dep fetches.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        protobuf-compiler git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Recreate the sibling-checkout layout the path-deps expect:
#   /src/ASAPQuery-backend, /src/ASAPCollector/asap-precompute-rs,
#   /src/asap_sketchlib, /src/ASAPCollector/asap-gorilla-rust
COPY . ASAPQuery-backend
COPY --from=asap-precompute-rs . ASAPCollector/asap-precompute-rs
COPY --from=asap-sketchlib     . asap_sketchlib
COPY --from=asap-gorilla-rust  . ASAPCollector/asap-gorilla-rust

RUN cd ASAPQuery-backend && cargo build --release --bin data_plane

# Runtime image: binary + CA certs.
FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
# Default --output-dir for the rolling log file.
RUN mkdir -p /var/log/asap
COPY --from=build /src/ASAPQuery-backend/target/release/data_plane \
    /usr/local/bin/data_plane

ENV RUST_LOG=info \
    ASAP_BACKEND_PORT=9090 \
    ASAP_BACKEND_METRICS_PORT=9465
# query 9091, OTLP ingest 4317/4318, backend 9090, metrics 9465.
EXPOSE 9090 9091 9465 4317 4318
ENTRYPOINT ["/usr/local/bin/data_plane"]
