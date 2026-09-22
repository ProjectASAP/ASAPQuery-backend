# Builds the data_plane (backend) binary → asap/data-plane:dev.
#
# Repo-ownership (2026-05): each ASAPQuery-backend crate owns its own image
# Dockerfile here — companion: control_plane/Dockerfile
# Dockerfile. ASAPCollector only orchestrates + references images by name; the
# old cross-repo combined builder (ASAPCollector deploy/docker/Dockerfile.backend)
# is retired.
#
# Build from the backend checkout; Cargo fetches public Git dependencies
# at the revisions recorded in Cargo.lock.
#
#   docker build -f data_plane/Dockerfile -t asap/data-plane:dev .
FROM rust:1.90-bookworm AS build
WORKDIR /src

# protoc for the backend's prost-build usages; git for any git-dep fetches.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        protobuf-compiler git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY . ASAPQuery-backend

RUN cd ASAPQuery-backend && cargo build --locked --release --bin data_plane

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
