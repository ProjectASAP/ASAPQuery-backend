# Builds the control_plane (controller) binary → asap/control-plane:dev.
#
# Build from the backend checkout; Cargo fetches public Git dependencies
# at the revisions recorded in Cargo.lock.
#
#   docker build -f control_plane/Dockerfile -t asap/control-plane:dev .
FROM rust:1.90-bookworm AS build
WORKDIR /src

# protoc for control_plane/build.rs (prost-build/tonic-build compile
# proto/opamp.proto + proto/feedback.proto); git for git-dep fetches.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        protobuf-compiler git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY . ASAPQuery-backend
RUN cd ASAPQuery-backend && cargo build --locked --release --bin control_plane --bin control_plane_quote_snapshot --example compile_workload_artifact

# Runtime image: binary + CA certs.
FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=build /src/ASAPQuery-backend/target/release/control_plane \
    /usr/local/bin/control_plane
COPY --from=build /src/ASAPQuery-backend/target/release/control_plane_quote_snapshot \
    /usr/local/bin/control_plane_quote_snapshot
COPY --from=build /src/ASAPQuery-backend/target/release/examples/compile_workload_artifact \
    /usr/local/bin/compile_workload_artifact

ENV RUST_LOG=info
# OpAMP ws 4320, controller gRPC 4321, controller HTTP 8080.
EXPOSE 4320 4321 8080
ENTRYPOINT ["/usr/local/bin/control_plane"]
