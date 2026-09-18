# Builds the control_plane (controller) binary → asap/control-plane:dev.
#
# Companion to data_plane/Dockerfile (same repo-ownership + build-context
# rationale — see that file's header). Splitting the controller into its own
# image (it used to ship inside the combined asap/query-backend:dev) is part of
# the 2026-05 reorg: each crate owns its image, named after the crate.
#
# Even though the controller doesn't compile asap-precompute-rs/asap-gorilla-rust
# directly, the workspace manifest must still resolve every member's path-deps
# (data_plane path-deps those siblings), so the same build-contexts are supplied.
#
#   DOCKER_BUILDKIT=1 docker build -f control_plane/Dockerfile \
#     --build-context asap-precompute-rs=/path/ASAPCollector/asap-precompute-rs \
#     --build-context asap-sketchlib=/path/asap_sketchlib \
#     --build-context asap-gorilla-rust=/path/ASAPCollector/asap-gorilla-rust \
#     -t asap/control-plane:dev .
FROM rust:1.90-bookworm AS build
WORKDIR /src

# protoc for control_plane/build.rs (prost-build/tonic-build compile
# proto/opamp.proto + proto/feedback.proto); git for git-dep fetches.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        protobuf-compiler git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Same sibling-checkout layout as data_plane/Dockerfile so workspace path-deps
# resolve during the build.
COPY . ASAPQuery-backend
# hadolint ignore=DL3022
COPY --from=asap-precompute-rs . ASAPCollector/asap-precompute-rs
# hadolint ignore=DL3022
COPY --from=asap-sketchlib     . asap_sketchlib
# hadolint ignore=DL3022
COPY --from=asap-gorilla-rust  . ASAPCollector/asap-gorilla-rust

# See data_plane/Dockerfile: resolve private sibling dependencies from the
# build contexts instead of requiring Docker to authenticate to GitHub.
RUN mkdir -p ASAPQuery-backend/.cargo && \
    printf '%s\n' \
      '[patch."https://github.com/ProjectASAP/ASAPCollector"]' \
      'asap-precompute-rs = { path = "/src/ASAPCollector/asap-precompute-rs" }' \
      '' \
      '[patch."https://github.com/ProjectASAP/asap_sketchlib"]' \
      'asap_sketchlib = { path = "/src/asap_sketchlib" }' \
      > ASAPQuery-backend/.cargo/config.toml

WORKDIR /src/ASAPQuery-backend
RUN cargo build --release --bin control_plane --bin control_plane_quote_snapshot

# Runtime image: binary + CA certs.
FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=build /src/ASAPQuery-backend/target/release/control_plane \
    /usr/local/bin/control_plane
COPY --from=build /src/ASAPQuery-backend/target/release/control_plane_quote_snapshot \
    /usr/local/bin/control_plane_quote_snapshot

ENV RUST_LOG=info
# OpAMP ws 4320, controller gRPC 4321, controller HTTP 8080.
EXPOSE 4320 4321 8080
ENTRYPOINT ["/usr/local/bin/control_plane"]
