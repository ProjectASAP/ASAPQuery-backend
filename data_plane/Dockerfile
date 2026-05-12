# QueryEngine Rust/Dockerfile
# Multi-stage build for Rust application

FROM rust:1.89 AS builder

LABEL maintainer="SketchDB Team"
LABEL description="QueryEngine Rust service for SketchDB"

WORKDIR /code

# Required by prost-build dependencies (e.g., asap_sketchlib) during cargo build
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy the workspace's shared crates (formerly under asap-common/, moved to
# crates/ in the Phase 8 refactor — 2026-05-10).
COPY crates ./crates

COPY Cargo.toml ./
COPY Cargo.lock ./
COPY data_plane/Cargo.toml ./data_plane/

# Create dummy source files so Cargo can resolve all workspace members
# All explicit [[bin]] targets in Cargo.toml must have stubs here for the dependency cache layer
RUN mkdir -p data_plane/src/bin \
    && echo "fn main() {}" > data_plane/src/main.rs \
    && echo "fn main() {}" > data_plane/src/bin/test_e2e_precompute.rs \
    && echo "fn main() {}" > data_plane/src/bin/bench_precompute_sketch.rs \
    && echo "fn main() {}" > data_plane/src/bin/e2e_quickstart_resource_test.rs \
    && mkdir -p data_plane/benches && echo "fn main() {}" > data_plane/benches/simple_store_bench.rs

# Build dependencies (this layer will be cached)
WORKDIR /code/data_plane
RUN cargo build --release && rm -rf src/

# Copy source code
COPY data_plane/src ./src

# Build the actual application
RUN touch src/main.rs && cargo build --release

# Runtime stage with Ubuntu 24.04 (has newer glibc/libstdc++)
FROM ubuntu:24.04

WORKDIR /app

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary
COPY --from=builder /code/target/release/data_plane /usr/local/bin/data_plane

# Expose the HTTP server port
EXPOSE 8088

# Note: Running as root to match Python QueryEngine behavior
# This allows writing to mounted volumes without permission issues

# Use ENTRYPOINT to allow passing command line arguments
ENTRYPOINT ["data_plane"]
