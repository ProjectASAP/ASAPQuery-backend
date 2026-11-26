# syntax=docker/dockerfile:1
# The backend's binaries in one image: control_plane (workload -> plan file)
# and data_plane (serves a plan: ingestion and query routes). Build from the
# repository root:
#
#   docker build -f deployment/Dockerfile -t asap-backend .
#   docker run --rm asap-backend data_plane --help
#
# deployment/docker-compose.yml plans a workload and serves the plan.
FROM rust:1.99.0-slim-bookworm AS build
# git: Cargo fetches the pinned git dependencies with the git CLI (.cargo/config.toml).
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p control_plane -p data_plane \
 && install -m 0755 target/release/control_plane target/release/data_plane /usr/local/bin/

FROM debian:bookworm-20261104-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /data asap \
 && mkdir /data && chown asap /data
COPY --from=build /usr/local/bin/control_plane /usr/local/bin/data_plane /usr/local/bin/
USER asap
# data_plane writes its views (--view-dir, default asap-views) under the working directory.
WORKDIR /data
EXPOSE 8088
CMD ["data_plane", "--help"]
