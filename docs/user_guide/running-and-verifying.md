# Run and verify the backend

## Prerequisites

Clone `ASAPCollector`, `ASAPQuery-backend`, and `asap_sketchlib` as sibling
directories. Install the Rust toolchain required by the workspace, then build:

```bash
cargo build --workspace
```

For a fully configured system, prefer the ASAPCollector deployment harness. The
commands below are useful for component development and diagnostics.

## Inspect runtime options

```bash
cargo run -p data_plane -- --help
```

The data plane requires a streaming configuration file. A minimal development
launch follows this shape:

```bash
cargo run -p data_plane -- \
  --streaming-config PATH_TO_STREAMING_CONFIG \
  --http-port 8088 \
  --enable-otel-ingest \
  --otel-grpc-port 4317 \
  --otel-http-port 4318
```

Use a checked-in or controller-emitted streaming configuration appropriate to
your workload; do not invent aggregation identities by hand for production.

Run the control plane with its defaults:

```bash
cargo run -p control_plane
```

Important default listeners are:

| Component | Endpoint | Purpose |
| --- | --- | --- |
| Control plane | `http://localhost:8080` | Planning and configuration API |
| Control plane | `ws://localhost:4320/v1/opamp` | Collector OpAMP connection |
| Data plane | `http://localhost:8088` | PromQL-compatible query and diagnostics |
| Data plane | `localhost:4317` | OTLP gRPC ingest when enabled |
| Data plane | `localhost:4318/v1/metrics` | OTLP HTTP ingest when enabled |

Deployment configuration can override these values.

## Verify readiness

Check data-plane health:

```bash
curl -fsS http://localhost:8088/api/v1/health
```

Inspect installed runtime state when diagnosing plan or routing problems:

```bash
curl -fsS http://localhost:8088/api/v1/streaming-config
curl -fsS http://localhost:8088/api/v1/backend-plan
curl -fsS http://localhost:8088/api/v1/storage_routing
```

Run an instant query:

```bash
curl -fsS --get http://localhost:8088/api/v1/query \
  --data-urlencode 'query=up'
```

A ready result has `status: success` and the expected `infos` annotations for
data source and accuracy. Health alone does not prove that plans, summary data,
or exact fallback are available.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Build cannot resolve a path dependency | Verify the three sibling repository names and locations |
| Data plane exits immediately | Supply `--streaming-config` and check YAML errors and port conflicts |
| OTLP connection is refused | Start with `--enable-otel-ingest` and verify ports 4317/4318 |
| Query returns no compatible summary | Inspect streaming config, BackendPlan, storage routing, metric labels, and window |
| Unsupported query fails | Configure an exact backend or use a supported planned query |
| Result comes from an unexpected tier | Inspect `infos` provenance and the installed storage routing table |
| Results are stale | Check collector export, OTLP ingest, active-window state, and source timestamps |

Capture component commits, effective configuration, relevant logs, diagnostic
endpoint output, and one complete query response when reporting a problem.
Remove credentials and environment secrets first.
