# Run the ASAPQuery compatibility profile

The `asapquery` runtime profile runs without ASAPCollector. It accepts
Prometheus Remote Write v1 and sends unsupported or not-yet-ready PromQL to the
same Prometheus server as an exact fallback.

For a complete, self-checking run with a real Prometheus server:

```bash
./scripts/e2e.sh asapquery-demo
```

The command builds the production backend, starts pinned Prometheus and
Pushgateway containers, drives raw samples (including a counter reset), checks
all declared instant and range warm queries, compares an unplanned query with a
direct Prometheus fallback response, and writes status/results/metrics under
`target/asapquery-demo-evidence`. It requires Docker, `curl`, and Python 3.

For the hermetic production-process conformance suite (no Docker):

```bash
./scripts/e2e.sh asapquery
```

Start Prometheus first, configure it to write to the backend, and keep its
normal local storage enabled:

```yaml
remote_write:
  - url: http://127.0.0.1:9091/api/v1/write
```

Then start the backend:

```bash
cargo run -p data_plane -- \
  --profile asapquery \
  --planning-snapshot /etc/asapquery/workload.json \
  --prometheus-server http://127.0.0.1:9090 \
  --forward-unsupported-queries \
  --http-port 9091 \
  --output-dir /tmp/asapquery-backend
```

The profile checks Prometheus's `/-/healthy` endpoint before opening its public
listener. It rejects OTLP ingest, monitor coordination, persistence, backfill,
schema eviction, archive routing flags, and the legacy `--streaming-config`
bootstrap. Remote Write and PromQL share the
backend listener:

- `POST /api/v1/write`
- `GET` or `POST /api/v1/query`
- `GET` or `POST /api/v1/query_range`

Prometheus should use the standard Remote Write v1 headers
`Content-Encoding: snappy`, `Content-Type: application/x-protobuf`, and
`X-Prometheus-Remote-Write-Version: 0.1.0`. The receiver validates the entire
batch before enqueueing it. Invalid batches return `400` or `413`; exhausted
deduplication or worker-queue capacity returns retryable `503`; accepted batches
return `204`.

The deduplication horizon must cover allowed lateness plus the deployment's
maximum expected Prometheus retry interval. Configure those assumptions with
`--precompute-allowed-lateness-ms`,
`--remote-write-expected-retry-interval-ms`, and
`--remote-write-dedup-horizon-ms`. Startup fails when the horizon is too short.

Receiver evidence is exported from `/metrics` as
`asap_remote_write_requests_total`, `asap_remote_write_samples_total`,
`asap_remote_write_stale_markers_total`,
`asap_remote_write_duplicates_total`,
`asap_remote_write_rejected_requests_total`, and
`asap_remote_write_bytes_total`.

`workload.json` is a versioned `BackendLocalPlanningSnapshot`. Its
`query_workload` and `data_workload` fields deserialize directly into the
canonical ASAPPlanner types; `implementation` contains only backend-owned
cost/window evidence, and `environment.target` must be
`backend_local_remote_write`. Startup validates the workload, calls the pinned
ASAPPlanner, compiles matching SummaryCatalog/PrecomputePlan/QueryPlan views, and
installs the resulting immutable snapshot before accepting traffic. It does
not create or wait for a CollectorPlan.

A complete canonical input is checked in at
`docs/examples/asapquery-planning-snapshot.json`.

For reproducibility/debugging, `--physical-plan` remains an alternative to
`--planning-snapshot`; exactly one is required. `physical-plan.json` is the
JSON representation accepted by
`POST /api/v1/physical-plan`: a `SummaryCatalog`, `PrecomputePlan`,
`TransmissionPlan`, and authoritative `QueryPlan` DAG with one shared plan
identity and version. For this profile, the precompute ingest contract must be
`prometheus_remote_write_v1` at `/api/v1/write`; raw writes are rejected if a
different plan is active. Startup validates all fingerprints, schemas, query
bindings, lifecycle fields, and transmission rules before constructing the
single active snapshot. Runtime replacement uses `POST /api/v1/physical-plan`
to stage the complete successor and `POST /api/v1/physical-plan/activate` for
the atomic cutover. The old partial `/backend-plan` endpoint has been removed;
the `/streaming-config` compatibility endpoint is not attached in this profile.

Query serving uses only the installed `QueryPlan` DAG. A query absent from that
DAG is a capability miss and goes to the exact Prometheus fallback; the backend
does not search materialization candidates while serving.

Per-series window queries also retain exact fallback when the raw producer cannot
preserve every source label. For example, bare `sum_over_time(m[1m])` must return
one value per series; a pooled accumulator cannot replace those rows. Explicit
additive reductions such as `sum(sum_over_time(m[1m]))` and grouped variants can
still use warm state. The demo uses this explicit global sum and forwards its
bare quantile query to Prometheus.

Counter `rate` and `increase` queries currently use the exact Prometheus
fallback in this raw-ingest profile. The backend does not install pooled counter
state because independent series can reset or arrive at the same timestamp.
Other supported queries in the workload can still use warm summaries. Previously
generated artifacts containing raw counter state are rejected; regenerate them
from their workload snapshots.

Activation and warm readiness are deliberately separate. A newly activated
generation exposes each materialization as `materializing`; the QueryPlan path
promotes it through `ready` to `serving` only after its closed-window coverage
fully spans the requested interval. Missing, partial, stale, or generation-raced
coverage is a capability miss, never a partial warm success. Inspect the
per-materialization state and observed coverage through
`GET /api/v1/physical-plan/status`.

Snapshot schema version `1` currently accepts fixed-interval repeating PromQL
queries with explicit whole-second lookbacks and fresh ingestion-rate
evidence. Unsupported snapshot semantics fail startup rather than silently
inventing cost or placement evidence.
