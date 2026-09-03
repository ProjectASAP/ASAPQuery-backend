# Run the ASAPQuery compatibility profile

The `asapquery` runtime profile runs without ASAPCollector. It accepts
Prometheus Remote Write v1 and sends unsupported or not-yet-ready PromQL to the
same Prometheus server as an exact fallback.

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
  --streaming-config data_plane/examples/asapquery/streaming-config.yaml \
  --prometheus-server http://127.0.0.1:9090 \
  --forward-unsupported-queries \
  --http-port 9091 \
  --output-dir /tmp/asapquery-backend
```

The profile checks Prometheus's `/-/healthy` endpoint before opening its public
listener. It rejects OTLP ingest, monitor coordination, persistence, backfill,
schema eviction, and archive routing flags. Remote Write and PromQL share the
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

The example streaming config is a checked-in precompute-plan fixture. Startup
loading of canonical `QueryWorkload` and `DataWorkload` snapshots and automatic
backend-only compilation remain required before the profile meets the complete
compatibility contract in the design document.
