# Developer guide

This guide covers repository setup, component boundaries, and verification.
Focused guides document individual extension points.

## Code architecture

The Cargo workspace contains four members:

| Member | Responsibility |
| --- | --- |
| `control_plane` | Workload analysis, physical planning, plan publication, OpAMP, and control APIs |
| `data_plane` | OTLP ingest, summary state, query routing/readout, and PromQL-compatible HTTP APIs |
| `crates/asap_types` | Backend-owned shared data contracts |
| `crates/asap_otel_proto` | Generated modified-OTLP types used by ingest |

The data-plane build also uses `ASAPCollector/asap-precompute-rs` and a sibling
`asap_sketchlib` checkout. Keep all three repositories as siblings:

```text
repos/
├── ASAPCollector/
├── ASAPQuery-backend/
└── asap_sketchlib/
```

## Build and test

From the ASAPQuery-backend root:

```bash
cargo build --workspace
cargo test --workspace
```

Use package-scoped commands while iterating:

```bash
cargo test -p control_plane
cargo test -p data_plane
cargo test -p asap_types
cargo test -p asap_otel_proto
```

Before committing Rust changes, run the repository's formatting and lint gates:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Some integration paths need external services or the paired deployment in
ASAPCollector. A passing unit suite does not establish end-to-end plan
application or query accuracy; use the Collector MVP harness for those claims.

## Run components locally

Inspect current arguments before composing a local command:

```bash
cargo run -p data_plane -- --help
```

The data plane requires `--streaming-config`. Its defaults expose the query API
on port 8088; OTLP gRPC/HTTP ingest is enabled with `--enable-otel-ingest` and
defaults to ports 4317/4318. The control plane uses environment configuration
and defaults its HTTP API to `0.0.0.0:8080` and OpAMP listener to
`0.0.0.0:4320`.

The control plane currently has no command-line help surface; configure it with
the documented environment variables and component deployment files.

## Extension guides

- [Adding a summary family](adding-summary-family.md)
- [Planner adapter and physical compiler](../../control_plane/docs/developer_docs/planner-and-physical-compiler.md)
- [Runtime plan publication](../../control_plane/docs/developer_docs/runtime-plan-publication.md)
- [BackendPlan runtime](../../data_plane/docs/developer_docs/backend-plan-runtime.md)
- [OTLP summary ingestion](../../data_plane/docs/developer_docs/otlp-summary-ingestion.md)
- [Query routing and readout](../../data_plane/docs/developer_docs/query-routing-and-readout.md)
- [Summary storage and series identity](../../data_plane/docs/developer_docs/summary-storage-and-series-identity.md)
- [Protocol and fallback extensions](../../data_plane/docs/developer_docs/extension-points.md)

When extending a public contract, update its focused guide and verify both
producer and consumer. Private helpers are intentionally not documented as
stable interfaces.
