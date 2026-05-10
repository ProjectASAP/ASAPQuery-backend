# Phase 9 — Controller crate move into ASAPQuery-backend

Goal: relocate the `controller` Rust crate so the backend binary owns
it and can embed an in-process OpAMP server. No more separate
`asap-controller` container. (See `design-controller-into-backend.md`.)

## Files moved

`/mydata/ASAPCollector/controller/` → `/mydata/ASAPQuery-backend/controller/`

Filesystem move via `mv` (cross-repo, so `git mv` was not viable). The
ASAPCollector working tree now shows the controller files as `D`
(deletes) under `git status`; the ASAPQuery-backend working tree shows
them as untracked. The full subtree (`src/`, `proto/`, `build.rs`,
`Cargo.toml`, `Cargo.lock`, `docs/`, `sketch_capabilities.yml`,
`sketch_params_default.yml`) moved intact.

## Cargo.toml diffs

- `/mydata/ASAPQuery-backend/Cargo.toml` — added `"controller"` to
  `[workspace] members`.
- `/mydata/ASAPQuery-backend/asap-query-engine/Cargo.toml` — added
  `controller = { path = "../controller" }` under `[dependencies]`,
  with a comment block flagging that in-process OpAMP wiring is a
  follow-up after Phase 4.
- `/mydata/ASAPQuery-backend/controller/Cargo.toml` — **no edits
  needed**. Audit confirmed it has zero path-deps to other ASAPCollector
  crates (only the `[[bin]] path = "src/main.rs"` self-reference).
- `asap-query-engine/src/main.rs` — added a top-of-file comment block
  noting the new in-process status.

## Build verification

- `cargo build --release -p controller` (ASAPQuery-backend workspace):
  `Finished release profile [optimized] target(s) in 51.15s`
- `target/release/controller` binary present (22 MB, expected size).
- `cargo build --release` of ASAPCollector siblings:
  - `asap-precompute-rs`: `Finished release profile`
  - `asap-gorilla-rust`: `Finished release profile`
- Full backend `cargo build --release` failed with a **pre-existing**
  error in `asap-query-engine/src/drivers/ingest/otel.rs:193` —
  `missing field "unknown_series_ids" in ExportMetricsServiceResponse`.
  This is unrelated to Phase 9 (it predates the controller move and
  reproduces against `main` before any of the changes here). Out of
  scope for this task.

## Agent OpAMP endpoint updates

11 agent YAMLs under `/mydata/ASAPCollector/deploy/configs/` had
`ws://controller:4320/v1/opamp` rewritten to
`ws://backend:4320/v1/opamp` via `sed -i`:

  asap-otel-agent-{kll,hll,cs,cms}-direct.yaml,
  asap-otel-agent-b3-delta-direct.yaml,
  asap-otel-agent-b0-prometheus.yaml,
  asap-otel-agent-b1-serf-prometheus.yaml,
  asap-otel-agent-b5-gorilla-prometheus.yaml,
  asap-otel-agent-b6-asap-single-sketch.yaml,
  asap-otel-agent-b6-gorilla-s3.yaml,
  asap-otel-agent-allsketches.yaml.

Post-grep confirms zero remaining `ws://controller:4320` references.

## run_demo.sh changes

`/mydata/mvp-multinode/run_demo.sh`, `backend_up()` for the asap arm:

- Removed the standalone `asap-controller` `docker run` block (image
  `asap/controller:dev`, ports 4320/4321/8080).
- Backend container now carries the controller env vars that previously
  lived on the controller container: `CONTROLLER_OPAMP_ADDR`,
  `CONTROLLER_GRPC_ADDR`, `CONTROLLER_WORKLOADS`,
  `USE_TYPED_STAGE_SPLIT`, plus the `controller=debug` `RUST_LOG`
  filter.
- Removed the now-meaningless `ASAP_CONTROLLER_URL=http://controller:8080/...`
  env var (the backend talks to the controller in-process).
- Added a volume mount for `mvp-workload.yaml` so the in-process
  controller can read its workload spec.

## Dockerfile changes

- `/mydata/ASAPCollector/deploy/docker/Dockerfile.controller` — deleted
  (`git rm`). No separate image.
- `/mydata/ASAPCollector/deploy/docker/Dockerfile.backend` — read and
  verified unchanged. Its `--build-context backend-src=...` already
  pulls in the entire ASAPQuery-backend tree, which now contains the
  controller crate. The existing `cd ASAPQuery-backend && cargo build
  --release --bin query_engine_rust` step still works as-is; no extra
  build context needed.

## Issues encountered

None worth flagging from the move itself. The pre-existing
`unknown_series_ids` error in the otel driver was already present
before this phase and is tracked separately.
