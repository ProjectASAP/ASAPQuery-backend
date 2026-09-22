# E2E physical-DAG walkthrough for Milind

This is a reproducible inspection path from workload inputs to installed state
and query execution. Start with the checked-in demonstration, then replace its
workload and samples with a dataset replay. The demo's declared costs are not
measurements, and neither a compiled plan nor an HTTP success proves a benefit.

Commands below run from the ASAPQuery-backend repository root. Use the repository
Rust toolchain and dependency checkout setup; the workspace currently expects
sibling ASAPCollector and asap_sketchlib checkouts as declared in Cargo.toml.
Docker, curl, Python 3 and jq are needed for the interactive Prometheus demo.
Record backend/Planner/sketch-library revisions and input hashes for a comparison.

## a. Workload and ERP → post-ASAP candidates

The input [demonstration snapshot](../examples/asapquery-compatibility-demo-snapshot.json)
is a `BackendLocalPlanningSnapshot`. Its `query_workload` and `data_workload`
use ASAPPlanner's shared types. `implementation` supplies backend capability,
window and cost inputs; optional `implementation.erp` supplies ERP evidence.
`environment.target` is `backend_local_remote_write` for this path.

```bash
mkdir -p target/physical-dag-inspection
jq '{snapshot_version, query_workload, data_workload, implementation, environment}' \
  docs/examples/asapquery-compatibility-demo-snapshot.json
cargo run --locked -p control_plane --example calibration_candidates -- \
  docs/examples/asapquery-compatibility-demo-snapshot.json \
  > target/physical-dag-inspection/candidates.json
jq '.candidates[] | {candidate_index, planner_selected_queries, unavailable_reason}' \
  target/physical-dag-inspection/candidates.json
```

`calibration_candidates` exports bindable alternatives and their actual node/edge
structure; it does **not** choose an evaluation winner. Unsupported candidates
retain `unavailable_reason`. Some leaf metadata is explicitly Debug-encoded;
this diagnostic forest is not an installable IR format.

Planner owns query semantics, summary families, parameters and candidate
selection. ERP can affect supported evidence-based choices, but supplying an
artifact does not prove that it was eligible or used. Freshness, source/update
semantics, parameters and accuracy constraints still apply. The checked-in demo
is not an empirical-ERP benchmark. For deployment, the backend automatically computes workload costs from ERP or
analytical unit resources and physical demand. Optional calibrated overrides use
the [cost evidence workflow](../examples/workload-cost-evidence.md). Do not relabel
analytical demo costs as measured evidence.

For the existing observation → ERP-selected KLL → installed HTTP correctness
fixture, see [ERP process validation](../developer_docs/erp-process-validation.md):

```bash
cargo test --locked -p data_plane --test asapquery_compatibility_process_e2e \
  erp_planning_process -- --nocapture
```

Its `ERP_PLANNED` and `ERP_WARM` records expose the fitted observation, available
profiles, selected parameters, installed identity and actual response. This is a
separate correctness fixture, not measured evidence for the demo workload.

## b. Selected SDS → control-plane execution plans

Run the normal snapshot compiler, without selecting a candidate index or
substituting a preferred sketch family:

```bash
cargo run --locked -p control_plane --example compile_workload_artifact -- \
  "$ASAPQUERY_PLANNING_SNAPSHOT" \
  --dot target/physical-dag-inspection/selected.dot \
  > target/physical-dag-inspection/selected.json
jq '{summary_catalog, precompute_plan, query_plan}' target/physical-dag-inspection/selected.json \
  > target/physical-dag-inspection/physical-plan.json
jq '{summary_catalog, precompute_plan, query_plan}' \
  target/physical-dag-inspection/selected.json
dot -Tsvg target/physical-dag-inspection/selected.dot \
  -o target/physical-dag-inspection/selected.svg
```

`compile_workload_artifact` calls the same snapshot compiler used by startup.
Set `ASAPQUERY_PLANNING_SNAPSHOT` to the workload snapshot. Without an explicit
provider override it uses automatic ERP/analytical workload costing. The output
includes the selected plan, logical selection trace, resource assumptions and
complete cost comparison. Both PromQL and MetricsQL use this path.

The compiler derives sibling plans from the selected post-ASAP DAG:

| Artifact | What to inspect |
| --- | --- |
| `SummaryCatalog` | Source/filter/grouping `DataDescriptor`, accumulator/readout `SummaryDescriptor`, stable `SummaryDefinitionId`, physical layout and origin |
| `PrecomputePlan` | Actual materializations, semantic window/slide, stored layout, ingest protocol, schemas, producer bindings and installed executable subDAGs |
| `QueryPlan` | Query roots, exact dependencies, materialization bindings, readout operation and output grouping |
| `TransmissionPlan` | Producer frame identity/encoding policy; a backend-local plan need not have external collector rules |

These are the existing shared contracts, not separate optimizer or storage DTOs.
FullWindow, Pane and HierarchicalRollup are explicit representations. A backend
must not invent a pane width from a query interval. Layout, cadence and origin
participate in identity, cost and coverage checks.

## c. Samples → physical SID and SummaryInstanceId

For the simplest live run:

```bash
export ASAPQUERY_PLANNING_SNAPSHOT=/absolute/path/priced-snapshot.json
./scripts/e2e.sh asapquery-demo
```

The [demo runner](../../demos/asapquery/run.sh) starts pinned Prometheus and
Pushgateway containers, starts the production backend with the snapshot, emits
gauge/counter/latency samples (including a counter reset), compares queries and
writes evidence under `target/asapquery-demo-evidence`. Its cleanup stops its own
processes; inspect the saved output after it exits.

For a separately managed Prometheus, configure:

```yaml
remote_write:
  - url: http://127.0.0.1:9091/api/v1/write
```

Then keep the backend running for inspection:

```bash
cargo run --locked -p data_plane -- \
  --profile asapquery \
  --planning-snapshot "$ASAPQUERY_PLANNING_SNAPSHOT" \
  --prometheus-server http://127.0.0.1:9090 \
  --forward-unsupported-queries --http-port 9091 \
  --output-dir target/physical-dag-inspection/runtime
curl -fsS http://127.0.0.1:9091/api/v1/physical-plan/status | jq .
curl -fsS http://127.0.0.1:9091/api/v1/summary-inventory | jq .
```

Run the curls from another terminal. An installed plan can precede ready
instances. `SummaryDefinitionId` identifies reusable state semantics, while the
resolver allocates a physical SID for a population/lifetime. Catalog-bound
metadata exposes `SummaryInstanceId` and concrete window/group coordinates.
Do not use a query-local node number or a plan definition ID as a physical SID.
Persisted SID lifetimes and catalog generations matter on restart/reactivation.

Remote Write v1 uses Snappy-compressed protobuf `WriteRequest`, not JSON posted
to this URL. Accepted scalar samples enter bounded queues, workers update the
selected accumulators, and the sink writes SketchStore. `204` means atomic
bounded queue admission and in-memory dedup bookkeeping, **not** durable
accumulator commit. There is no durable raw-sample WAL or recovered receiver
dedup state. Native histograms and exemplars are rejected. Stale markers are
recognized, deduplicated and counted, then excluded from numeric aggregation;
this is not propagation of a lifecycle event to the worker.

Legacy population routing works on main. Versioned canonical routing has an
explicit configuration identity; its installation is still gated until the
complete-population runtime/CP integration is accepted. Do not edit only the
outer state's version or assume new routing has been activated by this guide.

## d. Precompute subDAG and completion

```bash
jq '.precompute_plan | {materializations, executable_dags}' \
  target/physical-dag-inspection/selected.json
```

Look for explicit maintenance-time operations and frontier bindings. Raw
materializations receive samples; a derived materialization consumes its
bound immutable input program and must not receive raw backfill/collector jobs.
The runtime supports only its validated operator, schema, grouping and window
subset. A traversable DAG does not prove every operator can execute.

For finite replay only, after sending the entire dataset:

```bash
curl -fsS -X POST http://127.0.0.1:9091/api/v1/precompute/drain | jq .
```

This is a finite-input closure operation, not a timer or an ordinary flush for a
live Prometheus deployment. It closes that receiver; later writes in the closed
generation are rejected. Durable completion and retry/recovery are distinct
from queue admission. See [maintenance and completion](../developer_docs/maintenance-replay.md).

Prometheus `WriteRequest` has no authoritative event-time watermark or
producer/partition roster. Worker maximum timestamps and inactivity therefore
cannot prove continuous global completion. The existing typed
`SummaryWatermarkBarrier` carries catalog generation, producer/partition/epoch,
sequence and watermark; `MultiSourceCoordinator` validates its registered
partition set. `OutputSink::advance_summary_watermark` is an internal Rust API,
not a public HTTP barrier endpoint. External producer binding, durable ordering
and continuous scheduling must be completed before claiming that live
remote-write automatically supplies this proof.

## e. QueryPlan → physical DAG → SID-bound readout

```bash
jq '.query_plan.entries' target/physical-dag-inspection/selected.json
curl -sS -D target/physical-dag-inspection/query.headers --get \
  http://127.0.0.1:9091/api/v1/query \
  --data-urlencode 'query=sum(sum_over_time(asap_demo_gauge[5s]))' \
  > target/physical-dag-inspection/query.json
jq . target/physical-dag-inspection/query.json
```

Use a registered query and an evaluation time covered by the samples; absence
of readiness should remain visible. A QueryPlan `ReadMaterialization` binds a
summary definition/window/grouping. The store resolves compatible physical
instances under the active catalog; it checks coverage and decodes actual state
before the typed readout. Exact residual leaves can coexist with summary reads.

Inspect both HTTP headers and JSON. A successful response is not automatically
warm: check actual `infos`, `x-asap-execution`, errors, labels, timestamps and the
exact-backend trace. An external-only installed DAG is not ASAP acceleration.
An error or missing result must not carry a successful warm classification.
Compare complete series/label sets; never normalize away output-label errors.

The finite multi-source stack has real single-population process coverage.
Wider canonical multi-group activation is separately under validation: a
20/40-group `quantile(0.9, ...)` probe exposed a DDS rank/interpolation mismatch
(PromQL mathematical reference 38). Do not use the pending activation as a passed accuracy or
performance result. The actual selected family's semantics and error bound must
pass before that scope is described as supported.

## f. Dataset replay or a fake exporter

For a scrape-based synthetic source, use the demo's Pushgateway emission loop
and [Prometheus configuration](../../demos/asapquery/prometheus.yml). Prometheus
performs normal scraping and remote-write encoding. Sending exposition text
directly to `/api/v1/write` is not equivalent.

For timestamp-preserving recorded input, use the existing replay tool:

```bash
cargo build --locked -p control_plane --example compile_workload_artifact
cargo build --locked -p data_plane --bin data_plane
python3 tools/o11y-execution/replay.py \
  --metrics /path/metrics.txt --queries /path/queries.json \
  --snapshot /path/costed-version2-snapshot.json \
  --compiler target/debug/examples/compile_workload_artifact \
  --data-plane target/debug/data_plane \
  --exact-url http://127.0.0.1:9090 --output /path/new-run
```

These paths are operator-supplied inputs, not bundled datasets. Use a fresh,
dedicated exact Prometheus with its remote-write receiver enabled; the tool
sends the same validated samples to both sides. Preserve input generator
revision/seed, file hashes, original query occurrences and evaluation times.
See [replay requirements and output schema](../../tools/o11y-execution/README.md).
Inspect `planning.json`, `installed.json`, `ingestion.json`, `queries.json` and,
when comparing, `comparison.json`. A finished run is not proof of benefit.

## Prometheus, VictoriaMetrics and ClickHouse boundaries

| Backend | Drop-in surface and current boundary |
| --- | --- |
| Prometheus | Remote Write v1 samples plus PromQL instant/range HTTP; keep Prometheus as the exact fallback and raw-data authority. The strict `asapquery` profile has explicit startup exclusions. |
| VictoriaMetrics | The broader backend has a MetricsQL compile/adapter path. `compile_workload_artifact SNAPSHOT.json --metricsql` uses it. This does not turn the strict Prometheus profile into a general VM replacement; counter boundary semantics and unsupported expressions must retain exact routing. |
| ClickHouse | The broader backend has a SQL workload compiler and typed exact/relational execution. `compile_clickhouse_workload` reads its own `ClickHouseSqlAutomaticWorkload` JSON from stdin; it does not consume the Prometheus snapshot. List/Map/Tuple support does not imply arbitrary lambdas/counter SQL or full protocol compatibility. External-only DAG coverage is not summary acceleration. |

Use the existing process suites to validate a specific supported protocol shape;
`./scripts/e2e.sh asapquery` exercises production wire/worker/store/query paths
with a mock exact upstream; it does not start a real Prometheus server.
The focused production-path matrix can also run directly:

```bash
cargo test --locked -p data_plane --test asapquery_compatibility_process_e2e \
  collector_free_profile_serves_complete_matrix_and_falls_back_exactly -- --exact
```

This focused test uses a mock exact upstream and removes its temporary artifacts
on completion. It passed against clean backend, Collector and sketch-library
checkouts; it is not evidence of a real Prometheus server deployment.
`asapquery-demo` is the separate real-Prometheus/Pushgateway run. Report
correctness, fallbacks, build/update cost, query latency and whole-deployment
resources separately. Do not infer a cross-backend speedup from this walkthrough.
