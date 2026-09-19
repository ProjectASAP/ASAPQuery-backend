[![MVP CI](https://github.com/ProjectASAP/ASAPQuery-backend/actions/workflows/mvp-ci.yml/badge.svg)](https://github.com/ProjectASAP/ASAPQuery-backend/actions/workflows/mvp-ci.yml)

# ASAPQuery-backend

ASAPQuery-backend compiles and executes planned observability queries using
materialized exact accumulators and sketches, with configured exact backends
for unsupported or unavailable results. It contains the backend control plane,
summary storage, precompute runtime and query adapters. Query semantics and
post-ASAP candidate generation come from the external
[ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner) dependency.

Start with the [E2E physical-DAG walkthrough](docs/evaluation/e2e-physical-dag.md)
for workload inputs, ERP evidence, selected plans, sample ingestion, storage
inspection and query execution. The [documentation index](docs/README.md) links
the detailed guides. This README describes the repository entry points; it does
not promise a universal latency improvement or a single accuracy bound across
summary families.

## Planning, state and execution

```text
Workload + capabilities + cost/accuracy evidence
                    |
          external ASAPPlanner
          selected post-ASAP DAG
                    |
       backend PhysicalCompiler
                    |
       authoritative SummaryCatalog
       + shared physical-plan publication
           /                        \
   PrecomputePlan                 QueryPlan
   producer/input subDAGs         query/readout subDAGs
           |                        |
    SummaryStore <---- bound state reads
           |                        |
   physical instances       result or exact fallback
```

The backend compiler consumes the selected Planner DAG and places supported
operations. It preserves semantic nodes and bindings rather than rediscovering
query intent from metric names. ERP evidence can influence eligible choices;
its presence alone does not prove selection or measured benefit.

| Contract or component | Responsibility |
| --- | --- |
| `SummaryCatalog` | Authoritative generation and descriptors for data, summary semantics, population, windows and materialization identity. Sibling plans validate against the same catalog. |
| `PrecomputePlan` | Materializations, input schemas, producer bindings and installed precompute subDAGs. Window size, slide, layout and origin are explicit contracts. |
| `ProducerContract`, `CollectorPlan`, `TransmissionPlan` | Producer/materialization authorization, external collector work and state-frame encoding/sequence rules. They are shared producer/consumer contracts; backend-local Remote Write does not require ASAPCollector. |
| `QueryPlan` | Language-tagged query entries, exact dependencies, typed operations and materialization/readout bindings. Query subDAGs execute against the active installed snapshot. |
| SummaryStore (`SketchStore`) | Physical state, indexes, lifecycle and persistence. A `SummaryDefinitionId` identifies the planned materialization. SID identifies a physical stored series/lifetime; `SummaryInstanceId` identifies a concrete definition/window/group instance. Planner node IDs are not storage IDs. |
| Physical-plan publication | Publishes catalog, precompute, query, collector and transmission views together; invalid bindings reject installation rather than silently selecting replacement state. |

See the [physical compiler](docs/developer_docs/control-plane/physical-compiler.md),
[catalog/runtime design](docs/developer_docs/query-engine/catalog-physical-plan-runtime.md)
and [maintenance protocol](docs/developer_docs/maintenance-replay.md).

## Interfaces and support boundaries

```mermaid
flowchart TB
  subgraph planning["Planning and installed contracts"]
    PL["External ASAPPlanner"] --> CP["Backend control plane"]
    CP --> CAT["Authoritative SummaryCatalog"]
    CP --> PP["PrecomputePlan"]
    CP --> QP["QueryPlan"]
    CP --> PC["Producer contracts / CollectorPlan"]
    CP --> TP["TransmissionPlan"]
  end

  subgraph ingest["Sample and state ingestion"]
    RW["Prometheus Remote Write v1"] --> RX["RW receiver / bounded queue"]
    RX --> PD["Precompute DAG"]
    EP["External producers: modified OTLP / state"] --> PV["Producer / transmission validation"]
    PV --> PD
    PD --> SS["SummaryStore: physical series and window/group instances"]
  end

  subgraph queries["Query adapters and execution"]
    PQ["PromQL"] --> QD["PromQL query DAG"]
    MQ["MetricsQL"] --> MA["MetricsQL adapter"]
    SQL["ClickHouse SQL"] --> SD["Typed SQL query DAG"]
    QD --> QR["Shared query runtime"]
    MA --> QR
    SD --> QR
    QR -->|"bound state reads"| SS
    QD -->|"unsupported / unavailable"| PE["Prometheus exact backend"]
    MA -->|"unsupported / unavailable"| VE["VictoriaMetrics exact backend"]
    SD -->|"unsupported / unavailable"| CE["ClickHouse exact backend"]
    QR --> W["Warm: successful summary execution"]
    QR --> H["Hybrid: summaries plus supported exact dependencies"]
    PE --> F["Exact fallback result"]
    VE --> F
    CE --> F
    CE -->|"supported exact SQL subtree"| SD
  end

  subgraph completion["Independent completion authority"]
    B["Typed watermark / barrier: installed producer and partition scope"]
    B --> CC["Continuous coordinator: closure activation gated"]
    CC -.->|"gated completion contract"| PD
  end

  PP -.-> PD
  QP -.-> QR
  PC -.-> PV
  TP -.-> PV
  PC -.-> B
  CAT -.-> SS
  CAT -.-> QR
```

Dotted edges show installed contracts or explicitly gated paths. The barrier is
independent of Remote Write samples; this figure does not advertise a public
barrier endpoint or continuous/sliding activation. Warm and hybrid labels apply
only to successful execution with the corresponding actual summary reads;
external-only DAG execution is not acceleration.

| Surface | Current path and boundary |
| --- | --- |
| Prometheus | `/api/v1/write` accepts Remote Write **v1** scalar samples. PromQL instant/range queries use installed plans with configured Prometheus exact fallback. The collector-free `asapquery` profile is a bounded compatibility profile, not every distributed/persistent deployment option. |
| VictoriaMetrics | A separate MetricsQL adapter and listener lower supported expressions through Planner and the shared runtime. Unsupported syntax or unavailable coverage retains VictoriaMetrics exact fallback. See [MetricsQL support](docs/developer_docs/query-engine/victoriametrics-metricsql-support.md). |
| ClickHouse | An optional SQL HTTP adapter uses the same catalog/publication with typed relational and mixed exact/summary DAG execution. Supported collection/scalar operations are explicit; arbitrary SQL, lambdas and counter queries are not implied. External-only DAG execution is not summary acceleration. See [SQL support](docs/developer_docs/query-engine/clickhouse-sql-support.md). |
| External producers | The distributed path accepts configured collector/state input, including modified OTLP. It is distinct from standard Prometheus Remote Write and uses the shared producer/transmission contracts. |

### Query workload input

Planning input and serving traffic are separate. Sending a PromQL request from
Grafana does not declare that it is a recurring dashboard query. Declare known
query classes through the control plane; send each evaluation to the data-plane
query endpoint.

At startup, set `CONTROLLER_WORKLOADS` to a YAML registry containing the query
text and deployment hints:

```yaml
- metric_name: http_requests_total
  query_string: "sum by (region) (rate(http_requests_total[5m]))"
  accuracy_sla: 0.99
  assign_to_role: agent
  grouping_labels: [region]
  repeat_every: 30s
```

`query_string` is the preferred source for metric, aggregation, filters,
grouping and range-window semantics. Optional registry fields include
`sketch_family_override`, `sample_p`, `distinct_keys_per_window`, `item_label`,
`monitor` and `repeat_every`. `accuracy_sla` is the legacy success fraction:
`1.0` requests exact results and `0.99` permits epsilon `0.01`. An unknown key
is rejected rather than ignored, and a registry file that exists but does not
parse fails startup instead of degrading to an empty registry.

The startup registry and the legacy planning API carry the same evaluation
cadence, so a declaration is costed identically through either entry point.
`POST /api/v1/plan` (`CONTROLLER_ADDR`, default port `8080`):

```bash
curl -X POST http://localhost:8080/api/v1/plan \
  -H 'Content-Type: application/json' \
  -d '{
    "query_string": "sum by (region) (rate(http_requests_total[5m]))",
    "repeat_every": "30s",
    "accuracy_sla": 0.99,
    "latency_sla": "1s",
    "workload": {
      "series_count": 100000,
      "samples_per_sec_per_series": 0.0667,
      "bytes_per_raw_sample": 100,
      "distinct_keys_per_window": null,
      "data_distribution": "zipf",
      "memory_budget_bytes": 268435456
    }
  }'
```

`repeat_every` describes an expected cadence—for example, a dashboard panel
refreshed every 30 seconds—but does not schedule evaluations. Its current use is
narrow: the legacy deployment cost path uses it as the flush-period proxy for
batch-mode plans. Window-mode plans use their configured window duration, and
the canonical physical-plan request does not yet model general query recurrence.
Do not assume that changing `repeat_every` will change summary selection.

The `workload` object describes the incoming data stream and may be omitted to
use conservative defaults. The API also accepts explicit `metric_name`,
`aggregations`, `time_window`, `group_by_labels` and `label_filters` instead of
a query string.

Once the matching physical plan is installed and its state is ready, clients
execute the query through the data plane's Prometheus-compatible API:

```bash
curl -G http://localhost:8088/api/v1/query \
  --data-urlencode 'query=sum by (region) (rate(http_requests_total[5m]))'
```

Range evaluations use `/api/v1/query_range` with `query`, `start`, `end` and
`step`. Serving requests do not update `repeat_every` automatically. End-to-end
dashboard-frequency-aware planning still requires a typed recurrence field on
the canonical workload/publication path and an observer or explicit workload
registration to populate it.

Remote Write `204` acknowledges atomic bounded queue admission, **not** durable
accumulator publication or a raw write-ahead log. Deduplication is in memory;
there is no durable raw-WAL replay guarantee. Stale markers are recognized and
excluded from numeric aggregation, not propagated as general lifecycle events.
Native histograms and exemplars are rejected in this v1 path.

Remote Write does not carry an authoritative completion watermark. Installed
producer partition rosters and typed barriers define a separate completion
scope; roster membership alone does not provide authentication, durable progress
or continuous scheduling. Finite immutable maintenance has explicit source,
population, window and operator gates. Continuous/sliding activation must not be
inferred from available metadata or a successful ingest response.

Fallback and execution provenance must reflect the path that actually produced
a successful result. Missing coverage and unsupported operations must not be
reported as warm success. Accuracy and benefit claims need the selected family's
contract and workload-specific evidence. Thanos/object-store integrations are
optional deployment paths, not the default architecture or a required quickstart.

## Build and inspect: shared setup

Run these steps in Bash on Linux. The Docker commands below use host networking;
other platforms need equivalent port/address configuration. Each backend runbook
has its own ports and artifacts and can be run independently after this setup.

### 1. Check out the backend and its sibling dependencies

```bash
mkdir -p asap-workspace
cd asap-workspace
git clone https://github.com/ProjectASAP/ASAPQuery-backend.git
git clone https://github.com/ProjectASAP/ASAPCollector.git
git clone https://github.com/ProjectASAP/asap_sketchlib.git
cd ASAPQuery-backend
# Follow this backend revision's CI dependency pin, rather than a copied SHA.
ASAP_SKETCH_REF="$(awk '/repository: ProjectASAP\/asap_sketchlib/{found=1;next} found && /^[[:space:]]*ref:/{print $2;exit}' .github/workflows/mvp-ci.yml)"
git -C ../asap_sketchlib checkout "${ASAP_SKETCH_REF:-main}"
git -C ../ASAPCollector checkout main
```

[MVP CI](.github/workflows/mvp-ci.yml) is the source for compatible dependency
checkouts; currently Collector uses its default branch. For reproducible runs,
record all three exact revisions. ASAPPlanner is fetched at the revision pinned
in Cargo manifests; do not substitute an unrelated local planner checkout.

### 2. Check prerequisites and build

Install Rust through rustup and install `protoc`, `curl`, `jq`, Python 3 and Git
using your system package manager. Docker is needed only for the real
Prometheus/Pushgateway demo below. Rust 1.98.0 is the toolchain used for these
local command checks.

```bash
rustup toolchain install 1.98.0 --profile minimal --component rustfmt --component clippy
rustc +1.98.0 --version
protoc --version
curl --version
jq --version
python3 --version
# For Docker-backed demos only:
docker version

cargo +1.98.0 fetch --locked
cargo +1.98.0 build --locked -p control_plane -p data_plane
cargo +1.98.0 build --locked -p control_plane \
  --example calibration_candidates --example compile_workload_artifact \
  --example compile_clickhouse_workload
target/debug/data_plane --help
mkdir -p target/readme-evidence
{
  git rev-parse HEAD
  git -C ../ASAPCollector rev-parse HEAD
  git -C ../asap_sketchlib rev-parse HEAD
} > target/readme-evidence/revisions.txt
```

The executable is `target/debug/data_plane`, or `target/release/data_plane` if
you build with `--release`. Access to the pinned Git dependencies is required.

### 3. Export candidates and select a deployable plan by complete cost

```bash
target/debug/examples/calibration_candidates \
  docs/examples/asapquery-compatibility-demo-snapshot.json \
  > target/readme-evidence/candidates.json
jq '.candidates[] | {candidate_index, unavailable_reason}' \
  target/readme-evidence/candidates.json

target/debug/examples/compile_workload_artifact \
  "$ASAPQUERY_PLANNING_SNAPSHOT" \
  --dot target/readme-evidence/selected.dot \
  > target/readme-evidence/selected.json
jq '.cost_comparison' target/readme-evidence/selected.json
jq '.summary_catalog' target/readme-evidence/selected.json
jq '.precompute_plan | {materializations, executable_dags}' \
  target/readme-evidence/selected.json
jq '.query_plan.entries' target/readme-evidence/selected.json
jq '.precompute_plan.schemas[] | {materialization, schema_id}' \
  target/readme-evidence/selected.json
dot -Tsvg target/readme-evidence/selected.dot \
  -o target/readme-evidence/selected.svg
```

Candidate discovery accepts the checked-in unquoted templates. Deployment and
selected-plan inspection require `ASAPQUERY_PLANNING_SNAPSHOT` to point to a
snapshot with complete, valid workload cost evidence. Prepare that input using
the [cost evidence workflow](docs/examples/workload-cost-evidence.md).
There is one snapshot compiler: it compares complete executable alternatives,
including exact fallback. Materialization IDs are definitions, not physical SIDs.
For `--metricsql`, collect quotes for the MetricsQL frontend.

## Prometheus runbook

### Start real Prometheus and Pushgateway, then the backend

The checked-in [Prometheus YAML](demos/asapquery/prometheus.yml) scrapes
Pushgateway at `127.0.0.1:19092` every second and includes:

```yaml
remote_write:
  - url: http://127.0.0.1:19091/api/v1/write
    queue_config:
      min_shards: 1
      max_shards: 1
      batch_send_deadline: 1s
```

Use unused ports 19090–19092 and container names below. Prometheus must be healthy
before the `asapquery` profile starts. Initial Remote Write retries while the
backend starts are expected.

```bash
mkdir -p target/readme-evidence/prometheus
docker run -d --name asap-readme-pushgateway --network host \
  prom/pushgateway:v1.9.0 --web.listen-address=:19092
docker run -d --name asap-readme-prometheus --network host \
  -v "$PWD/demos/asapquery/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  prom/prometheus:v2.55.1 --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus --web.listen-address=:19090
for attempt in $(seq 1 60); do
  curl -fsS http://127.0.0.1:19090/-/healthy && break
  sleep 1
done
curl -fsS http://127.0.0.1:19090/-/healthy

target/debug/data_plane --profile asapquery \
  --planning-snapshot "$ASAPQUERY_PLANNING_SNAPSHOT" \
  --prometheus-server http://127.0.0.1:19090 \
  --forward-unsupported-queries --http-port 19091 \
  --output-dir target/readme-evidence/prometheus/runtime \
  > target/readme-evidence/prometheus/backend.log 2>&1 &
echo $! > target/readme-evidence/prometheus/backend.pid
for attempt in $(seq 1 60); do
  curl -fsS http://127.0.0.1:19091/api/v1/health && break
  sleep 1
done
curl -fsS http://127.0.0.1:19091/api/v1/health
```

### Send samples and inspect execution

Pushgateway accepts exposition text; Prometheus performs the real scrape and
Snappy/protobuf Remote Write encoding. Do not post this text to `/api/v1/write`.

```bash
for sample in $(seq 1 15); do
  printf '# TYPE asap_demo_gauge gauge\nasap_demo_gauge %s\n' "$sample" |
    curl -fsS --data-binary @- http://127.0.0.1:19092/metrics/job/asapquery-demo
  sleep 1
done
curl -fsS http://127.0.0.1:19091/api/v1/physical-plan/status \
  | tee target/readme-evidence/prometheus/status.json | jq .
curl -fsS http://127.0.0.1:19091/api/v1/summary-inventory \
  | tee target/readme-evidence/prometheus/inventory.json | jq .
curl -sS -D target/readme-evidence/prometheus/query.headers --get \
  http://127.0.0.1:19091/api/v1/query \
  --data-urlencode 'query=sum(sum_over_time(asap_demo_gauge[5s]))' \
  > target/readme-evidence/prometheus/query.json
jq '{status, data, infos}' target/readme-evidence/prometheus/query.json
cat target/readme-evidence/prometheus/query.headers
```

Check actual response provenance and window coverage; a successful HTTP response
alone is not warm execution. For **a separately completed finite replay only**,
after every input has been delivered, the closure operation is:

```bash
curl -fsS -X POST http://127.0.0.1:19091/api/v1/precompute/drain | jq .
```

Do not run that command against the live scraping setup above: it closes that
receiver generation and later writes are rejected. It is not a periodic flush
or an implicit continuous watermark. The real finite maintenance process test
is available separately:

```bash
cargo +1.98.0 test --locked -p data_plane --test asapquery_compatibility_process_e2e \
  immutable_maintenance_process::complete_group_maintenance_is_automatic_and_durable \
  -- --exact --nocapture
```

### Validate and clean up

The focused protocol matrix uses the real backend process with a **mock exact
upstream**, and removes temporary artifacts. The second command runs the
separate **real Prometheus/Pushgateway** demo and handles its own cleanup.
Stop the manual setup before the real demo because it uses the same ports.

```bash
kill "$(cat target/readme-evidence/prometheus/backend.pid)"
docker rm -f asap-readme-prometheus asap-readme-pushgateway
cargo +1.98.0 test --locked -p data_plane --test asapquery_compatibility_process_e2e \
  collector_free_profile_serves_complete_matrix_and_falls_back_exactly -- --exact
export ASAPQUERY_PLANNING_SNAPSHOT=/absolute/path/priced-snapshot.json
./scripts/e2e.sh asapquery-demo
```

Keep `target/readme-evidence/prometheus/` and
`target/asapquery-demo-evidence/` for inspection; delete them yourself only after
you no longer need the logs, outputs and runtime state.

## VictoriaMetrics / MetricsQL runbook

This repository has a real backend-process MetricsQL fixture, but not a bundled
self-contained native VictoriaMetrics deployment demo. The fixture starts the
backend, installs an actual MetricsQL plan, sends Remote Write samples, checks
labeled results, and shuts down its child process. Its exact upstream is a
health-only test service, **not native VictoriaMetrics**.

```bash
mkdir -p target/readme-evidence/victoriametrics
cargo +1.98.0 test --locked -p data_plane --test asapquery_compatibility_process_e2e \
  distinct_planning_process::distinct_range_uses_planner_selected_hll_and_source_labels \
  -- --exact --nocapture \
  > target/readme-evidence/victoriametrics/process.log 2>&1
```

`DISTINCT_PLANNED` and `DISTINCT_INSTALLED` in the log expose selection and the
shared installed publication. The fixture checks the actual MetricsQL listener;
it is not a native differential or a performance benchmark.

For an **existing native VM service** at port 8428, the following independently
starts an exact-fallback adapter. Load the intended dataset into that service
using its normal ingestion setup first. The empty bootstrap deliberately has
no accelerated bindings; this smoke request must not be called warm.

```bash
printf 'aggregations: []\n' > target/readme-evidence/victoriametrics/bootstrap.yaml
target/debug/data_plane \
  --streaming-config target/readme-evidence/victoriametrics/bootstrap.yaml \
  --http-port 19080 --victoriametrics-http-port 19081 \
  --victoriametrics-url http://127.0.0.1:8428 \
  --output-dir target/readme-evidence/victoriametrics/runtime \
  > target/readme-evidence/victoriametrics/backend.log 2>&1 &
echo $! > target/readme-evidence/victoriametrics/backend.pid
for attempt in $(seq 1 60); do
  curl -fsS http://127.0.0.1:19080/api/v1/health && break
  sleep 1
done
curl -fsS http://127.0.0.1:19080/api/v1/health
curl -sS -D target/readme-evidence/victoriametrics/query.headers --get \
  http://127.0.0.1:19081/api/v1/query \
  --data-urlencode 'query=default_rollup(asap_demo_gauge[5s])' \
  > target/readme-evidence/victoriametrics/query.json
jq . target/readme-evidence/victoriametrics/query.json
curl -fsS http://127.0.0.1:19080/api/v1/physical-plan/status | jq .
kill "$(cat target/readme-evidence/victoriametrics/backend.pid)"
```

To inspect supported MetricsQL planning independently:

```bash
target/debug/examples/compile_workload_artifact \
  "$ASAPQUERY_PLANNING_SNAPSHOT" --metricsql \
  > target/readme-evidence/victoriametrics/selected.json
jq '.query_plan.entries' \
  target/readme-evidence/victoriametrics/selected.json
```

This does not install or ingest the artifact by itself. Use the fixture's normal
stage/activate path for an integrated example. Keep the logs, headers and JSON
under `target/readme-evidence/victoriametrics/`; see
[MetricsQL support](docs/developer_docs/query-engine/victoriametrics-metricsql-support.md)
for tenant paths and unsupported constructs.

## ClickHouse / SQL runbook

Use a **dedicated test ClickHouse server** at `http://127.0.0.1:8123`, with
credentials supplied through `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` if needed.
The real process fixture drops and recreates `default.telemetry` and
`default.divisors` at the start of each scenario, and leaves the final tables
on the dedicated server; do not point it at a production database. It loads samples,
compiles an actual mixed query, publishes the shared plans, queries the backend,
and cleans up its child backend. The native server remains yours to manage.

```bash
mkdir -p target/readme-evidence/clickhouse
export CLICKHOUSE_URL=http://127.0.0.1:8123
curl -fsS --user "${CLICKHOUSE_USER:-default}:${CLICKHOUSE_PASSWORD:-}" \
  "$CLICKHOUSE_URL/ping"
CLICKHOUSE_PLANNING_ARTIFACT="$PWD/target/readme-evidence/clickhouse/planning.json" \
  cargo +1.98.0 test --locked -p data_plane --test clickhouse_differential_e2e \
  compiled_publication_executes_mixed_dag_in_data_plane_process \
  -- --exact --nocapture \
  > target/readme-evidence/clickhouse/process.log 2>&1
jq '{selection_trace, catalog: .publication.summary_catalog}' \
  target/readme-evidence/clickhouse/planning.json
```

The fixture runs SUM, COUNT and MAX scenarios. `planning.json` is overwritten
per scenario and retains the last publication; keep `process.log` for the complete
run. Without `CLICKHOUSE_URL` the real-server test skips; that is not a successful
native test. See [SQL support](docs/developer_docs/query-engine/clickhouse-sql-support.md)
for the typed backfill and format boundaries.

### Compile a concrete SQL input without preset materializations

The example reads `ClickHouseSqlAutomaticWorkload` JSON from stdin, not a
Prometheus snapshot. Reuse the current compiler envelope from the shared setup
and provide a typed table schema plus a fixed evaluation interval:

```bash
python3 - <<'PY'
import json
from pathlib import Path
root = Path('target/readme-evidence')
envelope = json.loads((root / 'selected.json').read_text())['precompute_plan']['envelope']
envelope['plan_id'] = 9001
envelope['plan_version'] = 1
workload = {
    'envelope': envelope,
    'tables': {'telemetry': {
        'columns': [
            {'name': 'timestamp_ms', 'dtype': 'timestamp', 'nullable': False},
            {'name': 'value', 'dtype': 'float64', 'nullable': False},
            {'name': 'metric', 'dtype': 'utf8', 'nullable': False}],
        'time_index': 0, 'unique_keys': [], 'closed': True}},
    'accuracy': 'Exact',
    'queries': [{'sql': 'SELECT sum(value) FROM telemetry WHERE timestamp_ms >= 0 AND timestamp_ms < 2000',
                 'start_ms': 0, 'end_ms': 2000, 'cumulative': True}]}
(root / 'clickhouse/workload.json').write_text(json.dumps(workload, indent=2))
PY
target/debug/examples/compile_clickhouse_workload \
  < target/readme-evidence/clickhouse/workload.json \
  > target/readme-evidence/clickhouse/compiled.json
jq '{selection_trace, catalog: .install.summary_catalog, query: .install.query_plan}' \
  target/readme-evidence/clickhouse/compiled.json
```

The fixture above supplies real table data and exercises publication/execution;
this offline command alone does neither. For a standalone **exact-fallback**
listener against that native service:

```bash
printf 'aggregations: []\n' > target/readme-evidence/clickhouse/bootstrap.yaml
target/debug/data_plane \
  --streaming-config target/readme-evidence/clickhouse/bootstrap.yaml \
  --http-port 19082 --clickhouse-http-port 19083 \
  --clickhouse-url "$CLICKHOUSE_URL" --clickhouse-database default \
  --output-dir target/readme-evidence/clickhouse/runtime \
  > target/readme-evidence/clickhouse/backend.log 2>&1 &
echo $! > target/readme-evidence/clickhouse/backend.pid
for attempt in $(seq 1 60); do
  curl -fsS http://127.0.0.1:19082/api/v1/health && break
  sleep 1
done
curl -fsS http://127.0.0.1:19082/api/v1/health
curl -sS -D target/readme-evidence/clickhouse/query.headers --get \
  --user "${CLICKHOUSE_USER:-default}:${CLICKHOUSE_PASSWORD:-}" \
  http://127.0.0.1:19083/ --data-urlencode 'query=SELECT 1 FORMAT JSON' \
  > target/readme-evidence/clickhouse/query.json
jq . target/readme-evidence/clickhouse/query.json
curl -fsS http://127.0.0.1:19082/api/v1/physical-plan/status | jq .
kill "$(cat target/readme-evidence/clickhouse/backend.pid)"
```

For authenticated requests, set `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`
for the curl commands above, or send `X-ClickHouse-User` and `X-ClickHouse-Key`
headers. The proxy forwards incoming authentication headers. Backend
`--clickhouse-user` and `--clickhouse-password` configure backfill access; they
do not automatically authenticate proxy fallback requests. The process fixture
uses its matching environment variables. Retain planning/selection artifacts, response headers,
JSON and process logs. A successful SQL response can be exact fallback or an
external-only DAG; only actual summary reads establish ASAP/hybrid execution.
The fixture's mixed-DAG assertions are stronger than a successful `SELECT 1`.

For recorded datasets, see the [replay guide](docs/user_guide/o11y-replay.md) and
[execution calibration](tools/o11y-execution/CALIBRATION.md). For synthetic fake
metrics, Google and Alibaba query expressions and cross-engine measurements, see
the [dataset-specific accuracy evaluation](tools/shared-workload/ACCURACY_E2E.md).
Report correctness,
fallbacks, build/update cost and whole-deployment resources separately from
query latency. Manual examples leave evidence directories intact; remove them
only when no longer needed. PID-file cleanup commands apply only to the
still-running backend started by that manual run. The ClickHouse fixture leaves
its final tables on your dedicated native server; remove those test tables or
discard that test server separately after collecting evidence.

## Repository map

| Path | Contents |
| --- | --- |
| [control_plane/](control_plane/) | Planner adapters, physical compilation, placement/cost evaluation, publication and runtime configuration. |
| [crates/asap_types/](crates/asap_types/) | Shared catalog, SDS identities, plan, grouping, window and producer contracts. |
| [crates/asap_otel_proto/](crates/asap_otel_proto/) | Modified OTLP protobuf bindings for supported external state ingestion. |
| [data_plane/src/precompute_engine/](data_plane/src/precompute_engine/) | Ingest-time operators, coordination and immutable maintenance execution. |
| [data_plane/src/storage_engines/sketch_db/](data_plane/src/storage_engines/sketch_db/) | SummaryStore implementation, physical indexes, lifecycle, backfill and persistence. |
| [data_plane/src/query_engines/](data_plane/src/query_engines/) | Summary readout, shared DAG execution, protocol adapters and exact routing. |
| [data_plane/src/drivers/](data_plane/src/drivers/) | HTTP, ingestion and control-plane interfaces. |
| [data_plane/tests/](data_plane/tests/) | Runtime and process integration tests. |
| [demos/](demos/), [scripts/](scripts/), [tools/](tools/) | Runnable demonstrations, validation and workload/evaluation tooling. |
| [docs/](docs/README.md) | Architecture, operator guides, support boundaries and evaluation evidence. |

## License

MIT — see [LICENSE](LICENSE).
