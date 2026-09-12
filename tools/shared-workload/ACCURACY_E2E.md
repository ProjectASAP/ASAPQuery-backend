# Dataset-specific accuracy and cost evaluation

For developers running controlled experiments. This component binds the shared
PromQL/SQL templates to three datasets, loads identical samples, and compares
Prometheus, ClickHouse, native VictoriaMetrics, ASAP PromQL and ASAP SQL responses
at repeated evaluation timestamps. It records accuracy, HTTP latency and optional
Linux component accounting. It does **not** claim that the backend already
accelerates every expression in the corpus.

## Query matrix

`accuracy_suite.py manifest` emits **concrete PromQL and ClickHouse SQL** for each
profile, with/without an equality label filter. Only SQL evaluation time
`{eval_ms}` and selector lookback `{lookback_ms}` remain runtime parameters.
Quantiles are 0.5, 0.75, 0.9, 0.95, 0.99; windows are 1m, 10m, 1h, 6h, 24h.
PromQL syntax is `topk by (label_0) (3, metric)` and
`quantile by (label_0) (0.9, metric)`: k/q are arguments, not grouping labels.

| Family | PromQL shape | Evaluation interval |
|---|---|---|
| 1 | `sum by (G) (M)` | 1s |
| 2 | `topk by (G) (3, M)` | 1s |
| 3 | `quantile by (G) (q, M)` | 1s |
| 4 | `sum_over_time(M[T])` | 1m |
| 5 | `quantile_over_time(q, M[T])` | 1m |
| 6 | `rate(C[T])` | 1m |
| 7 | `sum by (G) (rate(C[T]))` | 1m |
| 8 | `sum by (G) (sum_over_time(M[T]))` | 1m |
| 9 | `topk by (G) (3, rate(C[T]))` | 1m |
| 10 | `quantile_over_time(0.9,M[T]) / quantile_over_time(0.5,M[T])` | 1m |

Also includes spatial count, count_over_time, increase, nested sum/topk and
temporal-to-spatial compositions. This is a finite sensitivity corpus, **not**
exhaustive coverage of arbitrary-depth SpatialAgg*, all binary operators, all
label matcher operators or vector matching modifiers. `ready` means a template
can be evaluated; it is not a declaration of planner/summary support.

SQL uses the [shared normalized schema and translations](README.md): latest sample
per complete series identity for spatial queries, `(t-T,t]` temporal bounds,
linearly interpolated quantiles, and reset-corrected, boundary-extrapolated
rate/increase. Rate is not translated as a naive slope. Pin engine versions;
the existing [translation evidence](fixture-results.md) targets Prometheus 3.14.0
and ClickHouse 26.8.2.7, not all versions. TopK compares labels strictly in this
runner: equally valid tied winners can fail comparison and require review.
Do not interpret these failures as numeric error without inspecting the cutoff.

## Data selection and concrete expressions

| Dataset | M (gauge) | G (spatial group) | Series identity |
|---|---|---|---|
| Synthetic | `fake_metric` | `label_0` | label_0, label_1, job, instance |
| Google cluster | `google_cluster_cpu_rate` | `service` | original mapper attributes, including service, task, host |
| Alibaba 2018 | `alibaba_container_cpu_util` | `machine_id` | machine_id, container_id |

### Synthetic: Prometheus client fake metrics

This uses Python `prometheus_client` to encode deterministic fake samples with
the metric/label vocabulary from the repository's
[streaming example](../../data_plane/examples/promql/streaming_config.yaml).
It is an offline historical sample generator, **not** a running scraper; 100ms
is the exact sample timestamp interval, not a measured scrape scheduling SLA.
The OpenMetrics file is a historical import stream, not a single `/metrics`
response. Remote Write and SQL import use the matched JSONL rows.

For group index g, member m and step s, the gauge is
`1 + g % 19 + m + (s % 31)/31`. The separate cumulative
`fake_metric_counter_total` is `(s % 997)*(m+1)` and deliberately resets.
Families 6, 7, 9 and increase use that counter, not the fake gauge.
Examples: `quantile by(label_0)(0.95, fake_metric)` and
`sum by(label_0)(rate(fake_metric_counter_total[10m]))`.

Vary **group cardinality** over 10, 100, 1,000, 10,000, 100,000, 1,000,000.
Default four member series per group make grouped TopK/quantile nondegenerate.
There are two metrics, hence `2 * groups * members` total series and
`2 * groups * members * (duration_ms/100 + 1)` samples. A 24h, million-group cell
with four members contains **6,912,008,000,000 samples**. Generation is opt-in,
streaming and budget-limited; per-series ordering validation still uses memory
proportional to series cardinality. Do not run the entire matrix on a workstation.
For repeated full-window evaluations, generate history covering T **plus** the
evaluation span, not just T.

### Google cluster: CPU usage per task, aggregated by job/collection

Use the existing `ASAPCollector/datasets_eval/google_cluster/otlp_mapper.py`
JSONL export, **not** raw event CSV or scheduling requests. Select records with
`metric == "google_cluster_cpu_rate"`; retain `timestamp_ms`, `value` and all
`attributes`. Require `service`, `task`, `host`; do not drop task identity before
computing spatial quantiles or TopK.

For 2011 `task_usage`, service is `job-{job_id}`, host is `host-{machine_id}` and
task is the task index. For 2019 `instance_usage`, service is
`coll-{collection_id}` and task is the instance index. This groups usage by the
workload/job, rather than mixing unrelated jobs on a machine. Mapper-created
zone/rack labels are synthetic placement metadata, not measured topology.
Do not enable cardinality-cap hashing: merged identities invalidate accuracy.
See the [official Google trace formats](https://github.com/google/cluster-data).

Choose a contiguous trace interval and a deterministic set of **complete jobs**
(e.g. sorted job IDs), retaining every task's samples in that interval. Preserve
native timestamps and sampling density; do not upsample to synthetic 100ms or
fill gaps with zeros. Record trace release, source objects/shards, time interval,
selected IDs, mapper revision/options and exclusions alongside the generated
manifest. The generator hashes its input, but cannot infer this provenance from
an already-exported JSONL file. Do not pool 2011 and 2019 runs.

Examples: `sum by(service)(google_cluster_cpu_rate)` and
`quantile_over_time(0.9,google_cluster_cpu_rate[1h])`. For a filtered manifest use
`--filter-value job-ACTUAL_ID` (2011) or `coll-ACTUAL_ID` (2019); the default
`job-1234567890` is illustrative and must be replaced with a present ID.

CPU **rate** in the field name means a utilization gauge, not a cumulative
Prometheus counter. Rate/increase families are emitted as `not_applicable` and
skipped. A separately derived integrated counter would be a different experiment
and is not silently manufactured here.

### Alibaba 2018: container CPU utilization, aggregated by machine

Use headerless `container_usage.csv`, not `container_meta`, `batch_task`, resource
requests or event counts. The [official 2018 schema](https://github.com/alibaba/clusterdata/blob/master/cluster-trace-v2018/schema.txt)
defines 11 columns. Select column 1 `container_id`, column 2 `machine_id`,
column 3 `time_stamp` (seconds from trace start), column 4 `cpu_util_percent`.
Convert timestamp seconds to integer milliseconds without rounding; preserve
CPU values in source units, with no division by machine core count or invented
counter conversion. The [trace description](https://github.com/alibaba/clusterdata/blob/master/cluster-trace-v2018/trace_2018.md)
explains normalization and collection limitations.

Choose a contiguous interval and sorted machine IDs, retaining **all containers**
on selected machines. This makes spatial queries compare per-machine container
usage; grouping by container_id alone would often make spatial quantiles trivial.
Record source shard hashes, interval, selected IDs and any cleaning policy.
Negative CPU, the 101 sentinel and nonfinite values fail ingestion; clean them
explicitly and report exclusion counts instead of silently changing the sample
population. Duplicate/out-of-order samples within a series also fail. The tool
does not silently average collisions or sort an arbitrarily large input.

Examples: `topk by(machine_id)(3, alibaba_container_cpu_util)` and
`sum by(machine_id)(sum_over_time(alibaba_container_cpu_util[6h]))`.
Use `--filter-value m_ACTUAL_ID` for a machine in the selected data. Native
timestamps are retained, including gaps. Configure historical retention at each
receiver; do not independently shift time in different engines. CPU utilization
is a gauge, so counter families are N/A here too. The trace's roughly 4,000
machines do **not** establish natural million-group coverage; synthetic scale
results must be reported separately from real-trace results.

## Generate, load and compare

```sh
python3 -m pip install -r tools/shared-workload/requirements.txt
python3 tools/shared-workload/dataset.py --dataset synthetic \
  --groups 10 --members 4 --duration-ms 120000 --output /tmp/accuracy-data
python3 tools/shared-workload/accuracy_suite.py manifest \
  --dataset synthetic --output /tmp/accuracy-queries.json
# For traces instead:
# dataset.py --dataset google --input selected-mapper.jsonl --output NEW_DIR
# dataset.py --dataset alibaba --input selected-container_usage.csv --output NEW_DIR
# accuracy_suite.py manifest --dataset google --filter-value job-123 --output NEW_FILE
```

Provision **fresh isolated** baseline and fallback stores, with identical versions,
CPU/memory limits, retention, evaluation lookback and data. Create `raw_samples`
using the schema in [README.md](README.md) in an unused ClickHouse database.
Prometheus must enable its Remote Write receiver. Choose historical-retention
settings compatible with the dataset's timestamps. The loader performs writes;
never point it at production or reuse a partially loaded database.

Configure ASAP before loading: register the manifest's query strings and
`interval_ms` frequencies with the normal control-plane planning workflow, gather
independent calibration costs, compile/install its chosen plan and configure exact
fallbacks. Reuse the [production compiler/replay workflow](../o11y-execution/README.md)
and its [calibration requirements](../o11y-execution/CALIBRATION.md); this component
does not synthesize valid planning snapshots or force a benchmark-selected sketch.
Keep the compiler's candidates, selection, install request/status and versions as
experiment artifacts. An SQL translation may be executable by ClickHouse but not
recognized for summary acceleration by ASAP; such outcomes must remain visible.

```sh
python3 tools/shared-workload/load_dataset.py --data /tmp/accuracy-data \
  --remote-write http://127.0.0.1:29090/api/v1/write \
  --remote-write http://127.0.0.1:28428/api/v1/write \
  --remote-write http://127.0.0.1:19090/api/v1/write \
  --remote-write http://127.0.0.1:18089/api/v1/write \
  --clickhouse 'http://127.0.0.1:18123/?database=accuracy_fixture' \
  --output /tmp/accuracy-loaded.json
```

The endpoints above illustrate Prometheus baseline, VM, a separate Prometheus
fallback, and ASAP ingestion; replace them with actual configured receivers,
including additional SQL-backend ingestion/exact stores if deployed separately.
The receipt means each batch was accepted, **not** that summaries finished.
Invoke the backend's finite-input completion barrier before querying; the existing
replay workflow documents it. Verify all endpoints serve the intended loaded
dataset; receipt/profile/hash checks cannot authenticate arbitrary query URLs.
On partial loading failure, investigate and restart with fresh stores. No retries
or rollback hide partial ingestion.

```sh
python3 tools/shared-workload/accuracy_suite.py run \
  --manifest /tmp/accuracy-queries.json --loaded-data /tmp/accuracy-loaded.json \
  --query-name spatial_sum --query-name temporal_sum \
  --start-ms 1700000060000 --end-ms 1700000120000 \
  --prometheus http://127.0.0.1:29090 --victoriametrics http://127.0.0.1:28428 \
  --clickhouse 'http://127.0.0.1:18123/?database=accuracy_fixture' \
  --asap-prometheus http://127.0.0.1:18089 \
  --asap-clickhouse http://127.0.0.1:18090 \
  --require-warm --output /tmp/accuracy-results.jsonl
python3 tools/shared-workload/summarize_accuracy.py /tmp/accuracy-results.jsonl \
  --output /tmp/accuracy-summary.json
```

Repeat `--query-name` to bound a run. With no selection, all ready queries run;
short histories fail full-window checks for larger T. Both filtered and
unfiltered cases are evaluated. Absent filter IDs produce empty-oracle failures.
Results record missing/extra labels and samples, completeness, max absolute and
relative error, zero-baseline/nonfinite differences, tolerances, raw responses,
route evidence and per-engine timings. Defaults are rtol=1e-9, atol=1e-12; choose
and report an approximation budget explicitly, never tune it on evaluation data.
This tolerance is not a statistical sketch guarantee or quantile rank-error bound.

ClickHouse-vs-Prometheus oracle parity must pass before claiming SQL correctness.
VM's **native** MetricsQL result is compared separately: semantic differences are
reported, not relabeled as sketch error or silently normalized. The manifest also
preserves the separately named VM Prometheus variant for follow-up experiments.
VM mismatch is not part of the ASAP pass gate; failed HTTP requests are failures.
Empty oracles, warnings and malformed/duplicate results cannot establish accuracy.
`--require-warm` rejects hybrid, fallback and unknown provenance even when values
match. Without it, a pass means accuracy only, not acceleration.

This runner is **sequential historical replay**: 1s/1m control evaluation timestamp
spacing, not wall-clock dashboard concurrency. Latency is client-observed HTTP
service time (including failures and connection overhead). Engine order is fixed;
use repeated fresh trials and control cache/order effects before performance
claims. It is not a scheduled live-load or throughput benchmark.

## Component and whole-system resources

Use cgroup v2 with disjoint component cgroups. Supply `--components components.json`
to `accuracy_suite.py run` for request-interval CPU, memory snapshots, block I/O
and optional dedicated-network-namespace counters. Example configuration:

```json
{
  "asap_promql": {
    "data_plane": {"cgroup": "/sys/fs/cgroup/eval/asap", "data_directory": "/srv/eval/asap"},
    "collector": {"cgroup": "/sys/fs/cgroup/eval/collector"},
    "control_plane": {"cgroup": "/sys/fs/cgroup/eval/control"},
    "fallback": {"cgroup": "/sys/fs/cgroup/eval/fallback", "data_directory": "/srv/eval/fallback"}
  },
  "asap_sql": {},
  "prometheus": {"server": {"cgroup": "/sys/fs/cgroup/eval/prom", "data_directory": "/srv/eval/prom"}},
  "clickhouse": {"server": {"cgroup": "/sys/fs/cgroup/eval/ch", "data_directory": "/srv/eval/ch"}},
  "victoriametrics": {"server": {"cgroup": "/sys/fs/cgroup/eval/vm", "data_directory": "/srv/eval/vm"}}
}
```

Populate `asap_sql` with its real components; an empty set intentionally reports
unavailable totals. Include collector, control/data plane, exact fallback, broker
and any other component actually used. Shared dependencies count once **within**
each system. Reject parent/child cgroup overlap; never sum all five alternative
systems together and call it ASAP cost. `network_namespace_pid` optionally
identifies a distinct non-host namespace per component. Host `/proc/net/dev` is
not per-process accounting. Network RX/TX includes loopback and can count the same
transfer at both endpoints, so it is not summed into a misleading system byte total.

For a complete phase including ingestion, build, queries and background work:

```sh
python3 tools/shared-workload/resources.py --components components.json \
  --engine asap_promql --output /tmp/asap-phase.json -- /path/to/experiment-command
```

The wrapper runs the explicit command and propagates its exit status. Components
must already exist; run the same phase independently for each baseline. It reports
CPU time, block bytes, per-component and aggregate memory endpoints, sampled memory
peaks (100ms by default), and allocated storage blocks before/after for configured
disjoint data directories. Missing evidence stays null, not zero. Memory is cgroup
memory (including charged cache), not summary heap or process RSS. Sampled peaks
can miss short spikes; directory accounting can be expensive and runs outside the
measured command. The user-supplied component inventory defines the total; the tool
cannot prove no service was omitted. No automatic system-benefit ratio is emitted.

## Verification and remaining acceptance

```sh
python3 -m unittest discover -s tools/shared-workload -p 'test_*.py' -v
```

Tests cover profile binding, client-encoded samples, trace mappings, duplicate
rejection, all five HTTP adapters, repeated timestamps, fallback/error rejection,
matched loading/hash guards and whole-phase resource/exit-status boundaries.
A local Prometheus 3.5.0 `promtool tsdb create-blocks-from openmetrics` smoke test
imported 32 samples / 16 series spanning 100ms successfully; this verifies import
format only, not the newer query-boundary semantics targeted by the SQL.
HTTP fixtures validate the harness, **not** live engine
or planner support. Large trace runs, complete planned backend/SQL integration,
all-family warm coverage, production resource measurements, live dashboard load,
quantile rank error and tie-aware TopK accuracy remain separate acceptance work.
