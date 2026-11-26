# Three-arm runner for ASAPQuery's random workloads

The comparison harness. `$ASAPQ_BENCH` (this repository's `eval/`) is the working
directory (see `../README.md`). Paths that start with `../` are relative to this
directory (`eval/harness/`); the others are relative to `$ASAPQ_BENCH`.

Arms, on the same data and query schedule:

- `prometheus`: Prometheus 3.5.0 alone.
- `asapquery`: ASAPQuery's `query_engine_rust` (ASAPQuery main) with the
  workload's archived pilot plan (`workload-NN-pilot/{streaming,inference}_config.yaml`),
  forwarding what it cannot answer to its own Prometheus.
- `backend`: ASAPQuery-backend's `data_plane` installing ASAPPlanner's plan
  (per-query `InstallablePlan`s from the backend's own control plane, merged
  by `merge_plans.py`), with its own Prometheus for forwards.

Data: ASAPQuery's `paced_feeder`, with the table ASAPQuery renders from the
manifest (`random_workload_artifacts.paced_feeder_table`), seed 862, one feeder per
sink, real time (1 s of event time per second). `--label0` scales the series count.

```sh
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-target}
../build.sh                                   # feeder, engine, backend, converter
python3 prepare.py --set seed-862 --workload workload-01 --label0 5 \
    --out ../prep/seed-862.workload-01.s1000  # 4*5*10*5 = 1,000 series
python3 run.py --prep ../prep/seed-862.workload-01.s1000 --out ../runs/w01 --rounds 3
python3 report.py --prep ../prep/seed-862.workload-01.s1000 --run ../runs/w01
```

`run.py` runs the arms one after another (`--concurrent` runs them at once
on one feed start). Each arm: start its processes, feed from an aligned start
S for (longest lookback + rounds x longest interval), and send every query
at each evaluation time on its own interval grid from S + longest lookback,
`DELAY_S[arm]` after the evaluation time.

Targets: accuracy_sla 0.95 -> max_error 0.05, failure probability 0.3. The
backend plans with `raw_data_retained = false` and
`query_time_results_retained = false`, cost model 1 at the AWS Fargate
preset. Exact is the Prometheus arm's answer at the same offset from S.

## Rerunning only the backend arm

The feed (table, seed 862) and the schedule (offsets from the aligned feed
start) are fixed, so a later backend arm compares against an earlier run's
Prometheus and ASAPQuery arms:

```sh
python3 prepare.py --set seed-862 --workload workload-01 --label0 5 \
    --out ../prep/seed-862.workload-01.s1000 --backend-only   # re-plan after a re-pin
python3 run.py --prep ../prep/seed-862.workload-01.s1000 --arms backend --out ../runs/w01-backend2
python3 report.py --prep ../prep/seed-862.workload-01.s1000 --run ../runs/w01 \
    --arm-dir backend=../runs/w01-backend2/backend
```

`prepare.py` declares `data` non-negative (`values_non_negative`, ASAPPlanner
#780; its values are Pareto) only when the backend's pinned Planner has #780.

# ASAP data plane vs VictoriaMetrics demo

Two arms under test, one reference, on the same feed and query schedule:

- `asap`: ASAPQuery-backend's `data_plane --no-forward` alone (no Prometheus
  beside it), installing ASAPPlanner's joint plan (`prep/.../backend/plan.json`).
  A query the plan cannot answer is refused, never forwarded.
- `victoriametrics`: VictoriaMetrics single-node `v1.156.0` (the latest release, pinned with its
  sha256 in `../fetch_binaries.sh`), **no stream aggregation** (no
  `-streamAggr.config`), fed by the same paced feeder over Remote Write
  (`/api/v1/write`) and queried with `/api/v1/query` at the same evaluation
  times. Explicit flags (`run.py: VM_FLAGS`): `-retentionPeriod=1d`,
  `-search.latencyOffset=5s` (1 s below the 6 s query delay, so a query is
  answered at its own evaluation time, as on the other arms; the 30 s
  default would shift it 30 s back), `-search.maxQueryDuration=120s`
  (Prometheus' 2m), raised sample caps. Caches are left at their defaults
  (rollup result cache with `-search.cacheTimestampOffset=5m`,
  `-memory.allowedPercent=60`, instant-rollup cache only for windows over
  `-search.minWindowForInstantRollupOptimization=3h`); every effective flag
  is recorded in `<run>/victoriametrics/vm_flags.json`.
- `prometheus`: Prometheus 3.5.0 alone, the exact reference for accuracy
  (now with `--query.max-samples` raised so 1 h windows over many series
  are answered).

Every query is sent 6 s after its evaluation time on every arm (decided
for the comparison; `QUERY_DELAY_S` changes it for all alike; VM's latency offset follows
as delay - 1 s). VictoriaMetrics makes a received sample searchable up to
~1-2 s later; at 3 s a loaded host could miss the newest second, which
showed up as a difference from Prometheus at that evaluation.

Accuracy: the `asap` arm is checked against Prometheus (5% target, failure
probability 0.3). VictoriaMetrics is reported for performance; its answers'
difference from Prometheus is recorded (`max_rel_diff_vs_prometheus`) but
is **not** treated as an error: MetricsQL's `rate`/`increase` do not
extrapolate to the window's edges as PromQL's do, so an exact VM answer
differs from Prometheus' by design.

Planning is offline: `prepare.py` times the backend's `control_plane` on the
joint workload (wall seconds, CPU core-seconds and peak RSS from `wait4`,
`backend/plan_joint_summary.json`) and `demo_table.py` prints it as a
separate one-time line; it is never in an arm's CPU or RSS. Planning enforces
the accuracy guarantees unless `IGNORE_ACCURACY_GUARANTEES=1` (the earlier MVP
runs ignored them). `prepare.py` writes the `PlanningWorkload` itself in
ASAPPlanner main's format (`fixed_interval_at` demand, `column_signs`);
`WORKLOAD_EMITTER=converter` uses the older example binary.

Workloads: seed-862 `workload-01..03`, and `demo/dashboard-heavy`
(`../workloads/dashboard-heavy.json`: `quantile_over_time(0.99, data[1h])`,
`topk(10, sum by (region, service, job) (rate(data_total[1h])))`,
`max_over_time(data[30m])`, `sum by (region, service) (increase(data_total[1h]))`,
every 10-15 s). Its ingest phase is the longest window (1 h) per arm.
`demo/smoke` is the same shapes with 1-minute windows, for a quick check.

Placement (one host): `run.py --arm-cores N` pins each arm's engine
processes to its own cpuset of N cores (consecutive blocks from cpu 0 in
`--arms` order; `--arm-cpus` sets them explicitly); the feeders and the
harness (scheduler + query client) run on the remaining cores, and
`run_vm_demo.sh` moves the monitoring stack there too. Arms run one after
another on the same feed. Each arm's cpusets are recorded in
`resources.json`. At the end of each arm, run.py saves the engine's own
`/metrics` (`engine_metrics_end.txt`: the data plane's query-time result
hits/misses/computes and backlog, VictoriaMetrics' counters), which
`demo_table.py` prints.

```sh
SERIES="1000 10000" ARM_CORES=16 QUERY_PHASE_S=600 WORKLOADS="demo/dashboard-heavy" ../run_vm_demo.sh
```

`run_vm_demo.sh` downloads missing pinned binaries into `$BIN_DIR`, builds `paced_feeder`/`data_plane`/`control_plane`
there when missing (from `$BACKEND_DIR`), starts the monitoring stack
(`../../deployment/monitoring`: docker compose, or `run_binaries.sh` without docker), and
for each workload runs prepare → run (`--metrics-port 19093`) → `report.py`
→ `demo_table.py` (also into the monitoring textfile directory). `PROFILE=1`
runs `../profile/profile_query_phase.sh` beside it. Grafana:
`http://127.0.0.1:13000/d/asap-vs-vm`.

Ports: arms prometheus 19100, victoriametrics 19500, asap 19601; monitoring
Prometheus 19090, node-exporter 19091, process-exporter 19092, harness 19093,
Grafana 13000 (`GRAFANA_PORT`).

## Range queries (a separate measurement group)

`prepare.py --range STEP_S:SPAN_S[:REFRESH_S]` (e.g. `15:3600`: 1 h span,
15 s step, refreshed every 15 s, as a Grafana panel) turns every query into a
`/api/v1/query_range` request over `[t - span, t]` sent every refresh, to all
arms alike; ASAPPlanner plans the workload with `range_evaluation
{step_ms, span_ms}` on each entry. Each request reads span + window, so the
ingest phase grows by the span. Range runs go to their own prep/run
directories (`.range-15-3600` suffix) and are reported apart
(`demo_table.py` names the mode; report.py compares the matrices per series
and step). `run_vm_demo.sh` runs both groups (`MODES="instant range"`,
`RANGE=15:3600`).

## Disk

The data plane persists materialized views in `<run>/asap/data_plane-data`
(the flag is `ASAP_PERSIST_FLAG`, else detected from `data_plane --help`
among `--data-dir`, `--view-dir`, `--views-dir`, `--storage-dir`,
`--persist-dir`, `--state-dir`), the same disk as VictoriaMetrics'
`<run>/victoriametrics/victoria-metrics-data`. Every process' disk bytes
written and read (`/proc/<pid>/io` write_bytes/read_bytes) are recorded per
phase and shown per component; `resources.json` `storage_bytes` has each data
directory's size at the end.

## Live Grafana demo (`../demo/run_live_demo.sh`)

A long-running setup for showing the comparison in Grafana:
one paced feeder per engine (same seed and start, real time, continuous)
sends the same samples to the ASAP data plane (`--no-forward`, the plan of
`workloads/dashboard-live.json` planned with `range_evaluation` step 15 s,
span 1 h; views persisted in `demo/live/data/asap-views`), VictoriaMetrics (no
stream aggregation) and Prometheus 3.5 (exact). Each engine runs on its own
cpuset (`ASAP_CPUS`, `VM_CPUS`, `PROM_CPUS`; feeders and proxies on
`AUX_CPUS`). `dashboard-live` is dashboard-heavy's query shapes aggregated to
at most 20 output series (so a panel stays readable at 10k input series) at
1 sample/s.

Grafana queries each engine through `demo/live/proxy.py`, a pass-through that logs
every request (start/end/step, status, the data plane's refusal reason) to
`demo/live/logs/requests-*.jsonl` and exports `live_proxy_*` metrics. Data sources
`ASAP`, `VictoriaMetrics`, `Prometheus (exact)` are provisioned
(`../deployment/monitoring/grafana/provisioning/datasources/live.yml`); the dashboard
`asap-vs-vm-live` (`../deployment/monitoring/grafana/make_live_dashboard.py`) has one row
per query with the three panels side by side, all `query_range` over the last
hour at a 15 s step (min interval 15 s, max data points 11000), shifted 6 s
back (the query delay). Grafana aligns start and end down to a multiple of the step (UTC
dashboard), so every request lands on the plan's grid and its newest step's
windows are closed past the 2 s allowed lateness.

The data plane refuses a whole range request if any step reads a window it
has no rows for, so the ASAP panels answer once span + longest window
(2 h for `dashboard-live`) of data exist after the feed starts; before that
they show the refusal.

```sh
SERIES=10000 ../demo/run_live_demo.sh start  # plan, start, wire up monitoring
../demo/run_live_demo.sh status               # processes, ingestion, recent requests
../demo/run_live_demo.sh verify --since-minutes 30   # per data source and query: statuses, start/end/step seen
EXACT=1 ../demo/run_live_demo.sh compare      # ASAP and VM vs Prometheus on the plan's grid (needs EXACT=1 at start)
../demo/run_live_demo.sh browse --minutes 10  # headless browser holds the dashboard open; screenshots
ssh -N -L 13000:127.0.0.1:13000 <run host>   # then http://127.0.0.1:13000/d/asap-vs-vm-live
```

## Screenshots

`../deployment/monitoring/screenshot.sh <uid> <from> <to> <out dir> [var=value]` renders a
dashboard (whole page) and each panel alone to PNG with headless Chromium
(pinned Playwright image). `run_vm_demo.sh` does it for `asap-vs-vm` over each
run's time range with `SCREENSHOTS=1`. The report panels read gauges written
after the run, so render with `to` at or after the report time.

## Presentation dashboard (`asap-demo`)

`SERIES=1000 WORKLOAD=asap-demo ../demo/run_live_demo.sh start` runs the live setup
on `workloads/asap-demo.json` (gauge `lat` and counter `reqs` per series;
`http_requests_total` with 20 status codes, one 5xx, uniform increments so
5% of requests are 5xx; gauge `queue_depth`; all 1 sample/s) and generates
`../deployment/monitoring/grafana/make_demo_dashboard.py` -> uid `asap-demo`: ASAP vs
VictoriaMetrics only, resource usage only. A reference VictoriaMetrics with its
own feeder and no query load is the reference for `demo/live/accuracy.py` (max relative error per query
and the answers table, every minute, at a time on the plan's grid) and is
never shown. The Resources and Scale rows read the final experiment from
`REPORT_DATA` (a final experiment's report_data.json, not in this repository; keys in
`demo/live/experiment_to_textfile.py`); without it those rows stay empty. ASAP answers a panel once window + 1 h
span of data exist (2 h for the 1 h windows, 7 h for the 6 h window).

### Ingestion while querying (live)

`../demo/run_live_demo.sh load on|off` starts or stops the query load
(`demo/live/query_load.py`): the planned queries at their intervals, sent alike to
both engines, 6 s after each evaluation time, at most one request in flight
per engine and query. Starting and stopping it posts annotations on the
dashboards. `demo/live/ingest_exporter.py` (port 19724) exports, per engine:
- the feeder's start lag and its overrun ticks (from the feeder logs);
- what the feeder offers;
- answer freshness: VictoriaMetrics' `now - max(timestamp(lat))`, and ASAP's
  watermark lag;
- one series of `max_over_time(lat[30m])` from both engines.

The asap-demo row "Ingestion while querying" shows these. The live setup gives
each engine 12 cores (ASAP 24-35, VictoriaMetrics 36-47). The feeders, the
query load, the proxies and the browser use 48-55.
