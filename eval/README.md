# Evaluation: live demo and comparison tooling

Scripts that run the ASAP data plane next to other systems (see
[Baselines](#baselines)) on the same Remote Write feed and the same queries,
check them against a reference, and show them on Grafana. Code and
configuration only: run outputs are git-ignored (`.gitignore`). Paths below
are relative to this directory, `$ASAPQ_BENCH`.

| Path | What it holds |
|---|---|
| `demo/run_live_demo.sh` | The live Grafana demo: feeders, the ASAP data plane, VictoriaMetrics, a reference VictoriaMetrics (accuracy), optionally Prometheus (`EXACT=1`), logging proxies, exporters, a watchdog and the query load. |
| `scripts/run_vm_demo.sh` | The batch comparison: prepare, run the arms one after another on one feed, report. |
| `scripts/fetch_binaries.sh`, `scripts/build.sh` | Pinned third-party binaries (sha256-checked); builds of the paced feeder and this repository into `$BIN_DIR`. |
| `scripts/profile_query_phase.sh` | CPU profiles of each arm's query phase (`scripts/run_vm_demo.sh` with `PROFILE=1`). |
| `baselines/` | ClickHouse and Elasticsearch, the compared systems that run as their own service (`baselines/<system>/docker-compose.yml`); see [Baselines](#baselines). |
| `harness/` | `prepare.py` (scaled manifest, feeder table, the Planner's plan through `control_plane`), `run.py` (arms), `report.py`, `demo_table.py`, `tables.py`, `tables2.py`, `accuracy_vs_vm.py`, `compression.py`, `merge_plans.py`, `cgroup_limits.sh`. See `harness/README.md`. |
| `demo/live/` | The live demo's helpers: `proxy.py`, `query_load.py`, `accuracy.py`, `compare.py`, `verify.py`, `browse.py`, exporters, and `planner/cost_inputs.json`. |
| `../deployment/monitoring/` | Prometheus and process-exporter configuration, Grafana provisioning, the dashboard generators (`grafana/make_*_dashboard.py`), screenshots, Docker Compose or plain binaries (`run_binaries.sh`). |
| `workloads/` | Workload manifests. |
| `campaign/` | Records of the multi-host campaign: workload generators (`gen_*.py`), job queue, scale sweep, reruns, collection and summaries, and `paced-feeder-realistic.diff` (realistic feeder values; `scripts/build.sh` applies it). |

`demo/live/demo_sequence*.sh`, `demo/live/chain_*.sh`, `demo/live/after_sequence_limits.sh`
and most of `campaign/` are records of the sequences that were run: they name
specific builds (`$BENCH_ROOT/bin-live-<commit>`) and prepared directories,
and are kept to show how the measurements were made, not as entry points.

## Setup

Everything runs from one working directory, `$ASAPQ_BENCH`. Use this
directory and put the checkouts the scripts build next to it:

```sh
export ASAPQ_BENCH=$(git rev-parse --show-toplevel)/eval
export BIN_DIR=/path/to/bin            # pinned binaries and the built feeder/data_plane/control_plane
ln -s .. "$ASAPQ_BENCH/backend"        # this repository
git clone https://github.com/ProjectASAP/ASAPQuery "$ASAPQ_BENCH/ASAPQuery"   # paced_feeder
cd "$ASAPQ_BENCH"
scripts/fetch_binaries.sh              # VictoriaMetrics, Prometheus, exporters, Grafana, FlameGraph
scripts/build.sh                       # paced_feeder, data_plane, control_plane into $BIN_DIR
```

| Variable | Used by | Meaning |
|---|---|---|
| `ASAPQ_BENCH` | all | the working directory above (required) |
| `BIN_DIR` | all | binaries (required) |
| `BENCH_ENV` | shell scripts | optional file sourced first (toolchain paths) |
| `ASAP_PLANNER_DIR` | `scripts/build.sh`, `harness/prepare.py` | an ASAPPlanner checkout (optional for `scripts/build.sh`; `prepare.py` checks Planner features by commit) |
| `BENCH_ROOT` | `demo/live/` records, `campaign/` | directory holding other builds, checkouts and scratch files |
| `CAMPAIGN_DIR` | `campaign/` | the job queue's directory on each host |
| `BENCH_HOSTS`, `BUILD_HOST`, `REF_HOST`, `ENGINE_SSH`, `ENGINE_ADDR`, `NET_IFACE` | `campaign/` | hosts as `ssh` resolves them (driver hosts, build host, reference replay host, engine host) and the engine link's interface |

The headers of `demo/run_live_demo.sh` and `scripts/run_vm_demo.sh` list every other
variable (cpusets, limits, series, workloads, ports) with its default.
`harness/prepare.py` imports ASAPQuery's workload tools, so even `--help`
needs the ASAPQuery checkout.

## Live demo

```sh
SERIES=10000 demo/run_live_demo.sh start   # plan once, start feeders/engines/proxies, wire up monitoring
demo/run_live_demo.sh status               # processes, ingestion, Grafana requests answered/refused
demo/run_live_demo.sh load on 8            # 8 simulated users querying every engine
demo/run_live_demo.sh stop
```

`WORKLOAD` picks the manifest under `workloads/` (`dashboard-live` by
default, `asap-demo` for the presentation dashboard). Accuracy against the
reference VictoriaMetrics is exported every minute by `demo/live/accuracy.py`.
`EXACT=1` also runs Prometheus; `demo/run_live_demo.sh compare` needs it.
`LIMITS=1` runs both engines under the same enforced CPU, memory and disk
limits (`harness/cgroup_limits.sh`). Grafana listens on port 13000; the
dashboards are generated by `../deployment/monitoring/grafana/make_live_dashboard.py` and
`make_demo_dashboard.py`. The presentation dashboard's Resources and Scale
rows read `REPORT_DATA` (a final experiment's `report_data.json`, not kept
here); without it they stay empty.

## Comparison

```sh
SERIES=1000 QUERY_PHASE_S=600 WORKLOADS="demo/dashboard-heavy" scripts/run_vm_demo.sh
```

For each workload this prepares the plan (timed as a one-time offline
cost), runs the arms (`asap`, `victoriametrics`, optionally `prometheus`)
on the same feed and query schedule, each on its own cpuset, and writes
`report.py` and `demo_table.py` tables with each arm's absolute CPU,
memory, disk, latency and accuracy; Grafana's `asap-vs-vm` dashboard
(`../deployment/monitoring/grafana/make_dashboard.py`) shows the runs.
`harness/README.md` describes the arms, flags and outputs.

## Baselines

The systems the backend is compared with. "Harness arm" means
`harness/run.py --arms` starts, feeds and queries it; the other systems run
as their own service and are, for now, only what the backend's routes forward
to. No results are kept here.

| System | Version pinned in | How it runs | Harness arm |
|---|---|---|---|
| Prometheus | `scripts/fetch_binaries.sh`, `harness/common.py` | binary from `scripts/fetch_binaries.sh`, flags in `harness/run.py` | `prometheus` (the exact reference), and beside `backend`, `backend_alone` and `asapquery` |
| VictoriaMetrics single-node | `scripts/fetch_binaries.sh`, `harness/common.py` | binary from `scripts/fetch_binaries.sh`, flags in `harness/run.py` (`VM_SEARCH_FLAGS`) | `victoriametrics`; the raw store of `asap_forward`; the reference of `demo/run_live_demo.sh` |
| VictoriaMetrics cluster | `harness/common.py` (the single-node release) | `vmstorage`, `vmselect`, `vminsert` installed by hand as `$BIN_DIR/<component>-<version>` (not fetched) | `vmcluster` |
| ASAPQuery query engine | the ASAPQuery checkout | `ASAPQUERY_ENGINE=1 scripts/build.sh` | `asapquery` |
| ClickHouse | `baselines/clickhouse/docker-compose.yml` | `docker compose -f baselines/clickhouse/docker-compose.yml up -d`, then `data_plane --clickhouse-url http://localhost:8123` | none yet |
| Elasticsearch | `baselines/elasticsearch/docker-compose.yml` | `docker compose -f baselines/elasticsearch/docker-compose.yml up -d`, then `data_plane --elastic-url http://localhost:9200` | none yet |

Not ported yet:

- ClickHouse and Elasticsearch arms in `harness/run.py`: loading the same
  data into their tables or indexes, and SQL or Query DSL workloads. The
  backend forwards every SQL and Query DSL query today (no planned SQL
  answers), so such an arm would measure the forward path only.
- ASAPQuery's ClickHouse experiment runner and data loaders
  (`asap-tools/experiments/experiment_run_clickhouse.py`, ClickBench and H2O
  loads) and its Elasticsearch benchmarking scripts
  (`asap-tools/execution-utilities/elastic-asap-benchmarking/`): they are
  tied to ASAPQuery's experiment framework and query engine.
- PilotDB-style sampling baselines: ASAPQuery has no infrastructure for them
  to port.
