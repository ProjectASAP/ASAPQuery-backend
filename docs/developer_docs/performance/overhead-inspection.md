# Inspecting runtime overhead (#758)

Audience: developers measuring execution overhead. The baseline is direct sketch
readout and direct exact computation over the same raw observations. No standalone
sketch server is involved. This experiment does not measure planner quality or
replace the workload benefit gates in #759.

## Runtime model and controls

The backend has one multithreaded Tokio runtime. HTTP query handlers, ingestion,
precompute workers, periodic flushing and asynchronous maintenance share its
workers. `--precompute-num-workers` controls ingest worker **tasks**, not OS
threads. Increasing it does not allocate a separate pool. Synchronous CPU work
inside an async task occupies its current runtime worker until it yields.

`--runtime-workers N` fixes runtime workers. Without the flag,
`TOKIO_WORKER_THREADS` takes precedence over detected available parallelism.
`--runtime-max-blocking-threads N` caps the separate, lazy blocking pool (default
512); it is not a count of always-running threads or a cap on all process threads.
The file logger creates an additional writer thread. Libraries may create their
own threads; `/proc/self/task` inventories capture observed OS threads.

`--log-level` accepts a tracing filter, including `error` and `off`. `RUST_LOG`
overrides the flag. `--disable-console-log` and `--disable-file-log` independently
remove those sinks; disabling the file sink also removes its writer thread.
Ordinary deployments keep info logging by default. Invalid filters fail startup.

The server writes `runtime-config.json` even with logging disabled: resolved
workers, blocking pool ceiling, configured precompute tasks, effective filter,
thread inventory, available parallelism, Linux affinity/status and cgroup-v2
limits. `runtime-ready.json` captures threads after background task setup. These
are snapshots, not a claim that no thread will subsequently be created. Unknown
OS counters are null. Visible cgroup ancestors are recorded because their limits also apply. Limits
outside a container namespace remain unknown; record the deployment's CPU quota
and pinning alongside these artifacts.

## Reproducible three-layer experiment

Build once; do not include compilation in timing:

```sh
cargo build --release --locked -p data_plane --example overhead_inspect
RUST_LOG=off target/release/examples/overhead_inspect \
  --workers 1,2,4,8,16 --concurrency 1,2,4,8,16 \
  --requests 10000 --warmup 1000 --repeats 5 \
  --disable-console-log --disable-file-log --output target/overhead-off
```

Run on an otherwise idle host with fixed CPU affinity/quota. Record CPU model,
OS, Git revision and command. Each matrix cell is a fresh process; repeats retain
individual JSON reports and `matrix.csv` rather than merging unlike configurations. Keep raw reports
and compare run-to-run variation before drawing conclusions. Use request counts
large enough to amortize startup and CPU clock resolution.

The deterministic fixture has one series, one complete one-second pane and a
median query at a fixed timestamp. All layers use the same unsorted positive raw
values and alpha=0.01 DDSketch. Ingestion and sketch construction occur before
measurement. The sketch's result must meet the fixture's 2% median-error gate;
backend and HTTP results must match direct sketch readout. External forwarding
is disabled. The installed plan rejects fallback. This initial workload isolates
warm read overhead; it does not establish results for other sketch families,
series counts, merges across panes, range queries or concurrent ingestion.

| Layer | Timed work |
| --- | --- |
| `sketch` | Direct DDSketch median on the prebuilt sketch |
| `raw_exact` | Scratch copy and exact median selection over unsorted raw values |
| `backend` | Real ASAPQueryEngine, installed plan, SketchStore lookup/readout and result construction |
| `http` | Real HTTP server over loopback, parsing, routing, execution and response decoding |

For sketch and raw exact, `direct_kernel_seconds_per_query` measures a separate
synchronous loop with `black_box`, outside task dispatch. Concurrent samples for
those layers include submission to the runtime; do not call their entire latency
"sketch cost". Backend samples also include submission. HTTP uses reused client
connections. All responses pass correctness checks; failed queries are counted
and fail the experiment instead of becoming successful latency samples.

The driver uses a separate current-thread runtime. CPU reports include whole
process CPU and driver-thread CPU; their difference includes server workers and
auxiliary threads, not just query instructions. The per-completed-query ratio
includes CPU spent on failed requests. RSS/peak RSS include fixture, client,
server and report buffers; peak RSS is process-lifetime, not just the timed phase.
Use an external process profiler for allocation/lock attribution. Do not subtract
layer latencies and label the remainder "trait overhead".

## Arrival rate, saturation and logging

Without `--rate`, clients use bounded closed-loop concurrency. With `--rate 1000`,
arrivals follow a fixed schedule independent of response completion. At the
in-flight cap, arrivals are dropped and counted. Latency includes delay from the
scheduled arrival, while `service_latency_seconds` starts at task execution.
Report requested rate, actual throughput, drops and errors together with
p50/p95/p99. Successful-request percentiles do not describe dropped arrivals.
Any drops make `passed=false`, even though the scan continues to collect other
cells. The driver/timer has finite capacity: include scheduling delay in the
interpretation and increase rate gradually; an unsustained requested rate is not
proof of backend saturation.

Repeat the same matrix with `RUST_LOG=error`, `RUST_LOG=info` and `RUST_LOG=off`,
controlling sinks explicitly. A filter comparison is meaningful only if that
workload actually emits events at those levels. Keep production logging defaults
unchanged. The performance fixture intentionally does not run ingestion or
maintenance background loops; use the production server's runtime artifacts when
profiling those mixed workloads.

## Profiling and acceptance

On a host with Linux perf available, profile one cell at a time, for example:

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --release -p data_plane --example overhead_inspect
perf record -g --call-graph dwarf -- target/release/examples/overhead_inspect \
  --cell --runtime-workers 4 --concurrency 8 --layers backend \
  --requests 100000 --warmup 1000 --log-level off --disable-file-log \
  --disable-console-log --output target/overhead-profile
perf report
```

Collect CPU stacks and, where appropriate, allocation and off-CPU/lock profiles
before changing abstractions. The deliverable is a reproducible scaling curve
and an evidence-backed explanation of hot paths, not a predetermined speedup.
Unit tests check accounting and runtime controls; a small real four-layer smoke
run checks that the harness reaches the production execution paths. Performance
thresholds belong on a controlled host after variance is established, not noisy
shared CI. No claim about an eight-thread crossover is assumed.
