# Real-workload execution replay

Developer acceptance tooling. A completed run does not establish a benefit.
It invokes the **control-plane compiler**, which calls the pinned Planner,
then boots the production data plane with that compiler's atomic install request.
No family override, candidate index, or benchmark-selected winner is accepted.

## Inputs and prerequisites

- Original timestamped o11ybench OpenMetrics data. The strict numeric subset
  preserves names, labels and finite values; seconds become Remote Write
  milliseconds without rounding. Unsupported lines fail before any writes.
- A JSON corpus with `upstream_revision` and `queries`, preserving every upstream
  occurrence: `{id, query, eval_timestamp_ms}` (additional provenance is retained).
  Export the pinned upstream task queries, not the historical 27-query fixture.
  Provide generator revision, command and seed separately; a file hash cannot
  establish that provenance.
- A canonical **version 2** planning snapshot registering exactly the corpus's
  unique queries and containing valid complete-workload provider cost evidence.
  Collect evidence using `workload_cost_manifest`; keep calibration inputs and
  evaluation inputs distinct. Do not reuse the demo snapshot's declared costs.
  The harness does not invent registrations, cost quotes or unsupported bindings.
- A dedicated, empty Prometheus instance with its Remote Write receiver enabled,
  configured for the input's timestamp range. This process is also the fallback
  service; do not point the runner at a production/shared instance.

The lower-level `replay.py` uses externally managed Prometheus processes.
`run_comparison.py` provisions fresh baseline and fallback processes for each
trial. See [CALIBRATION.md](CALIBRATION.md) for measured cost evidence; calibration
enumerates candidates, while the replay compiler chooses the winner normally.

## Run

```sh
cargo build --locked -p control_plane --example compile_workload_artifact
cargo build --locked -p data_plane --bin data_plane
python3 tools/o11y-execution/replay.py \
  --metrics /path/metrics.txt --queries /path/queries.json \
  --snapshot /path/o11y-costed-snapshot.json \
  --compiler target/debug/examples/compile_workload_artifact \
  --data-plane target/debug/data_plane \
  --exact-url http://127.0.0.1:9090 --output /path/new-run
python3 -m unittest discover -s tools/o11y-execution -v
```

Use an unused backend port (`--port`, default 18089). The output directory must
not exist. The runner validates all samples, compiles the supplied workload,
records candidates/selected plan/costs, starts its own backend, sends identical
Remote Write batches to both services, and queries every corpus occurrence at its
original evaluation time. It stops only the backend process it started. Partial
ingest is journaled without automatic retries: restart with fresh services and a
new output directory after investigating a failure.

`planning.json` contains the envelope, cost comparison, lifecycle estimates and
install request. `installed.json` records runtime status. `queries.json` preserves
every response, occurrence ID, timing and `warm`, `exact_fallback` or `failed`
classification. Forwarded responses carry a backend-owned `x-asap-execution`
header; an unmarked success is not counted as warm or fallback. `ingestion.json`
records accepted batches; acceptance does not prove worker completion.

`execution_provenance` distinguishes summary-only `asap`, `hybrid`, and
`external_exact`, and records summary readouts and Prometheus exact-subquery
requests. Hybrid execution remains under `exact_fallback`; it is never reported
as summary-only warm execution. A typed residual DAG can retain selected summary
siblings, but a particular corpus may still produce no materializations.

The runner waits for the finite-input completion barrier. The first traversal is
called `first_pass`, not “cold cache”; later traversals are `repeat`. All failures
and fallback responses remain in the denominator. A completion file means the
replay finished, **not** that accuracy or benefit acceptance passed.

## Matched exact comparison

Add `--compare --exact-pid PID` to the same command. PID must identify the local
Prometheus serving `--exact-url`; verify that association before running. If it is
remote, omit PID: exact-service CPU and memory stay unavailable. The runner never
stops this externally managed process. Prometheus must have the same data and
evaluation range; both endpoints receive identical encoded sample batches.

Every occurrence is queried against both endpoints with identical PromQL and
time, alternating request order. Results are matched by full label set and sample
timestamp, not row order. `comparison.json` reports missing/extra series and
samples, completeness, absolute/relative error, zero-denominator mismatches,
failures, first-pass/repeat latency distributions and sequential service rate.
Duplicate series, failed responses, unsupported response types and warnings make
a result uncomparable. Matching one dataset is not a formal confidence guarantee.

The latency ratio is emitted only when **all** matched responses pass the recorded
comparison and succeed. Tolerances default to zero; explicit absolute/relative
tolerances preserve strict equality and raw errors alongside the decision. These
numeric tolerances do not establish a sketch's accuracy guarantee. Fallbacks
remain in the totals. The ratio measures query service time only, never amortized
end-to-end savings. Raw per-request timings allow other analyses without hiding
the unsuccessful portion of the workload.

Linux process counters are captured around HTTP calls and at startup, ingestion
and query boundaries. Backend calls also record exact-service CPU when available,
so forwarded work is visible. Ingestion journal timings and phase snapshots expose
the construction/update interval, including the finite-input drain. Counters
include background work and have CPU-tick resolution. RSS/HWM are **whole-process**
memory, not summary heap; HWM is process-lifetime peak, not isolated phase peak.
`store.json` preserves the backend's raw state counters. `--cpu-affinity` applies
one CPU set to the backend and supplied Prometheus processes/threads.
`--address-space-bytes` applies the same RLIMIT_AS, which limits virtual address
space, not RSS or combined process memory. These controls are optional and their
presence is recorded; they do not establish a full cgroup resource budget.

Use `--fallback-url` and `--fallback-pid` for a separate fresh Prometheus instance,
keeping `--exact-url`/`--exact-pid` for the baseline. Identical process IDs and
storage paths are rejected. Both receive identical input batches. Backend CPU
includes its fallback service CPU; baseline CPU remains separate. With no
separate fallback service, the report preserves the shared-cache limitation.
`--exact-storage` and `--fallback-storage` record logical file sizes separately;
backend output file sizes include logs and are not retained summary heap sizes.

`summarize.py` compares estimated and measured query CPU only after verifying the
CPU model, data hash, selected manifest and priced query multiplicities. It
excludes lifecycle components from that ratio. Full lifecycle benefit remains
unavailable until matching setup, update, residency and retirement measurements
exist. A shared fallback/baseline service can transfer cache warmth; alternating
order does not eliminate this confound.

## Fresh repeated trials

```sh
python3 tools/o11y-execution/run_comparison.py \
  --prometheus /path/prometheus \
  --metrics /path/metrics.txt --queries /path/queries.json \
  --snapshot /path/o11y-costed-snapshot.json \
  --compiler target/release/examples/compile_workload_artifact \
  --data-plane target/release/data_plane \
  --cpu-affinity 4,5 --trials 3 --repetitions 20 \
  --output /path/new-trials
python3 tools/o11y-execution/summarize.py /path/new-trials/trial-1/replay \
  --output /path/new-trials/trial-1/summary.json
```

Each trial uses new empty TSDB directories and three distinct ports. The wrapper
records commands and Prometheus startup/lifetime resources, then stops its own
services. `backend-lifecycle.json` records backend lifetime CPU and peak RSS using
per-PID `wait4`. Process exit is not a summary-retirement measurement. Fresh
processes do not imply evicted OS caches, fixed memory quotas, or a concurrent
throughput test. Every repetition preserves the original evaluation timestamps;
this run does not simulate a live dashboard's advancing query windows.

## Finite-input completion

After all Remote Write batches are accepted, the runner calls
`POST /api/v1/precompute/drain` and saves `drain.json`. The receiver seals input
before worker barriers are queued. Each worker publishes its trailing panes,
then acknowledges completion; prior processing or sink failures remain failures
on repeated drains. Queries begin only after a successful completion response.
Subsequent Remote Write requests return HTTP 409 for that process. Start a new
backend process for another input generation.

This endpoint is for a finite replay, not a live ingestion watermark. Closing a
trailing pane does not by itself prove its coverage matches every query window;
unsupported or incomplete readouts must still follow exact fallback. Typed raw
residual plans prepare their raw index during drain so setup costs remain visible.

### Fixed-time and advancing-window repetition sweep

`query_sweep.py` runs fresh paired trials for 1, 5, 20 and 100 evaluations,
first at fixed timestamps and then at advancing timestamps over identical
preloaded input. It accepts the same binary/input/snapshot arguments as
`run_comparison.py`, plus `--scope full_upstream_corpus` or
`--scope derived_subquery_child` and `--advance-step-ms 60000`. Advancing grids
end at the original evaluation timestamp; the launcher rejects grids outside
the actual input span. This measures moving query windows, not continuous ingestion.

Keep the original 28-occurrence corpus regression separate from any derived
subquery-child experiment. A derived query needs a matching registered workload
and separately calibrated snapshot. The sweep never edits quotes or supplies a
winning artifact: each fresh process invokes normal selection. The selected
snapshot's declared cost horizon stays fixed, so sweeping repetition counts
measures amortization rather than cost-optimal reselection at each count.

The sweep uses `--batch-resources`: CPU probes occur before queries, after the
first pass, and after repeats. Per-request latency, actual execution provenance
and paired correctness are retained without per-RPC `/proc` sampling or repeated
JSON writes. `query-batch-resources.json` retains raw counters; measurements below
ten CPU ticks are marked low resolution, and zero ticks never mean free work.
`sweep-summary.json` separates first-pass/repeat query CPU from setup, all input
updates/build, and setup+updates+queries. Backend totals include its dedicated
fallback and planning CPU. First pass means a fresh process, not an evicted OS
cache; retirement and continuously updated input are outside these totals.
