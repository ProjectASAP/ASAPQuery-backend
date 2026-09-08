# Real-workload execution replay

Developer acceptance tooling, stacked on #524 and the deployment/cost-selection
foundation through #505. This is an executable harness, not a published benefit
result. It invokes the **control-plane compiler**, which calls the pinned Planner,
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

The harness does not provision Prometheus or calibrate the cost provider. These
are required run inputs, not completed end-to-end acceptance evidence.

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

The settle interval is recorded, not a completion barrier. The first traversal is
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

The latency ratio is emitted only when **all** matched responses are exact-equal
and successful. Approximate discrepancies are still reported, but no arbitrary
error threshold is substituted for each query's accuracy contract. Fallbacks
remain in the totals. The ratio measures query service time only, never amortized
end-to-end savings. Raw per-request timings allow other analyses without hiding
the unsuccessful portion of the workload.

Linux process counters are captured around HTTP calls and at startup, ingestion
and query boundaries. Backend calls also record exact-service CPU when available,
so forwarded work is visible. Ingestion journal timings and phase snapshots expose
the construction/update interval, including the declared settle delay. Counters
include background work and have CPU-tick resolution. RSS/HWM are **whole-process**
memory, not summary heap; HWM is process-lifetime peak, not isolated phase peak.
`store.json` preserves the backend's raw state counters. Affinity/cgroup metadata
is recorded but equal resource budgets are not enforced by this harness.

The provider's estimated costs are preserved next to measured quantities without
pretending abstract model units are CPU nanoseconds. End-to-end benefit and
estimated/measured cost ratios remain null until their units, lifecycle scope,
exact-service startup/storage costs and resource budgets are matched. Separate
fresh-process/cache-controlled trials, a retained-state measurement, calibration
provenance and real-corpus execution evidence are still required before declaring
the five #524 acceptance criteria complete. The shared fallback/baseline service
can transfer cache warmth; alternating order does not eliminate this confound.
