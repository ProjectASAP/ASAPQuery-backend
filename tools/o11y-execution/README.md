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
replay finished, **not** that accuracy or benefit acceptance passed. The next
stacked PR adds matched exact queries and measurement/reporting.


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
unsupported or incomplete readouts must still follow exact fallback. This change
adds no local raw-query storage or hybrid operator execution.
