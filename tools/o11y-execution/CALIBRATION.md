# Measured CPU cost provider

This developer workflow prices whole candidates in CPU nanoseconds. Memory and
storage remain separate resource measurements; they are not converted to CPU
with arbitrary weights. The provider never chooses a winner. The control plane
compares its complete quotes and installs the selected artifact.

1. `discover_snapshot.py --corpus CORPUS --metrics METRICS --template
   docs/examples/asapquery-planning-snapshot.json --output DISCOVERY` registers
   every unique original query with exact accuracy. `--interval-ms` is an
   explicitly declared experimental demand (default 60 seconds). The bounded logical cost horizon is `--repetitions` (default 20) times that
   interval; it is separate from input timestamps and measured wall time.
   Duplicate corpus occurrences increase their registered query frequency, so
   the manifest accounts for all 28 occurrences, not merely 24 unique strings. Provenance preserves input hashes and source counts.
   This discovery snapshot contains an **uncalibrated unit enumeration seed**,
   not cost evidence; version 2 with no quotes cannot deploy it.
2. Run `calibration_candidates DISCOVERY`. It calls the control plane and Planner,
   and exports every bindable candidate with its manifest and install request.
   Errors remain in the output. These artifacts are solely for calibration.
3. Measure each candidate in a fresh isolated backend, including its exact service
   if used. Feed the complete original input and execute every original query.
   Record installation, ingestion/build/update, residency, and retirement CPU,
   and each query's inclusive process CPU over enough repetitions to exceed the
   operating system's accounting resolution. Validate correctness and record
   `warm`, `hybrid`, `exact_fallback`, or failure. Record memory and storage separately.
   Retain all raw artifacts. Do not reuse calibration timings as the independent
   held-out performance evaluation.
4. Run `update_global_profile.py --snapshot DISCOVERY --measurements MEASUREMENTS
   --sample-count COUNT --output PROFILE` to replace discovery evidence with a
   conservative shared CPU-only measured profile. Network/scan zero fields are
   explicitly excluded model dimensions, not measured zero traffic. Re-export the
   candidates, and ensure their implementation/manifests match what was measured.
   The snapshot currently has one shared implementation model, so this is a
   coarse global model; complete candidate quotes carry measured whole costs.
5. Supply measurements to `calibrate.py --candidates CANDIDATES --measurements
   MEASUREMENTS --metrics METRICS --snapshot SNAPSHOT --output COSTED_SNAPSHOT`.
   The output includes a separate attribution audit. Run the normal control-plane
   compiler on this costed snapshot, then perform an independent evaluation.

Measurement JSON uses this structure (numbers below describe fields, not quotes):

- `compiler_identity`: the exact backend and pinned Planner revisions used to
  export and execute the candidate. Candidate export, measurement, and cost
  evidence must agree; a stale binary fails before quote matching.
- `units`: `cpu_ns`; `data_snapshot_id`: `sha256:` followed by the input file hash.
- `candidates`: one record per measured `plan_id`, with the exact `manifest`,
  `executable`, and matching `horizon_seconds`.
- Each record's `horizon_phases` has `install`, `ingest_and_build`, `residency`,
  and `retirement`, each containing measured `cpu_ns` and `raw_measurement_file`.
- Each record's `queries` maps every manifest query ID to `cpu_ns`, `evaluations`,
  `classification`, `correct`, and `raw_measurement_file`.
- Optional `resources` preserves separately measured memory and storage.

Inclusive horizon CPU is assigned once to the first horizon component, and
inclusive per-query CPU once to the first component for that query. Other
components explicitly receive zero **because their work is included in that
measured total**, not because state update, output, or fallback is free. The
attribution audit records these groups. Missing measurements make a candidate
unavailable; failed or incorrect execution cannot obtain an executable quote.
This is attribution of inclusive measured costs, not individual operator timing.

The backend currently exposes its Planner-selected forest and a whole-workload
exact alternative. This workflow does not claim exhaustive algorithm or lifecycle
search, and its unit discovery seed can influence which forest becomes available.
A calibrated comparison is only between the candidates actually exported.

## VictoriaMetrics and readout-specific checks

Use `calibration_candidates SNAPSHOT --metricsql` and
`compile_workload_artifact SNAPSHOT --metricsql` to compile the shared parser
subset through the existing MetricsQL serving path. This preserves language-tagged
query entries and exact edges; do not change an emitted install artifact by hand.
`BackendLocalPlanningSnapshot::compile_metricsql()` exposes the same operation.

`calibrate_runtime.py --victoriametrics BINARY` starts a fresh VictoriaMetrics
fallback instead of Prometheus. Ingestion and drain use the normal backend port;
queries use `--metricsql-port`. `--exact-cache-bytes` controls VictoriaMetrics cache
allocation, not process RSS. Record the binary version, cache budget and CPU set.
`--disable-result-cache` sets VictoriaMetrics `-search.disableCache` (including
its use through backend exact edges) and sends `nocache=1` to both query endpoints, so repeated
identical timestamps measure query execution instead of the exact server's result
cache. Declare that policy in the comparison report.

A corpus occurrence may contain `accuracy_validation` with a `metric` and
`bound`. Supported validation units are `relative`, `absolute_bits`, and `exact`
(the last requires zero bound). In particular, entropy absolute bits cannot
borrow a relative tolerance. A rank-error contract needs a rank oracle and is
intentionally rejected by this scalar comparison helper; do not use value error
to certify KLL rank accuracy. Native unsupported exact functions remain failed
comparisons and cannot obtain an executable cost quote.

For offline UnivMon error evidence, `cargo run --release -p data_plane --example
univmon_erp_artifact -- samples.jsonl` measures two-pane merged readouts on the
first ten source populations. It requires equal observed shapes; differing
populations must be calibrated separately. The tool retains samples offline,
records observed maxima and serialized state bytes, and does not measure CPU or
calibrate a failure probability. Use a zero CPU objective weight for that artifact;
whole-candidate CPU comes from the independent runtime calibration above. Keep
held-out source populations and performance runs separate from these training
populations. This tool does not select a plan.

For the finite integer-valued distinct study, the offline artifact tool measures
HLL precisions 10, 12, and 14 as a predeclared grid, alongside the UnivMon grid.
It uses the first ten source series and shifts their values by 1e12 into a disjoint
hash namespace; inputs outside its documented integer domain are rejected. The
held-out query uses groups 3–9 with original values. Neither a training maximum
nor HLL's theoretical relative standard error is a probabilistic per-query error
bound. The held-out 5% target remains fixed, and failed configurations stay in the
measurement history. A second optional argument writes the actual observed shape
for bounded ERP matching. CPU fields in this offline artifact remain excluded;
production process calibration measures CPU separately.

Finite VictoriaMetrics replays call `/internal/force_flush` once after ingestion
and charge it to build CPU. Accepted imports may otherwise remain invisible to
queries for several seconds. This is a test barrier, not a production ingestion
policy; see the [VictoriaMetrics forced-flush contract](https://docs.victoriametrics.com/victoriametrics/#forced-flush).

`measure_native_exact.py` runs a separate fresh VictoriaMetrics process with no
backend proxy. Use the same input, query corpus, CPU affinity, cache budget and
cache policy as the candidate run. It records native query latencies, process
CPU/RSS, lifecycle CPU and storage after shutdown. Report backend-only analytical
resources separately from the candidate's combined backend + fallback service;
retaining an exact service does not make its raw storage or memory disappear.
Visibility validation scans can warm native data caches, so the first reported
request is a first workload query after validation, not a cold-storage query.
