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
   `warm`, `exact_fallback`, or failure. Record memory and storage separately.
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
