# O11y multi-language fallback experiment

This benchmark keeps all 27 source occurrences in the denominator. `corpus.json` preserves each PromQL expression byte-for-byte as MetricsQL and contains its exact ClickHouse SQL over `raw_samples(metric, labels, ts_ms, value)`. The SQL implements counter reset and boundary extrapolation, offset, subquery grids, and classic histogram interpolation rather than importing answers from another engine.

## Reproduce

The command below starts with an empty output directory. It deterministically generates the OpenMetrics fixture, Prometheus configuration and TSDB, and physical plan; records their SHA-256 hashes; builds the data-plane binary from that same clean HEAD; runs both production frontend/compiler auditors; provisions fresh Prometheus, VictoriaMetrics, ClickHouse, and data-plane state; then writes raw requests, structured comparisons, phase resources, and terminal-stage coverage.

```bash
CARGO_TARGET_DIR=/path/to/target python3 tools/o11y-multilang/reproduce.py \
  --backend-source "$PWD" \
  --output-dir tools/o11y-multilang/repro-fresh \
  --trials 1 --repetitions 3 --seed 20260910
```

The seed randomizes query order independently for each repetition. Engine order alternates. Each trial creates new storage and process state and removes it afterward. Container images are launched by immutable digest and the manifest also records their actual image IDs. Resource snapshots are totals for the mixed alternating query phase and cannot be attributed to native or ASAP mode independently. The manifest records the clean source HEAD, binary hash, generated-input hashes, lifecycle and ingest duration, and process CPU ticks, RSS/HWM, and storage at start, after ingest, and after queries.

## Observed result

The checked-in evidence is one fresh trial with three repetitions and 27 queries, or 81 requests per engine. All five endpoints completed 81/81 requests. Both ASAP listeners reported `exact_fallback` for 81/81, so this result measures fallback overhead and does not establish acceleration benefit.

- VictoriaMetrics median/p95: 2.36/9.12 ms; ASAP MetricsQL fallback: 5.03/6.34 ms.
- ClickHouse median/p95: 49.92/169.56 ms; ASAP ClickHouse fallback: 56.25/179.34 ms.
- Native ClickHouse versus ASAP ClickHouse: 81/81 structured matches.
- Native VictoriaMetrics versus ASAP MetricsQL: 81/81 structured matches.
- Prometheus versus VictoriaMetrics: 48/81 strict matches and 33/81 mismatches.
- Prometheus versus ClickHouse label/value SQL oracle: 81/81 matches; timestamps and result type are reported as protocol-noncomparable.

The offline fail-closed matrix reports that the parser/canonical stage accepts 12/27 MetricsQL expressions, the early planner accepts 7/27, and the production compiler/publication validator accepts 2/27. The SQL acceleration frontend accepts 0/27 of the exact ClickHouse dialect mappings. Every row records its offline terminal stage plus the HTTP-observed exact fallback. Although q03 and q16 pass offline publication validation, the benchmark physical plan deliberately contains no corpus sidecars; their catalog-miss classification is inferred from that artifact, while only exact fallback is observed on the wire. Binder, validator, and executor are not reached in this fallback-only experiment.

## Evidence files

- `repro-fresh/stage-coverage.json`: per-query parser, planner, compiler, publication, terminal, adapter, and fallback result.
- `repro-fresh/manifest.json`: hashes, immutable images, lifecycle timing, and phase resource snapshots.
- `repro-fresh/trial-0-raw.json`: every timed response and `x-asap-execution` value.
- `repro-fresh/trial-0-comparisons.json`: labels, value, timestamp, result type, and warnings for each required engine pair.
- `repro-fresh/trial-0-latency-summary.json`: query-only latency summary.

## Limitations

This is one fresh trial and does not estimate variance across trials. CPU is process scheduler ticks rather than normalized CPU time, and the process totals cover mixed alternating native and ASAP requests rather than either mode alone. Container writable-layer size is an operational proxy for VM and ClickHouse storage. The production corpus has no warm executions, so it cannot quantify acceleration benefit. Native-histogram exponential interpolation is outside q21, which consumes classic `_bucket` series.
