# Three-Way Query Benchmark Harness

This directory holds the docker-compose-based query benchmark harness for
the central paper claim:

> **ASAPQuery-backend (sketch + ad-hoc cold-store fallback) vs. Prometheus
> vs. VictoriaMetrics** on the same input stream.

The harness is intentionally thin. It reuses `asap-quickstart`'s
docker-compose for the ASAP stack (kafka, arroyo, planner, summary-ingest,
queryengine, prometheus, 7 pattern-based fake exporters), layers
VictoriaMetrics + a cold-store volume on top via an override, and adds
runner scripts that record per-query latency / throughput across all
three backends.

## Workloads

| Workload | Source | What it tests |
|----------|--------|---------------|
| **W1** | `queries/promql_suite.json` (14 queries) | ASAP's sketch path: sum, avg, max, min, quantiles at p50/p90/p95/p99, group-bys |
| **W2** | `queries/adhoc_suite.json` (7 queries) | ASAP's fallback path: regex matchers, label_replace, rate/increase, exact histograms — hits `ColdFallback` (LocalFsColdStore) first, then forwards to Prometheus for query shapes the cold tier doesn't support |
| **W3** | both suites, run directly against Prom & VM | baseline ground truth + industry comparison |

## Files

```
benchmarks/
├── docker-compose.yml          # OVERRIDE for asap-quickstart: adds VM + cold-store volume
├── config/
│   └── vmagent-scrape.yml      # VM scrape config (same exporters as Prom)
├── cold-store/                 # Bind-mounted into queryengine; seeded by seed_cold_store.py
├── queries/
│   ├── promql_suite.json       # W1 — pre-existing
│   └── adhoc_suite.json        # W2 — NEW
├── scripts/
│   ├── run_asap_workloads.py   # NEW: W1+W2 against ASAP, median + P99
│   ├── run_prom.py             # NEW: W1+W2 against Prometheus baseline
│   ├── run_vm.py               # NEW: W1+W2 against VictoriaMetrics
│   ├── run_concurrency_sweep.py  # NEW: throughput-vs-concurrency CSV
│   ├── compare_three_way.py    # NEW: renders three_way_eval.md
│   ├── seed_cold_store.py      # NEW: pre-populates cold-store for W2
│   ├── wait_for_stack.sh       # pre-existing — waits for ASAP/Prom/Arroyo healthy
│   ├── ingest_wait.sh          # pre-existing — waits for arroyo pipeline RUNNING
│   ├── run_asap.py             # pre-existing — kept for the existing CI gate
│   ├── run_baseline.py         # pre-existing — kept for the existing CI gate
│   └── compare.py              # pre-existing — kept for the existing CI gate
├── reports/                    # JSON / CSV / MD output lands here
└── run_full_eval.sh            # NEW: orchestrator
```

## Quick start

```bash
# from repo root
./benchmarks/run_full_eval.sh
```

That will:

1. `docker compose up -d` the merged asap-quickstart + benchmarks stack.
2. Wait for Prometheus, Arroyo, QueryEngine, VictoriaMetrics to be healthy.
3. Wait for the Arroyo pipeline `asap-demo` to reach RUNNING and let
   sketches accumulate (`ingest_wait.sh`).
4. Seed `benchmarks/cold-store/` with raw JSONL fixtures for W2.
5. Run W1+W2 against each backend (3 iterations per query, captures
   median + P99).
6. Run the concurrency sweep at C ∈ {1, 4, 16, 64} for 60s each.
7. Render `benchmarks/reports/three_way_eval.md`.

Pass `--down` to tear the stack down at the end, or `--skip-sweep` to
skip the 4×60s concurrency sweep.

## Manual / partial runs

```bash
# Only one runner
python3 benchmarks/scripts/run_asap_workloads.py --asap-url http://localhost:8088
python3 benchmarks/scripts/run_prom.py           --prometheus-url http://localhost:9090
python3 benchmarks/scripts/run_vm.py             --vm-url http://localhost:8428

# Smaller sweep
python3 benchmarks/scripts/run_concurrency_sweep.py --duration 10 --concurrency 1,8

# Re-render the report after edits
python3 benchmarks/scripts/compare_three_way.py
```

## Output artefacts

| File | Producer | Shape |
|------|----------|-------|
| `reports/asap_promql.json`  | `run_asap_workloads.py` | per-query latencies, median, P99, result data |
| `reports/asap_adhoc.json`   | `run_asap_workloads.py` | same shape, W2 |
| `reports/prom_promql.json`  | `run_prom.py`           | same shape, baseline |
| `reports/prom_adhoc.json`   | `run_prom.py`           | same shape, W2 |
| `reports/vm_promql.json`    | `run_vm.py`             | same shape, VM |
| `reports/vm_adhoc.json`     | `run_vm.py`             | same shape, W2 |
| `reports/concurrency_sweep.csv` | `run_concurrency_sweep.py` | `backend,concurrency,total_queries,throughput_qps,p50_ms,p99_ms` |
| `reports/three_way_eval.md` | `compare_three_way.py`  | Tables 1–3 + capability matrix |

## What it deliberately does NOT do (per scope)

- The user explicitly de-scoped controller polish, sketch reallocation, and
  backfill correctness. We rely on `asap-quickstart`'s pre-baked
  `controller-config.yaml` to hand `streaming_config.yaml` to
  `asap-summary-ingest` — whatever sketches that produces is fine; only
  the 14 W1 queries need to come back non-empty.
- The W2 cold-store fixtures are deterministic synthetic data, not a
  faithful replay of the live exporter stream. We only need the
  fallback path to return non-empty so latencies are real.
- Smoke tests assume Docker is installed locally. CI uses
  `accuracy_performance.yml` which already runs the existing
  `compare.py` gate; this harness is additive.

## Wiring assumptions / TODO

- `queryengine` reads `ASAP_COLD_STORE_ROOT` from env. Today the binary
  doesn't expose a `--cold-store-root` flag in `main.rs` — the wiring
  exists in `AdapterConfig::prometheus_promql_with_cold` and the
  `LocalFsColdStore` struct, but `main.rs` itself constructs
  `AdapterConfig::prometheus_promql(...)` (no cold tier). Until that
  flag lands, ASAP W2 results will reflect only the Prometheus
  forwarding leg, not the cold-store leg. The bind-mount + seed script
  + adhoc_suite.json are correct as-is and will start exercising the
  cold path the moment the flag is added.
- The concurrency sweep uses `ThreadPoolExecutor` and the `requests`
  library — fine for ≤64 concurrency on localhost; if we ever push to
  higher fan-out, switch to `aiohttp`.
