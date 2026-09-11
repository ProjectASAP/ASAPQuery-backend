# Original o11y SQL execution through automatic summary plans

The original 27-query corpus now has a real execution matrix: 3 warm queries, 18 successful exact fallbacks, and 6 failures on both the backend fallback and native ClickHouse. All 21 successful responses have equal decoded JSON metadata and data. This is per-query execution coverage; it does not establish acceleration for all 27 queries or one jointly installed dashboard workload.

The warm queries are q05 (12-hour maximum grouped by the native labels Map), q06 (12-hour maximum for the user-service metric), and q23 (top 2 groups by the 6-hour retry-backlog maximum). The compiler receives the original SQL, table schema, and evaluation range. It calls Planner, records its actual selection, builds the catalog and executable plans, and installs the selected exact MinMax summaries. No materialization family or winner is supplied by the evaluation script.

## Data and provenance

- Existing o11y-derived capture with deterministic metric aliases: `/mydata/query-benefit-evaluation/o11y-evaluation-1s-24h.prom`.
- Source SHA256: `527efecab22f935b7aa00a29d7ed8a187da902a223c4fef3991d433268b528f6`.
- Samples: 12,236,002 across 142 series, at 1-second sampling granularity.
- Observed timestamp span: 1788804926000–1788891296000 ms (86,370 seconds, approximately 24 hours).
- Imported audit copy: `/mydata/clickhouse-o11y-main-results/o11y-27-1s-24h.jsonl.gz`, 85,682,422 bytes. Its `.jsonl.manifest.json` records counts per metric and the unchanged source hash.
- Isolated ClickHouse destination: `asap_o11y27_eval.raw_samples` on port 28123.

The importer preserves metric names, labels, Float64 sample values, and integral-millisecond timestamps. It rejects duplicate/malformed labels and nonfinite samples instead of silently changing them. Metric aliases were already present in the source file; this run does not claim those names are emitted directly by upstream o11y-bench.

## Execution coverage

The original arithmetic timestamp bounds `(evaluation_time - window, evaluation_time]` remain unchanged in SQL. The compiler evaluates checked integer constants to bind their equivalent half-open source interval. Native Map labels stay one typed grouping column through backfill, SummaryStore, and query readout.

The six failures are q01, q02, q17, q18, q25, and q26. Both native ClickHouse and forwarded execution returned HTTP 500 with memory-limit errors under the 4 GiB server limit. The existing counter SQL uses indexed `arraySum` expressions that trigger large intermediate allocations. These failures are not counted as successful fallback or as an ASAP benefit. A separate diagnostic isolates the indexed array capture with two groups: ClickHouse query-log peak memory rises from 33.45 MB at 1,000 samples/group to 129.55 MB at 2,000, 513.77 MB at 4,000, and 2.05 GB at 8,000. Doubling samples approximately quadruples peak memory. This explains why the original long counter windows exhaust the limit; the diagnostic does not replace the original queries or count as workload acceleration. [Exact diagnostic SQL and query-log measurements](captured-array-memory-diagnostic.json) are included. HTTP summary memory is not used as a peak-memory measurement.

The other 24 queries still fail the Planner SQL frontend because of ClickHouse-specific syntax or functions, including lambda/tuple syntax, Map access, `mapConcat`, empty `map()`, and `modulo`. Eighteen nevertheless execute correctly through whole-query native fallback. Their fallback success demonstrates routing correctness, not local DAG support or acceleration.

## Measurement scope

Repeated-query results are recorded separately from the correctness matrix. Each selected query uses a fresh backend and restarted native ClickHouse process, with 2 CPU cores (60–61) and a 4 GiB memory cap per process. Query cache is disabled and ClickHouse `max_threads=2`. The raw dataset is unchanged. Three independent trials contain 1000 backend and 1000 native requests each, alternating route order.

The measurements include publication/backfill wall time, backend and source ClickHouse CPU during construction, process RSS/high-water marks, state directory bytes, and per-request latency/CPU. They exclude original data ingestion and offline planning costs. Estimated numeric Planner cost is not returned by the current SQL selection trace. No complete deployment cost reduction, moving-window behavior, or memory reduction is inferred from these fixed-window trials. The backend continues to require the external ClickHouse service for fallback.

## Measured results

All 9,000 backend requests were warm, and all 9,000 matched native/backend pairs had equal decoded JSON metadata and data. HTTP response bytes differ in formatting; this is not byte equality.

| Original query | Backend median latency | Native median latency | Speedup across three trials | Summary directory after shutdown |
| --- | ---: | ---: | ---: | ---: |
| q05: 12h maximum, five groups | 2.36–2.43 ms | 50.85–51.35 ms | 21.04–21.74× | 7,008 B |
| q06: 12h user-service maximum | 2.27–2.38 ms | 19.92–20.36 ms | 8.37–8.97× | 5,238 B |
| q23: TopK over 6h retry-backlog maximum | 2.37–2.43 ms | 33.64–35.41 ms | 13.84–14.70× | 6,122 B |

The serial request-time rate (requests divided by summed HTTP request durations, excluding controller gaps) was 399.7–422.3 versus 18.6–19.0 requests/s for q05, 419.7–434.6 versus 45.6–46.4 for q06, and 400.4–414.0 versus 27.2–28.0 for q23. This is not a concurrent saturation-throughput benchmark.

First requests after state construction took 4.99–5.67 versus 72.46–146.82 ms for q05, 4.36–5.14 versus 35.23–40.99 ms for q06, and 4.92–6.24 versus 36.71–47.29 ms for q23. They are separate from the repeated-query medians and do not represent empty filesystem-cache cold starts: construction has already read the source data.

Publication and backfill took approximately 4.29 seconds for q05, 1.77 seconds for q06, and 2.78 seconds for q23. Backend construction CPU was 2.73–2.80, 0.52–0.55, and 1.35–1.38 seconds respectively; the source ClickHouse process additionally used 1.00–1.29, 0.24–0.42, and 0.60–0.84 seconds. State-directory bytes include metadata and are not a comparison against the complete source database.

A separate q05 experiment restarted ClickHouse before each of three ASAP-only and native-only phases. The ASAP phase used a previously validated response only as a correctness oracle, never to construct state, and issued no native query during its 1,000 warm requests. This avoids attributing native query allocations to the ASAP deployment.

| Fresh deployment, startup through 1,000 queries | Backend plus retained ClickHouse | Native ClickHouse alone |
| --- | ---: | ---: |
| Process CPU, including summary construction | 11.16–12.33 s | 96.67–98.86 s |
| End-of-query combined RSS | 837.5–867.4 MiB | 798.6–837.8 MiB |

The CPU reduction survives including summary construction for this repeated workload. **Total deployment memory is higher with ASAP in all three pairs.** Summing individual process high-water marks is only an upper bound on simultaneous peak memory, so it is not reported as a measured deployment peak. Common initial raw-data ingestion and offline planning remain excluded. These results establish benefits for the three warm queries and do not establish benefits for the remaining 24 queries.

The release runtime was built from `c825e804`; driver checkout `eff81f48` adds formatting changes only. Binary and input hashes are recorded with the raw trials. The correctness matrix was produced before the final conservative output-format guards; the three warm paths were rerun with the final release runtime.

## Reproduction

Compile publications with `cargo run -p control_plane --example audit_clickhouse_corpus -- --automatic tools/o11y-sql-main-eval/corpus.json`. The automatic mode shares the existing audit executable, avoiding an additional large evaluation binary during workspace checks.

Use [the importer](import_openmetrics.py) with the source capture above, then [the automatic corpus compiler](../../control_plane/examples/support/automatic_clickhouse_corpus.rs). [The process driver](run_automatic.py) stages and activates each compiled publication and submits typed backfill jobs. [The matched runner](run_matched.sh) pins the container image, resource limits, executable hash, and input matrix, and restarts ClickHouse before each measured query/trial.

Raw execution artifacts remain at `/mydata/clickhouse-o11y-main-results/original27-map-runtime/`; raw timing traces remain at `/mydata/clickhouse-o11y-main-results/original27-matched/`. Reproducing the exact capture requires that source file; a fresh upstream capture can have different values and cardinality.

The separate [deployment runner](run_deployment_pair.sh) compares fresh ASAP-plus-ClickHouse and native-only phases. With `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` already set in the environment, run:

```bash
export MATRIX=/mydata/clickhouse-o11y-main-results/automatic-map-corpus.json
export BACKEND_BIN=/dev/shm/asap-clickhouse-release/release/data_plane
export RUNTIME_SOURCE_COMMIT=c825e804
RESULT_ROOT=/mydata/clickhouse-o11y-main-results/new-matched bash tools/o11y-sql-main-eval/run_matched.sh
EXACT_REFERENCE=/mydata/clickhouse-o11y-main-results/new-matched/trial-1-q05/q05/result.json RESULT_ROOT=/mydata/clickhouse-o11y-main-results/new-deployment bash tools/o11y-sql-main-eval/run_deployment_pair.sh
```

The runners require the dedicated source container and populated table described above and verify its pinned image and resource budget before restarting it. Use new output paths; do not point them at production services. Compact [coverage](automatic-map-coverage.json), [latency measurements](automatic-map-measurements.json), [deployment measurements](automatic-map-deployment-measurements.json), and [raw artifact hashes](automatic-map-artifact-manifest.json) accompany this report.
