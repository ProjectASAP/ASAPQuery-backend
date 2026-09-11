# Automatically selected SUM and MAX over a bounded o11y series

Measured 2026-09-11, source commit `a5dd4b3c`. The control plane now calls
ASAPPlanner and constructs the catalog from its selected DAG. The probe supplies
SQL, source schema, accuracy and time bounds; it does not supply a winning family
or a predeclared materialization. Planner selected exact `Sum` and
`MinMax(max)`, respectively. Full selected DAGs, catalog identities and candidate
traces are in [the artifacts](artifacts/automatic/).

This is a bounded aggregate sensitivity experiment using original o11y values.
It is not evidence that the original 27-query SQL workload is accelerated.
Grouped labels, general residual operators and original workload coverage still
need their own validation. A quantile probe failed frontend function registration;
there is no quantile benefit result here.

## Input and execution

The unchanged source series is
`service_cache_refresh_lag_seconds{job="user-service",instance="user-service:8082"}`.
Its 24-hour input has 86,371 samples at approximately one sample/second,
16,086,775 bytes, at
`/mydata/clickhouse-o11y-main-results/fine-1s-24h-series.jsonl`.
The extraction provenance and original full-dataset hash are retained in
[the prior source manifest](artifacts/fine-1s-24h-series.provenance.json).
The half-open query interval is `[1788848096001,1788891296001)`: 12 hours and
43,200 samples. Both engines query the same interval and values.

Each SQL query is `SELECT max(value) AS value ...` or `SELECT sum(value) AS value ...`
over that bounded interval. Three fresh-server trials per aggregate each alternate
1,000 backend and 1,000 ClickHouse requests. Both processes receive two CPUs,
CPU affinity 60–61 and a 4 GiB limit. The ClickHouse image is pinned, `max_threads`
is two and query caching is disabled. Other agents paused builds and workload
probes for the controlled measurements. An earlier interrupted, uncontrolled
SUM attempt was excluded and is recorded outside the committed results.

All 6,000 backend requests reported `warm`; all 12,000 requests returned HTTP 200.
Every parsed backend result exactly equals its ClickHouse baseline: SUM is
1,328,070 and MAX is 530. This is exact numeric equality, with no approximation
or tolerance. The installed plan and completed backfill are exercised through
separate production backend processes.

## Repeated queries

| Aggregate/trial | Backend median ms | ClickHouse median ms | Speedup | Backend CPU ms / 1,000 | ClickHouse exact CPU ms / 1,000 |
| --- | ---: | ---: | ---: | ---: | ---: |
| SUM 1 | 0.861 | 6.891 | 8.01x | 810 | 9,600 |
| SUM 2 | 0.854 | 6.671 | 7.81x | 770 | 8,360 |
| SUM 3 | 0.853 | 4.703 | 5.51x | 750 | 9,460 |
| MAX 1 | 0.853 | 6.776 | 7.95x | 770 | 8,240 |
| MAX 2 | 0.854 | 6.716 | 7.87x | 770 | 8,360 |
| MAX 3 | 0.854 | 7.591 | 8.88x | 770 | 12,130 |

Counting ClickHouse background CPU during the backend requests as well gives
820–920 ms per 1,000 warm requests. CPU uses Linux scheduler ticks, so individual
sub-millisecond CPU values are not claimed. Per-trial p95 latency and serial
service throughput are retained in the summaries; these are serial request
measurements, not saturated concurrent throughput.

## Build, memory and storage

Backfill took 1.32–1.42 seconds. Its backend CPU was 220–300 ms and its additional
ClickHouse source-read CPU was 160–210 ms. The enclosing setup-to-built interval
was 1.85–2.08 seconds and includes source loading, planning, installation and
startup. Source loading and backfill are separately recorded; planning,
installation and startup are not isolated measurements. First-query
latency was not consistently better: backend 4.06–7.51 ms versus ClickHouse
5.26–11.07 ms. The repeated-query gains do not erase construction costs.

Backend query-phase RSS was 50.4–54.9 MiB; retained ClickHouse RSS was
779.6–849.3 MiB. Per-phase RSS and observed process high-water marks are in the
artifacts. These are whole-process observations, not isolated allocation sizes.
The backend output directory occupied 5,795 bytes including persisted metadata;
ClickHouse reported 507,146 bytes for its 86,371-row table. Backend summaries cover
12 hours while that table retains 24 hours, so these sizes are **not a compression
ratio**.

The deployment still retains ClickHouse for exact queries and backfill. Adding
the backend therefore adds memory and state storage to that deployment. These
measurements demonstrate lower repeated-query latency and CPU for the served
summaries, **not lower combined deployment memory or storage**. Control-plane
planning memory is not included in backend RSS. The current fixed-window layout
is not cost-optimized; candidate traces explicitly report estimated cost as
unavailable rather than inventing a measured/estimated cost comparison.

## Reproduction and evidence

Build the release process probe, then use
[the matched-budget runner](run_matched_trial.sh) with
`CLICKHOUSE_BENCH_AGGREGATE=sum` or `max` and a unique trial suffix. Keep all
required environment inputs described by the script. The committed runtime
snapshots, executable hashes and checkout files identify these runs. The raw
traces remain under `/mydata/clickhouse-automatic-benefit-study`; their hashes are
in [the raw-trace manifest](artifacts/automatic/raw-traces.json).

For independent plan inspection,
`cargo run --release -p control_plane --example compile_clickhouse_workload`
reads an automatic SQL workload JSON from stdin and emits the publication,
install request and actual selection trace. Example workload files and failed
quantile diagnostics are in `/mydata/clickhouse-automatic-benefit-study`.
