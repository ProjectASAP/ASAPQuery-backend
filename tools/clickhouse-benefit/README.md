# ClickHouse warm-path benefit probe

This developer probe measures one bounded SQL `max` query through the production
ClickHouse listener and directly against the same ClickHouse table. It compiles
and installs a shared `QueryPlan`, backfills its selected materialization, checks
the warm result against ClickHouse, performs ten warmups, then alternates 100
warm and 100 exact requests. The JSON artifact retains every latency and the
backend/ClickHouse process counters around the query phase.

```sh
mkdir -p /path/to/results
CLICKHOUSE_URL=http://127.0.0.1:8123 \
CLICKHOUSE_USER=bench CLICKHOUSE_PASSWORD=bench \
CLICKHOUSE_PID="$(pidof clickhouse-server)" \
CLICKHOUSE_BENCH_OUTPUT=/path/to/results/q05-warm.json \
cargo test -p data_plane --test clickhouse_q05_process_e2e -- --nocapture
```

`CLICKHOUSE_PID` is optional. CPU is reported in Linux scheduler ticks and RSS
is whole-process RSS. `CLICKHOUSE_STORAGE` may name a readable data directory;
its recursive logical size is recorded. Build time covers database setup, plan
compilation/installation, backend startup, and backfill. It does not isolate
these components. The probe uses a dedicated table and drops that table at the
start of each run.

This probe is deliberately narrow. It does not represent the 27-query corpus,
does not exercise a hybrid DAG, and does not establish end-to-end workload
benefit. Run release binaries on an otherwise idle, dedicated ClickHouse process
for publishable measurements.

## Replay a real series

`extract_series.py` selects one exact series from an existing OpenMetrics trace,
retains its values and timestamps, and records the input hash. The probe requires
one series because its SQL output is a scalar. It cannot stand in for grouped
multi-series execution.

```sh
python3 tools/clickhouse-benefit/extract_series.py /path/to/data.prom \
  /path/to/series.jsonl \
  --series 'service_cache_refresh_lag_seconds{job="user-service",instance="user-service:8082"}'
CLICKHOUSE_BENCH_INPUT=/path/to/series.jsonl \
CLICKHOUSE_BENCH_METRIC=service_cache_refresh_lag_seconds \
CLICKHOUSE_BENCH_END_MS=1788891296001 \
CLICKHOUSE_BENCH_REPETITIONS=1000 \
CLICKHOUSE_BENCH_OUTPUT=/path/to/results/real-series.json \
CLICKHOUSE_URL=http://127.0.0.1:8123 \
cargo test --release -p data_plane --test clickhouse_q05_process_e2e -- --nocapture
```

The end timestamp is exclusive. For millisecond samples, adding one millisecond
to a PromQL evaluation timestamp expresses its `(t-12h, t]` window as the SQL
`[t-12h+1ms, t+1ms)` range. Both routes use that identical range. The complete
selected series is loaded into a dedicated MergeTree table; state backfill reads
the requested 12-hour window. Every measured response must equal the baseline,
and every accelerated response must retain `warm` provenance.

This measures runtime reuse of a **predeclared MinMax catalog**. Planner selects
the query DAG against that catalog, but the harness supplies the materialization
and pane size. It does not measure automatic materialization candidate selection,
estimated costs, or the original SQL corpus's metric-filter binding. Those remain
separate acceptance requirements. The source trace's own generation provenance
must accompany the extraction record; a scaled/interpolated trace is not native
one-second source data.

`summarize.py ARTIFACT.json` reports latency and coarse per-route process CPU
deltas. The first-query measurements are taken after state construction; they
are not cold filesystem-cache measurements. Process CPU includes background
work, and RSS/high-water marks cover the whole process. Input loading and
backfill happen before the repeated-query phase, and their costs remain in the
separate build-phase record.
The reported serial service rate is the reciprocal mean request latency, not a
concurrent throughput test. `CLICKHOUSE_BENCH_REPETITIONS` defaults to 100 per
route; longer runs reduce the relative scheduler-tick quantization error.
