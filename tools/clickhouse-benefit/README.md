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
