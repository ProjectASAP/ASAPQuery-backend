# O11y multi-language benefit experiment

This benchmark keeps all 27 source occurrences in the denominator. `tools/o11y-multilang/corpus.json` preserves each PromQL expression byte-for-byte as MetricsQL and assigns the matching ClickHouse SQL lookup. The SQL mapping reads a table produced from the same raw samples by a Prometheus-semantics loader; it must never ingest Prometheus/VM query answers. This indirection is required because native ClickHouse aggregates do not by themselves reproduce Prometheus counter reset, boundary extrapolation, staleness, subquery-grid, or histogram rules.

## Gate order

1. Validate corpus count, IDs, source hash, metric schema, evaluation timestamps, and raw-data hash.
2. Run both official ASAPPlanner frontends. Record parser, canonicalization, planner, compiler, publication validator, executor, adapter, and fallback independently for every occurrence. Unsupported rows remain failures/fallbacks in the denominator.
3. Start fresh Prometheus, VictoriaMetrics, and ClickHouse stores. Load identical raw samples. Build `o11y_prometheus_semantic_eval` from raw samples and verify it against Prometheus before timing.
4. Run fresh baseline and ASAP trials with alternating request order, fixed CPU affinity, separate storage paths, finite-ingest drain, and identical evaluation timestamps.
5. Compare full label sets, timestamps, values, warnings, response types, execution provenance, latency, CPU, RSS/HWM, storage bytes, ingest cost, and plan lifecycle cost.

A run is invalid if the semantic SQL loader is absent, its oracle comparison fails, any service shares a mutable storage directory, an unsupported row disappears from results, or an ASAP success lacks typed execution provenance.

## Prepared commands

```bash
python3 tools/o11y-multilang/audit_coverage.py \
  --corpus tools/o11y-multilang/corpus.json \
  --output artifacts/o11y-multilang/coverage.json \
  --dry-run-command 'cargo test -p control_plane metricsql_compilation_publishes_an_independent_sidecar_entry' \
  --dry-run-command 'cargo test -p control_plane clickhouse'
```

The full runner will require `--prometheus`, `--victoriametrics`, `--clickhouse-url`, `--metrics`, `--snapshot`, `--compiler`, `--data-plane`, `--trials`, `--repetitions`, `--cpu-affinity`, and a new output directory. Its manifest records binary hashes and exact commands. Per-query rows use this schema:

```json
{"id":"q01","language":"metricsql|clickhouse_sql","mode":"baseline|asap","trial":1,"repetition":0,"stage":"parser|canonical|planner|publication|validator|executor|adapter|fallback","status":"warm|exact_fallback|failed","typed_reason":null,"http_status":200,"latency_ns":0,"cpu_ns":0,"response_sha256":"...","comparable":true,"mismatch":null}
```
