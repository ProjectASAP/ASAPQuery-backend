# Shared current-series quantiles and TopK

Backend-local PromQL workload compilation can export a maintained current-value
alternative for `quantile(q, metric)` and `topk(k, metric)`, including `by` and
`without` grouping and selector label matchers. Parameters must be finite scalar
literals. Selector offsets, `@`, nested input expressions and MetricsQL use the
existing alternatives; they are not admitted by this implementation.

ASAPPlanner owns this transformation through the opt-in `MaintainedPopulationStrategy`
over canonical IR. It emits `MaintainPopulation` at maintenance time and
`ReadPopulation` at read time, with source/filter/group identity, quantile
consumers and the maximum requested k in its typed contract. Compatible producers
are shared by Planner CSE. The backend consumes these nodes, binds resource and
input-lag limits, and retains the Planner DAG in the installed plan. It does not
recognize this optimization by reparsing the query text. Other deployments must
opt in only when they can implement and price this maintenance contract.

For example, put p50, p90, p95, p99 and Top1/Top5 in one workload. Matching source,
selector and grouping contracts produce one `CurrentSeries` population with
`quantiles: true` and `max_k: 5`. Each registered query keeps its own readout.
Different sources, filters and groupings remain distinct. This state is exact:
it retains each series' current value in a shared ordered population, rather than
inserting all historical observations into a quantile sketch. Its memory grows
with series cardinality, even when only Top1 is requested. Those excluded series
are required to promote the correct replacement when a winner decreases or expires.

Accepted Remote Write batches update the state atomically under its lock. A newer
sample replaces the old value; stale markers remove the value. Older updates do
not resurrect a newer stale marker. Per-group readout arrays are shared until that
group changes. TopK-only populations cache just the largest registered k results;
quantile consumers also share an ordered value array.

Deployment uses complete workload quotes. The manifest deduplicates population
build, update, residency and retirement components across consumers and prices
individual readouts separately. Exporting a candidate does not establish a speedup.
A native exact alternative remains available for cost selection and execution fallback.

## Runtime coverage

- This implementation serves current evaluations, using Prometheus' default
  five-minute selector lookback. It requires a complete Remote Write feed for
  each registered metric; the producer must not omit matching series.
- It waits for five minutes of observed input coverage. Event-time gaps exceeding
  the installed input-lag bound restart this warmup. The bound comes from the
  declared sample interval plus staleness margin (60 seconds when unspecified).
- Input lag, historical evaluations, reads before already-expired state, an
  unobserved generation or exceeded resource bounds cause native fallback.
- State is in memory. Restart and generation replacement require warmup again;
  historical range queries continue to use native execution.
- Populations divide the configured retained-summary memory budget and cap series
  cardinality. Bounds include conservative space for labels, trees and caches.
  The existing Remote Write adapter accepts finite sample values and stale markers.

`/metrics` exposes `asap_current_series_populations` and
`asap_current_series_cache_builds_total` to verify reuse. These describe the active
in-memory population generation; they are not window-sketch materialization counts.

## Validation

Run the compiler regression and state tests, then the process acceptance test:

```sh
cargo +1.98.0 test --locked -p control_plane --lib current_series_quantiles_and_topk
cargo +1.98.0 test --locked -p data_plane --lib current_series
cargo +1.98.0 test --locked -p data_plane --test asapquery_compatibility_process_e2e current_series_quantiles_topk
```

For differential validation, set `ASAP_CURRENT_SERIES_PROMETHEUS_URL` to a **fresh**
Prometheus instance with `--web.enable-remote-write-receiver`, then run the process
test. It writes the same samples to both services and compares values and labels
for quantiles and TopK, including value replacement and staleness. Test quotes are
synthetic and must not be used as performance evidence.

The Planner population IR also represents table-row multisets. They share the
aggregate rule and readout vocabulary with current-series populations, but not
the membership contract. The backend remote-write executor admits only the
current-series variant; explicitly selected table-row state returns a capability
error until a row-update/deletion executor is available. Existing SQL window
summary compilation remains independent of this capability.

The generalization is covered by six SQL frontend tests in Planner (quantiles,
scalar readouts, TopK limits, grouping, filters and invalid value columns), backend
capability rejection, and the current-series process test against Prometheus 3.5.
The UnivMon process test also installs one shared materialization for distinct,
frequency L2 and entropy with identical input/window/parameters, checks all three
readouts against held-out raw values, and checks readout-specific missing-evidence
fallback. This establishes sharing and correctness, not a measured speedup.
