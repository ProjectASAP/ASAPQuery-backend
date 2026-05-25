# TODO — ASAPQuery-backend / sketchDB

_Last updated: 2026-05-01._

Post-session state: PRs #43–#71 landed. This doc enumerates what's
left for sketchDB to be paper-ready (VLDB / SIGMOD) + what's
deferred to future work.

See the design source at [`docs/design-sketch-db.md`](docs/design-sketch-db.md).

## Warm-tier query path closes the loop (2026-05-01)

The all-five-sketch query path from 2026-04-30 was wire-correct but
the live PromQL surface still returned empty even with data
demonstrably in the precompute store. Two PRs fixed that:

- **OTLP gRPC `max_decoding_message_size`** ([#70](https://github.com/ProjectASAP/ASAPQuery-backend/pull/70)).
  tonic's 4 MiB default rejected the gateway's first-window
  full-state DDSketch batch (~17 MiB at 1k cardinality). Gateway
  exporter looped on `decoded message length too large` forever.
  Bumped the receiver to 64 MiB, matching the `max_recv_msg_size_mib`
  value the agent and gateway already declare on their own OTLP
  receivers.

- **`range_query_into` overlap filter + closest-pane + response
  annotation** ([#71](https://github.com/ProjectASAP/ASAPQuery-backend/pull/71)).
  Two-part fix:

  1. **Overlap filter (store side).** `MutableEpoch::range_query_into`
     and `SealedEpoch::range_query_into` in
     `simple_map_store/common.rs` (and the on-disk parts variant
     in `per_key.rs:query_disk_parts`) used a "fully-contained"
     filter (`tr.0 < start || tr.0 > end || tr.1 > end → skip`).
     For tumbling windows of size W with a query range R, this
     matches at most `floor(R/W)` panes and only when both query
     endpoints land exactly on the pane grid. PromQL queries
     don't align to the grid (the wall-clock fractional portion
     of `query_time` is generally non-zero), so the strict filter
     returned 0 panes for every realistic query. Replaced with
     standard half-open overlap: keep `[tr.0, tr.1)` if `tr.1 > start
     && tr.0 < end`.

  2. **Closest-pane + annotation (engine + adapter side).** Per
     follow-up review: rather than merge multiple overlapping
     panes (slightly imprecise for sketch summaries), the engine
     now picks a **single closest pane** (max `tr.1`, tie-break
     on max `tr.0`) and threads the chosen `[start_ms, end_ms)`
     up through `QueryResult::with_window_used` to the Prometheus
     HTTP adapter, which adds it to the response's `infos` array
     as `precompute_window: [..., ...) ms (width N ms)`. Mirrors
     the existing `with_accuracy` annotation pattern. Now the
     caller sees exactly which precompute time range produced
     each value — important when the request range and the
     answered range differ.

### Live verification

```
$ curl '/api/v1/query?query=quantile_over_time(0.5, http_requests_total_latency_ms_quantile[1m])&time=$(now-90s)'
{"data":{"result":[{"metric":{"node":""},
                    "value":[..., "19.493849507395904"]}],
         "resultType":"vector"},
 "infos":["accuracy: ε=0.01, δ=0, kind=relative_quantile",
          "precompute_window: [1777655280000, 1777655310000) ms (width 30000 ms)"]}
```

Pre-fix: `result: []` with `No precomputed outputs found` even
though `runtime_info.earliest_timestamp_per_aggregation_id` was
populated and worker logs showed `Worker emitting 1 sketch outputs
for group (1, )` at every flush.

### Companion changes on the agent side

The collector-side path needed three connected fixes for delta
transmission to round-trip
([ASAPCollector#210](https://github.com/ProjectASAP/ASAPCollector/pull/210))
plus a windowed-processor pass-through to make multi-sketch
single-pipeline configs work
([ASAPCollector#211](https://github.com/ProjectASAP/ASAPCollector/pull/211)).
The backend-side delta apply path
(`apply_modified_otlp_delta_bytes` →
`{DDSketch,CMS,CountSketch,HLL}Accumulator::apply_proto_delta_bytes`)
was already in place; it just wasn't reachable until the agent
correctly tagged delta payloads on the typed encoding field and
stopped polluting the per-data-point attribute set with the
encoding string (which had broken the per-series snapshot cache
key).

- **Inference-YAML pattern coverage.** Expanded
  `asap-query-engine/examples/promql/inference_config.yaml` (and the
  SQL twin) with multi-quantile / wider-range / rate / increase /
  topk entries; closes
  [ASAPCollector PROGRESS.md follow-up #4](https://github.com/ProjectASAP/ASAPCollector/blob/main/PROGRESS.md#open-follow-ups-not-e2e-blockers)
  ("Inference config breadth"). New `tests/inference_yaml_pattern_coverage.rs`
  pins each family's YAML → `find_query_config` → `query_statistic`
  routing.

## All-five-sketch query path verification (2026-04-30)

Each sketch type now has a runtime-verified PromQL → backend path
through the modified-OTLP wire format (typed `Metric.data =
{DDSketch | KLLSketch | HLLSketch | CountSketch | CountMinSketch}`
data points). Specifically:

- **`query_statistic` for every sketch accumulator.** Implemented
  on `DDSketchAccumulator` (Quantile / Sum / Count / Min / Max),
  `HllSketchAccumulator` (Cardinality, with `Count` accepted as a
  Cardinality alias for the existing PromQL `count(...)` path),
  `CountSketchAccumulator` (Topk / Count / Sum, no-key fallback
  returns row-mean total), and `CountMinSketchAccumulator` (Count
  / Sum, no-key fallback returns the min-row sum — the canonical
  CMS total-event estimator that's exact when each insert
  increments one cell per row).
- **`accumulator_factory.rs`**: `DDSketchAccumulatorUpdater` wired
  in (alongside CMS / CountSketch / KLL / HLL updaters) so the
  precompute_engine recognises `AggregationType::DDSketch` from the
  `streaming.yaml` schema.
- **Modified-OTLP envelope decoders** for each sketch type land via
  the agent processors using sketchlib-go's `SerializePortable` /
  `SerializeMsgpack`; the backend's `from_sketchlib_proto_bytes` /
  `from_msgpack_bytes` constructors round-trip through
  `SketchEnvelope { sketch_state: Some(SketchState::*(state)) }`.

### Known reconciliation gap (cleanup, not a blocker)

- ~~`compatible_agg_types` in
  [`asap_types/src/capability_matching.rs`](asap-common/dependencies/rs/asap_types/src/capability_matching.rs)
  does not list `CountMinSketch` under `Statistic::Sum`, but
  [`promql_utilities/src/query_logics/logics.rs`](asap-common/dependencies/rs/promql_utilities/src/query_logics/logics.rs)
  treats CMS as the canonical approximator for both `Sum` and
  `Count`. The runtime e2e succeeds because the inference YAML's
  exact-match `find_query_config` path bypasses
  `find_compatible_aggregation`. Two tables → one table is the
  right cleanup.~~ **Closed by
  `fix/capability-matching-cms-sum-reconcile`.** `compatible_agg_types`
  now lists `CountMinSketch` under `Statistic::Sum` (and `MultipleSum`
  under `Statistic::Count`); both tables are kept in agreement by the
  `capability_canonical_map_agreement` test, which enumerates every
  `(Statistic, QueryTreatmentType)` pair and asserts the canonical map's
  output is contained in `compatible_agg_types(Statistic)`. The dead
  `Min/Max-Approximate → DatasketchesKLL` branch in
  `map_statistic_to_precompute_operator` was removed (KLL has no
  min/max query surface — that route would have produced runtime
  errors).
- CMS query without a paired `SetAggregator` /
  `DeltaSetAggregator` returns total volume, not per-key
  frequency. To drive `topk(N, …)` over CMS-tracked keys we need
  a key-aggregator processor on the agent. Tracked as a paper
  follow-up; out of scope for v1.
- **`IngestState.sketch_snapshots` is RAM-only.** Per-series
  snapshot cache that delta frames apply against is lost on
  backend restart. After a bounce, agents continue emitting
  `proto_delta` against their local snapshots, and the backend
  drops them as "delta-sketch arrived before any base snapshot"
  until the agent itself restarts. Persist to the existing
  per-key disk layer used by `SketchStore::with_persistence_per_key`,
  or add an OpAMP capability for backend → agent "send next
  frame as full state" signalling. Same item lives on the
  collector side
  ([`PROGRESS.md` follow-up #3](https://github.com/ProjectASAP/ASAPCollector/blob/main/PROGRESS.md));
  a fix on either side closes the gap.

## For paper submission (blocker)

### 1. Cold-query fallback — §5.2 of the sketch-DB design — **done (local-FS cold store)**

Initial v1 landed: [`drivers/query/fallback/s3_adapter.rs`](asap-query-engine/src/drivers/query/fallback/s3_adapter.rs)
is a `FallbackClient` that serves capability-misses from a
hour-bucketed JSONL raw store. The format (`raw/<metric>/YYYY/MM/DD/HH/part-NNNNNN.jsonl`)
is byte-identical to the S3 layout, so a future
`S3ColdStore: ColdStore` drops in with no adapter changes.

Follow-ups (not paper-blocking):

- **S3-backed `ColdStore` impl** next to the local-FS one; same
  trait, `aws-sdk-s3` list-objects-v2 for prefix pruning.
- **Richer query surface.** Today we compute `metric{...}`,
  `sum|count|avg|min|max(...)`. Regex matchers, `by (...)`
  grouping, and `rate/increase` over raw samples delegate to
  the chained inner fallback (typically Prometheus). Adding
  grouping + regex is ~200 LOC when needed.
- **Latency target.** Paper claim is ≤2× P99 vs. warm-hot —
  unverified until the multi-agent harness lands (blocker #6
  of `DataCollector/TODO.md`). The three-way query harness in
  [#66](https://github.com/ProjectASAP/ASAPQuery-backend/pull/66)
  (`benchmarks/run_full_eval.sh`) is the runner that will produce
  this number.
- ~~`asap-query-engine` `main.rs` wiring of `ASAP_COLD_STORE_ROOT`~~
  **done (P1, 2026-04-30).** `--cold-store-root` flag with
  `env = "ASAP_COLD_STORE_ROOT"` plumbed into a
  `build_adapter_config` helper that selects
  `prometheus_promql_with_cold` when set. Four unit tests pin
  the wiring matrix (cold × forward). Combine with
  `--forward-unsupported-queries` to keep Prom as the tail of
  the chain; without it, unsupported shapes return empty.

### 2. Accuracy-profile library per sketch type

`AccuracyProfile` trait landed in Phase 6.4 as a stub. Each
sketch type needs formally-derived bounds + empirical validation.

**Reference: the sketch-bench design doc**
<https://github.com/ProjectASAP/sketch-bench/blob/main/docs/DESIGN.md>.

Sketches to cover:
- **KLL** (datasketches-rs): `ε` vs `k` bound
- **DDSketch**: relative-error `α` bound
- **CMS** (count-min): `ε` bound from `w × d`
- **CMS-with-heap**: top-K error bound
- **HLL**: `δ` (std-err) bound from `p`

Deliverable: `AccuracyProfile::derive(&AggregationConfig)`
returns concrete (ε, δ) per sketch type — not a stub. Unit tests
against the sketch-bench corpus.

### 3. End-to-end capability-miss feedback loop test — **done (HTTP round-trip)**

HTTP-level e2e landed in
[`asap-query-engine/src/tests/capability_miss_http_e2e_tests.rs`](asap-query-engine/src/tests/capability_miss_http_e2e_tests.rs).
Spins up a real backend HTTP server + mock control plane HTTP
server, fires a PromQL `sum(metric)` query that capability-misses,
and measures wall-clock `time_to_plan_ready` from query issue to
the backend observing the new `StreamingConfig` via
`GET /api/v1/streaming-config`. Localhost floor: ~20 ms.

Also asserts:
- Mock control plane received the HTTP notify with the documented
  `{kind: "capability_miss", ...}` payload
- Backend's hot-reload handle has the exact `agg_id` the
  control plane pushed
- A repeat query on the same metric does **not** fire a second
  notify (loop is idempotent under query replay)

Follow-up (not paper-blocking):

- **Cross-process test.** Today's test is single-process with
  two HTTP servers. A docker-compose harness that wires a real
  DataCollector controller binary against the real backend
  binary is tracked by blocker #6 of `DataCollector/TODO.md`.
- **Time-to-first-hit over real data.** The "next query returns
  data" half of the story needs OTLP ingestion between the
  plan push and the repeat query — still tracked as an
  operational follow-up in the DataCollector repo.

### 4. Serialization format versioning tests — **done ([#65](https://github.com/ProjectASAP/ASAPQuery-backend/pull/65))**

`mod v2_forward_compat` in
[`asap-query-engine/src/tests/persist_format_versioning_tests.rs`](asap-query-engine/src/tests/persist_format_versioning_tests.rs)
covers all three persistence sites with three tests that pin the contract
to `PERSIST_FORMAT_VERSION + 1` (self-updating if the version is bumped):

- `schema_v1_with_future_version_falls_back_and_rewrites_clean` — tampers
  the JSON `version` field to v_current+1, asserts safe-fallback + rewrite
  at v_current with the new config's schemas and **no leakage from the
  bumped blob** (the no-data-corruption claim).
- `backfill_v1_with_future_version_falls_back_and_rewrites_clean` — same
  contract for `BackfillRegistry`, including `next_job_id` field presence
  post-fallback.
- `part_meta_with_future_version_returns_format_error` — `SketchStore`
  `meta.bin` has no fallback (parts are opaque), so the contract is a
  clean `PersistError::Format("unsupported version ...")`.

All three pass.

Follow-up (not paper-blocking): a docker-compose harness that bumps
`PERSIST_FORMAT_VERSION` in code, rebuilds, and restarts a running
backend with on-disk v1 state to verify the live restart path. The
unit tests cover the load path which is where the version-mismatch
logic lives, so this is just defense in depth.

### 5. Correctness proofs (for paper's theory section) — **done ([`docs/proofs.md`](docs/proofs.md))**

All three proofs landed in [`docs/proofs.md`](docs/proofs.md) §§2–4
(statement / setup-lemmas / proof / caveats / code-anchors per
proof; §1 reproduces the per-sketch accuracy bounds the proofs
treat as black boxes).

1. **`combine_statistic` correctness** across schema-timeline
   segments (`docs/proofs.md` §2): additive stats (Count / Sum /
   Min / Max) combined over non-overlapping segments equal the
   single-schema answer up to per-segment sketch error.
   Non-combinable stats (Quantile / Topk / Cardinality / Rate /
   Increase) return `Partial` with a bounded `covered` subset.
2. **Write-barrier safety** (`docs/proofs.md` §3): no sample
   ingested at wall-clock time `t > force_expire(agg_id).ts`
   appears in any query whose range includes `t'`. Follows from
   the `is_writable` check + schema lifecycle monotonicity.
3. **Backfill determinism** (`docs/proofs.md` §4): the §10.5
   invariants (time-disjoint, known agg, within retention) plus
   ordered raw-sample replay produce bit-identical sketches vs.
   live ingest for the same underlying samples.

## Future work (post-paper)

### F1. Phase 4: inter-window compaction

Long-running sketchDB needs compaction — merge adjacent small
windows into larger ones when accuracy can be re-derived, drop
sub-window duplicates from overlapping backfills. Today the
store only supports write + evict-by-agg, no inter-window
merge. Impacts long-term storage cost.

### F2. Multi-tier storage — §tier-2 read cache

Design draft exists
(`docs/design-simple-map-store-persistence.md` §tier-2 section).
Materialize as a read-through cache on top of the S3/disk
tier. Implementation deferred — current single-tier behaviour
is fine for v1 paper.

### F3. Cross-sketch-type combine

Combining KLL(k=200) with KLL(k=100), or KLL with DDSketch, for
the same logical quantile across a schema boundary. Out of
scope for v1 paper — operational practice has so far been
"new sketch config ⇒ new agg_id, new time-range", so combine
never sees heterogeneous types per metric.

### F4. OTLP ingest counter parity with other observability metrics

PR #51 added barrier-drop counter for OTLP paths. Other
silent-drop sites (decode errors, routing errors, unconfigured
metrics) could get the same treatment. Low-pri.
