# TODO — ASAPQuery-backend / sketchDB

Post-session state: PRs #43–#51 landed. This doc enumerates what's
left for sketchDB to be paper-ready (VLDB / SIGMOD) + what's
deferred to future work.

See the design source at [`docs/design-sketch-db.md`](docs/design-sketch-db.md).

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
  this number once `asap-query-engine`'s `main.rs` wires the
  `ASAP_COLD_STORE_ROOT` flag into the `prometheus_promql_with_cold`
  constructor — until then it exercises only the Prom-forwarding
  fallback leg.

### 2. Accuracy-profile library per sketch type

`AccuracyProfile` trait landed in Phase 6.4 as a stub. Each
sketch type needs formally-derived bounds + empirical validation.

**Reference: the sketchlib-bench design doc**
<https://github.com/ProjectASAP/sketchlib-bench/blob/main/docs/DESIGN.md>.

Sketches to cover:
- **KLL** (datasketches-rs): `ε` vs `k` bound
- **DDSketch**: relative-error `α` bound
- **CMS** (count-min): `ε` bound from `w × d`
- **CMS-with-heap**: top-K error bound
- **HLL**: `δ` (std-err) bound from `p`

Deliverable: `AccuracyProfile::derive(&AggregationConfig)`
returns concrete (ε, δ) per sketch type — not a stub. Unit tests
against the sketchlib-bench corpus.

### 3. End-to-end capability-miss feedback loop test — **done (HTTP round-trip)**

HTTP-level e2e landed in
[`asap-query-engine/src/tests/capability_miss_http_e2e_tests.rs`](asap-query-engine/src/tests/capability_miss_http_e2e_tests.rs).
Spins up a real backend HTTP server + mock controller HTTP
server, fires a PromQL `sum(metric)` query that capability-misses,
and measures wall-clock `time_to_plan_ready` from query issue to
the backend observing the new `StreamingConfig` via
`GET /api/v1/streaming-config`. Localhost floor: ~20 ms.

Also asserts:
- Mock controller received the HTTP notify with the documented
  `{kind: "capability_miss", ...}` payload
- Backend's hot-reload handle has the exact `agg_id` the
  controller pushed
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
- `part_meta_with_future_version_returns_format_error` — `SimpleMapStore`
  `meta.bin` has no fallback (parts are opaque), so the contract is a
  clean `PersistError::Format("unsupported version ...")`.

All three pass.

Follow-up (not paper-blocking): a docker-compose harness that bumps
`PERSIST_FORMAT_VERSION` in code, rebuilds, and restarts a running
backend with on-disk v1 state to verify the live restart path. The
unit tests cover the load path which is where the version-mismatch
logic lives, so this is just defense in depth.

### 5. Correctness proofs (for paper's theory section)

Three proofs to write up. Target location: `docs/proofs.md`;
seed the paper's §theory from it.

1. **`combine_statistic` correctness** across schema-timeline
   segments: additive stats (Count / Sum / Min / Max) combined
   over non-overlapping segments equal the single-schema
   answer up to per-segment sketch error. Non-combinable
   stats (Quantile / Topk / Cardinality / Rate / Increase)
   return `Partial` with a bounded `covered` subset.
2. **Write-barrier safety**: no sample ingested at wall-clock
   time `t > force_expire(agg_id).ts` appears in any query
   whose range includes `t'`. Follows from the `is_writable`
   check + schema lifecycle monotonicity.
3. **Backfill determinism**: the §10.5 invariants
   (time-disjoint, known agg, within retention) plus ordered
   raw-sample replay produce bit-identical sketches vs. live
   ingest for the same underlying samples.

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
