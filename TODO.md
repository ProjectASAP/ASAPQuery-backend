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
  of `DataCollector/TODO.md`).

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

### 3. End-to-end capability-miss feedback loop test

The flow exists as code (`ControllerClient::create_plan` on
backend side, `/api/v1/plan` route on controller side), but
isn't tested end-to-end. For the paper's "controller reacts to
workload drift" claim we need:

- Test harness spins up controller + backend in-process (or
  via Docker compose fixture)
- Issues query that causes capability-miss
- Asserts a new `StreamingConfig` arrives at the backend within
  `N` seconds
- Asserts next matching query hits (no miss)
- Measures time-to-plan-ready + time-to-first-hit

Not new engineering — testing the existing code path end-to-end.

### 4. Serialization format versioning tests

`PERSIST_FORMAT_VERSION = 1` exists but no migration test or
forward-compat story. Paper claim "sketchDB restarts without
data loss across format bumps" needs evidence.

- Check in a golden snapshot at v1 (schema registry +
  SimpleMapStore parts/manifest + backfill registry)
- Load with v2 code (intentional incompatible bump in a test)
- Verify expected migration OR safe fall-back-to-fresh (log +
  new empty registry) — per design doc
- Assert no crash, no data corruption

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
