# Sketch DB — Performance & Profiler

> **Scope.** A performance analysis of the sketch DB vs. Prometheus /
> VictoriaMetrics (§19), plus the design of the measurement substrate
> — the **Sketch Profiler** library — that provides the real numbers
> other sections depend on (§20).
>
> **Audience:** anyone deciding whether to adopt the sketch DB for a
> workload, tuning parameters, or building the cost model in the
> controller.
>
> **Status.** §19 is an analysis, not implementation. §20 describes a
> library that does not exist yet.

Section numbers match the original monolithic `design-sketch-db.md`.

---

## 19. Appendix: performance envelope vs Prometheus / VictoriaMetrics

This section estimates the performance of the sketch DB relative to
Prometheus and VictoriaMetrics on representative queries. Numbers are
**order-of-magnitude estimates** derived from published benchmarks and
sketch complexity bounds, not measurements from this codebase. They
motivate which workloads the design targets and which it explicitly
does not.

### 19.1 Reference workload

A realistic production scenario that stresses TSDB scanning:

- **10,000 hosts × 10 services = 100,000 time series**
- **100 ms scrape interval → 10 samples/sec/series**
- **Total ingest: 1 M samples/sec**
- **1 hour of data: 3.6 B samples** (one metric), or **54 B samples**
  if the metric is exposed as a Prometheus histogram with 15 bucket
  time series
- **1 day: 86.4 B samples** (plain) or **1.3 T samples** (histograms)

The 100 ms scrape is not extreme for targeted high-frequency workloads
(NCCL GPU collective metrics, network packet counters, HFT, 5G radio
metrics). Standard 15 s / 30 s scrape regimes produce lower pressure
but the same relative shape.

Baseline TSDB scan throughput used below:
- Prometheus: ~1 M samples/sec/core
- VictoriaMetrics: ~10 M samples/sec/core (published)

Sketch DB costs:
- KLL merge: ~1 μs per merge
- CMS merge: ~microseconds (size-bounded)
- HLL register OR: ~μs
- Quantile / cardinality compute: microseconds post-merge

### 19.2 Per-query-type estimates

| Query | Workload | Prom | VM | Sketch DB Tier 2 | Sketch DB Tier 1 |
|---|---|---|---|---|---|
| **p99 quantile, 1h, by service** (`histogram_quantile` style) | 1.5 M bucket series, 54 B samples involved in full scan | **Infeasible cold** (15 core-hours) — requires recording rules | **~6 min on 16 cores** cold; 10–60 s with cache | **~1 ms** (600 KLL merges) | **< 1 ms** (EH lookup) |
| **Top-K over 10 M customers, 1h** | 10 M series × 3600 × 10 = 360 B samples | **Not possible** | **15–60 min** or OOM | **~5–10 ms** (60 CMS+heap merges) | N/A |
| **Cardinality / count_distinct, 1d** | 10 M series enumerated | **Not possible** | **10–60 min** or OOM | **< 1 ms** (HLL estimate) | **< 1 ms** |
| **Low-card short-range** (`rate({service="auth"}[5m])`) | 3,000 samples | ~10 ms | ~2–5 ms | ~2 ms | ~1 ms |
| **Multi-sub-window dashboard** (p99 @ 1m / 5m / 15m / 1h on same series) | 4 independent scans | 4× single-panel (seconds to minutes) | 4× single-panel | 4× independent part scans | **1× — shared EH** |
| **Point read** of one specific series at one point in time | 1 chunk | ~5–10 ms | ~2–5 ms | **Not supported** → falls back to exact DB (+ ~0.5 ms routing) | Not supported |
| **Exact count** (`count_over_time({customer="X"}[1d])`) | varies | Exact, seconds | Exact, sub-second | **Approximate** (CMS, ε ≈ 10⁻³) in ~10 ms | N/A |

Columns where Sketch DB is "not supported" are where the design
explicitly defers to the exact DB — point reads and exact queries are
the job of the exact DB that also serves as the base relation (core
§2.1, §10.1).

### 19.3 Why sketch DB wins grow with scrape rate

The key observation that justifies treating 100 ms scrape as the
benchmark case:

- **TSDB scan cost scales linearly** with sample count, which scales
  linearly with scrape rate. 600× higher scrape rate → 600× more work
  for every aggregate query.
- **Sketch DB cost scales with sketch parameters, not sample count.**
  A KLL over 10⁶ samples and a KLL over 10⁹ samples have the same
  serialized size and the same quantile-query cost. At 100 ms scrape
  the ratio widens by roughly the scrape-rate ratio.

At 1 min scrape, the p99 dashboard query is **3 s on Prom vs 1 ms
sketch** — 1,000× speedup. At 100 ms scrape, the same query becomes
**30 min → infeasible on Prom vs 1 ms sketch** — the ratio goes to
infinity because the TSDB falls off a cliff that the sketch DB
doesn't see.

### 19.4 Storage cost

Sketch size depends on sketch parameters, not on input sample count —
this is the most important property for high-scrape-rate deployments.

| Storage object | 1 min scrape (100K series, 7 days) | 100 ms scrape (100K series, 7 days) |
|---|---|---|
| Prom/VM raw samples, compressed | ~100 GB | **~60 TB** |
| Sketch DB Tier 2, `grouping_labels = [service]`, 1 min windows | ~50 GB | **~50 GB** *(unchanged)* |
| Sketch DB Tier 2 + semantic compaction to 1 h windows | ~5 GB | **~5 GB** *(unchanged)* |
| Sketch DB Tier 1 (PromSketch), 5 min retention | ~100 MB | **~100 MB** *(unchanged)* |

Storage cost ratio vs TSDB goes from **~2×** at 1 min scrape to
**~10,000×** at 100 ms scrape. Break-even — the scrape rate at which
sketch DB becomes cheaper than TSDB in storage alone — is somewhere
around 10–30 s scrape depending on sketch parameters and grouping.
Above that, sketch DB is strictly cheaper; below that, TSDB is.

### 19.5 Qualitative shift: from "faster" to "feasible"

The quantitative speedups above hide a more important shift. At high
scrape rates, a subset of queries becomes **impossible** on TSDB:

| Query | 1 min scrape | 100 ms scrape |
|---|---|---|
| p99 dashboard over 1 h, 100 K series | Prom 3 s (tight) / VM 500 ms | Prom **30 min or OOM** / VM **6 min cold** |
| Top-K over 10 M customers | Prom **already infeasible** / VM 30 s | Prom impossible / VM **1–2 hours or OOM** |
| `count_distinct` over 1 day | Prom infeasible / VM 5–30 s | Prom infeasible / VM **OOM** |

Sketch DB stays at **< 10 ms** for all of these regardless of scrape
rate. So at 100 ms scrape the sketch DB is not "an optimization" — it
is the **only way** to serve these queries interactively.

### 19.6 Where TSDB still wins

The sketch DB intentionally does not replace TSDB for:

- **Point reads** of a single time series at a specific timestamp —
  TSDB chunk reads are already sub-millisecond; sketch DB adds routing
  overhead with no benefit.
- **Exact numerical answers** (financial reconciliation, regulatory
  reporting, billing ground truth) — sketches are approximate by
  design; the exact DB must serve these.
- **Ad-hoc exploration** of arbitrary label dimensions — sketches
  only answer along the `grouping_labels` they were built for. A query
  on an unplanned dimension falls through to the exact DB.
- **Low query rate** (few queries per day against a given metric) —
  the pre-computation cost of maintaining sketches is not amortized.
  The controller's cost model should decline to materialize a sketch
  for such metrics.

### 19.7 Cost-model break-even

Very rough formula the controller can use to decide whether a metric
is worth sketching:

```
sketch_value = query_rate
             × avg_series_covered_per_query
             × (tsdb_query_latency − sketch_query_latency)
             × retention_days
sketch_cost  = agent_cpu_for_sketching
             + backend_memory_for_live_sketches
             + backend_storage_for_parts
             + controller_planning_overhead

materialize_if: sketch_value > sketch_cost
```

For high-QPS dashboard metrics with high cardinality, `sketch_value`
easily dominates. For cold metrics or exact-required metrics, it does
not and the controller should leave them on the exact-DB path only.

### 19.8 Summary

| Dimension | Sketch DB advantage at 100 ms scrape |
|---|---|
| Aggregate queries (quantile, top-K, cardinality) | **10³–10⁶×** faster, often enabling queries TSDB can't serve |
| Storage for high-QPS metrics with grouping | **~10⁴× smaller** than raw TSDB |
| Short-window live queries with many sub-windows | **100–1000×** via Tier 1 EH locality |
| Agent → backend bandwidth | **~10–100×** via edge sketching |
| Point reads, exact queries, ad-hoc exploration | **0 or negative** — sketch DB defers to exact DB |
| Low-QPS / cold metrics | **Negative** — cost of materialization exceeds saving |

The sketch DB is therefore best understood as an **accelerator** over
the exact DB rather than a replacement: it carries the 90 % of query
traffic that fits its design pattern at orders-of-magnitude lower
cost, and relies on the exact DB for the remaining 10 % where
sketching has no advantage. This dual-role architecture is why the
exact DB is modeled as both the refresh source (core §10) and the
query fallback target (core §7.3) — the same storage serving two
purposes.

### 19.9 Theoretical accuracy bounds per sketch type

These are the formulas every `AccuracyProfile` (core §6.4) is derived
from. They are mathematical guarantees from the sketch's defining
papers, not empirical estimates — given parameters and a sketch
type, the controller and the query path know the bound exactly.

| Sketch | Parameters | Statistic answered | Error bound | Confidence |
|---|---|---|---|---|
| **CountMin (CMS)** | width `w`, depth `d` | point frequency `f̂` | `0 ≤ f̂ − f ≤ ε · ‖x‖₁` with `ε = e/w` | `1 − δ`, `δ = e^(−d)` |
| **CountSketch** | width `w`, depth `d` | unbiased frequency | `|f̂ − f| ≤ √(‖x‖₂² / w)` (one-σ) | asymptotic ~68 % at 1σ, ~95 % at 2σ |
| **CMS + heap** | `w, d, k` | top-k by count | top-k items returned exactly when their true count exceeds `(k+1)`-th item by ≥ `ε‖x‖₁` | `1 − δ` |
| **KLL** | `K` (default 200) | quantile rank | rank error ≤ `c / √K` (c ≈ 1) | asymptotic ~99 % at default K |
| **DDSketch** | relative accuracy `α` | quantile value | `\|q̂ − q\| / q ≤ α` | deterministic if backing store is unbounded; bounded variants degrade gracefully |
| **HLL** | `m = 2^p` registers | distinct count | std error `σ ≈ 1.04 / √m` | asymptotic ~95 % at 2σ |
| **UnivMon** | levels `L`, base sketch | many statistics polymorphically | inherits from the layer-`L` base sketch's bound | per-layer |
| **Typed aux columns** (count/sum/min/max) | n/a | their respective scalar | exact (no sketch involved) | 1.0 |

These formulas are encoded in `AccuracyProfile::per_statistic` as
`ErrorBound` variants. The query path returns them directly without
runtime computation.

### 19.10 Merge error propagation

When N entries of the same sketch type are merged (the common case
for range queries), the resulting bound depends on the sketch
family's algebraic properties:

| Sketch family | Merge type | Bound after merge |
|---|---|---|
| **CMS** | cell-wise sum | `ε` unchanged; `‖x‖₁` grows to the merged stream's L1 norm — bound stays the same shape |
| **CountSketch** | cell-wise sum | `σ` unchanged on the merged distribution; bound preserved |
| **HLL** | register-wise max | bound preserved exactly; merge is the natural set-OR of the underlying multisets |
| **KLL** | level-by-level | bound preserved with small overhead (≤ 2 % typically); merging N KLLs with `K = 200` still gives ~1 % rank error |
| **DDSketch** | bucket-wise sum | relative `α` preserved exactly |
| **Typed aux** | additive (sum/count) or extremum (min/max) | exact |

So the answer to "what is the bound after merging 60 KLL sketches
covering the last 1 hour?" is **the same bound as a single KLL** —
this is a designed property of mergeable sketches and is a major
reason to prefer them over non-mergeable approximations.

**Cross-segment combinations** (when the schema timeline crosses
a reconfigure boundary, core §7.3) are different — segments may use
different sketch types or parameters, so the combined bound is the
**worst** of any per-segment bound, with confidence multiplied:

```
combined_bound      = max(seg.bound for seg in segments)
combined_confidence = ∏(seg.confidence for seg in segments)
```

The `Provenance.segments_combined` field surfaces this so callers
know the answer is composite. For non-additive statistics (Quantile,
TopK) crossing a sketch-type boundary, the result is `Partial` with
the un-mergeable segments listed in `missing` — the user sees that
the answer covers only the segments where it could be computed, not
a silently-wrong combination.

---

## 20. Sketch profiler library

The accuracy bounds in §19.9 are mathematical guarantees, but the
**operational characteristics** (CPU, memory, latency, throughput)
of every sketch type are **measured**, not derived. Different
implementations of the same sketch family — sketchlib-rust vs
sketchlib-go, branch A vs branch B of either, different parameter
choices — have different real-world behaviour even when the
theoretical accuracy is identical. The controller's cost model
(core §15.2 `/api/v1/db/cost_estimate`, core §10.6 incremental-vs-
refresh decision) needs real measurements to plan well.

This section describes a separate library — the **Sketch Profiler**
— that is shared across the sketch DB, sketchlib-rust, sketchlib-go,
and the controller. It is not part of the sketch DB itself; it is the
measurement substrate the sketch DB and the controller both consume.

### 20.1 What it measures

For every `(sketch_type, parameters, sketchlib_version, hardware
profile)` tuple, the profiler collects:

| Metric | Definition | Use |
|---|---|---|
| **CPU per insert** | nanoseconds per `update(value)` call | agent CPU budget; admission control quotas (roadmap §11.1 `max_write_qps`) |
| **CPU per merge** | nanoseconds per `merge_with(other)` call | compaction cost (roadmap §9), refresh cost (core §10.3), query merge cost (core §7.3) |
| **CPU per estimate** | nanoseconds per `query_statistic(stat)` call | query latency (§19.2) |
| **Memory per accumulator** | bytes resident, including allocator overhead | admission-control `max_bytes_in_memory`; storage-cost estimate |
| **Wire size, serialized** | bytes after `serialize_to_bytes` (proto / msgpack) | agent → backend bandwidth (§4 of DataCollector#153); backend → store |
| **Empirical accuracy** | measured `\|estimate − ground_truth\|` on synthetic and real workloads | validates §19.9 theoretical bounds; flags regressions if a sketch implementation drifts from theory |
| **Insert throughput** | samples/sec/core sustainable before backpressure | agent CPU sizing |
| **Merge throughput** | merges/sec/core | compaction sizing |
| **Cold-start cost** | first-insert latency (allocator + JIT warmup) | cold-query SLA |

These are collected per sketch type, per parameter set, and per
target architecture (x86_64 vs arm64 vs the agent's actual CPU
model). The profiler stores results in a published catalogue that
the controller reads at planning time and the operator inspects
when picking parameters for a new aggregation.

### 20.2 How it runs

The profiler is a standalone binary in its own crate
(tentatively `sketch-profiler/`). It supports three modes:

- **Calibration** — runs all sketch types × a parameter grid against
  synthetic distributions (uniform, zipf, normal, heavy-tailed) plus
  a few real-world snapshots. Produces a baseline catalogue. Run
  once per sketchlib release, or whenever a sketch implementation
  changes. CI integration: a PR that touches sketchlib must include
  a re-run that shows no significant regression.
- **Drift watch** — runs in production as a low-priority sidecar
  task. Periodically samples a small subset of sketch types and
  parameters, compares against the catalogue. Alerts if measured
  CPU/memory/accuracy diverges by more than a threshold from the
  catalogue value (catches sketchlib version mismatches, hardware
  changes, allocator regressions).
- **What-if** — given a `(sketch_type, parameters, expected_qps,
  expected_cardinality)` tuple, returns predicted CPU/memory/latency
  numbers. The controller calls this at plan time. It also takes a
  query workload as input and returns the predicted `query_latency`
  per statistic.

### 20.3 Catalogue format

```rust
struct ProfilerCatalogue {
    /// Identity of the run that produced this catalogue.
    sketchlib_versions: HashMap<Lang, String>,  // {Rust: "0.4.2", Go: "0.4.0"}
    hardware: HardwareProfile,
    measured_at: DateTime,

    /// One entry per (sketch_type, parameter set) tuple.
    entries: Vec<ProfilerEntry>,
}

struct ProfilerEntry {
    sketch_type: SketchType,
    parameters: HashMap<String, Value>,

    /// Operational characteristics
    cpu_per_insert_ns: f64,
    cpu_per_merge_ns: f64,
    cpu_per_estimate_ns: HashMap<Statistic, f64>,
    memory_bytes: usize,
    wire_size_bytes: WireSize,         // {proto: u64, msgpack: u64, msgpack_delta: u64}
    insert_throughput_per_core: f64,
    merge_throughput_per_core: f64,
    cold_start_us: f64,

    /// Empirical accuracy under various distributions; cross-checked
    /// against the theoretical AccuracyProfile from §19.9.
    measured_error: HashMap<DistributionShape, EmpiricalError>,

    /// Which workload shapes this entry was tested against. Used to
    /// scope the validity of the measurement.
    tested_workloads: Vec<WorkloadShape>,
}
```

### 20.4 How the controller uses it

The controller's planner (core §15.2) replaces hand-coded
constants and crude formulas with calls into the profiler:

```
Old: cost_model.estimate_size(CMS, width=1024, depth=5) → "12 KB"
                                                          ^ hand-coded constant

New: profiler.lookup(CMS, width=1024, depth=5)
       → ProfilerEntry { memory_bytes: 12_512, wire_size_bytes: …,
                         cpu_per_insert_ns: 47.3, … }
```

This makes cost-based plan selection actually correct: when a plan
chooses CMS over CountSketch for a frequency query, the choice
reflects measured costs on the target hardware, not extrapolated
big-O.

The Pareto frontier endpoint (core §15.2 `/api/v1/db/cost_estimate`)
is fully driven by the profiler — every (parameter, accuracy, cost)
point on the frontier is a real measurement, and the recommended
parameter set is the one that minimises operator-weighted cost
under the user's accuracy SLA.

### 20.5 How the sketch DB uses it

- **Admission control quotas** (roadmap §11.1) are sized by reading
  the profiler's `memory_bytes` and `insert_throughput_per_core` and
  multiplying by the deployment's available headroom.
- **Compaction policy** (roadmap §9) uses `cpu_per_merge_ns` to decide
  how many entries can be compacted per tick within the configured CPU
  budget.
- **Backfill scheduling** (core §10.3) uses the profiler's insert/merge
  throughput to estimate job duration before launching.

### 20.6 Cross-repo placement and ownership

The profiler is **shared infrastructure** because it has no value
unless it covers all sketch implementations the system uses:

- **`sketch-profiler/` crate** — workspace member of ASAPQuery-backend.
  Owns the catalogue format, the calibration / drift / what-if
  drivers, the catalogue serializer.
- **sketchlib-rust** — exposes a `Bench` trait or similar so the
  profiler can call `update / merge / estimate` uniformly across
  sketch types.
- **sketchlib-go** — same story for Go-side measurements (matters
  for agent-side sketching where the Go implementation runs).
- **DataCollector controller** — reads the published catalogue and
  feeds it into the planner; does not run measurements itself.

A published catalogue (e.g. JSON in the sketchlib release artifacts)
is the contract between sketchlib releases and the controller. When
sketchlib bumps a version, the catalogue updates, and the controller
picks up new cost numbers without code changes.

### 20.7 Why this is a separate library, not part of the sketch DB

Three reasons:

1. **Scope**: the profiler measures sketches in isolation, not in the
   context of the storage engine. It belongs alongside sketchlib,
   not the sketch DB.
2. **Reuse**: the controller, the operator's parameter-tuning UI,
   the sketchlib CI all need it; only one of those is the sketch DB.
3. **Cadence**: the catalogue updates on sketchlib releases (low
   frequency); the sketch DB ships independently. Different release
   cadences imply different repos / different versioning.

### 20.8 Status

This library does not exist yet. It is called out here because:

- core §6.4 `AccuracyProfile.merge_propagation` and core §15
  `Provenance` need numbers that are most credibly produced by the
  profiler, not hand-derived;
- roadmap §11.1 quotas, core §15.2 cost-estimate, and roadmap §16
  implementation phases all reference "what the profiler will
  provide";
- treating it as a separate concern with its own design surface
  prevents this doc from sprawling further into measurement
  infrastructure that doesn't belong here.

A separate design doc (`design-sketch-profiler.md`) will follow.

---

## 21. Related approaches: wavelets and ML models as materialized views

The MV framing in core §2 treats sketches as "precomputed,
incrementally-maintainable summaries of a base relation." That
description is broader than sketches — wavelets and (some) ML models
fit it too. This section positions the sketch DB design against those
neighbours so future extensions can reason about which of them slot in
cleanly and which require contract changes.

The framing holds whenever five properties are present:

1. A **base relation** the summary is derived from.
2. **Deterministic derivation** (given parameters).
3. Either **incremental** or **refresh** maintenance semantics.
4. **Queries answerable without rescanning the base.**
5. A **known accuracy contract** (how wrong the answer can be).

Sketches hit all five. Wavelets and ML models hit some but not all —
the pattern of misses determines what it would take to treat them as
first-class citizens of the sketch DB.

### 21.1 Side-by-side comparison

| Dimension | Sketches (CMS / HLL / KLL / DDSketch) | Wavelets (DWT / Haar + thresholding) | ML models |
|---|---|---|---|
| Base relation | Raw sample stream | Signal / time series | Training set |
| Mergeable (monoid) | **Yes, by design** — associative + commutative merge is a defining property | **Partially** — Haar on aligned dyadic intervals merges cleanly; general DWT does not | **Rarely** — only linear / moment-based things (online PCA via covariance sums, linear regression normal equations, naive Bayes w/ conjugate priors). Neural nets are not monoids: training on A then B ≠ B then A |
| Incremental update cost | O(1) per sample | Amortized O(log n) for online / sliding DWT | Variable; SGD continuations risk catastrophic forgetting |
| Refresh cost | Cheap (replay stream) | Moderate (one DWT pass) | **Huge** — full retraining is why RAG exists as a workaround |
| Accuracy bound | **Provable, closed-form** from parameters (ε, δ, K, α, m) | Provable — L2 error bounded by discarded coefficient energy (Parseval) | **Empirical**, data-dependent; PAC / conformal bounds exist but are much weaker and narrower |
| Query classes answered | Fixed at design: count, sum, quantile, top-k, cardinality | Fixed: range sums, heavy hitters, point queries, wavelet-domain features | **Open-ended** — whatever the training objective was |
| View-definition formalism | An aggregation function + parameters | A basis transform + threshold | Training objective + architecture + hyperparameters + seed |

### 21.2 Wavelets — a sibling sketch family

Wavelets are essentially an alternative sketch family. The classical
AQP line of work
(Garofalakis, Gibbons, Matias, Vitter — "Approximate Query Processing
via Wavelets," VLDB 1998 onward) treats them as a direct alternative
to randomized sketches for range-sum and heavy-hitter workloads.

Compared to CMS / KLL:

- **Strength**: wavelets exploit signal structure. On smooth or
  low-entropy signals (diurnal telemetry, time-of-day patterns,
  histograms that concentrate on a few modes) thresholded wavelets
  produce dramatically smaller representations than sketches of
  comparable accuracy.
- **Weakness**: on high-entropy / uniform data their advantage
  disappears, because the thresholded coefficient set doesn't shrink.
- **Operational fit**: Haar wavelets on aligned dyadic intervals merge
  cleanly, which means a Haar-based agg could use the same
  `(agg_id, group_key, window)` storage as a CMS agg with only
  modest changes to the merge trait. Non-Haar wavelets would require
  either strict window alignment or a refresh-only maintenance
  strategy.

**Takeaway**: if a future `SketchType::Wavelet` were added, the
existing sketch-DB contracts (schema timeline, backfill, accuracy
profile, tier storage) generalize without structural change. The
`AccuracyProfile::ErrorBound` enum already has room for an
L2-energy-based variant.

### 21.3 ML models — MVs with weaker contracts

ML models fit the MV framing in the loose sense: they are precomputed,
queryable, compressed summaries of a base relation. But the sketch
DB's **four operational contracts** weaken or vanish:

- **Mergeability** (core §7.3 cross-segment combine) — lost for
  non-linear models. Only linear or moment-based models (running
  PCA, linear regression normal equations, conjugate Bayes,
  streaming k-means coresets) retain a monoid structure and can
  meaningfully merge across segments or time ranges.
- **Closed-form accuracy bound** (core §6.4) — lost. Replaced by
  empirical validation + sometimes conformal prediction bands. The
  `AccuracyProfile` contract would need a new `Empirical` variant
  that carries calibration data rather than a parameterized formula.
- **Deterministic rebuild** (core §10.5) — weak. Training is
  conditionally deterministic (seed + hyperparams + batch order +
  hardware), but reproducing bit-identical outputs across hardware is
  a well-known open problem in ML engineering.
- **Cheap refresh** — gone. Refresh cost is the dominant operational
  concern for large models; the two-tier maintenance strategy
  (incremental + refresh) that makes sketch-DB reconfigure painless
  does not translate — continual training and fine-tuning are poor
  substitutes for sketch refresh.

The interesting **sub-class** that does fit is linear / additive
models:

- **Online PCA / streaming covariance** — monoid via covariance sum;
  bounded error via eigenvalue bounds. Functionally a sketch.
- **Linear regression (normal equations form)** — `XᵀX` and `Xᵀy`
  accumulators merge by summation; bounds follow from standard linear
  algebra. Functionally a sketch.
- **Coreset-based clustering** (BIRCH, k-means coresets) — mergeable
  by construction; accuracy is a multiplicative factor on the optimal
  clustering cost.
- **Naive Bayes with conjugate priors** — parameter updates are
  additive in sufficient statistics.

Each of these could be added to the sketch DB as a `SketchType`
variant without changing the core contract. They would share schema
timeline, backfill, accuracy profile, and tier storage with the
existing sketches.

Neural / tree-ensemble / LLM models would require a parallel design
with weaker contracts: no merge, refresh-only maintenance,
empirical-only accuracy. That's closer to a **model registry** than a
sketch DB, and the open research literature
(Kraska et al. — SageDB; Hilprecht et al. — DeepDB; Yang et al. —
NeuroCard; DBEst / DBEst++) explores exactly that split. A pragmatic
integration path would be: host model artifacts beside sketches under
the same `agg_id` lifecycle and HTTP surface, but use a separate
storage engine internally — the `SketchDb` facade
(see [`design-sketch-db-pluggable.md`](./design-sketch-db-pluggable.md))
makes this kind of backend swap mechanical.

### 21.4 Practical implication for the sketch DB design

The sketch DB's architecture — `agg_id` lifecycle, schema timeline,
backfill-from-base, accuracy-as-metadata — generalizes without change
to:

1. **Sketches** (today).
2. **Wavelets** (mostly; Haar is trivial, general DWT needs window
   alignment).
3. **Linear-ish ML summaries** (PCA, linear regression, coresets,
   conjugate-prior Bayes).

It does **not** generalize cleanly to neural / tree-ensemble models
without relaxing the mergeability and closed-form-bound contracts.
Keeping those relaxations out of the core, and introducing them in a
companion "model view" subsystem if the need arises, preserves the
properties that make the sketch DB's behavior predictable.

