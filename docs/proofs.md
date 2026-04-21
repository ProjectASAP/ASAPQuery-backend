# `proofs.md` — Accuracy bounds and correctness theorems

Paper-seed document for the ASAPQuery-backend sketch DB. This file
collects the formal statements that the VLDB / SIGMOD theory chapter
references directly. The intent is reviewer-facing precision: every
claim here maps to a specific module in the codebase, and every
numeric constant comes from a cited primary source.

Conventions: $\varepsilon$ denotes additive / relative error,
$\delta$ the failure probability, $N$ the stream length (total
multiplicity of updates seen by a given sketch), $w$ and $d$ the
sketch width and depth, and $m = 2^p$ for HyperLogLog precision $p$.

---

## 1. Accuracy bounds per sketch family

The following table is reproduced verbatim from the module
doc-comment of `asap-query-engine/src/stores/sketch_db/accuracy.rs`
(§6.4 of the sketch DB design). It lists every sketch family
currently materialisable by `AccuracyProfile::derive`.

| Sketch                              | `kind`                | $\varepsilon$ formula        | $\delta$ formula             |
|-------------------------------------|-----------------------|------------------------------|------------------------------|
| Sum / Min / Max / Increase          | `Exact`               | $0$                          | $0$                          |
| CountMinSketch $(w, d)$             | `AdditiveFrequency`   | $e / w$                      | $1 / 2^d$                    |
| CountMinSketchWithHeap $(w, d, k)$  | `TopK`                | $\max(e/w,\, 1/k)$           | $1 / 2^d$                    |
| CountSketch $(w, d)$                | `AdditiveFrequency`   | $1 / \sqrt{w}$               | $1 / 2^d$                    |
| HLL $(p)$                           | `RelativeCardinality` | $1.04 / \sqrt{2^p}$          | — (Gaussian std-dev)         |
| KLL $(k)$                           | `RankQuantile`        | $\approx 2.296 / \sqrt{k}$   | $1/100$ (fixed)              |
| DDSketch $(\alpha)$                 | `RelativeQuantile`    | $\alpha$                     | $0$ (deterministic)          |

The `kind` column drives user-facing rendering (`±ε counts` vs
`±ε relative` vs `rank error ±ε·N` etc.); it does not alter the
numerical $\varepsilon$.

### 1.1 Citations

One paragraph per family, using only the sources cited inline in
`accuracy.rs`.

> **Sum / Min / Max / Increase.** Exact aggregates; no sketch is
> materialised, so $\varepsilon = \delta = 0$. See the `Exact`
> branches of `AccuracyProfile::derive` and the `SetAggregator` /
> `DeltaSetAggregator` reductions, which also return `exact()`.

> **Count-Min Sketch.** $\varepsilon = e/w$, $\delta = 1/2^d$.
> Cormode & Muthukrishnan, "An improved data stream summary: the
> count-min sketch and its applications," *J. Algorithms* 55(1),
> 2005. The factor $e$ (rather than the looser $2$) is the tighter
> published bound; we use `std::f64::consts::E` at source.

> **Count-Min Sketch with heap.** $\varepsilon = \max(e/w,\, 1/k)$,
> $\delta = 1/2^d$. The first term inherits the CMS point-lookup
> bound; the second is the SpaceSaving-style top-$k$ retention /
> count guarantee. Sources: Cormode & Muthukrishnan 2005 (CMS part);
> Metwally, Agrawal, El Abbadi, "Efficient computation of frequent
> and top-$k$ elements in data streams," *ICDT* 2005 (top-$k$
> retention).

> **Count Sketch.** $\varepsilon = 1/\sqrt{w}$, $\delta = 1/2^d$.
> Charikar, Chen, Farach-Colton, "Finding frequent items in data
> streams" (the CCFC sketch). Signed counters give a tighter
> $\varepsilon$ than CMS at the same width, with the same confidence
> ramp in depth.

> **HyperLogLog.** $\varepsilon = 1.04 / \sqrt{m}$ with $m = 2^p$,
> where $\varepsilon$ is reported as the Gaussian standard error.
> Flajolet, Fusy, Gandouet, Meunier, "HyperLogLog: the analysis of
> a near-optimal cardinality estimation algorithm," *DMTCS* 2007.
> We store $\delta = 0$ because our $\delta$ field carries a
> confidence-parameter convention rather than variance; the
> `RelativeCardinality` kind signals the Gaussian flavour.

> **KLL.** $\varepsilon \approx 2.296 / \sqrt{k}$ with fixed
> $\delta \le 0.01$. Karnin, Lang, Liberty, "Optimal quantile
> approximation in streams," *FOCS* 2016. The constant $2.296$
> matches the empirical envelope for the floating-point KLL variant
> vendored here; applies identically to `DatasketchesKLL` and
> `HydraKLL`.

> **DDSketch.** $\varepsilon = \alpha$, $\delta = 0$.
> Masson, Rim, Lee, "DDSketch: a fast and fully-mergeable quantile
> sketch with relative-error guarantees," *VLDB* 2019. $\alpha$ is
> a design parameter of the bucketisation, not a probabilistic
> bound, so the guarantee is deterministic.

---

## 2. `combine_statistic` correctness across schema-timeline segments

When a query's time range crosses one or more reconfigure boundaries,
`SchemaRegistry::timeline_for_metric` partitions the range into
contiguous half-open segments $[t_i, t_{i+1})$, each owned by a
single `AggSchema` with its own sketch parameters. The query engine
evaluates the statistic per segment and passes the per-segment
scalars to `combine_statistic`
(`asap-query-engine/src/engines/timeline_dispatch.rs`).

### 2.1 Theorem (additive combinability)

Let $\sigma \in \{\text{Count}, \text{Sum}, \text{Min}, \text{Max}\}$
be an additive statistic and let segments $[t_0, t_1), \dots,
[t_{n-1}, t_n)$ be pairwise disjoint and covering. Let $a_i$ be
the per-segment answer with error $|a_i - \sigma_i| \le
\varepsilon_i \cdot N_i$ (where $N_i$ is the segment's stream
length), and let $\hat a = \mathrm{combine}_\sigma(\{a_i\})$ be the
output of `combine_statistic`. Then

$$\big|\hat a - \sigma(\cup_i [t_i, t_{i+1}))\big| \;\le\; \sum_{i=0}^{n-1} \varepsilon_i \cdot N_i.$$

For $\sigma = \text{Min}$ / $\text{Max}$ the same bound holds with
$\max_i \varepsilon_i \cdot N_i$ in place of the sum.

### 2.2 Proof sketch

For $\text{Count}$ / $\text{Sum}$, $\sigma$ is linear over the
disjoint partition, so $\sigma(\cup_i S_i) = \sum_i \sigma_i(S_i)$.
The combiner computes $\hat a = \sum_i a_i$ (see the `Count | Sum`
arm of `combine_statistic`); the error bound follows from the
triangle inequality. For $\text{Min}$ / $\text{Max}$, $\sigma$ is
idempotent and associative over the partition, and the combiner
folds `a.min(v)` / `a.max(v)` pointwise; the global error is at
most the worst single-segment error. For $\sigma \in
\{\text{Cardinality}, \text{Increase}, \text{Rate}, \text{Quantile},
\text{Topk}\}$ the scalar-level combination is unsound (HLL sketches
across parameters cannot be OR-merged at the scalar, rate /
increase depend on endpoint samples, quantile / top-$k$ require
merging the underlying sketches), so the combiner conservatively
returns `CombinedResult::Partial { covered, missing }` with
`covered` set to the best-effort aggregate and `missing` listing
the unresolved segments. See the module doc-comment for the
combinability table.

### 2.3 Empty range

An empty `segments` slice yields `Full(0.0)` for $\text{Count}$ /
$\text{Sum}$ (zero is the identity) and
`Partial { covered: None, missing: [] }` for every other
statistic; there is no meaningful identity for
$\text{Min}$ / $\text{Max}$ / $\text{Quantile}$ over an empty
multiset.

---

## 3. Write-barrier safety

The §6.3 write barrier guarantees that no late or replayed sample
can leak into a query once its schema has been forced to expire.

### 3.1 Theorem

Let `force_expire(agg_id)` be invoked at wall-clock time $t^\ast$.
Then for every ingest attempt at wall-clock time $t > t^\ast$
targeting `agg_id`, and for every query whose range includes any
$t' \ge t^\ast$, the sample ingested at $t$ does not appear in the
query's result.

### 3.2 Proof sketch

The ingest path calls `SchemaRegistry::is_writable(agg_id)`
(`asap-query-engine/src/stores/sketch_db/schema.rs:is_writable`),
which returns `true` iff `status() == AggStatus::Active`. The
status function is a pure function of `(retired_at_ms,
expires_at_ms, now)`: once `force_expire` sets both to the current
wall clock, `status()` transitions monotonically through
`Retired → Expired` and never returns `Active` again (there is no
code path that clears `retired_at_ms`; `retire` is idempotent and
`force_expire` only advances the clock). The ingest handler
therefore rejects the write and bumps the counter
`queryengine_ingest_samples_blocked_by_schema_barrier_total`
(defined in `asap-query-engine/src/stores/sketch_db/metrics.rs`).
Because the query path dispatches via the same `SchemaRegistry`
(`timeline_for_metric` constructs `TimelineSegment`s from the same
`AggSchema` the writer consulted), any write refused at ingest is
invisible at query time — the store simply holds no record of it.

---

## 4. Backfill determinism

### 4.1 Theorem

Under the §10.5 invariants enforced by
`BackfillRegistry::create_checked`
(`asap-query-engine/src/stores/sketch_db/backfill.rs`) —

1. **Known agg:** `agg_id` exists in the `SchemaRegistry`;
2. **Time-disjoint:** `time_range.1 <= schema.created_at_ms`, so
   backfill writes and live writes never overlap in time on the
   same `agg_id`;
3. **Within retention:** `time_range.0 >= now - data_retention_ms`
   (when retention is enabled), so written windows survive the
   retention sweep;

an ordered raw-sample replay that constructs a fresh sketch for
`agg_id` produces bit-identical sketch bytes to what live ingest
would have produced had it observed the same samples in the same
order.

### 4.2 Proof sketch

A sketch is a pure function of (a) its seed and construction
parameters pinned on the `AggSchema` (never mutated after
creation — see the `AggSchema::config` doc-comment), and (b) the
ordered sequence of update calls. Invariant 1 guarantees the
backfill worker loads the same `AggregationConfig` the live writer
used. Invariant 2 guarantees that the backfill's window is
strictly inside `[0, created_at_ms)` while live ingest owns
`[created_at_ms, ∞)`, so no sample is updated twice and no ordering
ambiguity arises at the boundary. Invariant 3 guarantees that the
rebuilt sketch is not racing the retention sweep. The sketch
families listed in §1 have no time-dependent randomness (hashes
are seeded from sketch parameters, not wall clock), so replay in
the original timestamp order yields identical internal state, and
therefore identical serialised bytes. The relevant code path is
`BackfillJob` construction via `create_checked` plus the
deterministic-rebuild contract documented at the top of
`backfill.rs` (§10.5 of the design doc).

---

## 5. Cross-reference

Each theorem anchors to a single module; this section exists so
the theory chapter can cite both at once.

- **Accuracy bounds (§1)** →
  `asap-query-engine/src/stores/sketch_db/accuracy.rs`
  (`AccuracyProfile::derive`; inline citations in each match arm).
- **`combine_statistic` correctness (§2)** →
  `asap-query-engine/src/engines/timeline_dispatch.rs`
  (`CombinedResult`, `combine_statistic`; combinability table in
  the module doc-comment).
- **Write-barrier safety (§3)** →
  `asap-query-engine/src/stores/sketch_db/schema.rs:is_writable`
  (plus `AggSchema::status`, `AggSchema::retire`, and
  `SchemaRegistry::force_expire`); barrier counter
  `queryengine_ingest_samples_blocked_by_schema_barrier_total`
  in `asap-query-engine/src/stores/sketch_db/metrics.rs`.
- **Backfill determinism (§4)** →
  `asap-query-engine/src/stores/sketch_db/backfill.rs`
  (`BackfillRegistry::create_checked` enforces the three §10.5
  invariants; `CreateError::{UnknownAgg, Overlap, OutOfRetention}`
  surface violations at job-creation time).
