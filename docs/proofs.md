# `proofs.md` — Correctness theorems

Paper-seed document for the ASAPQuery-backend sketch DB. The §theory
chapter of the VLDB / SIGMOD paper draws directly from the three
proofs in §§2–4. This file is reviewer-facing: every claim is bound
to a runtime function in `asap-query-engine/src/`, and every
invariant the proof rests on is anchored to where the code enforces
it.

§1 collects the per-sketch-family accuracy bounds the proofs cite.
The numerical $\varepsilon$ / $\delta$ formulas come from
`asap-query-engine/src/stores/sketch_db/accuracy.rs`
(`AccuracyProfile::derive`); the proofs treat the bounds as black
boxes (per-segment input).

Conventions: $\varepsilon$ denotes additive / relative error,
$\delta$ the failure probability, $N$ the stream length (total
multiplicity of updates seen by a given sketch), $w$ and $d$ the
sketch width and depth, and $m = 2^p$ for HyperLogLog precision $p$.

Cross-links to design docs:

- §1 / §2 reference the schema timeline of
  [`design-sketch-db.md`](./design-sketch-db.md) §7 and the
  combinability table of `query-engines/timeline_dispatch.rs`.
- §3 references the §6.3 write barrier of
  [`design-sketch-db-core.md`](./design-sketch-db-core.md).
- §4 references the §10.5 deterministic-rebuild contract of
  [`design-sketch-db-core.md`](./design-sketch-db-core.md).

Code anchors below cite **symbols, not line numbers** (line numbers
drift; symbols can be relocated by `grep -n 'fn <name>'`). Each
citation gives the file path + the function or type name as it
appears in the source today.

---

## 1. Accuracy bounds per sketch family

The following table is reproduced from
`asap-query-engine/src/stores/sketch_db/accuracy.rs` (§6.4 of the
sketch DB design). It lists every sketch family currently
materialisable by `AccuracyProfile::derive`.

| Sketch                              | `kind`                | $\varepsilon$ formula        | $\delta$ formula             |
|-------------------------------------|-----------------------|------------------------------|------------------------------|
| Sum / Min / Max / Increase          | `Exact`               | $0$                          | $0$                          |
| CountMinSketch $(w, d)$             | `AdditiveFrequency`   | $e / w$                      | $1 / 2^d$                    |
| CountMinSketchWithHeap $(w, d, k)$  | `TopK`                | $\max(e/w,\, 1/k)$           | $1 / 2^d$                    |
| CountSketch $(w, d)$                | `AdditiveFrequency`   | $1 / \sqrt{w}$               | $1 / 2^d$                    |
| HLL $(p)$                           | `RelativeCardinality` | $1.04 / \sqrt{2^p}$          | — (Gaussian std-dev)         |
| KLL $(k)$                           | `RankQuantile`        | $\approx 2.296 / \sqrt{k}$   | $1/100$ (fixed)              |
| DDSketch $(\alpha)$                 | `RelativeQuantile`    | $\alpha$                     | $0$ (deterministic)          |

The `kind` column drives user-facing rendering; it does not alter
the numerical $\varepsilon$. Sources are cited inline in
`accuracy.rs` (Cormode & Muthukrishnan 2005 for CMS; Charikar–Chen–
Farach-Colton for CountSketch; Flajolet et al. 2007 for HLL; Karnin–
Lang–Liberty 2016 for KLL; Masson–Rim–Lee 2019 for DDSketch).

The proofs in §§2–4 take these per-segment bounds as inputs; they
do not re-derive them.

---

## 2. `combine_statistic` correctness across schema-timeline segments

When a query's time range crosses one or more reconfigure
boundaries, `SchemaRegistry::timeline_for_metric` partitions the
range into contiguous half-open segments
$[t_0, t_1), [t_1, t_2), \dots, [t_{n-1}, t_n)$, each owned by a
single `AggSchema` with its own sketch parameters. The query
engine evaluates the statistic per segment and feeds the per-
segment scalars to `combine_statistic`
(`asap-query-engine/src/query-engines/timeline_dispatch.rs`).

### 2.1 Statement

Let $\sigma$ be a statistic, let $S = \cup_{i=0}^{n-1} [t_i, t_{i+1})$
be the query range partitioned into pairwise disjoint covering
segments by `timeline_for_metric`, and let $a_i$ be the per-segment
estimate for $\sigma$ on segment $i$, with per-segment error bound

$$
\big| a_i - \sigma_i(S_i) \big| \;\le\; B_i,
$$

where $B_i = \varepsilon_i \cdot N_i$ for the
`AdditiveFrequency`/`Relative` sketch families of §1 and $B_i = 0$
for the `Exact` family. Let
$\hat a = \mathrm{combine\_statistic}(\sigma, \{a_i\}, \emptyset)$.

**(a) Additive case.** If
$\sigma \in \{\mathrm{Count}, \mathrm{Sum}\}$, then
`combine_statistic` returns `Full(`$\hat a$`)` and

$$
\big|\hat a - \sigma(S)\big| \;\le\; \sum_{i=0}^{n-1} B_i.
$$

**(b) Idempotent case.** If
$\sigma \in \{\mathrm{Min}, \mathrm{Max}\}$, then
`combine_statistic` returns `Full(`$\hat a$`)` and

$$
\big|\hat a - \sigma(S)\big| \;\le\; \max_{0 \le i < n} B_i.
$$

**(c) Non-combinable case.** If
$\sigma \in \{\mathrm{Cardinality}, \mathrm{Quantile},
\mathrm{Topk}, \mathrm{Rate}, \mathrm{Increase}\}$,
`combine_statistic` returns
`Partial { covered, missing }`, where `missing` is the unresolved-
segment list passed in by the caller (possibly empty) and `covered`
is `None` (no scalar combiner is sound). Soundness is preserved by
construction: `Partial` is treated by the caller as "do not surface
as a single answer."

### 2.2 Setup / Lemmas

**L2.1 — Disjoint covering by `timeline_for_metric`.** For any
metric $m$ and query range $[t_1, t_2]$, the segments returned by
`SchemaRegistry::timeline_for_metric(m, t1, t2)`
(`asap-query-engine/src/stores/sketch_db/schema.rs:timeline_for_metric`)
are pairwise disjoint, sorted by `start_ms`, and each is owned by
exactly one `agg_id`. The owner's range is
$[\mathtt{created\_at\_ms},\,\mathtt{own\_end})$ with `own_end`
defined as `min(next.created_at_ms, retired_at_ms)` (open segment
$\to u64::\mathrm{MAX}$ for the currently-Active schema). The
function clips each segment to the query range and skips segments
of zero length, so the returned partition is a refinement of the
query range with no overlaps. (Note: gaps may exist where no
schema was Active; the caller folds those into the `unresolved`
input to `combine_statistic`.)

**L2.2 — Linearity of additive aggregates over disjoint sets.** For
$\sigma \in \{\mathrm{Count}, \mathrm{Sum}\}$,
$\sigma(\cup_i S_i) = \sum_i \sigma(S_i)$ when the $S_i$ are
pairwise disjoint. This is set-theoretic, not sketch-specific.

**L2.3 — Idempotence + associativity of `min` / `max` over
multisets.** For $\sigma \in \{\mathrm{Min}, \mathrm{Max}\}$,
$\sigma(\cup_i S_i) = \sigma(\{\sigma(S_i) : 0 \le i < n\})$ when
the $S_i$ are pairwise disjoint and at least one is non-empty.
Idempotence + associativity together justify the pointwise fold.

**L2.4 — Triangle inequality on real numbers.** Standard.

**L2.5 — Combiner implementation.** `combine_statistic`
(`asap-query-engine/src/query-engines/timeline_dispatch.rs:combine_statistic`)
folds segments via `fold(0.0, +)` for `Count` / `Sum`,
`fold(None, |a,v| Some(a.map_or(v, |a| a.min(v))))` for `Min` (mut.
mut. for `Max`), and returns `None` (so a `Partial` wrapper) for
`Cardinality / Quantile / Topk / Rate / Increase`. Empty input
short-circuits to `Full(0.0)` for additive statistics and
`Partial { covered: None, missing: [] }` otherwise.

**L2.6 — Per-segment error bound.** For each segment $i$ the
sketch family's published bound (Cormode–Muthukrishnan; Karnin–
Lang–Liberty; etc., as reproduced in §1) gives
$|a_i - \sigma_i(S_i)| \le B_i$. The combiner takes $a_i$ as a
black-box scalar; it does not reach into the sketch.

### 2.3 Proof

We treat the three cases.

**Case (a): additive ($\sigma \in \{\mathrm{Count}, \mathrm{Sum}\}$).**
The combiner returns $\hat a = \sum_i a_i$ (L2.5). By L2.2,
$\sigma(S) = \sum_i \sigma_i(S_i)$. Therefore

$$
|\hat a - \sigma(S)| = \Big| \sum_i a_i - \sum_i \sigma_i(S_i) \Big|
  = \Big| \sum_i (a_i - \sigma_i(S_i)) \Big|
  \overset{L2.4}{\le} \sum_i |a_i - \sigma_i(S_i)|
  \overset{L2.6}{\le} \sum_i B_i.
$$

The combiner's output is `Full(`$\hat a$`)` because the input
unresolved-list is empty (L2.5).

**Case (b): idempotent ($\sigma \in \{\mathrm{Min}, \mathrm{Max}\}$).**
WLOG $\sigma = \mathrm{Min}$. The combiner returns
$\hat a = \min_i a_i$ (L2.5). By L2.3,
$\sigma(S) = \min_i \sigma_i(S_i)$. Pick any $i^\ast$ achieving the
combiner's argmin, and any $j^\ast$ achieving the true argmin.
Then

$$
\hat a - \sigma(S)
  = a_{i^\ast} - \sigma_{j^\ast}(S_{j^\ast})
  \le a_{j^\ast} - \sigma_{j^\ast}(S_{j^\ast})  \le B_{j^\ast}
  \le \max_i B_i,
$$

where the first inequality is by minimality of $a_{i^\ast}$, the
second is L2.6 on segment $j^\ast$, and the third is trivial.
Symmetrically,

$$
\sigma(S) - \hat a
  = \sigma_{j^\ast}(S_{j^\ast}) - a_{i^\ast}
  \le \sigma_{i^\ast}(S_{i^\ast}) - a_{i^\ast}
  \le B_{i^\ast}
  \le \max_i B_i,
$$

so $|\hat a - \sigma(S)| \le \max_i B_i$.

**Case (c): non-combinable.** The combiner unconditionally returns
`Partial { covered: None, missing: unresolved }` (L2.5,
non-combinable arm of the `match`). The contract is that `Partial`
is **not** a single-schema answer; the caller (the engine) routes
non-combinable cases to fallback per
`ASAPQueryEngine::try_handle_query_promql_via_timeline`. There is
nothing further to prove.

### 2.4 Caveats

- **Segment coverage of underlying samples.** The proof assumes
  each segment's sketch contains every sample the agg actually
  ingested for that range. Sketch parameter changes mid-segment
  cannot occur (an agg's `AggregationConfig` is pinned at creation;
  `AggSchema::config` is immutable), but a reconfigure that
  retires one agg and creates another may leave a brief gap if no
  agg is Active at some $t \in [t_i, t_{i+1})$. Such gaps are
  surfaced as `unresolved` segments by the caller and are not
  silently elided.
- **`Cardinality` is non-combinable at the scalar level only.**
  HLL register OR-merge would give a sound combine, but the inputs
  to `combine_statistic` are post-`query_statistic` scalars. A
  future "merge-then-evaluate" path would need a different proof.
- **`Increase` / `Rate` use endpoint samples.** Stitching two
  per-segment rates at a boundary does not give the cross-boundary
  rate even when the segments are time-disjoint. The combiner
  conservatively returns `Partial`; the caller may delegate to a
  fallback (raw-sample re-evaluation against the cold store).
- **Numerical floating-point drift.** Summing $n$ floats accrues
  $O(n \cdot \mathrm{ulp})$ rounding error. Pairwise / Kahan
  summation would tighten the constant; today's combiner uses naive
  accumulation. This is below sketch error for the sketches in §1
  but worth noting for an exact `Sum` over $\sim 10^9$ segments.
- **Concurrent reconfigure during a query.** The proof assumes
  `timeline_for_metric` returns a consistent snapshot. The function
  acquires a single `RwLock` read before partitioning, so a
  concurrent `reconcile` either hasn't started or is fully visible —
  no torn read. A reconfigure that retires an agg between segment
  evaluations is allowed by the design (the engine fetches each
  segment's sketch under the same registry view).
- **Empty range.** An empty `segments` slice (no schema covers the
  query range) yields `Full(0.0)` for `Count` / `Sum` and
  `Partial { covered: None, missing: [] }` otherwise. The empty
  case is meaningful only for additive statistics; the proof above
  presupposes $n \ge 1$ for case (b).

### 2.5 Code anchors

- `asap-query-engine/src/query-engines/timeline_dispatch.rs`
  - `combine_statistic` — the per-statistic fold (additive arm,
    `Min` / `Max` arm, non-combinable arm).
  - `CombinedResult::{Full, Partial}` — the `Full` / `Partial`
    discrimination relied on in §2.3.
  - Module doc-comment — combinability table.
- `asap-query-engine/src/stores/sketch_db/schema.rs`
  - `SchemaRegistry::timeline_for_metric` — disjoint covering
    (L2.1).
  - `AggSchema::config` — pinned-at-creation invariant (caveat 1).
- `asap-query-engine/src/query-engines/asap_query/engine.rs`
  - `ASAPQueryEngine::try_handle_query_promql_via_timeline` — caller
    that wires per-segment evaluation into `combine_statistic`.
- `asap-query-engine/src/stores/sketch_db/accuracy.rs`
  - `AccuracyProfile::derive` — source of $B_i$ (L2.6).

---

## 3. Write-barrier safety

The §6.3 write barrier (`design-sketch-db-core.md` §6.3) guarantees
that no late or replayed sample can leak into a query once its
schema has been forced to expire. This proof formalises that.

### 3.1 Statement

Let $a$ be an `agg_id`, let $t^\ast$ be the wall-clock time at which
`SchemaRegistry::force_expire(a)` is invoked and returns
`Some(_)`, and let $s$ be any sample submitted to the ingest path
targeting $a$ at wall-clock time $t > t^\ast$. Then for every
query $Q$ executed at any wall-clock time $t_Q \ge t^\ast$ whose
range $[t_1, t_2]$ includes any $t' \ge t^\ast$, the sample $s$
does not appear in $Q$'s result.

### 3.2 Setup / Lemmas

**L3.1 — `force_expire` is monotonic.** When
`force_expire(a)` returns `Some(_)` at wall-clock time $t^\ast$,
the schema's `retired_at_ms` and `expires_at_ms` are both set to
the value of `now_ms()` captured atomically inside the function
body, under the registry's `RwLock` write guard
(`asap-query-engine/src/stores/sketch_db/schema.rs:force_expire`).

**L3.2 — Status is a pure function of timestamps.**
`AggSchema::status` (`schema.rs:status`) is a pure function of
`(retired_at_ms, expires_at_ms, now_ms())`. With both fields set
to $t^\ast$, the match arm
`(Some(_), Some(exp)) if now >= exp` fires for every subsequent
`now >= t^\ast`, returning `AggStatus::Expired`. The arm returning
`AggStatus::Active` requires `retired_at_ms.is_none()`, which is
unreachable after L3.1 (no code path clears `retired_at_ms`;
`AggSchema::retire` is idempotent, `force_expire` only advances).

**L3.3 — Barrier is in the ingest hot path.** Every sample that
reaches the ingest router passes through
`route_decoded_samples` in
`asap-query-engine/src/precompute_engine/ingest_handler.rs`, which
unconditionally calls `state.schemas.is_writable(config.aggregation_id)`
(see the loop body around the `!state.schemas.is_writable(…)`
guard) before adding the sample to its `by_group` map. Samples
that fail this check are routed to the
`samples_blocked_by_schema_barrier` counter and are not entered
into `by_group`, so they are not forwarded to any worker.

**L3.4 — `is_writable` rejects non-Active.**
`SchemaRegistry::is_writable(agg_id)`
(`schema.rs:is_writable` on the registry, delegating to
`AggSchema::is_writable` on the schema) returns `true` iff
`status() == AggStatus::Active`, by direct match
(`matches!(self.status(), AggStatus::Active)`).

**L3.5 — Workers only see what the router forwards.** The
`WorkerMessage::GroupSamples` payload carries only the samples
that survived the barrier check (`route_decoded_samples` constructs
the payload from `by_group`; samples blocked at L3.3 are not in
`by_group`). Workers have no other ingest channel.

**L3.6 — Queries read only what workers wrote.** The store
(`SketchStore` and friends, `asap-query-engine/src/stores/`)
exposes no API that returns samples not previously written through
`insert_precomputed_output_batch`. Queries flow through
`ASAPQueryEngine::query_*`, which read the same store the workers
wrote into.

### 3.3 Proof

Direct, by chasing the sample's path.

1. By assumption, $s$ is submitted at $t > t^\ast$ targeting `a`.
2. By L3.1, at $t^\ast$ the registry stores
   `retired_at_ms = expires_at_ms = t^\ast` for `a`.
3. By L3.2, for every wall-clock query of $a$'s status performed
   at any time $\ge t^\ast$, `status() = Expired` $\ne$ `Active`.
4. By L3.3, $s$ enters the ingest router and the router calls
   `is_writable(a)` to decide whether to admit it.
5. At the moment of step 4, `now >= t > t^\ast`, so by L3.2 the
   `is_writable` call observes `Expired`. By L3.4 the call returns
   `false`.
6. By L3.3, $s$ is dropped (and the
   `samples_blocked_by_schema_barrier` counter is bumped); $s$ is
   not added to any `by_group` entry.
7. By L3.5, no worker ever sees $s$.
8. By L3.6, no query ever sees $s$.

In particular $Q$, which reads through the same store, returns a
result that does not contain $s$. $\blacksquare$

### 3.4 Caveats

- **Crash-window samples.** A sample $s$ that has been deserialised,
  routed through `route_decoded_samples`, **and** passed the
  `is_writable` check before $t^\ast$ but is still in flight to
  the worker at $t^\ast$ is admitted. The proof's $t$ is the time
  the barrier check happens, not the sample's wall-clock arrival.
  Operationally the gap is microseconds; bounding it formally
  would require a fence on the worker channel, which isn't
  implemented.
- **Clock skew.** `now_ms()` is the local monotonic-ish wall clock
  of the backend process. If the operator forces expiry from an
  external endpoint while the process clock is skewed backwards,
  some "post-$t^\ast$" samples may carry timestamps that look
  pre-$t^\ast$. The barrier checks `now`, not the sample's
  embedded `timestamp_ms`, so the safety argument holds against
  process-local time; it does not promise consistency with an
  external NTP source's view.
- **Persistence-on-restart.** If the process restarts between
  `force_expire` and the sample's arrival, the barrier holds iff
  the registry was persisted (L3.1's mutation reaches disk via
  `save_to_disk_if_persistent` inside `force_expire`). If
  persistence is disabled (`persist_path = None`), the registry is
  rebuilt from the current `StreamingConfig` only — an
  Expired-but-still-listed agg would re-emerge as Active. The
  documented production setting uses persistence; this proof
  assumes that.
- **Bypass paths.** The proof rests on every ingest entry point
  going through `route_decoded_samples`. If a future driver writes
  directly to the store, the barrier doesn't fire. Today the only
  ingest-side writers are the Prometheus remote-write handler, the
  OTLP gRPC handler, and the OTLP HTTP handler, all of which fan
  in through `route_decoded_samples`. Backfill writes are exempt
  by design (proof §4 covers them) and are blocked by §10.5
  time-disjointness from the barrier's domain.
- **Counter-only path.** A sample dropped by the barrier increments
  `queryengine_ingest_samples_blocked_by_schema_barrier_total`.
  The proof does not say anything about that counter's correctness
  — only that the sample doesn't reach the store.

### 3.5 Code anchors

- `asap-query-engine/src/stores/sketch_db/schema.rs`
  - `SchemaRegistry::force_expire` — sets `retired_at_ms` and
    `expires_at_ms` to `now_ms()` (L3.1).
  - `AggSchema::status` — pure function of timestamps (L3.2).
  - `AggSchema::is_writable` — `status() == Active` check (L3.4).
  - `SchemaRegistry::is_writable` — registry-level wrapper (L3.4).
  - `AggStatus` — the three-state enum.
  - `now_ms` (file-private helper).
- `asap-query-engine/src/precompute_engine/ingest_handler.rs`
  - `route_decoded_samples` — calls `is_writable` per sample,
    drops on `false`, bumps counter (L3.3).
  - `samples_blocked_by_schema_barrier` (atomic on
    `IngestState`).
- `asap-query-engine/src/stores/sketch_db/metrics.rs`
  - `SAMPLES_BLOCKED_BY_SCHEMA_BARRIER` — Prometheus counter
    (`queryengine_ingest_samples_blocked_by_schema_barrier_total`).
- Tests:
  `asap-query-engine/src/precompute_engine/ingest_handler.rs::tests::barrier_counter_increments_after_force_expire`
  exercises the path end-to-end.

---

## 4. Backfill determinism

§10.5 of the design doc claims that an offline backfill, fed the
same raw samples in the same order that live ingest would have
seen, builds a sketch byte-identical to the live one. This proof
formalises that claim under the §10.5 invariants enforced at job
creation.

### 4.1 Statement

Let `agg_id` $a$ be known to the schema registry with
`AggSchema` $\Sigma$, and let
`BackfillRegistry::create_checked(_, a, (s, e), _, _, retention)`
return `Ok(job_id)`. By construction (see L4.1 below), the job
satisfies:

1. **Known agg:** $\Sigma \ne \bot$;
2. **Time-disjoint:** $e \le \Sigma.\mathtt{created\_at\_ms}$;
3. **Within retention:** if `retention = Some(R)`, then
   $s \ge \mathrm{now}() - R$.

Let $X = (x_1, x_2, \dots, x_N)$ be the raw-sample sequence the
backfill processor reads in ingest order via `RawSampleReader` for
window $w \subseteq [s, e)$. Let $\sigma_B$ be the sketch produced
by `build_backfilled_accumulator(`$\Sigma$.config, $X$`)` and let
$\sigma_L$ be the sketch a hypothetical live-ingest worker would
have produced from the same $X$ in the same order using
`create_accumulator_updater(`$\Sigma$.config`)` followed by
`update_single` / `update_keyed` per sample. Then

$$
\sigma_B.\mathrm{serialize\_to\_bytes}() \;=\;
\sigma_L.\mathrm{serialize\_to\_bytes}().
$$

### 4.2 Setup / Lemmas

**L4.1 — Invariants enforced by `create_checked`.**
`BackfillRegistry::create_checked`
(`asap-query-engine/src/stores/sketch_db/backfill.rs:create_checked`)
returns `Ok` only after:

- `schemas.get(agg_id) = Some(_)` (else `CreateError::UnknownAgg`),
- `time_range.1 <= schema.created_at_ms` (else
  `CreateError::Overlap`),
- if `data_retention_ms = Some(R)`,
  `time_range.0 >= now_ms().saturating_sub(R)` (else
  `CreateError::OutOfRetention`).

These are the three §10.5 invariants verbatim.

**L4.2 — Same construction factory.** Both paths build their
accumulator via the same factory:

- Live: `create_accumulator_updater(config)` in
  `asap-query-engine/src/precompute_engine/accumulator_factory.rs`,
  then `update_single` / `update_keyed` in ingest order.
- Backfill: `build_backfilled_accumulator(config, samples)` in
  `asap-query-engine/src/stores/sketch_db/backfill_window_builder.rs`,
  whose body is exactly
  `let mut updater = create_accumulator_updater(config); for s in samples { updater.update_*(...) } updater.take_accumulator()`.

The two paths therefore differ only in (i) where the samples
come from and (ii) the wall-clock time at which each call happens.

**L4.3 — Pinned config.** `AggSchema::config: AggregationConfig` is
set on `AggSchema::new_active` and never mutated thereafter
(`schema.rs`); the comment on the field says verbatim *"Pinned at
schema creation; never mutated."* So the `config` argument both
paths feed into `create_accumulator_updater` is bit-identical for
the same `agg_id`.

**L4.4 — Sketch construction is a pure function of (config, sample
sequence).** The accumulator types built by
`create_accumulator_updater` (e.g. `SumAccumulator`,
`DDSketchAccumulator`, `KllAccumulator`,
`HllSketchAccumulator`, `CountSketchAccumulator`,
`CountMinSketchAccumulator`) seed any randomness from the `config`
parameters (hash seeds, sketch sizes), not from wall-clock or any
process-global RNG. The §10.5 design-doc constraint *"The hash
function seed for CMS / CountSketch / HLL must be part of
`AggregationConfig.parameters` and stable across Native and
Backfill paths"* is exactly this lemma.

**L4.5 — `serialize_to_bytes` is deterministic.** All
`AggregateCore` impls produce a serialisation that depends only on
the accumulator's internal state (no embedded wall-clock,
allocator-address, or HashMap-iteration-order fields). The pinning
test
`asap-query-engine/src/stores/sketch_db/backfill_processor.rs::tests::backfill_builds_bit_identical_sum_accumulator_to_live`
locks this invariant for `SumAccumulator` and is the canary for
the rest of the family.

**L4.6 — Time-disjointness eliminates ordering ambiguity.** L4.1
gives $e \le \Sigma.\mathtt{created\_at\_ms}$. Live ingest writes
exactly $[\Sigma.\mathtt{created\_at\_ms}, \infty)$ (the §6 schema
lifecycle says `is_writable = false` before `created_at_ms`,
because the schema doesn't exist yet, and `is_writable = true`
afterwards while `Active`). Backfill writes
$[s, e) \subseteq [0, \Sigma.\mathtt{created\_at\_ms})$. The two
ranges are disjoint by L4.1, so for any sample $x$ at most one
path ever observes it; there is no race, no double-update, and no
ordering ambiguity at the boundary.

**L4.7 — Retention check makes outputs observable.** L4.1 gives
$s \ge \mathrm{now}() - R$, so every window the backfill writes is
within the store's retention horizon at job-creation time. The
proof itself only needs equality of *internal* sketch state; this
lemma is included so the caveat list in §4.4 doesn't lose track of
why this check is part of "§10.5."

**L4.8 — Sample-stream alignment.** Both paths see the same
$(\mathrm{label}, \mathrm{timestamp}, \mathrm{value})$ sequence for
window $w$ in the same order. For the **live** path this is the
order in which the underlying ingest channel delivered samples; for
the **backfill** path this is the order returned by
`RawSampleReader::read_samples`, whose contract (top of
`raw_sample_reader.rs`) requires *"samples should be returned in
**ingest order** per-series — §10.5 requires deterministic replay,
and the contract is easiest to satisfy at the reader layer."* The
proof presupposes this contract holds; see caveat (a).

### 4.3 Proof

By induction on the length $N$ of the sample stream $X$.

**Base $N = 0$.** Both paths return the accumulator constructed by
`create_accumulator_updater(config)` with no updates applied. By
L4.3 the config is bit-identical; by L4.4 the empty-state
accumulator is a pure function of the config; therefore
$\sigma_B = \sigma_L$ at the byte level, and L4.5 lifts that to
`serialize_to_bytes`.

**Step $N \to N + 1$.** Assume the two paths agree after the first
$N$ updates. Both paths now apply the same single update — either
`update_single(value, ts)` or `update_keyed(key, value, ts)` —
where `key` is computed by the same `extract_aggregated_key`
function on the same `(labels, config)` (used by the live worker
in `precompute_engine/worker.rs::extract_aggregated_key_from_series`
and by the backfill builder via the package-private
`extract_aggregated_key` in
`backfill_window_builder.rs`; the doc-comment on
`build_backfilled_accumulator` calls out the parity explicitly).
By L4.4 the update is a pure function of (prior state, value,
timestamp, key); since the prior states agree by IH and all
arguments agree by hypothesis, the post states agree.

**Termination + serialisation.** After all $N$ samples, both
paths call `take_accumulator()` on their `Box<dyn
AccumulatorUpdater>` and then `serialize_to_bytes()` on the
resulting `Box<dyn AggregateCore>`. By L4.5 the byte output is a
pure function of the accumulator's state, which we have shown to
agree. Hence
$\sigma_B.\mathrm{serialize\_to\_bytes}() =
\sigma_L.\mathrm{serialize\_to\_bytes}()$. $\blacksquare$

### 4.4 Caveats

- **Reader-side ordering (L4.8).** The proof assumes the
  `RawSampleReader` returns samples in ingest order. Concrete
  reader impls (Prometheus HTTP, S3 Gorilla, ClickHouse) must
  honour this; if a reader returns timestamp-sorted samples that
  re-order in-second arrivals, the backfilled sketch can still
  diverge from live for sketches whose state depends on update
  order (KLL sampling decisions; DDSketch buffer eviction order).
  The contract is documented; enforcement is per-impl.
- **DataCollector sketch-built deployments.** When live ingest
  runs through the DataCollector OTLP path, the sketch is built by
  `sketchlib-go` (Go) and the backend only deserialises. Bit-
  identical determinism vs. backfill (which builds via the Rust
  `asap_sketchlib`) requires Go and Rust sketch builds to agree
  byte-for-byte. Cross-language byte parity for DDSketch / KLL /
  CountSketch landed in 2026-05-05 (PRs #40/#41/#42 +
  #43/#44/#45); HLL / CMS variants are in flight per
  `design-asap-precompute-rs.md`. For deployments where
  this parity hasn't landed, "bit-identical" in §4.1 weakens to
  "agree within sketch error bound $\varepsilon$ of §1."
- **HashMap-iteration order in grouping.** The backfill processor
  groups by `group_key` into a `HashMap<String, Vec<RawSample>>`
  and iterates groups in HashMap-iteration order. Within each
  group the per-sample order is preserved (Vec push-order); the
  proof above is per-group. Cross-group order does not affect the
  per-(agg_id, group_key, window) sketch, which is the unit of
  the §4.1 claim.
- **Timestamp-bound sketches.** Sketches that compute their state
  from `(value, timestamp_ms)` (as opposed to ignoring timestamp)
  are deterministic in this proof iff the timestamp is the
  ingest-side timestamp, not the wall clock at update time.
  `update_single(value, ts)` / `update_keyed(key, value, ts)`
  pass the sample's `ts`, not `now()`, so the lemma holds. Any
  future accumulator that uses `now()` internally would break
  this.
- **Schema retired mid-backfill.** L4.6 says the time-ranges are
  disjoint at job-creation. If the agg is retired while the job
  is running and the backfill processor attempts to look up the
  config via `config_for_agg`, it surfaces the error (`agg_id …
  not in current StreamingConfig — retired mid-backfill?`). The
  in-progress windows that already wrote complete; later windows
  fail the job. The proof's claim is per-window: each
  successfully-written window is bit-identical; failed windows
  are absent.
- **Concurrent retention sweep.** The retention check at job
  creation (L4.7) is at $\mathrm{now}()$; if the job is long-
  running and retention catches up to $s$ before the job
  completes, the early windows may be evicted from the store
  *after* the bit-identical write. The proof says nothing about
  read-back of evicted windows.
- **Cross-deployment determinism.** The proof is *intra-process*:
  same `config`, same backend binary. Different versions of the
  same backend with different dependency versions of
  `asap_sketchlib` may serialise the same logical state to
  different bytes. The serialisation-format-versioning tests
  (`tests/persist_format_versioning_tests.rs`) cover the on-disk
  format; this proof does not extend across format-version bumps.

### 4.5 Code anchors

- `asap-query-engine/src/stores/sketch_db/backfill.rs`
  - `BackfillRegistry::create_checked` — enforces the three
    §10.5 invariants (L4.1).
  - `CreateError::{UnknownAgg, Overlap, OutOfRetention}` — the
    three failure modes.
- `asap-query-engine/src/stores/sketch_db/backfill_window_builder.rs`
  - `build_backfilled_accumulator` — the backfill-side
    construction used in §4.1 and L4.2.
  - `extract_aggregated_key` — keyed-grouping function shared
    semantically with the live worker.
- `asap-query-engine/src/precompute_engine/accumulator_factory.rs`
  - `create_accumulator_updater` — the shared factory (L4.2).
- `asap-query-engine/src/stores/sketch_db/backfill_processor.rs`
  - `BackfillWindowProcessor::process_window` — calls
    `build_backfilled_accumulator` per group, writes via
    `Store::insert_precomputed_output_batch`.
  - module doc-comment "Determinism (§10.5)" — explicit
    invariant reference.
  - test
    `tests::backfill_builds_bit_identical_sum_accumulator_to_live` —
    runtime canary for L4.4 + L4.5 on `SumAccumulator`.
- `asap-query-engine/src/stores/sketch_db/raw_sample_reader.rs`
  - `RawSampleReader::read_samples` — ingest-order contract
    (L4.8).
- `asap-query-engine/src/stores/sketch_db/schema.rs`
  - `AggSchema::config` — pinned-at-creation invariant (L4.3).
  - `AggSchema::new_active` — `created_at_ms` capture used by
    L4.6.

---

## 5. Cross-reference

Each theorem anchors to a single module; this section exists so
the paper's §theory chapter can cite both at once.

- **Accuracy bounds (§1)** →
  `asap-query-engine/src/stores/sketch_db/accuracy.rs`
  (`AccuracyProfile::derive`).
- **`combine_statistic` correctness (§2)** →
  `asap-query-engine/src/query-engines/timeline_dispatch.rs`
  (`CombinedResult`, `combine_statistic`).
- **Write-barrier safety (§3)** →
  `asap-query-engine/src/stores/sketch_db/schema.rs`
  (`is_writable`, `status`, `retire`, `force_expire`); barrier
  counter `SAMPLES_BLOCKED_BY_SCHEMA_BARRIER` in
  `asap-query-engine/src/stores/sketch_db/metrics.rs`.
- **Backfill determinism (§4)** →
  `asap-query-engine/src/stores/sketch_db/backfill.rs`
  (`BackfillRegistry::create_checked`); construction parity in
  `backfill_window_builder.rs::build_backfilled_accumulator`;
  pinning test in
  `backfill_processor.rs::tests::backfill_builds_bit_identical_sum_accumulator_to_live`.
