# ASAP holistic edge→backend compression design

Status: DRAFT for review. Informed by the offline micro-benchmark in this dir
(`./compressbench -dir data_serf` on the Chimp/Serf real datasets) and the
Gorilla / VictoriaMetrics / Serf literature.

## 0. Goals & principles

One pass over raw data at the edge feeds BOTH the cold (raw archive) and the
warm (sketch/aggregation) processors (existing parse-once framework). We add a
shared per-series **offset / frame-of-reference** so the numbers each consumer
actually stores are small, then bit-pack — minimizing **bits, bandwidth,
memory, CPU**.

Unifying principle (the "offset / common-bits" idea, generalized):
> Find the common base in each structure's natural integer representation,
> subtract it, store small residuals bit-packed, exploit sparsity, and
> re-base the frame when it drifts.

Hard requirements:
- **Cold = lossless** (raw archive). Warm sketches keep their existing
  approximation guarantees; the encoding adds no extra error.
- **Backend ingests edge-compressed chunks WITHOUT decode+reinsert.** Decode is
  pushed to the (rare) READ path: a custom Thanos StoreAPI decodes chunks to
  XOR `AggrChunk`s at query time, so a stock PromQL engine queries them while
  S3 holds the compact custom format. Writes are far more frequent than cold
  reads, so this is where decode belongs.

Benchmark headline (real Chimp/Serf datasets, lossless, block-avg 1000/chunk):
- Fixed-decimal series (11/12 datasets — temps, stocks, sensors, pressure,
  GPS, dust, wind, grid): **VM-style integer FOR+delta beats Gorilla ~4.8×
  avg (up to ~10×)**. This *is* the offset idea, on the integer-scaled values.
- Genuinely high-precision float (float32-derived, 15 sig digits): VM can't
  stay decimal-exact → falls back to bit-pattern (worse); **Gorilla-XOR wins**.
- ⇒ The codec must be a per-block **best-of-N including Gorilla-XOR**, not
  "VM replaces Gorilla".
- Decode CPU: VM ~41 ns/sample vs Gorilla ~71 — VM decode is *faster*, good
  for the decode-on-read path.

---

## 1. Cold raw chunk format

### 1.1 Part (one S3 object per tenant / 2h-block / shard)
```
[part header]   magic "ASAPCC1" | u8 version | i64 block_start_ms | i64 block_end_ms | uvarint series_count
[chunks]        per-series chunks, concatenated
[index]         sorted by series: { labels(symbol refs), u64 chunk_off, u32 chunk_len, i64 min_ts, i64 max_ts }
[symbol table]  deduped label strings (the index references offsets here)
[footer]        u64 index_off | u64 index_len | u64 symtab_off | u32 crc32c
```
The index + symbol table let the StoreAPI answer `Series(matchers, mint, maxt)`
without scanning chunk bodies.

### 1.2 Per-series chunk
```
[chunk header]
  u8  codec_tag          # see 1.3
  uvarint n_samples
  ts:  i64 t0_delta(block-relative) then delta-of-delta varints   # timestamps
  # value codec params (codec-specific):
  tag INT_FOR_DELTA / INT_FOR_DOD:
      i8       scale_exp           # decimal exponent e: int_v = round(v * 10^-e); v = int_v * 10^e
      zigzag-varint base           # the FOR reference (frame base; see drift, 1.4)
      zigzag-varint first_residual
  tag GORILLA_XOR:   (no extra params; standard XOR stream)
[chunk body]
  bit-packed residual stream (INT_*), or XOR stream (GORILLA)
```

### 1.3 codec_tag
```
0 = GORILLA_XOR        lossless float64 (fallback for high-precision floats)
1 = INT_FOR_DELTA      scale→int64, FOR(base), delta, bit-pack          (gauges)
2 = INT_FOR_DOD        scale→int64, FOR(base), delta-of-delta, bit-pack (counters / timestamps)
All three are LOSSLESS. No lossy (Serf) and no zstd-wrapping — kept deliberately
simple: the INT_* FOR+delta already captures the win on fixed-decimal data, and
Gorilla-XOR is the lossless fallback for true high-precision floats.
```

### 1.4 Encoder: per-series-per-block best-of-N with exactness check
```
fn encode_chunk(values, opts):
    cands = []
    # INT path — ONLY if it round-trips EXACTLY (the lib/decimal precision trap)
    (ok, e, ints) = try_scale_to_int64(values)        # find decimal exp e s.t. round-trip exact
    if ok:
        cands += encode_int(ints, FOR_DELTA)            # tag 1
        cands += encode_int(ints, FOR_DOD)              # tag 2
    # Gorilla is always valid + lossless
    cands += encode_gorilla_xor(values)                 # tag 0
    return argmin(cands, key=byte_len)                  # smallest — all candidates lossless
```
`try_scale_to_int64` is the load-bearing guard: never ship INT_* unless decode
reproduces the original float64 bit-exactly (else a naive VM-decimal silently
introduces ~1e-12 error — confirmed on Motor-temp in the benchmark).

`base` re-bases on **drift** within a block: if a residual would overflow the
chosen bit-width, cut the chunk early and start a new chunk with a fresh base
(the cold analogue of "offset drift → new Full"; see §3).

### 1.5 Backend write path (NO decode)
On ingest, validate the part header/crc, store the object to S3, and register
its series→part entries in the manifest/index. No decode, no re-encode.

### 1.6 Decode-on-read StoreAPI (the compat shim)
Implements the Thanos `storepb.StoreServer`:
```
Series(req {matchers, min_t, max_t}) -> stream of SeriesResponse:
    for part in manifest.parts_overlapping(min_t, max_t):
        for series in part.index.matching(req.matchers):
            for chunk in series.chunks_overlapping(min_t, max_t):
                samples = decode(chunk)                 # by codec_tag; INT_* adds base + scale
                emit AggrChunk{ raw: xor_encode(samples) }   # hand PromQL a standard XOR chunk
```
Thanos-query unions this with the >=2h store-gateway path, exactly as the
current gorilla-merger StoreAPI does — we extend that StoreAPI's `Series()`.

### 1.7 Grouped layout — shared timestamp column (cross-series)

Measured (`/mydata/xseries-bench`, real cluster groups @2h/15s k=50 + synthetic).
Two cross-series levers; only one pays.

- **Shared timestamp column — ADOPT.** Same-metric series in a part share ONE
  timestamp column instead of every chunk carrying its own. Because INT_FOR
  values compress so well, timestamps are **36–52% of per-series bytes** on real
  groups; sharing the column across a k-series group removes ≈ ts_frac·(k−1)/k →
  **−43% on the real cluster aggregate**, correlation-INdependent, low-risk.
  (The Heracles VLDB'21 result, confirmed here.) Part layout becomes:
  per-metric group = `{ one shared ts column (delta-of-delta) }` + `{ per-series
  value chunks: codec_tag + residuals, NO ts }`. The decode-on-read StoreAPI
  zips the shared ts with each series' values. Warm sketch parts get the same
  win (same-window sketch series share the window-end column).
- **Cross-series value base — do NOT adopt by default.** Per-timestamp base
  `b(t)=min` + per-series value residuals. Measured **net-NEGATIVE (−3.5% vs
  shared-ts aggregate)**: it only wins for near-identical-replica series, and
  the predictor is the **noise/signal ratio, NOT correlation** (even ρ=0.99
  lost +2.6% when noisy) — per-series delta-of-delta already extracted the
  per-series common bits, so a noisy cross-series base just adds entropy. Trap:
  must be done in the integer domain (naive float subtraction breaks decimal
  representability, ~2× bloat). Optional per-group cost-based opt-in only.

Takeaway: the remaining cross-series "common bits" worth taking are the
**timestamps** (shared column, −43%), not the values (already extracted
per-series).

---

## 2. Warm sketch encoding

Same offset idea, applied at each sketch's natural representation. The offset
is a property of the **Full-snapshot epoch** (see §3): a Full carries the
re-based offset/scale in its header; all Deltas until the next Full encode
residuals in that frame; backend reconstructs absolute values at merge/query.

| family | offset on raw values? | encoding |
|---|---|---|
| SUM / COUNT | yes (linear) | ship `Σresidual` (narrow int, varint) + N + epoch offset; backend `Σresidual + (ΣN)·offset`. Counter sums: ship per-window increment (delta), no fixed offset. |
| KLL (quantile) | yes (shift-equivariant: `q(X−c)=q(X)−c`) | store sampled values as `(v−offset)` fixed-point (i16/i32 vs f64 ⇒ ~½ size); within a level the sorted samples delta-encode; backend adds offset to the quantile result. |
| DDSketch (current family) | NO (log-scale; `v−c`→0 breaks relative error) | FOR+delta on the **bucket-index** array (`min_index` + Δindex varints) + varint counts. |
| HLL (cardinality) | NO (hash) | **sparse mode** (low card: sorted non-zero registers, delta+varint = HLL++) + 6-bit dense packing. |
| CMS / CountSketch (freq/topk) | NO (hash) | narrow counters + per-row FOR + bit-pack; cross-window delta of the matrix; topk heap shipped as k entries. |

Cross-cutting warm levers:
- **Delta transmission** (already in place: ProtoFull/ProtoDelta + the
  delta-stitching carry-in): ship only changed sketch state between Fulls.
- **Offset rides in the Full header only**; Deltas carry residuals only.
- All Deltas in a Full-epoch share one offset frame ⇒ they remain mergeable
  (merging `(v−off₁)` and `(v−off₂)` residual-KLLs would be garbage).

### 2.1 Audit of the current serialization (measured)

The backend does NOT re-serialize: `asap_sketchlib` and `sketchlib-go` share one
cross-language wire format, and the backend stores the wire bytes **opaquely**
(`SketchSampleState{bytes, encoding}`; flushed parts write `sketch_bytes`
verbatim, no part-level recompression). So **wire format = sketch_db storage =
disk-part bytes** — optimizing `sketchlib-go`'s `Serialize*` wins on bandwidth,
warm memory, AND cold disk at once.

Measured (N=5000/window; harness `/mydata/sketch-audit`):

| family | current encoding | bytes | headroom | verdict |
|---|---|---|---|---|
| HLL p=14 | dense 1 byte/register × 16384 (flat, any cardinality) | 16,532 | sparse full-state (delta-idx) → 5–50× for low card; 6-bit dense pack 1.34× | **P1** |
| KLL k=200 | raw f64 items array | 2,157 | `(v−offset)` fixed-point f64→~4 B → ~2× | **P2** |
| DDSketch α=.01 | dense varint counts keyed by FOR offset base, zigzag | 556 | already FOR+varint; sparse would be *larger* (85% occupancy) | skip |
| CMS / CountSketch 3×4096 | sint64 zigzag-varint (~1 B/cell) | ~12.5 KB | per-row FOR ~0 gain | skip |
| SUM/COUNT | OTLP Sum dp, raw f64/group | ~8 B/grp | OTLP framing dominates; residual marginal | skip |

So the warm scope narrows to **two changes**:
- **P1 — HLL sparse full-state serialize** (HLL++ style: sorted non-zero
  registers, delta+varint; fall back to 6-bit-packed dense above the crossover
  ~6k nonzero regs). The lib already has a sparse *delta* path (`hll/delta.go`),
  just not for full-state. Biggest lever — and since the 16 KB dense state is
  stored **uncompressed** per instance, this also cuts **warm SketchStore
  memory** 5–50× for low-cardinality series (not just wire, which gzip masks).
- **P2 — KLL value-offset/quantization** (`(v−offset)` fixed-point, ~2×) — the
  offset idea, measured.

DDSketch (already FOR+varint), CMS/CountSketch (already zigzag-varint, off the
legacy float64 matrix), and SUM/COUNT (OTLP-framing-bound) are already
well-encoded — do NOT touch. This **supersedes** the "FOR+delta on DDSketch
indices" / "narrow CMS counters" rows in the table above, which the audit shows
are redundant.

---

## 3. Offset drift → re-base (Full) — unifies warm & cold

A frame stays valid only while residuals fit it. Re-base the frame (warm: emit
a new **Full sketch**; cold: cut the chunk and start a new **base**) when:
- **Correctness**: a residual would overflow the chosen narrow width → MUST
  re-base.
- **Efficiency** (optional): residuals now need K more bits than a re-base
  would → re-base to reclaim ratio (K threshold weighed vs Full cost).

Plus a **max-interval heartbeat Full** even without drift, because a Full is a
self-contained base needed for: (a) durability/recovery — without a recent Full,
a lost Delta makes the chain undecodable (cold S3 + restart-recovery depend on a
recent base); (b) a newly-joining consumer/query window needs a base.

Rule: **emit Full when (drift) OR (heartbeat elapsed)**.

Consequences:
- Full cadence becomes **per-series adaptive**: stable gauges almost never
  re-base (nearly all Delta); volatile series re-base often (but volatile data
  is inherently less compressible — cost lands where it should).
- **Counters are the exception**: use delta-of-value (base = previous sample,
  auto-re-bases every sample, never "drifts") — so drift→Full is the gauge/FOR
  story; counters just delta.

Minimal change to today's pipeline: the agent already emits periodic Fulls;
add **drift** as a second Full trigger; backend delta-stitching already treats
a Full as the carry-in base, so a re-base is just a new base.

### 3.1 v1 policy (concrete, zero-tuning)

Ship the simplest correct version first; defer the only tunable knob.

- **Drift trigger = hard/overflow ONLY.** Per frame, pick the residual integer
  width from the Full's observed range (i16 if it fits, else i32, else i64);
  re-base (warm: emit Full; cold: cut chunk + new base) the moment a residual
  would exceed that width. This is a correctness bound, **not a tunable**.
- **Heartbeat = fixed interval.** Emit a Full at least every **N windows** even
  without drift. Default `N` = the existing agent Full cadence (today's
  ProtoFull period); for cold, the natural chunk bound (a time-block / ≤~120-
  sample chunk) already serves as the heartbeat. Bounds crash-loss and the
  query base-lookback to ≤ one heartbeat.
- **Counters:** delta-of-value (no drift); the heartbeat Full still applies
  (recovery / new-consumer base).

**DEFERRED — soft/efficiency drift (the tunable `K`):** re-base when residuals
waste `> K` bits vs a fresh frame. Skipped in v1 — it's a second-order
optimization and the only thing that would need per-shape tuning. Add it ONLY
if observation shows long-lived frames whose residuals widen (compression
silently degrading) without ever overflowing. Until then **v1 needs no tuning**.

---

## 4. Shared per-series value stats from parse-once (NOT a shared offset constant)

What is shared between the cold path and the warm sketches is the **parse-once
computation**, not a single offset constant:

- **Shared (compute once, both consume):** the per-series **decimal scale
  exponent** — which MUST be identical (one series has one natural precision, so
  cold INT_FOR and KLL fixed-point land in the same integer domain and stay
  mutually consistent) — plus the per-series value stats (running min/range).
- **NOT shared — the actual FOR base differs by cadence.** Cold re-bases the
  base **per block** (each chunk takes its own min/first for the tightest
  per-block residual width); warm sketches re-base **per Full-epoch** (the base
  must stay stable so a run of Deltas remains mergeable, §3). These cadences
  conflict — forcing one shared base would hurt whichever tier it's wrong for —
  so the actual subtracted constant generally differs even though both derive
  from the same parse-once stats.
- **Scope — only the raw-value-offset families:** cold INT_* values ↔ KLL ↔ SUM
  (all subtract a reference from the same raw values). DDSketch (FOR on bucket
  *indices*), HLL (hash), and CMS/CountSketch (counters) have **no shared
  raw-value offset** — their compression uses their own structure (§2.1).
- **Kind matches per shape, not as a constant:** VM uses min-FOR for *gauges*
  (same kind as KLL's value-offset) but delta-of-delta for *counters* (base =
  first value), which aligns with the warm side's SUM-as-delta — so the
  correspondence is gauge→FOR / counter→delta, not one shared number.

CPU note: the offset does NOT reduce sketch-build cost (hashing/compaction is
fixed) — **sampling (§6) is what closes that CPU gap**. Compression's CPU wins
come from parse-once (done) + decode-on-read (cold decode off the ingest hot
path) + VM's cheaper decode.

---

## 5. Open decisions
1. ~~Drift / heartbeat thresholds~~ **DECIDED (v1 — see §3.1)**: drift =
   hard/overflow only (correctness bound, no tunable); heartbeat = fixed `N`
   windows (= existing Full cadence). Soft/efficiency-`K` drift DEFERRED as a
   later optimization — v1 needs no tuning.
2. ~~Which warm sketch families get the offset/FOR re-encoding first~~
   **RESOLVED by the §2.1 audit**: P1 = HLL sparse full-state (5–50×, + cuts
   warm memory), P2 = KLL value-offset (~2×). DDSketch / CMS / CountSketch /
   SUM are already well-encoded — skip.

Explicitly OUT of scope (decided): no zstd-wrapped variants, and no lossy/Serf
option — cold stays purely lossless with the {Gorilla-XOR, INT_FOR_DELTA,
INT_FOR_DOD} best-of-N.

---

## 6. Sampling × compression composition

Sampling-enhanced sketches (inverse-probability bucket/counter updates,
hash-threshold key sampling, weighted KLL insertion — each with its own derived
error bound) compose with the compression scheme above. They sit on ORTHOGONAL
cost axes and are COMPLEMENTARY:
- **Compression** (FOR / delta / sparse / shared-ts) cuts BYTES — wire,
  sketch_db memory, disk parts.
- **Sampling** cuts INGEST CPU + update rate — each item triggers a sketch
  update only with probability `p`. This is the axis compression structurally
  CANNOT touch: offset/FOR don't reduce the hashing/compaction build cost;
  sampling does. (§4's note "offset doesn't reduce sketch-build CPU" — sampling
  is what closes that gap.)

**Scope**: sampling applies to the WARM sketch path only. The cold raw archive
is NOT sampled (it is the lossless backup; sampling would lose data).

### 6.1 The composition rule
> Store the RAW SAMPLED integer state + one global `p` per frame; apply the
> `×1/p` rescale at QUERY, not at store.

Storing the `1/p`-rescaled (inflated, often fractional) state would break
varint/FOR and bloat. Storing the raw sampled accumulation — e.g. DDSketch
`m_b = Σ Z_i` (≈ `p·n_b`, a SMALLER integer) + `p`, rescaled `m_b/p` at query —
keeps counts as small integers, so FOR/delta/varint (and the common-bits idea)
keep working. It is also numerically cleaner (integer accumulation, no per-update
fraction). `p` rides in the Full-epoch frame header alongside the offset.

### 6.2 Per-family interaction
| family | sampling (CPU↓) | compression | interaction |
|---|---|---|---|
| HLL | hash-threshold (also thins registers) | sparse full-state (P1) | **strong synergy**: sampling zeroes more registers → sparser → sparse encoding wins more AND stays sparse up to ~`1/p`× higher true cardinality before the dense crossover. Query `n̂/p`. |
| DDSketch | bucket-update (writes→`p`) | index FOR+varint (done) | orthogonal; bucket SET ≈ unchanged so index encoding unchanged; store sampled counts (smaller int) + `p` → count varint smaller. |
| KLL | weighted insert (inserts→`p`) | value-offset/quantize (P2) | orthogonal; state still `k` items → offset-encode them; weight = global `(1/p)·2^h` (no per-item cost). |
| CMS / CountSketch (Nitro) | sampled counter update (writes→`pd` or fixed `s`) | zigzag-varint (done) | synergy: store sampled small-int counts + `p` → varint smaller; writes cut to `pd`. |

### 6.3 Cross-cutting
- **Delta transmission**: sampling → fewer updates/window → fewer changed cells
  → smaller delta frames. Merging sampled windows is benign — sampling error
  `ε_s ∝ 1/√(pN)` shrinks as more windows merge (larger N).
- **Full-epoch frame**: `p` is a frame-level constant in the Full header (like
  the offset); deltas inherit it.

### 6.4 Cautions
- **Error budgets ADD**: `ε_total = sketch error + sampling ε_s (+ KLL value-
  offset quantization)`, and must fit the metric's accuracy SLA. Family bounds
  (derived separately): DDSketch `ε_s = O(√(log(B/δ)/pN))`; KLL `ε_total ≈
  ε_k + ε_s` with the design balance `pN ≳ k²`; HLL `RSE ≈ √((1−p)/(pn) +
  1.04²/m)`; Nitro adds variance `((1−p)/p)·Σ a_t²`.
- **`p` is a per-metric control-plane knob**: chosen per metric from the
  expected N (rate/cardinality) + accuracy SLA. Fits the existing
  controller-driven model exactly — the controller already annotates each
  metric's tier + sketch type; it adds `p` the same way.

### 6.5 Early benefits — offline Go benchmark (pre-integration)

Measured against the REAL sketchlib-go sketches (harness `/mydata/sampling-bench`,
`go run .`, ~16s; UNSAMPLED vs SAMPLED-at-`p` vs EXACT ground truth, with the
§6.1 composition rule applied — raw sampled state stored, `×1/p` at query). **All
§7 bounds held empirically** across the `p`/`N`/`k`/`m` sweeps.

| family | benefit @ `p=0.1` | accuracy | safe-`p` |
|---|---|---|---|
| DDSketch (α=1%) | 10× fewer bucket writes (1e6→1e5), ~6× wall-clock (239→34 ms) | q99 rank err ~0.005 (within ε_s); value relErr ≈ α | `p`≈0.05–0.1 (q99 suffers first at tiny `p`) |
| KLL | 10× fewer inserts, ~6× wall-clock | rank err tracks ε_s **iff `pN≳k²`**; below it q99 jumps (N=1e6,k=400,p=0.1 → pN<k² → relErr 0.225) | **`pN ≳ k²`** |
| HLL (m=16384) | up to ~100× fewer updates (`p=0.01`) | UNBIASED: relErr 0.017 vs RSE bound 0.013 @ n=1e6 | `p` down to 0.01 for n≥1e5; **hash-threshold only** |
| CMS / CountSketch | 10× fewer updates | heavy-hitter relErr ~1–2% @ N≥1e5; per-key err ~√(f(1−p)/p) | `p`≈0.1 |

Findings:
- **CPU benefit is real and ~linear in `p`** (updates ∝ `p`; ~6× wall-clock at
  `p=0.1`) — exactly the build-cost axis compression can't touch.
- **`pN≳k²` for KLL is a hard regime boundary**, confirmed empirically (not just
  asymptotic): cross below it and q99 error jumps.
- **HLL must use hash-threshold, NEVER per-occurrence** (per-occurrence relErr
  blew up 0.66→3.8 by over-counting high-multiplicity keys). Implementation
  gotcha: the threshold hash MUST be **independent of HLL's canonical register
  hash** — using the same hash correlated the kept set with the register layout
  and gave 15–84% bias until a separate seed was used.
- **Serialization composes (§6.1 rule):** the sampled proto state is never larger
  than unsampled — DDSketch sparser buckets, KLL fewer retained items, HLL/CMS
  smaller below register saturation. Sampling doesn't bloat the wire.

Caveats (documented in the harness README): HLL precision `m` is a compile-time
const in sketchlib-go (the `m`-sweep is analytical in the RSE column); the HLL
"sparser→smaller" win only holds below register saturation (distinct ≪ `m`) — at
n=1e6 registers saturate and sampled/unsampled sizes converge.

### 6.6 Geometric sampling (NitroSketch) — cheap equivalent implementation
Per-update Bernoulli(p) (a coin per update) wastes RNG. NitroSketch's
**geometric sampling** instead draws, after each kept update, a `Geometric(p)`
number of updates to SKIP, and jumps ahead. This is **statistically identical**
to per-update Bernoulli(p) (gaps `~Geometric(p)` ⟺ each update kept iid w.p. `p`),
so §7's bounds, unbiasedness, and the §6.1 composition rule are all unchanged —
it only amortizes the sampling RNG to ~`O(p)` draws per item (and drops the
per-item branch), on top of the "fewer sketch updates" win.
- **Applies to the per-update/per-item families:** DDSketch (bucket updates),
  KLL (per-item insert), CMS / CountSketch / Nitro (counter updates).
- **NOT HLL:** HLL samples by hash-threshold on the distinct KEY
  (`u(h(x))<p`) — a value-determined decision, not a stream-position coin — so
  geometric skip-ahead doesn't apply; and its hash-compare is already O(1) with
  no RNG, so it needs no such optimization.
- **Orthogonal to shared-ts (§1.7) and the offset (§4)** — it changes only HOW
  the kept set is generated and WHICH counter updates land, never timestamps or
  the value base: (a) cold raw isn't sampled at all; (b) warm "shared-ts" is the
  window-end column, fixed by the window cadence which sampling doesn't change;
  (c) the offset comes from the per-series value magnitude, which a uniform
  random `p`-subset preserves.
- **Impl note:** over a multi-series interleaved stream a single global geometric
  counter gives each series ~`p` in expectation (with per-series variance);
  per-series warm sketches typically keep a per-series counter.

---

## 7. Sampling-enhanced sketches: algorithms & error-bound derivations

Formal backing for the bounds cited in §6. The rule: **sampling must respect
each sketch's algebraic structure.**

| Sketch | Core update | Valid sampling |
|---|---|---|
| DDSketch | additive bucket count | inverse-probability bucket update |
| KLL | weighted samples + randomized compaction | inverse-probability item weight, then normal compaction |
| HLL | max register | hash-threshold element sampling, then rescale |
| CMS / CountSketch / Nitro | additive counters | inverse-probability counter update |

Let $0<p\le 1$, and $Y_i=Z_i/p$ with $Z_i\sim\mathrm{Bernoulli}(p)$, so
$\mathbb{E}[Y_i]=1$ and $\mathrm{Var}(Y_i)=\mathbb{E}[Y_i^2]-1=\frac{1-p}{p}$.

### 7.1 DDSketch — inverse-probability bucket update
Bucket $b(x)=\lceil\log x/\log\gamma\rceil$, $\gamma=\frac{1+\alpha}{1-\alpha}$;
representative relative error $\alpha$. Update: for $x_i$, add $Y_i$ to
$\widehat C[b_i]$ (i.e. $+1/p$ w.p. $p$). Sampled count
$\widehat n_b=\sum_{i:b(x_i)=b}Y_i$.
- **Unbiased:** $\mathbb{E}[\widehat n_b]=\sum_{i}\mathbb{E}[Y_i]=n_b$.
- **Prefix concentration** (quantiles use prefix counts $N_{\le b}$):
  $\mathrm{Var}(\widehat N_{\le b})=N_{\le b}\frac{1-p}{p}$. Bernstein + union over
  $B$ buckets ⇒ w.p. $1-\delta$,
  $\sup_b|\widehat N_{\le b}-N_{\le b}|=O\!\big(\sqrt{N\log(B/\delta)/p}+\log(B/\delta)/p\big)$.
  Rank error $\epsilon_s=O\!\big(\sqrt{\log(B/\delta)/(pN)}+\log(B/\delta)/(pN)\big)$.
- **Quantile bound:** $\;\tilde x_q\in(1\pm\alpha)\,x_{q\pm\epsilon_s}\;$ w.p.
  $1-\delta$. The $\alpha$ now applies to $x_{q\pm\epsilon_s}$, not $x_q$ (the
  pure relative-value guarantee is only preserved when $x_{q\pm\epsilon_s}$ is
  near $x_q$). Cost: bucket writes/item $1\to p$.

### 7.2 KLL — weighted update sampling
Original rank error $\epsilon_k=O(\frac1k\sqrt{\log(1/\delta)})$. Update: w.p.
$p$ insert $x_i$ with weight $1/p$ (a virtual level-$(-1)$ compactor; at level
$h$ the weight is $2^h/p$); compact normally.
- **Weighted CDF unbiased:** $\widehat F_s(t)=\frac1N\sum_i\frac{Z_i}{p}\mathbf 1\{x_i\le t\}$,
  $\mathbb{E}[\widehat F_s(t)]=F(t)$.
- **Concentration:** $\mathrm{Var}(\widehat F_s(t))=\frac{N_t}{N^2}\frac{1-p}{p}\le\frac{1-p}{pN}$;
  DKW/union over breakpoints ⇒ $\sup_t|\widehat F_s-F|\le\epsilon_s=O(\sqrt{\log(1/\delta)/(pN)})$.
- **Combined:** $\sup_t|\widehat F_{KLL}-F|\le\underbrace{|\widehat F_{KLL}-\widehat F_s|}_{\le\,\epsilon_k}+\underbrace{|\widehat F_s-F|}_{\le\,\epsilon_s}$, so
  $\;\tilde x_q\in[x_{q-(\epsilon_k+\epsilon_s)},\,x_{q+(\epsilon_k+\epsilon_s)}]$.
  (Martingale view: sampling adds one more zero-mean term to KLL's compaction-error
  variance budget.)
- **Design balance:** set $\epsilon_s\approx\epsilon_k$ with
  $\epsilon_s\approx 1/\sqrt{pN}$, $\epsilon_k\approx 1/k$ ⇒ **$pN\approx k^2$** —
  the effective number of sampled updates should be $\gtrsim k^2$ or sampling
  dominates. Cost: insertions/item $1\to p$; memory $O(k\log(pN))$.

### 7.3 HLL — hash-threshold sampling
RSE $\approx 1.04/\sqrt m$. The update is $R[j]\leftarrow\max(R[j],\rho(x))$ —
**a max, not additive**, so inverse-probability register updates are INVALID.
Algorithm: keep a distinct key iff $u(h(x))<p$ (a stable hash ⇒ the same key
always gets the same decision); update normally; estimate $\widehat n=\widehat n_s/p$.
- **Why not per-occurrence:** a freq-$f_x$ key would be kept w.p. $1-(1-p)^{f_x}$
  — frequency-dependent, hence biased for distinct counting. Hash-threshold gives
  every distinct key the same $\Pr[\text{kept}]=p$.
- **Unbiased:** $n_s=\sum_{x\in D}I_x$, $I_x\sim\mathrm{Bernoulli}(p)$ ⇒
  $\mathbb{E}[n_s/p]=n$.
- **Concentration:** $n_s\sim\mathrm{Binomial}(n,p)$; Chernoff ⇒
  $\epsilon_s=\sqrt{3\log(2/\delta)/(pn)}$.
- **Combined RSE:** $\frac{|\widehat n-n|}{n}\lesssim\epsilon_s+\epsilon_h$, i.e.
  $\;\mathrm{RSE}\approx\sqrt{\frac{1-p}{pn}+\frac{1.04^2}{m}}$ (sampling term +
  HLL term). Cost: register writes/item $\to p$.

### 7.4 Nitro / additive counters (CMS, CountSketch)
Update $C_{r,h_r(x)}\mathrel{+}=a_r(x)$ ($a=w$ for Count-Min; $a=s_r(x)w$,
$s_r\in\{\pm1\}$, for CountSketch). Sampled: add $\frac{Z}{p}a$.
- **Unbiased counters:** $\mathbb{E}[\widehat C_{r,c}]=C_{r,c}$ (the core
  NitroSketch argument).
- **Variance:** for a counter with updates $a_1..a_T$,
  $\mathrm{Var}(\widehat C-C)=\frac{1-p}{p}\sum_t a_t^2$; Bernstein ⇒
  $|\widehat C-C|=O\!\big(\sqrt{\frac{1-p}{p}(\sum a_t^2)\log(1/\delta)}+\frac{a_{\max}}{p}\log(1/\delta)\big)$.
- **CountSketch:** median over $d$ rows; total error = hash-collision + sampling;
  per-row variance gains $\frac{1-p}{p}\sum_{i:h_r(x_i)=h_r(x)}w_i^2$; the median
  still drops failure prob exponentially in $d$.
- **Count-Min:** the deterministic no-underestimate property is LOST (self-updates
  may be skipped); analyze as a biased-up collision estimator + zero-mean sampling
  noise.
- **Fixed-$s$-row variant:** select exactly $s$ of $d$ rows per item, scale by
  $d/s$; unbiased with a deterministic write count $s$ (set $p=s/d$). Cost:
  counter writes/item $d\to pd$ (or fixed $s$).

### 7.5 Design rules
- **Additive update ⇒ inverse-probability update sampling** (DDSketch, CMS,
  CountSketch, Nitro).
- **Max update ⇒ threshold sampling + final rescale** (HLL).
- **Compaction update ⇒ weighted sampling + weighted rank analysis** (KLL).

All four are unbiased; the added error is the $\epsilon_s$ / variance term above,
which §6.4 folds into the per-metric accuracy budget (and §6's composition rule —
store the raw sampled integer state + global `p`, rescale at query — keeps these
estimators compressible).
