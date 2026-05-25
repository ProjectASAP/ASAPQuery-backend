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

---

## 4. Shared per-series offset from parse-once

Compute the per-series reference once in the single parse pass (running
min/last-value + the decimal scale exponent), reuse for BOTH the cold chunk and
the warm sketch. If values are quantized once to fixed-point, both gorilla-cold
and KLL store the quantized form ⇒ consistent + no recompute. CPU note: offset
does NOT reduce sketch-build cost (hashing/compaction is fixed); the CPU wins
come from parse-once (done) + decode-on-read (cold decode off the ingest hot
path) + VM's cheaper decode.

---

## 5. Open decisions
1. Drift thresholds: correctness (overflow) is forced; the efficiency K-bit
   threshold + heartbeat Full interval need tuning (per-shape defaults).
2. Which warm sketch families get the offset/FOR re-encoding first (SUM+KLL are
   the clean wins; DDSketch index-FOR + HLL sparse depend on what the current
   sketchlib-go / asap-sketchlib serialization already does — audit in progress
   to size the gain).

Explicitly OUT of scope (decided): no zstd-wrapped variants, and no lossy/Serf
option — cold stays purely lossless with the {Gorilla-XOR, INT_FOR_DELTA,
INT_FOR_DOD} best-of-N.
