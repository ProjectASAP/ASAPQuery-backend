# gorilla-merger — decode-free ingest + compaction benchmarks

Empirical measurements for the decode-free chunk-stitch ingest path and the
offline compaction-for-ratio step (the `feat/merger-direct-chunk-merge` work).
Numbers are medians; hardware: Intel Xeon E5-2660 v2, `/tmp` on spinning ext3.

The merger measurements below are **reproducible from this repo** via
`internal/merger/ingest_bench_test.go`:

```
# ingest CPU/mem (decode-free vs decode->re-encode)
go test ./internal/merger -bench='^BenchmarkIngest' -benchmem -run='^$' -benchtime=200x -count=3
go test ./internal/merger -bench='BenchmarkIngestDecodeFreeCodecOnly' -benchmem -run='^$' -benchtime=2000x -count=5
# compression ratio (tiny L1 blocks vs re-chunked L2)
go test ./internal/merger -run='TestMeasureCompressionRatio' -v
```

The **edge-processor** and **query-latency** numbers are included for the
end-to-end story; the edge benchmarks live in the ASAPCollector repo
(`asap-gorilla-go` + `asapedgeprocessor`), and the query-latency numbers come
from a live Thanos stack (MinIO + merger + store-gateway + thanos-query).

---

## 1. Merger ingest — CPU / memory (decode-free vs decode→re-encode)

The PR replaces the old `decode XOR → iterate every sample → tsdb head
Appender.Append → Commit` hot path with a decode-free `chunkenc.FromData →
chunks.Writer.WriteChunks` stitch (raw XOR bytes land verbatim). Corpus: 100
series × one 120-sample XOR chunk = 12,000 samples in one ASAPFRG1 frame.

| path | ns/op | B/op | allocs/op | notes |
|---|---|---|---|---|
| decode-free, codec only (NEW) | 12,881 | 7,296 | 101 | FromData + chunks.Meta, no iterate, no disk |
| decode→re-encode, WAL off (OLD) | 8,563,374 | 2,534,166 | 2,893 | decode + per-sample head append, head only |

**Codec work, new vs old: ~665× less CPU, ~29× fewer allocations, ~350× fewer bytes.**

**Caveat — this is a CPU/GC win, not a latency win.** Both paths still fsync a
block to persist, and on slow disk that fsync dominates wall-time (the full
`buildBlock` — chunks + index + tombstones + meta + rename — is ~481 ms/op,
~37,000× the codec). The value is *merger CPU headroom and reduced GC pressure*
when fielding many agents, not lower per-block latency on fsync-bound storage.
Even the full `buildBlock` does ~1,000 allocs/op vs the old path's ~2,900.

## 2. Compaction — compression ratio (tiny L1 blocks vs re-chunked L2)

50 series × 600 samples; the agent flushes small chunks, the compactor
re-chunks to exactly 120 samples/chunk. `chunkB/s` = chunks-dir bytes/sample;
`totB/s` = (chunks + index) bytes/sample.

| agent chunk (samples) | L1 chunkB/s | L1 totB/s | L1 chunks | L1 blocks | L2 chunkB/s | L2 totB/s | L2 chunks | **chunkB ratio** | **totB ratio** |
|---|---|---|---|---|---|---|---|---|---|
| 10 | 8.67 | 19.10 | 3000 | 60 | 7.16 | 7.39 | 250 | 1.21× | **2.58×** |
| 30 | 7.73 | 11.20 | 1000 | 20 | 7.29 | 7.52 | 250 | 1.06× | **1.49×** |
| 60 | 7.37 | 9.11 | 500 | 10 | 7.23 | 7.45 | 250 | 1.02× | **1.22×** |

The raw XOR-payload win is modest (1.02–1.21×); the real win is **total on-disk
bytes (chunks + index): 1.22× → 2.58×**, scaling up the smaller the agent's
chunks are. It comes from folding many tiny per-block indexes into one via block
fusion (60 L1 blocks → 1 L2 block), more than from longer XOR runs — precisely
the resource-limited-agent regime this step targets, and it's the footprint that
ships to S3.

## 3. Edge processor — CPU / memory / bandwidth (context; benches in ASAPCollector)

The gorilla edge encode (XOR + ASAPFRG1 build) is cheap enough for a
resource-limited agent, but emits a ratio-suboptimal stream at small flush sizes.

- CPU: ~1.5–2.6 µs/sample (codec), ~3 µs/sample (full `ConsumeMetrics`); linear in cardinality.
- Memory: ~1.8 KB retained per active series (open-chunk + pending state).
- Bandwidth (emitted ASAPFRG1 bytes/sample vs raw 16 B/sample; OTLP input ≈ 57 B/sample):

| edge chunk (samples) | bytes/sample | vs raw 16B | degradation vs 120 |
|---|---|---|---|
| 120 | 7.16 | 0.45× | 1.00× (best) |
| 60 | 7.86 | 0.49× | 1.10× |
| 30 | 9.32 | 0.58× | 1.30× |
| 10 | 15.16 | 0.95× | **2.12×** |

At the small chunks a constrained agent emits, bytes/sample degrades ~2.12×
(≈ raw). This is exactly the bandwidth the merger's offline re-chunk recovers —
the edge↔merger numbers corroborate (~2.1× left at the edge, ~2.58× total
on-disk recovered at the merger).

## 4. Query latency — thanos-query→merger vs thanos-query→MinIO (live stack)

Two isolated thanos-query instances (one `--endpoint=merger:10907`, one
`--endpoint=store-gateway:10901`), 100 and 1000 series, 6 query types.

- **Warm:** the two paths are statistically tied (differences within run-to-run
  noise; sign flips between queries). No consistent advantage for the merger's
  local TSDB read.
- **Cold (MinIO/store-gateway):** cold first-touch ≈ warm p50 — the store-gateway's
  index-header build + first chunk GET from co-located MinIO is single-digit-to-tens
  of ms, swamped by the TSDB chunk-decode + PromQL-eval cost both paths pay.
- **Correctness:** byte-identical values across both paths, all queries, both scales.

The merger-vs-MinIO latency divergence the design anticipates only appears at
large object counts / big index / network-distant S3 — not reproducible in a
single-host harness with one small block.

---

### Verdict

Both PR claims hold, with the framing the data forces: the decode-free ingest is
a large **CPU + allocation/GC** win (not a latency win — fsync-bound), and the
compaction delivers a real **1.2×–2.6× on-disk footprint** reduction (driven by
per-tiny-block index elimination) in the small-edge-chunk regime it targets. The
read paths are correctness-identical and latency-tied at the tested scale.
