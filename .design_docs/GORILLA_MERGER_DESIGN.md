# Gorilla Merger — design (Go, co-located; Thanos-Receive pattern)

Status: draft for review. Supersedes the GORILLA1 compactor direction AND the
earlier Model-A (finalizer-on-cut, accept-the-lag) variant.

## Goal

Cold/raw tier: agents emit compact, Gorilla-XOR-compressed fragments; the
backend merges ~1min agent chunks into large **2h Prometheus TSDB blocks** with
**one S3 PUT per block** (minimize S3 insert cost), serves the **pending <2h
window** for fallback exact queries, and lets queries **union** pending + S3.
Small CPU/mem/disk where possible.

## Decisions

- **Final S3 format = Prometheus TSDB blocks** (Thanos-queriable). Only proven
  TSDB writer is Go's `prometheus/tsdb`, so the merger is **Go, co-located**
  with the backend (Option #1; Rust/cgo rejected — no perf gain, hybrid-build
  cost).
- **Edge→merger wire = XOR-chunk fragments** (~1.3 B/sample) via a **shared
  binary codec in `github.com/ProjectASAP/asap-gorilla-go`** (both the edge
  encoder and merger decoder import it — single source of truth, no JSON/base64).
- **2h merge window, one PUT per block.** Thanos **Compactor is optional**
  (blocks are already 2h). Agents do NOT write blocks or PUT to S3.
- **Merger = Thanos-Receive pattern** (this is the load-bearing decision, set by
  the requirement to query the pending window): an embedded `tsdb.DB` (2h block
  range + WAL) fed by a fragment HTTP frontend, exposed as a **Thanos StoreAPI**
  via `thanos store.TSDBStore`, with a `thanos shipper` uploading cut blocks to
  S3. "Thanos Receive with an XOR-fragment frontend instead of remote-write."
- **Union via thanos-query.** thanos-query fans out to the merger's StoreAPI
  (<2h pending) + thanos-store-gateway (≥2h S3). The backend's existing
  `ThanosQueryEngine` (HTTP-forward to thanos-query) is UNCHANGED.
- **GORILLA1 is superseded** and slated for deletion (the codebase's own "Phase
  δ" legacy-leg removal; task #30).

## Topology

```
EDGE (OTel collector, asap_edge cold)          BACKEND merger (NEW Go binary, co-located)
─────────────────────────────────────          ──────────────────────────────────────────
asap_edge shared decode + key (per dp)
  └─ cold: StreamingFragmentEncoder
       raw -> XOR chunk (chunkenc)
       watermark-bounded, no index/S3
  └─ ship via shared binary codec  ──HTTP──▶    POST /ingest/gorilla
       (gorilla.EncodeFragmentBatch, gzip)        └─ gorilla.DecodeFragmentBatch
                                                   └─ tsdb.DB.Appender().Append(labels,t,v)
                                                 tsdb.DB: 2h head + WAL  (the pending window)
                                                 store.TSDBStore ── Thanos StoreAPI (gRPC) ◀─┐
                                                 shipper: cut 2h block -> ONE PUT -> S3      │
                                                                                  │          │
thanos-query  fan-out + union ───────────────────────────────────────────────────│──────────┘
   ├─ merger StoreAPI         (recent, <2h pending)                               │
   └─ thanos-store-gateway ── S3 TSDB blocks (≥2h) ◀──────────────────────────────┘

ASAP fallback PromQL ─▶ ThanosQueryEngine (forward.rs, UNCHANGED) ─▶ thanos-query ─▶ union
```

## Wire contract (shared codec, task #31)

Add to `asap-gorilla-go` (imported by both repos):
`EncodeFragmentBatch([]Fragment) []byte` / `DecodeFragmentBatch([]byte)
([]Fragment, error)`. Binary frame (HTTP POST body, `Content-Encoding: gzip`):

```
magic "ASAPFRG1" (8B) | version u8 | fragment_count uvarint
repeat:
  metric_name      : uvarint len + bytes
  label_count      : uvarint
  repeat: name (uvarint len+bytes), value (uvarint len+bytes)   # sorted by name
  min_time_ms      : varint (zigzag)
  max_time_ms      : varint (zigzag)
  sample_count     : uvarint
  encoding         : u8 (0 = XOR / chunkenc.EncXOR)
  chunk_len        : uvarint
  chunk_bytes      : [chunk_len]   # the raw chunkenc XOR chunk
```

No base64, no JSON (drops +33% + reflection). Replaces the OTLP-attribute
`MarshalFragment` smuggling.

## Edge (Track 1, task #29)

- `asap_edge` cold tier swaps `StreamingTSDBBlockBuilder` → `gorilla.
  StreamingFragmentEncoder` (raw → `chunkenc` XOR chunks on a watermark; no
  index, no S3). Reuse the fused shared decode + `gorilla.SeriesKey`.
- Replace `blockShipper` (tars a TSDB block) with a **fragment shipper**:
  `gorilla.EncodeFragmentBatch` → gzip → `POST cold.ship_endpoint`
  (`/ingest/gorilla`). Batched per flush tick; ret/backoff as today.
- Edge stays cheap: bounded OOO buffer, no block build, no S3.

## Merger (Track 2, task #26) — Thanos-Receive style

New Go module in the backend repo (own `go.mod`, imports `asap-gorilla-go` +
`prometheus/prometheus/tsdb` + `thanos-io/thanos`). Proposed `gorilla-merger/`
with `cmd/gorilla-merger/main.go`.

### Ingest
- `POST /ingest/gorilla`: gunzip → `gorilla.DecodeFragmentBatch` → for each
  fragment, `chunkenc.FromData(EncXOR, data)` → iterate samples → `appender.
  Append(labels, t, v)`. 200 after the appender commits (WAL durable).
- Cross-agent fan-in: same labels (incl. agent/source external label) → same
  series in the DB. Distinct agents carry a distinguishing external label.

### Open-window store + StoreAPI (the union mechanism)
- `tsdb.Open(dir, …)` with `MinBlockDuration = MaxBlockDuration = 2h` → the head
  holds the pending window; WAL = durability + bounded mem.
- `store.NewTSDBStore(db, externalLabels, component)` → serve the **Thanos
  StoreAPI** over gRPC. Register this endpoint in **thanos-query**'s store list
  (task #32) so it fans in alongside store-gateway.

### Cut → ship (one PUT/block)
- Head auto-compacts at the 2h boundary → block on local disk.
- `shipper.New(…, bucket, …)` watches the dir, uploads each new block to S3
  (one PUT set per block: chunks + index + meta.json) with the **Thanos
  `thanos{}` meta** (shipper writes it). Local retention short — drop blocks
  once shipped + store-gateway has them.

### Cost levers
- CPU: decode→append (XOR→samples; centralized). No agent-side index build.
- Mem: 2h head (in-mem chunks) + WAL — conservative vs Receive's 2h default.
- Disk: WAL + the not-yet-shipped block(s); bounded by ship cadence + retention.
- S3: ONE PUT per 2h block (the explicit goal).

## Read path — UNCHANGED in the backend

`ThanosQueryEngine` (`data_plane/.../thanos_query_engine/forward.rs`) HTTP-
forwards archive PromQL to thanos-query (`ASAP_THANOS_QUERY_URL`, default
`http://thanos-query:10903`). thanos-query unions merger-StoreAPI + store-
gateway. The legacy `GorillaS3Store` (GORILLA1) leg is deleted in Phase δ (#30).
Recency for warm/sketch queries is still served by the warm tier; the freshness
probe cache is unaffected.

## Cleanup (Track 3)
- Rewire `main.rs` archive selection to Path A2 (set `ASAP_THANOS_QUERY_URL`);
  delete legacy `GorillaS3Store` leg (#30).
- Delete GORILLA1: Go encoder (`gorilla.go` Build*/EncodeSeriesBody + bit
  encoders), Rust store/decoder, abandoned `compactor.rs`, stale strings (#30).
- Retire `gateway_fragment` OTel role (#27).

## Deploy additions
- thanos-query store list += merger StoreAPI endpoint (#32).
- merger S3 bucket = the bucket thanos-store-gateway watches.

## Non-goals
- No change to warm tier (sum + sketches) output.
- No downsampling in the merger (Thanos Compactor owns it, and is optional).
