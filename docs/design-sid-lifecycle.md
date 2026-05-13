# Sid lifecycle across asapcollector and asapquery-backend

**Status:** in effect since PR #190 + PR #192 (May 2026). Replaces the
prior content-addressed `compute_sketch_sid` / `compute_sid` model.

This doc describes how a **series identifier (sid)** — the compact u64
the backend uses internally to key per-series state — is minted,
distributed, persisted, and recovered as data flows from the
asapcollector through the asapquery-backend. It also discusses how to
extend the design to a multi-shard backend without breaking the
identity contract.

---

## 1. Identity contract

> **`sid = registry-allocated u64 for the triple (metric_name,
> attrs_fingerprint, agg_kind_canonical)`**

- **Authority:** exactly one process — the backend's `SeriesIdResolver`
  — mints sids. `AtomicU64::fetch_add(1)` gives uniqueness by
  construction, not as a probability.
- **Wire shape:** `u64`. `0` is reserved for "unresolved" on the wire.
- **Bandwidth optimization:** once a sender caches a sid, subsequent
  emits for the same series omit the `attributes` field on the wire
  and send the sid alone — the receiver disambiguates `sid → (metric,
  attrs, agg_kind)` through its own cache.
- **Identity granularity:** different aggregations over the same
  series get **different** sids. A DDSketch and a Sum on
  `http_latency_ms{zone=z0}` mint two distinct sids because their
  `agg_kind_canonical` strings differ. This matches the historical
  identity model `compute_sketch_sid` enforced via its 4-input hash.
- **Stability:** for the lifetime of a resolver's cache (in-memory +
  WAL), the same triple always returns the same sid. Across resolver
  resets (no persistence, or fresh deployment) sids are NOT stable —
  recovery is automatic via the `unknown_series_ids` eviction
  primitive (§4).
- **Determinism across hosts:** the model is **deliberately not**
  host-independent today. Each backend mints its own sid space. §5
  discusses extending to a deterministic distributed scheme.

The `agg_kind_canonical` string is produced by
`AggKind::canonical_string()` in
`data_plane/src/storage_engines/sketch_db/data/mod.rs`. Stable form:

```
sketch:<kind>:<config>          e.g.  sketch:DDSketch:D:0.01
                                       sketch:Kll:K:200
                                       sketch:Hll:H:14
precompute:<agg_type>:<params>  e.g.  precompute:Sum:
                                       precompute:DatasketchesKLL:k=200;
```

Two `AggKind` values that compare equal MUST produce the same
canonical string; two that differ in any observable parameter MUST
produce different strings.

## 2. Component responsibilities

```
                 asapcollector                              asapquery-backend
   ─────────────────────────────────             ──────────────────────────────────────────

   Patched OTel-Go exporter                       drivers/ingest/otel.rs
     • per-tenant dictionary                        • OTLP gRPC + HTTP receivers
     • canonical_attrs_fingerprint                  • per-DP wire dispatch (§3)
     • cache hit → omit attrs
     • cache miss → send attrs                    drivers/ingest/series_resolver.rs
                                                    • SeriesIdResolver (single mint)
   Gateway (when present)                           • FilePersistence (WAL "ASAPSRP\x02")
     • transparent forwarder for sid-bearing
       DPs (same sid passes through)              storage_engines/sketch_db/index/mod.rs
     • for rollup-output identities, the            • SketchStore
       gateway calls back to the backend's              instances: sid → SketchInstanceMetadata
       SeriesIdResolver (same flow as the              series:    sid → SidStoreData (columnar)
       agent — it's just a hop closer)                 classify(sid) → Hit / Ghost / Unknown

                                                  precompute_engine/output_sink.rs
                                                    • SketchStoreSink (live precompute output)
                                                    • mints sid via resolver, writes to SketchStore

                                                  storage_engines/sketch_db/backfill/processor.rs
                                                    • BackfillWindowProcessor
                                                    • mints sid via resolver, writes to SketchStore

                                                  query_engines/asap_query_engine/engine.rs
                                                    • ASAPQueryEngine (warm-tier reads)
                                                    • discovers sids via
                                                      SketchStore::instances_matching(metric, gbk)
```

The fingerprint algorithm is shared: both sides compute
`canonical_attrs_fingerprint(attrs) = sort_by_key(attrs).join("k=v;")`,
defined in `data_plane/src/drivers/ingest/series_resolver.rs` and
mirrored in
`opentelemetry-go-patch/.../internal/series/dictionary.go::attributesFingerprint`.

## 3. End-to-end data flow

### 3.1 Architecture diagram

```
                ASAPCOLLECTOR                                          ASAPQUERY-BACKEND
   ────────────────────────────────────                     ──────────────────────────────────────────

   Patched OTel-Go exporter
   ┌─────────────────────────────────┐                       ┌────────────────────────────────────────────┐
   │ per-tenant sid dictionary       │                       │ drivers/ingest/otel.rs                     │
   │ HashMap<(metric, attrs_fp),sid> │     gRPC Export       │ ┌────────────────────────────────────────┐ │
   │                                 │  ───────────────────▶ │ │ SeriesIdResolver  (single mint auth)   │ │
   │ fp = canonical_attrs_           │  resource_metrics:    │ │                                        │ │
   │       fingerprint(attrs)        │   DPs with sids       │ │  next_sid : AtomicU64::fetch_add(1)    │ │
   │       sort by key, "k=v;..."    │   + optional attrs    │ │  cache    : DashMap<                   │ │
   │                                 │   + sketch bytes      │ │    (metric, fp, agg_kind_canonical),   │ │
   │ PER DATAPOINT:                  │                       │ │    u64                                 │ │
   │  hit  →  send (sid, ∅, bytes)   │     Export Reply      │ │  >                                     │ │
   │  miss →  send (0, attrs, bytes) │  ◀───────────────────  │ │                                        │ │
   │                                 │  series_assignments   │ │  resolve(m, fp, ak) -> sid             │ │
   │ ON REPLY:                       │  unknown_series_ids   │ │   idempotent; fsync-per-mint via       │ │
   │  cache new assignments          │                       │ │   FilePersistence  (WAL "ASAPSRP\x02") │ │
   │  evict unknown sids             │                       │ │   replay on open() restores cache      │ │
   │   → next emit will send attrs   │                       │ └─────┬──────────────────────────────────┘ │
   └─────────────────────────────────┘                       │       │ resolve(...)                       │
                                                             │       │                                    │
                                                             │   ┌───┴────────────┐                       │
                                                             │   ▼                ▼                       │
                                                             │ SKETCH INGEST   PRECOMPUTE INGEST          │
                                                             │ otel.rs:879     output_sink.rs +           │
                                                             │                 backfill/processor.rs      │
                                                             │ ak = AggKind::  ak = AggKind::             │
                                                             │  Sketch{         Precompute{               │
                                                             │   kind,           agg_type,                │
                                                             │   config}         params}                  │
                                                             │ .canonical_     .canonical_                │
                                                             │  string()        string()                  │
                                                             │ "sketch:        "precompute:Sum:"          │
                                                             │  DDSketch:                                 │
                                                             │  D:0.01"                                   │
                                                             │       │                │                   │
                                                             │       └────────┬───────┘                   │
                                                             │                ▼                           │
                                                             │ ┌──────────────────────────────────────┐   │
                                                             │ │ SketchStore  (sid → state)           │   │
                                                             │ │  instances : HashMap<sid, MetaT>     │   │
                                                             │ │  series    : DashMap<sid, SidStoreT> │   │
                                                             │ │  classify(sid) → Hit/Ghost/Unknown   │   │
                                                             │ └──────────────┬───────────────────────┘   │
                                                             │                │ query                     │
                                                             │ ┌──────────────▼───────────────────────┐   │
                                                             │ │ ASAPQueryEngine  (warm tier)         │   │
                                                             │ │  analyze_promql →                    │   │
                                                             │ │  WarmTierCandidate(metric, gbk, cap) │   │
                                                             │ │  → instances_matching → [sids]       │   │
                                                             │ │  → SketchReducer.evaluate            │   │
                                                             │ └──────────────────────────────────────┘   │
                                                             └────────────────────────────────────────────┘
```

### 3.2 Wire cases (per DP inside an Export)

```
   sid     attrs    backend action                              backend reply
   ─────   ─────    ──────────────────────────────────────      ──────────────────────────────────────────
   0       set      resolver.resolve(m, fp, ak)  → mint/hit     series_assignments += {fp → sid}
   ≠0      set      resolver.resolve → s                        series_assignments += {fp → s};
                    if sender_sid ≠ s →  stale                  if sender_sid ≠ s:  unknown += [sender_sid]
   ≠0      none     SketchStore.classify(sid):
                      Hit   → append sketch_bytes               (no reply for this DP)
                      else  → drop, signal sender               unknown += [sid]
   0       none     invalid wire shape                          drop
```

### 3.3 Backend-internal sources of sid mints

| Path | File | When | `agg_kind` |
|---|---|---|---|
| OTel sketch ingest | `drivers/ingest/otel.rs::route_modified_otlp_sketches_to_precompute` | gRPC/HTTP Export with sketch DPs | `Sketch { kind, config }` |
| Live precompute output | `precompute_engine/output_sink.rs::SketchStoreSink::append_to_index` | Window completed by `PrecomputeEngine` worker | `Precompute { agg_type, params }` |
| Backfill window write | `storage_engines/sketch_db/backfill/processor.rs::BackfillWindowProcessor::process_window` | Replay from archive (Prometheus / S3 source) | `Precompute { agg_type, params }` |

All three paths receive an `Arc<SeriesIdResolver>` at construction and
call `resolver.resolve(metric, fp, agg_kind_canonical)` to mint.
`SketchStore::ingest_precompute_for_agg_config` takes the mint
authority as a closure parameter so the storage layer stays free of
any layer-inverted dependency on `drivers::ingest`.

## 4. Failure recovery

The single recovery primitive is `unknown_series_ids` in the
`ExportMetricsServiceResponse`. The collector evicts any sid listed
there; the next Export sends the corresponding attrs; the resolver
re-mints (or cache-hits) and replies via `series_assignments`. The
same primitive handles every cache-divergence failure mode.

### 4.1 Sequence: cold start (collector cached, backend never seen this sid)

```
   Collector                            Backend
       │                                    │
       │  Export DP{ sid=42, attrs=∅ }      │   (collector has cached sid 42)
       │ ──────────────────────────────────▶│
       │                                    │   SketchStore.classify(42) = Unknown
       │                                    │   resolver.cache has no (m, fp, ak)→42
       │                                    │
       │  Reply: unknown_series_ids = [42]  │
       │ ◀──────────────────────────────────│
       │                                    │
   evict 42                                 │
       │                                    │
       │  Export DP{ sid=0, attrs=...,      │
       │             sketch_bytes = ... }   │
       │ ──────────────────────────────────▶│
       │                                    │   resolver.resolve(m, fp, ak)
       │                                    │     → fresh mint: sid = 1
       │                                    │   SketchStore.register({ sid:1, ... })
       │                                    │   SketchStore.append_sample(1, ...)
       │                                    │
       │  Reply: series_assignments=[(fp,1)]│
       │ ◀──────────────────────────────────│
       │                                    │
   cache (fp → 1)                           │
   (next emit can omit attrs)               │
```

### 4.2 Sequence: stale sender sid (e.g. after a backend wipe + restart)

```
   Collector                            Backend
       │                                    │
       │  Export DP{ sid=99, attrs=..., }   │   (sender thinks 99 is right)
       │ ──────────────────────────────────▶│
       │                                    │   resolver.resolve(m, fp, ak) → 7
       │                                    │     (cache miss or hit on different sid)
       │                                    │   sender_sid (99) ≠ resolver_sid (7)
       │                                    │     → unknown += [99]
       │                                    │   SketchStore.register({ sid:7, ... })
       │                                    │   SketchStore.append_sample(7, ...)
       │                                    │
       │  Reply:                            │
       │   series_assignments = [(fp, 7)]   │
       │   unknown_series_ids = [99]        │
       │ ◀──────────────────────────────────│
       │                                    │
   evict 99                                 │
   cache (fp → 7)                           │
```

### 4.3 Sequence: backend restart **with** persistence (WAL replay)

```
   Before restart:
     • Collector cache:    (fp → 42)
     • Backend resolver:   (m, fp, ak) → 42  in cache
                           record  on disk in WAL
     • SketchStore:        instances[42] = MetaT{...}

   Backend restart:
     SeriesIdResolver::open(path) → replay WAL → cache restored,
                                                 next_sid = max_replayed + 1

   Collector                            Backend
       │                                    │
       │  Export DP{ sid=42, attrs=∅,       │
       │             sketch_bytes = ... }   │
       │ ──────────────────────────────────▶│
       │                                    │   SketchStore.classify(42) = Hit
       │                                    │   SketchStore.append_sample(42, ...)
       │                                    │
       │  Reply: (no signals)               │
       │ ◀──────────────────────────────────│
       │                                    │
   (no eviction, no extra round trip;       │
    sender's cache stays valid; WAL replay  │
    absorbed the restart transparently)     │
```

### 4.4 Sequence: backend restart **without** persistence

Same shape as §4.1 (cold start) for every active series. With M live
identities the collector pays one extra round trip per identity on
first post-restart emit — bandwidth blip, but no data loss.

### 4.5 Durability semantics

- `FilePersistence::append(sid, metric, fp, agg_kind_canonical)` calls
  `fsync` before returning. Callers are blocked until the record is on
  stable storage. The resolver only returns the sid to its caller
  after this returns successfully.
- A torn write at EOF (kernel buffered bytes but the metadata flush
  was interrupted) is detected at replay via short-read on any record
  field; the file is truncated to the last durable record's offset.
- WAL format v2: 8-byte magic header `b"ASAPSRP\x02"`, then a stream
  of records each shaped as `(u64 sid LE, u32 metric_len LE, metric
  utf8, u32 fp_len LE, fp utf8, u32 agg_kind_len LE, agg_kind_canonical
  utf8)`. Append-only; sids never get rewritten, so the log grows in
  proportion to live cardinality (~250-400 bytes per record).
- `xxhash_rust::xxh64` is no longer imported in
  `sketch_db::data` — content-addressed sid hashing is gone from the
  data plane entirely.

## 5. Scaling to distributed asapquery-backend

The single-backend design above relies on one `SeriesIdResolver`
process being the sole sid mint. To scale write or read throughput
beyond a single host, the system needs more than one backend. This
section sketches the extension; **none of it is implemented today**.

### 5.1 The two coordination problems

1. **Mint coordination.** When two backends each run a
   `SeriesIdResolver::resolve(m, fp, ak)` they will produce different
   sids — both correct in their own local namespace, but ambiguous to
   any agent that talks to both. Without a coordination protocol, sids
   are *not* globally unique.

2. **Query fan-out.** A query for `metric` needs to consult every
   backend that owns sids for that metric. The query path either
   knows the shard topology, or proxies through a coordinator that
   does.

### 5.2 Recommended sharding axis: `hash(tenant, metric)`

```
                  DISTRIBUTED ASAPQUERY-BACKEND  (sharded by tenant + metric)
                  ────────────────────────────────────────────────────────────

                                ┌───────────────────────────┐
                                │  Routing layer            │
                                │  (collector-side or       │
                                │   gateway-side)           │
                                │                           │
                                │  shard_id(metric,tenant)  │
                                │   = consistent_hash(...)  │
                                │   % N_shards              │
                                └────────────┬──────────────┘
                                             │
                ┌────────────────────────────┼─────────────────────────────┐
                ▼                            ▼                             ▼
        ┌───────────────────┐       ┌───────────────────┐         ┌───────────────────┐
        │ Backend shard 0   │       │ Backend shard 1   │   ...   │ Backend shard N   │
        │                   │       │                   │         │                   │
        │ SeriesIdResolver  │       │ SeriesIdResolver  │         │ SeriesIdResolver  │
        │   sid space:      │       │   sid space:      │         │   sid space:      │
        │   [0<<56,         │       │   [1<<56,         │         │   [N<<56,         │
        │     (1<<56)-1]    │       │     (2<<56)-1]    │         │     ((N+1)<<56)-1]│
        │                   │       │                   │         │                   │
        │ SketchStore       │       │ SketchStore       │         │ SketchStore       │
        │ WAL (per-shard)   │       │ WAL (per-shard)   │         │ WAL (per-shard)   │
        └─────────┬─────────┘       └─────────┬─────────┘         └─────────┬─────────┘
                  │                           │                             │
                  └───────────────────────────┴─────────────────────────────┘
                                              ▲
                                              │  PromQL  /  api/v1/query
                                              │
                                  ┌───────────┴────────────┐
                                  │  Query coordinator     │
                                  │                        │
                                  │  • Determine shard set │
                                  │    for the query's     │
                                  │    metric              │
                                  │  • Fan out             │
                                  │  • Per-shard reducer   │
                                  │  • Combine results     │
                                  └────────────────────────┘
```

The sharding key is `(tenant, metric)`, not `(tenant, metric, attrs)`.
A single metric's series live on one shard, so warm-tier queries that
need to merge sketches across attribute values stay local to one
backend. Cross-shard queries (e.g. spanning multiple metrics) need
the coordinator to fan out.

### 5.3 ID-space partitioning (no coordination on mint)

Reserve the **top 8 bits** of the u64 sid for `shard_id`. The bottom
56 bits are a per-shard local counter:

```
sid layout (u64, little-endian on wire):

  ┌────────────────┬───────────────────────────────────────────────────────┐
  │  shard_id : 8  │              local_counter : 56                       │
  └────────────────┴───────────────────────────────────────────────────────┘
                   ▲                       ▲
                   │                       │
                   │                       └──  next_sid: AtomicU64::fetch_add(1)
                   │                            with shard_id pre-OR'd in
                   │
                   └── assigned at backend boot from
                       deployment topology (e.g. statefulset ordinal,
                       Kubernetes Pod label, etcd lease)
```

Properties:

- **No coordination on the mint path.** Each backend's resolver runs
  the same `fetch_add` it does today; the `shard_id` is OR'd in.
- **Global uniqueness by construction.** Different shards cannot
  produce the same u64 because their top 8 bits differ.
- **Cap:** 256 shards × ~72 quadrillion sids each. Comfortable even
  for very large multi-tenant deployments.
- **Routing-by-sid is O(1):** `shard_id = (sid >> 56)`. The query
  coordinator extracts it and routes per-sid reducer queries to the
  owning shard without a lookup table.
- **Backend identity:** the `shard_id` must be stable across restarts
  for a given backend instance. Source it from a durable identity
  (statefulset ordinal + zone, or an etcd lease). A mismatch on
  restart would re-mint existing identities under a different `shard_id`
  → data loss; treat `shard_id` as part of the WAL header and refuse
  to open a WAL whose embedded id disagrees with the runtime id.

### 5.4 Query-side fan-out

Inside the query coordinator:

1. Parse PromQL via the existing control plane analyzer (single
   authority — see `control_plane/src/warm_tier_analysis.rs`).
2. Extract the candidate's `metric`. Compute `shard_set =
   sharder(tenant, metric)`. For most queries this is exactly one
   shard.
3. For each shard, issue a `SketchStore::instances_matching(metric,
   gbk) → [sids]` RPC. Backends respond with sids in their own range,
   plus the per-sid metadata the reducer needs.
4. Issue per-shard `SketchReducer.evaluate(sids, function, args, t0,
   t1)` RPCs in parallel. Each shard reduces locally; coordinator
   merges via the existing `combine_statistic` logic.
5. Stitch with archive via `EngineRouter` exactly as today
   (capability-miss → fail over to Thanos).

`series_assignments` returned to the collector contains sids whose top
8 bits identify the owning shard. The collector caches the binding as
opaque u64; subsequent emits for the same `(metric, fp)` always go to
the same shard because routing is keyed on `(tenant, metric)` not on
the sid value.

### 5.5 High-availability within a shard

Per-shard, three options:

| Approach | Cost | RPO | RTO |
|---|---|---|---|
| **Active / passive with shared WAL on S3** | Standby replays WAL from S3 on failover. | Up to `fsync` interval | Seconds |
| **Active / passive with EBS-snapshot WAL** | Standby attaches the EBS volume. | Zero (sync replica) | Tens of seconds |
| **Raft on the WAL** | Three replicas commit each `append` via Raft. | Zero (quorum durability) | Seconds |

The Raft option is the strongest but requires every mint to wait on a
quorum write — slower than today's local `fsync`. Active/passive on
S3 matches the pattern already used for the SketchStore's persistence
tier and is the natural first step.

### 5.6 Migration path from single-backend to sharded

A single-backend deployment maps cleanly to `N_shards = 1`,
`shard_id = 0`. The 8 high bits of every minted sid would be `0x00`,
which is what today's sids effectively look like (max u64 minted is
nowhere near `1<<56`). So:

1. **Now:** ship `shard_id` into the WAL header and the resolver's
   `next_sid` initialization, defaulted to `0`. No behavior change for
   single-backend deploys.
2. **Two-shard step:** bring up a second backend at `shard_id = 1`.
   Stand up a routing layer (collector-side first; gateway-side
   later) that hashes `(tenant, metric)` and forwards. Existing
   single-shard sids stay valid (top bits stay `0x00`); new mints on
   the second backend land in `[1<<56, …)`. Query coordinator fans
   out across both.
3. **N-shard:** scale horizontally. Each addition costs a config push
   to the routing layer and a fresh backend with a fresh `shard_id`.

The migration is fully online: no sid wire-format change, no
collector update required.

### 5.7 Cross-shard rebalancing

Resharding is the hard problem. Once series are minted on shard K,
moving them to shard K' requires re-minting (because `shard_id` is
embedded in the sid). Options:

- **Live drain + re-mint:** route all new emits for a `(tenant,
  metric)` to the new shard. Old shard's sids stay until their data
  TTLs out (matches the existing `SchemaEvictionService` shape — sids
  retire and expire on a clock).
- **Bulk re-emit:** force the collector to evict all sids for a given
  `(tenant, metric)` via an out-of-band signal (e.g. a control-plane
  message that pre-populates `unknown_series_ids` on the next
  Export). Costs a round trip per identity but completes in one
  refresh cycle.

Neither approach is implemented; the migration path in §5.6 doesn't
need them at first since single → sharded only adds shards, it doesn't
move existing series.

## 6. Open questions

1. **Should `ResolveSeriesIDs` RPC stay or go?** Today's `SeriesQuery`
   proto carries only `(metric, fp)` — under the new identity model
   the handshake can't produce the correct sid (no `agg_kind`). The
   RPC is currently stubbed to return empty `assignments` with a WARN
   log. Either drop it entirely (proto change), or extend
   `SeriesQuery` with `agg_kind_canonical`.

2. **WAL compaction.** The WAL grows in proportion to live cardinality
   (~25-40 GB at 100M sids). At what threshold is a snapshot + log
   truncation cycle worth scheduling? The append-only shape is fine
   for the foreseeable scale; the open question is operational, not
   structural.

3. **Multi-tenant isolation of `next_sid`.** Today's resolver shares
   one counter across all tenants. A noisy-tenant flood could exhaust
   the bottom 56 bits faster than steady-state cardinality suggests.
   Reserving a per-tenant subspace inside the bottom 56 bits would
   address this; the cost is reduced per-tenant headroom. Not urgent.

4. **Collector-side multi-shard cache.** When the system sharates, the
   collector's local dictionary `HashMap<(metric, fp), sid>` is still
   correct (the sid is opaque to the collector), but the routing
   layer needs to direct each emit to the right shard. Is the routing
   table colocated with the exporter or moved into the gateway? Latency
   vs. operational complexity trade-off.

5. **Cross-shard PromQL semantics.** Queries over `metric` that's
   sharded on `(tenant, metric)` stay local. Queries over multiple
   metrics fan out trivially. But aggregations like `sum by (...) (
   m_a + m_b )` (binary op across two metrics that live on different
   shards) need a strategy — coordinator-side join, or push-down by
   sketch combinability? Outside the sid lifecycle proper but
   relevant when adopting §5.

## 7. References

### Code

- Resolver + WAL: `data_plane/src/drivers/ingest/series_resolver.rs`
- OTel ingest path: `data_plane/src/drivers/ingest/otel.rs`
  (`route_modified_otlp_sketches_to_precompute`,
  `MetricsServiceImpl::export`, `MetricsServiceImpl::resolve_series_i_ds`)
- Precompute output: `data_plane/src/precompute_engine/output_sink.rs::SketchStoreSink`
- Backfill output: `data_plane/src/storage_engines/sketch_db/backfill/processor.rs::BackfillWindowProcessor`
- Storage layer: `data_plane/src/storage_engines/sketch_db/index/mod.rs::SketchStore`,
  `ingest_precompute_for_agg_config`
- Identity canonicalization: `data_plane/src/storage_engines/sketch_db/data/mod.rs::AggKind::canonical_string`
- Proto: `crates/asap_otel_proto/proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto`

### Related design docs

- `docs/design-controller-into-backend.md` §5.4 — original
  idempotency invariant for `ResolveSeriesIDs`.
- `docs/design-sketch-db.md` — SketchStore lifecycle, ghost sids,
  per-sid columnar storage.
- `docs/design-sketch-db-pluggable.md` — how `AggKind` discriminates
  sketch vs precompute payloads at the storage layer.

### Landed PRs implementing this design

- **#190** (commits 84abac7, 3ac90cf, f72b533) — registry-allocated
  sid + WAL persistence + identity contract on the OTel ingest path.
- **#192** (commit 9c8c614, merged as 8f923db) — migrate precompute
  output and backfill to the same resolver; delete `compute_sid` and
  helpers.
