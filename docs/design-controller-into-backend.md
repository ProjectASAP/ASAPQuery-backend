# Controller-into-backend refactor

> Status: design draft
> Author: drafted 2026-05-10 from a multi-node MVP sweep that surfaced
> the architectural mismatch between the agent-side metric-name
> rewrites (`_quantile`, `_topk`) and the "same PromQL, same metric
> name across all arms" comparison requirement.

## 1. Motivation

The current B0/B1/ASAP three-arm comparison fails apples-to-apples
because the ASAP arm answers a *different metric name* than B0/B1:

| arm     | metric name PromQL is fired against     | who emits this name |
|---------|------------------------------------------|---------------------|
| B0      | `http_requests_total_latency_ms`         | fake-exporter SDK   |
| B1      | `http_requests_total_latency_ms`         | fake-exporter SDK   |
| ASAP    | `http_requests_total_latency_ms_quantile`| `gateway-aggregate-from-raw.yaml`'s `metric_suffix: "_quantile"` rewrites the gauge name during sketch encoding |

A user firing the same PromQL — `quantile_over_time(0.99,
http_requests_total_latency_ms[30s])` — gets results from
B0/B1 (Prometheus runs the quantile over raw gauge samples) but
nothing from ASAP, because the ASAP backend's pattern matcher is
keyed on the suffixed name `..._quantile`.

The fix is not "add raw-name patterns to the matcher." The
fix is to remove the rewrites and design the backend so the
storage and query layers are keyed on the *raw metric name +
raw labels* that the SDK actually emits. The encoding (sketch
type, payload format) is a wire-level attribute on each sample,
not a name suffix.

## 2. Current architecture (what we change away from)

```
┌──────────────────────────────────────────────────────────────────┐
│ ASAPCollector repo                                              │
│                                                                  │
│  fake-exporter ──OTLP──▶ asap-otel agent                        │
│                            │ (sketch processors emit            │
│                            │  ..._quantile / ..._topk renamed   │
│                            │  metrics)                          │
│                            ▼                                    │
│                          gateway (sketch-merge)                 │
│                            │                                    │
│                            │                                    │
│  controller/ ──OpAMP──▶ agents (push per-metric sketch plan)    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                              │ OTLP w/ renamed metrics
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ ASAPQuery-backend repo                                          │
│                                                                  │
│  asap-query-engine                                              │
│    SimpleStore                                                  │
│      indexed by aggregation_id (integer assigned by             │
│        StreamingConfig::from_yaml_data on ingest)               │
│    SimpleEngine query path                                      │
│      PromQL → find_query_config (exact pattern match)           │
│        ├─ hit: dispatch to aggregation_id                       │
│        └─ miss: capability_matching by Statistic::{Sum,         │
│                Quantile, etc.}                                  │
│                ├─ hit: dispatch to compatible aggregation       │
│                └─ miss: fall through to BackendStorageRouting   │
│                    ├─ shape ∈ [count, topk, rate_post_hoc]      │
│                    │    → ThanosForwardEngine                   │
│                    └─ else → SimpleEngine (404 if miss)         │
│                                                                  │
│ asap-common/                                                    │
│   asap_types (StorageBackend, AggregationCapability, …)         │
│   promql_utilities, datafusion_summary_library                  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

Three structural problems:

1. **Metric names are mutated mid-pipeline**, breaking same-PromQL
   comparison across arms.
2. **Two-repo split with shared `asap-common`**: type changes need
   coordinated PRs in both repos.
3. **Backend's query layer is plan-blind**: pattern matching at query
   time is a cache for the controller's plan that nobody primed; when
   the cache misses, the matcher reasons from `Statistic::*`
   capabilities that don't carry the controller's intent.

## 3. Target architecture

```
┌──────────────────────────────────────────────────────────────────┐
│ ASAPCollector repo (edge-only after refactor)                   │
│                                                                  │
│  fake-exporter / SDK  ──OTLP──▶ asap-otel agent                 │
│                                   │  sketch processors emit     │
│                                   │  the SAME metric name as    │
│                                   │  input; payload shape is    │
│                                   │  sketch-typed (DDSketch /   │
│                                   │  KLL / HLL / CMS / CS) but  │
│                                   │  metric.Name is unchanged.  │
│                                   ▼                             │
│                                gateway (sketch-merge,           │
│                                          name-preserving)       │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                                   │ OTLP w/ raw metric names
                                   │ (sketch payloads attached)
                                   ▼
┌──────────────────────────────────────────────────────────────────┐
│ ASAPQuery-backend repo (controller + sketch store + query)      │
│                                                                  │
│  controller/                                                     │
│    L1 query_language → … → L5 stage_split                       │
│    capability map: (metric_name, query_shape) → sketch_kind     │
│    OpAMP server (originating from this host, pushes to agents)  │
│                                                                  │
│  asap-query-engine                                              │
│    PrecomputeEngine                                             │
│      receives OTLP (sketch payloads w/ raw metric names)        │
│      may merge sketches across agents — otherwise pass-through  │
│    SimpleStore                                                  │
│      key: (raw_metric_name, raw_labels, capability_set)         │
│      value: sketch state (encoded per the controller's plan)    │
│    Query path                                                   │
│      PromQL parse                                               │
│      → controller.capability_for(metric, query_shape)           │
│        ├─ Some(capability):                                     │
│        │     SimpleStore.get(metric, labels, capability)        │
│        │       ├─ hit: return sketch result                     │
│        │       └─ miss: forward to Thanos (raw fallback)        │
│        └─ None (controller doesn't plan this query):            │
│              forward to Thanos                                  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                                   │ raw counter samples (Gorilla
                                   │  XOR compressed, original    
                                   │  metric names preserved)     
                                   ▼                              
                              MinIO / S3                          
                              (Thanos store-gateway sees these as
                              TSDB blocks and answers PromQL via
                              thanos-query)                       
```

Key design rules:

- **No metric-name rewriting anywhere in the data path.** The name
  the SDK emits is the name on the wire is the name in the backend
  index is the name the user fires PromQL against. Wire encoding
  type (raw counter / DDSketch / KLL / …) lives in the OTLP
  pdata.Metric variant tag, not the name.
- **MinIO holds raw samples only.** Gorilla-XOR-compressed for
  efficiency, but no sketch state, no aggregations, no merged
  outputs. Thanos store-gateway → thanos-query exposes them as
  Prometheus-compatible TSDB blocks.
- **Warm tier holds all sketch state.** Indexed by `(metric_name,
  labels, capability_set)`. Capabilities come from the controller's
  per-metric plan.
- **Controller is in-process with the query engine.** No more
  pattern matching at query time — the controller's capability map
  IS the query dispatch table.
- **OpAMP server lives on the backend host.** It pushes plans down
  to agents. Agents only need a way to reach the backend for OpAMP
  + OTLP; they don't have their own controller dependency.

## 4. Phased migration plan

Each phase is a minimum-merge unit: the system builds and the
multi-node demo passes after each phase, with progressively more
of the new design landed.

### Phase 1 — Strip metric-name rewrites (1–2 hours)

Audit every site that suffixes a metric name and remove the suffix.
Keep the wire-level encoding hint in the OTLP pdata variant.

**Files to inspect and patch:**

- `deploy/configs/gateway-aggregate-from-raw.yaml` — currently
  `metric_suffix: "_quantile"`. Remove or set to empty.
- `opentelemetry-collector-contrib-patch/processor/{ddsketch,kll,
  hll,countsketch,countminsketch}processor/` — search for
  any name-suffix logic in `processor.go` / `factory.go`.
- `asap-precompute-go/` and `asap-precompute-rs/` — same audit.
- `gorillas3processor/processor.go` — verify it preserves the input
  name when writing TSDB blocks.

**Acceptance test:** ASAP backend, after one full window, has
`http_requests_total_latency_ms` (raw) in its store, NOT
`..._quantile`. Verified via `curl asap-backend:9091/api/v1/series`
or by reading the OTLP wire bytes.

**Risk:** breaks `backend-inference.yaml` patterns (which are
keyed on `_quantile`). Phase 2 replaces pattern matching anyway,
so this is intentional.

### Phase 2 — Backend routing: warm-miss → Thanos (30 min)

`backend-storage-routing.yaml` currently routes by query shape:
`[count, topk, rate_post_hoc]` to archive, everything else to
warm. Change semantics: try warm first; on miss (no aggregation
matches the metric+labels+capability), fall through to Thanos.

**Files:**

- `asap-query-engine/src/routing/backend_storage_routing.rs` —
  swap "shape allow-list" for "warm-first, archive-fallthrough".
- `asap-query-engine/src/routing/query_engine_routing.rs` (EngineRouter) — add
  a `query_with_fallthrough` path.

**Acceptance test:** Same PromQL `count(http_requests_total)` and
`sum_over_time(http_requests_total[1m])` works on B0, B1, and
ASAP. ASAP's response carries `data_source: thanos_archive` for
queries that fall through, `data_source: warm` for those that
hit a sketch.

### Phase 3 — Reindex SimpleStore by `(metric_name, labels, capability)` (2–3 days)

Today's `SimpleStore.get_aggregation(aggregation_id: u64)` becomes
`SimpleStore.get(metric_name: &str, labels: &LabelSet, capability:
&Capability)`. Streaming-config ingest no longer assigns integer
IDs; it stores under the natural tuple.

**Files:**

- `asap-query-engine/src/stores/sketch_db/simple_map_store/{mod,
  per_key,common_state}.rs` — refactor key type.
- `asap-query-engine/src/streaming_engine.rs` — ingest path: when
  an OTLP sketch sample arrives with `(metric_name, labels)`, look
  up its capability from the in-process controller's plan and
  store under that triple.
- `asap-types/src/capability_matching.rs` — make `Capability` the
  index key type (probably a small enum + sketch-family tag).

**Acceptance test:** Backend's `/internal/store-dump` endpoint
shows entries like
`{metric: "http_requests_total_latency_ms", labels: {...},
capability: "QuantileApprox(DDSketch)"}` instead of integer
aggregation IDs.

**Migration risk:** Streaming-config YAML format changes
(`backend-streaming.yaml` no longer needs `aggregationId`). All
downstream tests under `asap-query-engine/tests/` need updating.

### Phase 4 — Move `controller/` from ASAPCollector to ASAPQuery-backend (3–5 days)

Physically relocate the crate. Both are Rust, both already use
prost-build for OTel proto compilation, so the build surface is
compatible.

**Steps in order:**

1. Copy `ASAPCollector/controller/` → `ASAPQuery-backend/controller/`.
2. Add `controller` to ASAPQuery-backend's Cargo workspace; remove
   from ASAPCollector's.
3. ASAPQuery-backend's `asap-query-engine` Cargo.toml gets `controller
   = { path = "../controller" }`.
4. Backend binary embeds the controller's L4 `sketch_algebra` as a
   library call (no more HTTP capability-miss notifications between
   processes — same process now).
5. Move OpAMP server: `controller/src/opamp/` continues to listen,
   but the listening host is now the backend node, not a separate
   controller container. Update agent-config OpAMP endpoints from
   `ws://controller:4320/v1/opamp` to
   `ws://backend:4320/v1/opamp`.
6. Delete `ASAPCollector/controller/` after green CI.
7. Update `deploy/docker/Dockerfile.controller` to be a no-op
   (or delete) — the controller is no longer a standalone image.
   The `Dockerfile.backend` build context now includes
   `controller/`.

**Acceptance test:** Single backend container starts both the
query HTTP API (port 9091) and the OpAMP server (port 4320).
Agent connects to `ws://backend:4320/v1/opamp`, receives plan,
emits sketches, backend stores them. Same multi-node demo runs.

### Phase 5 — Delete `asap-common` (1–2 days)

Audit each crate under `asap-common/dependencies/rs/`:

- `asap_types`: most types are backend-internal — move into
  `asap-query-engine/src/types/`. The wire types (`SketchEnvelope`,
  `Statistic`) are shared with edge processors via OTLP proto, so
  no Rust-to-Go path-dep needed.
- `promql_utilities`: backend-only — move into
  `asap-query-engine/src/promql/`.
- `datafusion_summary_library`: backend-only — same.
- Anything Go-side actually used by edge processors (e.g.
  sketchlib-go interop) is already in `sketchlib-go` itself, not
  in `asap-common`.

**Steps:**

1. List every Rust file under `asap-common/dependencies/rs/`.
   Bucket into "backend internal" vs "wire shared".
2. Move "backend internal" files into `asap-query-engine` or
   `asap-types` (a renamed minimal types-only crate kept inside
   ASAPQuery-backend).
3. Delete `asap-common/`.
4. Update `Cargo.toml` path-deps in both ASAPCollector and
   ASAPQuery-backend.

**Acceptance test:** ASAPCollector's `cargo build --release`
runs without referencing `asap-common`. ASAPQuery-backend's
`cargo build --release` produces the backend binary.

## 4.5 Capability model — what the backend index keys on

The controller's capability map and the backend's SimpleStore index
share one type:

```rust
enum Capability {
    QuantileApprox(SketchKind),    // SketchKind ∈ {DDSketch, KLL}
    CardinalityApprox,             // single sketch family: HLL
    FrequencyTopk(SketchKind),     // SketchKind ∈ {CountMin, CountSketch}
    // SumOverTime, RateOverTime — answered from raw counter via Thanos,
    // not via warm-tier sketches; no Capability variant needed.
}
```

`CountMin` vs `CountSketch` are statistically distinct (CMS biased/cheap,
CS unbiased/slightly more expensive) but answer the SAME PromQL family
(`topk`, `frequency_of`). Treating them as alternative `SketchKind`
implementations of the same `Capability::FrequencyTopk` lets the
controller choose at plan time without affecting query routing.

### Group-by labels are part of the index key

A query like `topk(10, sum by (zone, service) (http_requests_total))`
folds away every label except `{zone, service}` at sketch ingest time.
The remaining label set IS the group-by keys. Backend's index entry:

```
key: (raw_metric_name="http_requests_total",
      group_by={"zone", "service"},
      capability=FrequencyTopk(CountMin))
value: CMS state — d×w cell matrix keyed by hash(zone||service)
```

A single metric can have many entries — one per
`(group_by_set, capability)` tuple the controller's plan covers:

| (metric, group_by, capability)                                | sketch  |
|---------------------------------------------------------------|---------|
| (`http_requests_total`, `{zone}`, FrequencyTopk(CountMin))    | CMS-1   |
| (`http_requests_total`, `{service}`, FrequencyTopk(CountMin)) | CMS-2   |
| (`http_requests_total`, `{zone}`, QuantileApprox(DDSketch))   | DD-1    |
| (`http_requests_total_latency_ms`, `{zone}`, QuantileApprox(DDSketch)) | DD-2 |

Query path: parse PromQL → derive `(metric, group_by, capability)` →
SimpleStore.get() → hit (return sketch eval) or miss (forward raw to
Thanos).

## 4.6 OTLP metadata model + backend store layout

### What to transmit (proto patch direction)

Today's per-DataPoint fields duplicate state the sketch payload itself
encodes (count/sum/min/max for DD/KLL, cardinality for HLL,
sample_count for CMS). They also repeat sketch-instance config on every
DP (epsilon/delta on CS, rows/cols on CMS, precision on HLL). Both are
wasteful and create cache-invalidation bugs.

Phase 1.5 proto patch (metrics.proto):

- **Drop precomputed values from each `*DataPoint` message**: count,
  sum, min, max, cardinality, sample_count. The sketch payload (or its
  msgpack-encoded state) is the source of truth.
- **Lift per-instance sketch config from DataPoint up to the parent
  sketch message**:

```protobuf
message DDSketch {
  repeated DDSketchDataPoint data_points = 1;
  AggregationTemporality aggregation_temporality = 2;
  double relative_accuracy = 3;            // DDSketch α
}
message KLLSketch        { … uint32 k = 3; }
message HLLSketch        { … uint32 precision = 3; }
message CountSketch      { … int32 rows = 3; int32 cols = 4; }   // depth, width
message CountMinSketch   { … int32 rows = 3; int32 cols = 4; }   // depth, width
```

DataPoint messages keep only the per-window mutable state:
`attributes`, `start_time_unix_nano`, `time_unix_nano`, `sketch`
(payload bytes), `encoding` (PROTO/MSGPACK ± DELTA), `flags`.

### Backend store layout — two-level index

```
SketchStore
├─ instances : HashMap<(metric_name, group_by_keys, Capability), SketchInstanceMetadata>
└─ series    : HashMap<instance_id, Vec<SketchTimeSeries>>

SketchInstanceMetadata {
    metric_name: String,                       // raw input name
    group_by_keys: BTreeSet<String>,           // surviving label KEY set (not values)
    capability: Capability,                    // derived from sketch_type
    sketch_type: SketchKind,                   // DDSketch/KLL/HLL/CMS/CS
    sketch_config: SketchConfig,               // ε, δ, k, precision, rows, cols (enum per kind)
    accuracy: AccuracyBound,                   // derived: (eps_relative, conf_1-δ)
    first_seen_ts: i64,
}

SketchTimeSeries {
    instance_id: u64,                          // FK
    series_label_values: BTreeMap<String,String>,  // group-by VALUES per series
    samples: BTreeMap<TimestampMs, SketchState>,    // window_end → bytes + encoding
}
```

### Ingest mapping (OTLP → store)

| OTLP source                                        | store target                              |
|----------------------------------------------------|-------------------------------------------|
| `Metric.name`                                      | `instance.metric_name`                    |
| `Metric.data_case` (oneof tag)                     | `instance.sketch_type` → `Capability`     |
| Parent sketch container's config fields            | `instance.sketch_config`                  |
| `dp.attributes.keys()`                             | `instance.group_by_keys` (sorted)         |
| `dp.attributes`                                    | `series.series_label_values`              |
| `dp.time_unix_nano`                                | `series.samples` map key                  |
| `dp.sketch + dp.encoding`                          | `series.samples` map value                |

### Query mapping (PromQL → store lookup)

```
parse PromQL  →  (metric_name, query_function, group_by_keys_requested)
capability    =  Capability::for_function(query_function)
                 (e.g. quantile_over_time → QuantileApprox)
instance      =  instances.get((metric_name,
                               group_by_keys_requested,
                               capability))
match instance:
    Some(inst) →  decode payload(s) from inst.sketch_type, evaluate
                  via inst.sketch_config (uses ε/δ/precision/k correctly)
    None       →  forward query to Thanos archive
```

### group_by_keys: implicit via `DataPoint.attributes` (decided)

`DataPoint.attributes` IS the OTel term for what Prometheus calls
"labels" — the dimensional identity of a data point. After the agent's
`AggregateBy` rollup at sketch-insert time:

- All non-group-by label values are folded INTO the sketch state.
- `dp.attributes` reflects only the surviving group-by dimensions.
- Therefore `dp.attributes.keys()` = group-by KEY set (= store
  instance index component); `dp.attributes` (as map) = group-by
  VALUES for the per-series record.

This matches stock-OTel and Prometheus semantics for `sum by (zone)`
— the resulting series carries only `zone`. Zero proto change.

**Convention contract:**

1. Agent processor MUST strip non-group-by labels via the `AggregateBy`
   config before emitting the sketch DP.
2. Backend trusts `dp.attributes.keys()` as the group-by KEY set when
   building the SketchInstanceMetadata index entry.
3. **Convention violations are correctness-preserving but inefficient.**
   A processor that leaks extra labels causes the backend to perceive
   a larger group-by set → more SketchInstanceMetadata entries than
   intended (one per leaked-label combination). Query semantics stay
   correct (each instance answers its own group-by); the cost is RAM
   from over-fragmenting the index.

## 5. Open questions

- **OpAMP origination host**: the runbook's compose currently has
  `controller:4320` and `backend:9091` as distinct services. After
  Phase 4 they collapse to one container. Do we keep two ports
  (4320 OpAMP + 9091 query) or unify?
- **Stale `_quantile` patterns in `backend-inference.yaml`**: do
  we keep this file at all after Phase 3 (no more pattern
  matching), or repurpose as the controller's bootstrap plan?
- **Edge-runtime parity**: `asap-precompute-rs` (used by the
  Rust edge agent path) currently uses some `asap-common` types.
  Phase 5 needs to leave a thin wire-types crate accessible to
  edge runtimes — name it `asap-wire-types` and put it in
  ASAPCollector? Or in a third repo?
- **`_topk` and similar suffixes**: Phase 1 removes `_quantile`.
  Are there other suffixes (`_topk`, `_uniques`, `_count`) added
  by other processors? Need to grep more thoroughly.

## 5.4 Series-ID namespace — centralized via asap-query-backend (decided)

The patched OTLP protocol already supports series_id minting +
SeriesAssignment response (`opentelemetry-go-patch/exporters/otlp/otlpmetric/otlpmetricgrpc/`
+ `opentelemetry-collector-patch/receiver/otlpreceiver/internal/metrics/series_cache.go`).

Today's implementation is **per-hop**: each receiver in the chain
(agent's OTLP receiver, gateway's OTLP receiver, backend's OTLP
receiver) mints its own series_ids in its own namespace. Same series
gets registered 3× across (fake-exporter→agent, agent→gateway,
gateway→backend) and the namespaces don't share meaning.

**New design — centralize series_id minting at asap-query-backend:**

- The backend (which now also hosts the controller — Phase 4) is the
  single authoritative minter of series_ids.
- Agents and gateway DO NOT mint their own series_ids. They forward
  the original Export upstream, propagate the SeriesAssignment
  response back downstream, and cache the (metric, attrs_fp) →
  sid_GLOBAL mapping.
- Once an agent has cached a sid for a given series, all subsequent
  Exports use sid_GLOBAL directly. The gateway sees sid != 0 and
  forwards verbatim to backend without any cache lookup of its own.
- Backend's SketchStore can use sid_GLOBAL directly as the index key:
  `series.get(sid_GLOBAL) → SketchInstanceMetadata + per-window state`.

Wire savings at steady state: every Export carries ~8 B `series_id`
per DP instead of the full attribute set (~12-50 B per DP for our
{zone, rack, node, pod, producer_id} schema).

### Idempotency invariant on `ResolveSeriesIDs`

Backend's resolution is content-addressable: same `(metric_name,
attribute_set)` input MUST produce the same `series_id` output for
the lifetime of backend's cache. No fresh mint on re-resolution of
an existing identity. Implementation is the standard
"compute-or-mint" pattern:

```rust
fn resolve(&self, metric: &str, attrs: &AttrSet) -> u64 {
    let fp = canonical_fingerprint(metric, attrs);
    self.cache.entry(fp).or_insert_with(|| self.next_sid())
}
```

This is what makes attribute-fallback recovery work cleanly: when an
agent re-emits its `(metric, attrs)` after a crash, backend returns
the SAME sid that was assigned before the crash. Sketch state in the
backend stored under that sid stays coherent across the agent's gap.

Cache fingerprint contract (already in the patched proto):
- Both sender and backend MUST compute `attrs_fp` from
  sorted-by-key attribute (key,value) pairs in a stable
  serialization
- The proto's `SeriesAssignment.attributes_fingerprint` field locks
  this contract; senders use the SAME algorithm to look up cache hits

### Fault tolerance — attribute-carrying fallback is the universal recovery path

Anytime ANY component loses confidence in its sid cache (cold start,
crash recovery, partition reconnect, backend restart without
persistence), it falls back to emitting `(metric_name, full_attributes)`
+ `series_id = 0`. Backend looks up `(metric, attrs_fp)` in its own
cache:
- Cache hit → return existing sid (= same as pre-failure)
- Cache miss → mint new sid (controller may also re-plan for this
  series), return in SeriesAssignment

The bootstrap path and the recovery path are identical: empty
sender-side cache → emit with attributes → resolve sids → cache →
compact emission. No special "recovery mode" code path needed.

### Wire protocol addition for cache invalidation

```protobuf
message ExportMetricsServiceResponse {
  ExportMetricsPartialSuccess partial_success = 1;
  repeated SeriesAssignment series_assignments = 2;

  // NEW: sids the backend didn't recognize in this Export. The sender
  // MUST evict these from its cache; subsequent emits MUST re-attach
  // attributes so backend can re-resolve. Used after backend restart
  // without cache persistence, or any other sid-cache divergence.
  repeated uint64 unknown_series_ids = 3;
}
```

Per-DP `series_id` field semantics:
- `sid != 0, attributes empty`     → use cached sid (compact mode)
- `sid == 0, attributes populated` → first-time / forced-resolve path
- `sid != 0, attributes populated` → optional belt-and-suspenders
  (sender wants to re-confirm)

### Failure scenario matrix

| failure                                    | recovery (no special code path)                 |
|--------------------------------------------|--------------------------------------------------|
| Agent crash + restart                      | empty cache → attribute fallback → backend cache returns SAME sid → resume |
| Gateway crash + restart                    | same, for the gateway's rolled-up identity cache |
| Backend restart WITH persisted sid-cache   | transparent; cached sids still valid              |
| Backend restart WITHOUT persistence        | response.unknown_series_ids → senders evict → next emit with attributes → backend re-mints (fresh sids) → resume. Old sketch state in backend was also wiped, so no orphan reference. |
| Network partition                          | senders buffer/retry; cache unchanged on both sides; reconnect resumes |

### Sid-cache durability options

- **Option A (recommended for production)**: backend persists
  `(metric, attrs_fp) → sid` to its on-disk store alongside sketch
  state. Restart reloads; sids preserved. Co-located with the
  sketches' own durability requirement.
- **Option B (alternative)**: deterministic sid = stable_hash(metric,
  sorted_attrs). Stateless backend; collisions possible at 64-bit
  hash (~10⁻¹⁰ at 1M series). Mitigations: use 128-bit identifier
  proto change, or backend collision-detection that returns
  unknown_series_ids on rare conflicts.
- **MVP (current)**: no persistence — sid-cache and sketch state both
  in-memory; both wiped consistently on backend restart; recovery
  via the attribute-fallback path described above.

Implementation cost: the gateway changes from a terminating OTLP
receiver (currently mints its own sids) to a transparent forwarder
of upstream-provided sids. This is a small patch to the gateway's
processor pipeline + a new "series-id resolver" gRPC method on the
backend.

### "Ghost" sids — registered but never carrying state

A subtle consequence of centralized minting + per-hop wire identity:
some sids exist only as metadata, never see sketch state.

Example:
```
Agent processes per-rack:
  registers (lat_ms, {zone=z0, rack=r00}) → sid=42
  registers (lat_ms, {zone=z0, rack=r01}) → sid=43
  emits sid=42, sid=43 to gateway (compact mode, wire savings ✓)

Gateway rollup: drop rack, merge by zone:
  registers (lat_ms, {zone=z0}) → sid=99
  emits ONLY sid=99 (merged) to backend

Backend metadata cache:  sid=42, sid=43, sid=99 all exist
Backend sketch state:                       only sid=99 accumulates
```

`sid=42` and `sid=43` are **ghost sids** — registered identities the
backend tracks for metadata + query routing, but no sketch state ever
arrives because the gateway folded them into sid=99.

This is by design, not a bug:
- Agent's per-rack registration is the *wire identity* on the
  agent→gateway hop, where wire savings matter most. Removing it
  forces attribute-carrying mode on the hottest edge.
- Backend's per-rack metadata is what makes a future user query like
  `count(http_requests_total{zone=z0, rack=r00})` resolve correctly
  (to a Thanos fallthrough — no warm sketch exists, but raw archive
  has the answer).

**Storage layer requirement**: backend MUST NOT assume "every sid in
metadata has sketch state":

```rust
fn query(&self, sid: u64, ...) -> QueryResult {
    let metadata = self.instances.get(sid)?;
    match self.series.get(sid) {
        Some(series) if !series.is_empty() =>
            evaluate_sketch(series, ...),                    // active warm hit
        _ =>
            Forward::Thanos {                                // ghost — fallthrough
                metric: metadata.metric_name,
                labels: metadata.group_by_keys,
            }
    }
}
```

Three query outcomes per sid lookup:
- metadata + series have state    → warm-tier hit, evaluate sketch
- metadata only (ghost)           → Thanos fallthrough on the registered identity
- no metadata (unknown sid)       → response.unknown_series_ids; sender re-registers; fall through with original attrs if available

Memory cost: each SketchInstanceMetadata is ~70-100 B. At 1M ghost
sids ≈ 70-100 MB — trivial vs active sketch state (KB-MB each). No
prune needed for MVP.

Three design alternatives rejected:
1. Don't register pre-merge identities → loses agent→gateway wire compression.
2. Gateway notifies backend "merge sid=42→sid=99" → adds protocol complexity, breaks per-rack query routing.
3. Periodic prune of ghost sids → complicates fault tolerance (delayed packet after prune mints new sid).

### Sketch merging composes correctly with centralized sids

Sid encodes the identity tuple `(metric, group_by_labels)`, NOT the
physical source/path. Three merge cases:

**Case A — identity-preserving merge** (most common):
```
Agent A emits  (lat_ms, {zone=z0})   — sketch over A's data
Agent B emits  (lat_ms, {zone=z0})   — sketch over B's data
Both register identity tuple → backend returns same sid=42 to both

Gateway's sketch-merge processor receives 2× sid=42 per window,
merges payloads, emits Export(sid=42, merged) to backend.
sid passes through; gateway is transparent for the sid.
```

**Case B — identity-changing rollup at gateway**:
```
Agent A emits  (lat_ms, {zone=z0, rack=r00})  — sid=42
Agent A emits  (lat_ms, {zone=z0, rack=r01})  — sid=43

Gateway's rollup processor drops `rack`, merges by zone alone:
  output identity = (lat_ms, {zone=z0})
  gateway looks up its OWN cache for this rolled-up identity
  cache miss → ResolveSeriesIDs → backend mints sid=99
  emits Export(sid=99, merged)

Different identity → different sid. Same registration flow at gateway.
```

**Case C — cross-metric or cross-capability synthetic merge**: same
logic as B — output identity differs from any input, gateway resolves
a fresh sid via backend.

Single rule for gateway:
```
output_identity = (metric, surviving_labels)
if output_identity == input_identity:
    output.sid = input.sid                         // Case A
else:
    output.sid = gateway_cache.get_or_resolve(output_identity)  // B / C
```

Backend remains the sole minter — every fresh `(metric, group_by)`
tuple from anywhere in the pipeline asks backend; backend dedups
identical tuples to the same sid. Each LOGICAL series has exactly
one sid regardless of how many merge stages it traverses.

Backend's per-(sid, window) storage may still receive multiple
sketches per window (e.g. redundant gateways, no merge). Storage
layer handles this with a per-(sid, window) merge step using
`SketchInstanceMetadata.sketch_type` to pick the algorithm
(DDSketch.merge, HLL.union, CMS row-add, etc.). Same problem with
or without centralized sids — sids don't worsen it.

## 5.5 Future-work — Phase 6: multi-window batching per ScopeMetrics

Today each agent emit produces one `Metric` with one window's worth of
`DataPoint`s (the sketch state at window-close). Wire framing per
emit: ResourceMetrics + ScopeMetrics + Metric envelopes ≈ 60-100 bytes
of overhead before the first DataPoint.

Optimisation: if an agent flushes every N windows instead of every 1
window, it can pack N consecutive windows of the same metric into one
`Metric.data_points[]` list — N DataPoints with monotonically advancing
`time_unix_nano`. The framing overhead amortises by N×, and `Metric.name`
+ `Metric.unit` + parent sketch container's config fields
(`relative_accuracy`, `precision`, etc.) are all sent ONCE per N windows.

Trade-off: introduces flush-latency per emit = N × window_duration. At
N=4 with window=30s, agent buffers up to 2 minutes of state before
emit — affects criterion ⑥ freshness but not correctness or query
results. Operators choose N per their freshness budget.

Tracked as Phase 6.

## 6. What does NOT change

- OTel data plane (OTLP gRPC, sketch payload variants in pdata).
- Gorilla XOR chunk format, MinIO bucket layout for raw archive.
- Thanos store-gateway / thanos-query / thanos-compact sidecars.
- B0 / B1 baselines (raw → Prometheus PRW).
- Multi-node deploy topology.
