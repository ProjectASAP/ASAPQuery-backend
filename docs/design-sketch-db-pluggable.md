# Design: Sketch DB as a Pluggable Component

Companion to the sketch DB design set — see
[`design-sketch-db.md`](./design-sketch-db.md) for the index and
[`design-sketch-db-core.md`](./design-sketch-db-core.md) for the
as-built contract. Those docs define *what* the sketch DB is; this
one defines *how it detaches* from
`asap-query-engine` so it can be consumed either as a **library crate** embedded
in another process or as a **standalone binary** (a sketch-store service) that
any component in the system can talk to over the network.

---

## 1. Problem

Today the sketch DB is spread across `asap-query-engine/src/stores/sketch_db/`
and reaches into the rest of the crate freely:

- `data_model::{AggregateCore, PrecomputedOutput, KeyByLabelValues}` is in
  `asap-query-engine`, not under `sketch_db`.
- Accumulator implementations live in `asap-query-engine/src/precompute_operators/`.
- `schema::SchemaRegistry` is constructed by `main.rs` and handed into both
  the ingest path and the query engine.
- HTTP endpoints for `/api/v1/streaming-config`, backfill triggers, and
  schema reads are registered on the query engine's Axum router.
- The `Store` trait (`stores/traits.rs`) uses `asap-query-engine` types in
  its method signatures (`PrecomputedOutput`, `AggregateCore`, `KeyByLabelValues`).

Consequences:

1. **No way to embed the store elsewhere.** A downstream product (e.g. a
   dedicated summary-serving tier, or a test harness, or a notebook) has to
   pull in all of `asap-query-engine` — planner client, drivers, query
   engine, the lot.
2. **No way to run it as its own process.** You can't scale ingest-write
   throughput independently of query read throughput, and you can't put the
   sketch store on a different fleet from the query engine.
3. **Blast radius on refactors.** A change inside `sketch_db/` can touch
   types shared with query planning because the boundary isn't enforced.

We want a single artifact — `sketch-db` — that any caller can consume in two
interchangeable shapes:

- **Library mode**: `sketch-db` is a Rust crate you link into your process.
  Zero network hops. Used by `asap-query-engine` today, by tests, by anyone
  who wants an in-process store.
- **Binary mode**: `sketch-db-server` is a daemon that exposes the same API
  over gRPC. Used when sketch storage needs to scale/deploy independently,
  or when a non-Rust client wants to read/write sketches.

Both shapes share **one** Rust API surface, one schema, one on-disk format.

Scope is a refactor with no change to the sketch DB's semantics. The
design-sketch-db-core.md contract (agg_id immutability, write barrier, timeline
dispatch, accuracy profiles, backfill) is preserved verbatim.

---

## 2. Goals / Non-Goals

**Goals**
- Self-contained crate: `sketch-db` compiles without `asap-query-engine`.
- Stable public API: changes inside the crate don't ripple outward.
- Binary drop-in: the same crate + a thin `main.rs` + a gRPC adapter is a
  runnable service.
- Zero semantics drift vs. today's in-process store.
- Incremental migration — no flag day.

**Non-Goals**
- Replication, sharding, or multi-node consistency. Still single-node per
  `design-simple-map-store-persistence.md`.
- Rewriting the accumulator algorithms (they already live in
  `asap-common/sketch-core`).
- A new wire format for sketches (reuse the existing OTLP `SketchEnvelope`).
- A SQL/PromQL-level query interface at the sketch DB boundary — the sketch
  DB remains a **storage engine**, not a query engine. PromQL stays in
  `asap-query-engine`.

---

## 3. Current Coupling Map

What the boundary has to cut:

| Piece | Today's location | Who owns it after split |
|---|---|---|
| `Store` trait | `asap-query-engine/src/stores/traits.rs` | `sketch-db` (public) |
| `SimpleMapStore` + LSM | `asap-query-engine/src/stores/sketch_db/simple_map_store/` | `sketch-db` (public) |
| `SchemaRegistry`, `AggSchema` | `asap-query-engine/src/stores/sketch_db/schema.rs` | `sketch-db` (public) |
| Backfill types / worker / registry | `asap-query-engine/src/stores/sketch_db/backfill*.rs` | `sketch-db` (public) |
| `AggregateCore` trait | `asap-query-engine/src/data_model/` | `sketch-db` (public trait) |
| Concrete accumulators (HLL, KLL, DDSketch, CMS, …) | `asap-query-engine/src/precompute_operators/` | **new crate** `sketch-accumulators` (depends on `sketch-db` for the trait, on `sketch-core` for the algorithms) |
| `PrecomputedOutput`, `KeyByLabelValues` | `asap-query-engine/src/data_model/` | `sketch-db` (public; neutral names) |
| Streaming-config swap HTTP handler | `asap-query-engine/src/main.rs` | caller (but `SchemaRegistry::reconcile(...)` is the real entry point and stays in `sketch-db`) |
| Backfill HTTP trigger endpoints | `asap-query-engine` | callable via either crate-level API or gRPC |
| Ingest path (OTLP receivers, delta cache, series router, worker pool) | `asap-query-engine/src/drivers/`, `precompute_engine/` | **stays in `asap-query-engine`** — this is the *caller*, not the store |

Key insight: **the sketch DB is not the ingest pipeline**. The ingest path
lives above the store and calls `insert_precomputed_output` when a window
closes. That call site is exactly where the library-vs-binary switch
happens.

---

## 4. Two Deployment Shapes

### 4.1 Library mode (default, matches today)

```
┌──────────────────────── asap-query-engine process ────────────────────────┐
│                                                                           │
│  ingest pipeline ──► SketchDb::insert(...)       (Rust call, same proc)   │
│  query engine   ──► SketchDb::query(...)         (Rust call, same proc)   │
│                                                                           │
│                        sketch-db crate                                    │
│                     (SchemaRegistry + Store + backfill + LSM)             │
└───────────────────────────────────────────────────────────────────────────┘
```

Zero overhead, used for embedded deployments and tests. This is the shape
every call site uses today; after the refactor it keeps working, just
against a cleaner API.

### 4.2 Binary mode

```
┌─ asap-query-engine ─┐     gRPC      ┌─ sketch-db-server ─┐
│                     │ ◄───────────► │                    │
│  ingest pipeline    │  insert/      │  SchemaRegistry    │
│  query engine       │  query/       │  Store (LSM)       │
│                     │  schema RPC   │  Backfill workers  │
└─────────────────────┘               └────────────────────┘
                                               │
                                         local disk
```

The server is a thin wrapper: `main.rs` + a gRPC adapter that maps RPC
methods one-to-one onto the `SketchDb` façade. No business logic lives in
the server crate.

The client side is a `SketchDbClient` that implements **the same public
trait as the in-process store**, so the ingest and query paths don't know
which mode they're in — it's a compile-time / config-time swap.

---

## 5. Crate Layout

Two new workspace members under a new top-level directory:

```
asap-common/
  sketch-db/                       (new)
    Cargo.toml
    src/
      lib.rs                       ← re-exports public API
      api.rs                       ← SketchDb trait + facade
      schema/                      ← moved from asap-query-engine
        mod.rs
        registry.rs
        timeline.rs
        accuracy.rs
      store/
        mod.rs                     ← Store trait (today's traits.rs)
        simple_map_store/          ← moved wholesale
          global.rs
          per_key.rs
          persistence/
      backfill/                    ← moved wholesale
        mod.rs
        job.rs
        worker.rs
        registry.rs
      data_model.rs                ← PrecomputedOutput, KeyByLabelValues, AggregateCore trait
      metrics.rs
    proto/
      sketch_db.proto              ← gRPC service (only compiled under `grpc` feature)

  sketch-accumulators/             (new — concrete AggregateCore impls)
    src/
      hll.rs
      kll.rs
      ddsketch.rs
      count_min.rs
      count_sketch.rs
      hydra_kll.rs
      set_aggregator.rs
      sum_min_max.rs
      factory.rs                   ← AggregationType → Box<dyn AggregateCore>

sketch-db-server/                  (new — thin binary)
  Cargo.toml
  src/
    main.rs                        ← clap, tracing init, config load
    service.rs                     ← tonic service impl; delegates to sketch_db::SketchDb
```

### Why split accumulators out?

The `Store` and `SchemaRegistry` don't need to know *which* accumulators
exist — they only need `AggregateCore`. Keeping concrete implementations in
a sibling crate means the server binary can be built with a minimal set
(or an operator can swap in custom ones without forking the core).

### Crate feature flags

On `sketch-db`:
- `grpc` — compiles `proto/` via `tonic-build`, enables the
  `SketchDbClient` gRPC client. Off by default (library users don't need
  it).
- `persistence` — LSM persistence layer. On by default.

Workspace dependency rules after the split:

```
sketch-core ──────────────────────► (no internal deps)
asap_types  ──────────────────────► (no internal deps)
sketch-db   ──► sketch-core, asap_types
sketch-accumulators ──► sketch-db, sketch-core
sketch-db-server ──► sketch-db (feat=grpc), sketch-accumulators
asap-query-engine ──► sketch-db, sketch-accumulators, ...
```

No cycles, and `asap-query-engine` shrinks.

---

## 6. Public API (Rust)

One façade type + the existing traits, re-exported from `sketch_db::prelude`:

```rust
// sketch-db/src/api.rs
pub struct SketchDb {
    schema: Arc<SchemaRegistry>,
    store: Arc<dyn Store>,
    backfill: Arc<BackfillRegistry>,
}

impl SketchDb {
    /// Embedded construction — opens the on-disk state at `data_dir`.
    pub fn open(cfg: SketchDbConfig) -> Result<Self, SketchDbError>;

    // ── write path ─────────────────────────────────────────────
    pub fn insert(&self, out: PrecomputedOutput, core: Box<dyn AggregateCore>)
        -> Result<(), SketchDbError>;
    pub fn insert_batch(&self, items: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>)
        -> Result<(), SketchDbError>;

    // ── read path ──────────────────────────────────────────────
    pub fn query(&self, metric: &str, agg_id: u64, t1: u64, t2: u64)
        -> Result<TimestampedBucketsMap, SketchDbError>;
    pub fn query_exact(&self, metric: &str, agg_id: u64, ws: u64, we: u64)
        -> Result<TimestampedBucketsMap, SketchDbError>;

    // ── schema / lifecycle ─────────────────────────────────────
    pub fn is_writable(&self, agg_id: u64) -> bool;
    pub fn get_schema(&self, agg_id: u64) -> Option<AggSchema>;
    pub fn timeline_for_metric(&self, metric: &str, t1: u64, t2: u64) -> Vec<TimelineSegment>;
    pub fn reconcile(&self, cfg: &StreamingConfig) -> ReconcileSummary;
    pub fn drop_agg_id(&self, agg_id: u64) -> Result<usize, SketchDbError>;

    // ── backfill ───────────────────────────────────────────────
    pub fn submit_backfill(&self, job: BackfillJob) -> Result<BackfillJobId, SketchDbError>;
    pub fn backfill_status(&self, id: BackfillJobId) -> Option<BackfillStatus>;
}
```

This is the entire external surface. Everything in `asap-query-engine` that
talks to the store today goes through one of these methods; nothing
reaches into `schema::` or `store::` internals directly.

### Error type

Replace the current `Box<dyn Error + Send + Sync>` in the `Store` trait
with a concrete `SketchDbError` enum (unknown agg_id, schema barrier drop,
disk full, codec mismatch, …). Makes the binary-mode gRPC mapping clean
and gives callers actionable variants.

---

## 7. Wire Protocol (Binary Mode)

gRPC via `tonic`. Proto lives at `sketch-db/proto/sketch_db.proto`. One
service, methods that mirror the Rust API 1:1:

```proto
service SketchDb {
  // Write
  rpc Insert(InsertRequest) returns (InsertResponse);
  rpc InsertBatch(stream InsertRequest) returns (InsertBatchResponse);

  // Read
  rpc Query(QueryRequest) returns (QueryResponse);
  rpc QueryExact(QueryExactRequest) returns (QueryResponse);

  // Schema
  rpc IsWritable(AggIdRequest) returns (BoolResponse);
  rpc GetSchema(AggIdRequest) returns (AggSchemaResponse);
  rpc TimelineForMetric(TimelineRequest) returns (TimelineResponse);
  rpc Reconcile(StreamingConfigProto) returns (ReconcileSummaryProto);
  rpc DropAggId(AggIdRequest) returns (DropResponse);

  // Backfill
  rpc SubmitBackfill(BackfillJobProto) returns (BackfillJobIdProto);
  rpc BackfillStatus(BackfillJobIdProto) returns (BackfillStatusProto);
}
```

### Sketch payload encoding

Accumulators cross the wire as **the same `SketchEnvelope` oneof** the
ingest path already uses (see `asap-common/dependencies/rs/asap_otel_proto/`).
No new codec. `InsertRequest` carries `bytes sketch_envelope = N` plus the
typed aux columns (`count/sum/min/max`) as primitive fields.

### Streaming

`InsertBatch` is client-streaming so the ingest fleet can pipeline window
closes without one RTT per sketch. `Query` is unary — a typical response
fits one message. If a future query type returns large timelines, add a
server-streaming variant at that time.

### Backpressure & retries

- Server advertises a soft `max_inflight_writes` per connection.
- Writes are **idempotent on (agg_id, group_key, window_start)** — the
  store already dedupes on that key, so clients can safely retry.
- Queries are naturally idempotent.

### Auth & isolation

Out of scope for v1; document the expectation that the binary runs behind
a trusted network boundary, same as today's query engine. Add mTLS as a
follow-up when someone needs it.

---

## 8. Ingest & Query Path Changes

The ingest and query paths don't care which mode the store runs in — they
hold an `Arc<dyn SketchDbHandle>`. Today that's the in-process `SketchDb`.
In binary mode it's a `SketchDbClient` backed by gRPC.

```rust
// sketch-db/src/api.rs
pub trait SketchDbHandle: Send + Sync {
    fn insert(&self, ...) -> Result<(), SketchDbError>;
    fn query(&self, ...) -> Result<TimestampedBucketsMap, SketchDbError>;
    fn is_writable(&self, agg_id: u64) -> bool;
    // ... same methods as SketchDb
}

impl SketchDbHandle for SketchDb { /* direct */ }
impl SketchDbHandle for SketchDbClient { /* gRPC */ }
```

`asap-query-engine/src/main.rs` picks the impl from config:

```rust
let handle: Arc<dyn SketchDbHandle> = match cfg.sketch_db {
    SketchDbTarget::Embedded(c) => Arc::new(SketchDb::open(c)?),
    SketchDbTarget::Remote { endpoint } => Arc::new(SketchDbClient::connect(endpoint).await?),
};
```

Nothing downstream changes.

### Latency note

Binary mode adds one network hop per window close on the write side and
one per query on the read side. Batching (`InsertBatch` stream) keeps the
write-side overhead amortized; query-side latency becomes noticeable only
for sub-second PromQL use cases and should be benchmarked before adoption.

---

## 9. On-Disk Format & Compatibility

Formats do not change:
- LSM parts (`part_{id}/{meta,data,index}.bin`) — unchanged.
- `SchemaRegistry` snapshot JSON — unchanged.
- Manifest append-only log — unchanged.

So a directory written by today's in-process store opens cleanly under the
new `SketchDb::open(...)` in either deployment shape. The existing
`persist_format_versioning_tests.rs` coverage carries over.

---

## 10. Migration Plan

Six steps, each independently mergeable; the code compiles and all tests
pass between steps.

| Step | What moves | Risk |
|---|---|---|
| **M1** | Create empty `sketch-db` crate; move `Store` trait, `PrecomputedOutput`, `KeyByLabelValues`, `AggregateCore` trait definition (not impls) into it. Re-export from `asap-query-engine` for source compat. | Low — mechanical. |
| **M2** | Move `stores/sketch_db/{schema, backfill*, simple_map_store, accuracy, metrics}` into `sketch-db`. Update imports. | Medium — large rename. |
| **M3** | Split concrete accumulators into `sketch-accumulators` crate. `asap-query-engine` depends on both. | Low — accumulators are already mostly self-contained under `precompute_operators/`. |
| **M4** | Introduce `SketchDb` façade + `SketchDbHandle` trait. Migrate call sites (`main.rs`, precompute engine, query engines, tests) to use the façade instead of reaching into the registry/store directly. | Medium — touches many files, but each edit is local. |
| **M5** | Add `grpc` feature, `proto/sketch_db.proto`, `tonic` codegen, `SketchDbClient`. Unit-test the client against an in-process server. | Medium — new dep surface. |
| **M6** | Create `sketch-db-server` binary crate. Wire through config in `asap-query-engine` for `Embedded` vs. `Remote`. Add an e2e test that runs ingest→server→query across a loopback gRPC link. | Medium — new deployment artifact. |

At the end of M4 the library split is complete and shippable; M5 and M6
are the binary-mode delta and can land later without blocking anything.

---

## 11. Open Questions

1. **Backfill scheduling across the boundary.** The controller
   (`asap-planner-rs`) triggers backfill jobs today via HTTP against the
   query engine. Should that redirect through the new gRPC service in
   binary mode (preferred, keeps state in one place), or should the
   planner talk to the store directly? → proposed: planner talks to the
   store; query engine proxies only if back-compat requires it.
2. **Metrics.** Prometheus `/metrics` endpoint — does `sketch-db-server`
   expose its own, or does the query engine scrape both? → proposed:
   server exposes its own; query engine's `/metrics` no longer reports
   store internals in binary mode.
3. **Accumulator registration.** In binary mode, the server must know how
   to deserialize every sketch type the ingest fleet sends. If a client
   adds a new type (the `adding-a-new-sketch.md` flow), the server also
   has to ship with the matching accumulator. → proposed: both build
   against the same pinned `sketch-accumulators` version; bump in
   lockstep. Revisit when anyone asks for hot plug-in accumulators.
4. **Config schema drift.** `StreamingConfig` is currently defined in
   `asap-common/dependencies/rs/asap_types/`. That stays shared. If it
   ever forks by deployment shape, we'll need versioned reconcile RPCs.

---

## 12. Summary

- The sketch DB today is semantically a separable component but
  structurally embedded in `asap-query-engine`.
- Extract it into a `sketch-db` crate with one façade (`SketchDb`) and
  one handle trait (`SketchDbHandle`). Concrete accumulators go into
  a sibling `sketch-accumulators` crate.
- A thin `sketch-db-server` binary plus a `grpc` feature flag turns the
  same crate into a standalone service; clients use `SketchDbClient`
  that implements the same handle trait.
- Ingest and query paths swap between embedded and remote via one
  config line. On-disk format, schema contract, and sketch semantics
  are all unchanged.
- Migrate in six small steps; library extraction (M1–M4) is useful
  even if the binary (M5–M6) never ships.
