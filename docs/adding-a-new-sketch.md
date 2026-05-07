# How to add a new sketch type to the pipeline

**Audience:** contributors who want to extend the sketch DB with a
new sketch family (a new `KLL`-grade or `HLL`-grade algorithm).

**Scope:** mechanical checklist across the three repos that need to
agree on a new sketch type. **Architectural rationale lives in
[`design-sketch-db.md`](design-sketch-db.md)** — this doc is the
recipe for *implementing* a new sketch once the decision to add it
has been made.

**Estimated effort:** ~4,000–7,000 LoC across **3 repos** (sketchlib,
DataCollector, ASAPQuery-backend), plus controller and docs. Plan a
multi-week effort with at least one cross-repo coordination
checkpoint.

---

## When to add a new sketch type vs. tune an existing one

Before starting, exhaust the cheap alternatives:

1. **Can existing sketches answer this query class?** The current
   set covers frequency (CMS / CountSketch), top-K (CMS+heap),
   cardinality (HLL), quantile (KLL / DDSketch), exact aux scalars
   (Sum / Min / Max / Increase). Most observability questions map
   onto one of these.
2. **Can a parameter change satisfy the SLA?** Bigger `K` for KLL,
   larger `width` for CMS, more registers for HLL. The `Sketch
   Profiler Library` (sketch DB design §20) tells the controller
   the cost trade-off; often this is enough.
3. **Is the new sketch genuinely novel (different statistic class,
   better cost frontier, different mergeability properties), or is
   it just a micro-optimisation?** Micro-optimisations belong inside
   an existing sketch's implementation, not as a new top-level
   sketch.

Only proceed with this guide if the new sketch genuinely opens a
query class or cost regime no current sketch can serve.

---

## The cross-repo work, in order

This is the **critical-path order**. Doing it in any other order
will leave you with broken intermediate states.

### Phase A. sketchlib (the algorithm)

You ship the algorithm before any consumer can use it.

#### A.1 sketchlib-rust

```
sketches/Foo/
├── mod.rs              FooSketch struct + new() + insert() + estimate()
├── merge.rs            merge() with associativity + commutativity tests
├── delta.rs            (optional) ComputeDelta + ApplyDelta for delta wire format
├── portable.rs         SerializeProtoBytes / DeserializeFromProtoBytes
├── msgpack.rs          (optional) SerializeMsgpack / DeserializeMsgpack
└── tests              property-based tests + accuracy vs ground truth
```

#### A.2 sketchlib-go

Mirror of A.1 in Go. The proto layout MUST be byte-identical so
cross-language round-trip works.

#### A.3 Shared proto schema

```
proto/foo/foo.proto:
    message FooState {
        // sketch-specific fields, all numbered for forward-compat
        uint32 param1 = 1;
        uint32 param2 = 2;
        repeated uint64 buckets = 3 [packed = true];
    }

proto/sketchlib.proto:
    message SketchEnvelope {
        oneof sketch_state {
            CountMinState count_min = 1;
            // existing variants...
            FooState foo = N;       // <-- new variant, never reuse a number
        }
    }
```

#### A.4 Cross-language CI test

A test that:
1. Constructs a `FooSketch` in Go, inserts samples, serialises.
2. Reads the bytes in Rust, deserialises, queries.
3. Asserts the queried statistic is within the sketch's theoretical
   error bound (§19.9 of the sketch DB design doc).

This is non-negotiable. Without it the Rust and Go sides will drift
and bugs will only surface in production via the modified-OTLP path.

#### A.5 Profiler entry (in same repo as sketchlib)

Run the Sketch Profiler Library (sketch DB design §20) calibration
mode against the new sketch type with a parameter grid. Commit the
resulting `ProfilerEntry` rows to the published catalogue. The
controller will not be able to plan with this sketch until the
catalogue knows about it.

---

### Phase B. DataCollector wire format

Now the agent and the backend can agree on bytes.

#### B.1 Modified opentelemetry-proto patch

`opentelemetry-proto-patch/opentelemetry/proto/metrics/v1/metrics.proto`:

```proto
message Metric {
    oneof data {
        Gauge gauge = 5;
        // existing 7, 9, 10, 11, 13, 14, 15, 16, 17...
        FooSketch foosketch = N;     // <-- new tag, never reuse
    }
}

message FooSketch {
    repeated FooSketchDataPoint data_points = 1;
    AggregationTemporality aggregation_temporality = 2;
}

message FooSketchDataPoint {
    repeated KeyValue attributes = 1;
    fixed64 start_time_unix_nano = 2;
    fixed64 time_unix_nano = 3;
    fixed64 count = 4;            // typed aux (sketch DB design §6.4)
    double sum = 5;
    double min = 6;
    double max = 7;
    bytes sketch = 8;             // serialised FooState
    FooSketchEncoding encoding = 9;
    uint32 flags = 10;
    uint64 series_id = 11;
}

enum FooSketchEncoding {
    FOO_SKETCH_ENCODING_UNSPECIFIED = 0;
    FOO_SKETCH_ENCODING_PROTO = 1;
    FOO_SKETCH_ENCODING_PROTO_DELTA = 2;
    FOO_SKETCH_ENCODING_MSGPACK = 3;
    FOO_SKETCH_ENCODING_MSGPACK_DELTA = 4;
}
```

Even if you only ship one encoding initially, reserve the four
standard variants so the upgrade path matches the existing sketch
families.

#### B.2 pmetric typed accessors

`opentelemetry-collector-patch/pdata/pmetric/`:

- `MetricTypeFooSketch` constant in the metric type enum
- `Metric.SetEmptyFooSketch()` / `Metric.FooSketch()` accessors
- `FooSketch.DataPoints()` / `FooSketchDataPointSlice` etc.
- `FooSketchEncoding` type alias + the four encoding constants

Most of this is mechanical — pattern-match on what an existing
sketch family did (e.g. CountMinSketch in commit history) and
duplicate.

---

### Phase C. DataCollector OTel processor

The agent-side processor that produces FooSketch metrics.

#### C.1 New processor

```
opentelemetry-collector-contrib-patch/processor/foosketchprocessor/
├── factory.go         processor.NewFactory(component.MustNewType("foo"), …)
├── config.go          Config { Mode, MetricName, GroupBy, sketch params,
│                              TransmitSketch, Encoding, DeltaTransmission, … }
├── processor.go       ConsumeMetrics():
│                        - read incoming Gauge/Sum data points
│                        - update FooSketch per (group_key, window)
│                        - on emission: m.SetEmptyFooSketch(); fill
│                          attributes / count / sum / min / max / sketch
│                          bytes / encoding via the typed pmetric accessors
├── go.mod             require sketchlib-go + replace to local path
└── tests              unit tests for batch and window modes
```

Use existing processors (countminsketchprocessor, kllprocessor) as
templates — the structure is identical aside from the sketch type.

#### C.2 asap-otel builder config

`opentelemetry-collector-contrib-patch/cmd/asap-otel/builder-config.yaml`:

```yaml
processors:
  # existing entries...
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/processor/foosketchprocessor v0.x.0
    path: ./processor/foosketchprocessor
```

Rebuild the asap-otel binary, confirm it accepts a config
with `processors: foo:` and emits FooSketch data points the backend
can decode.

---

### Phase D. ASAPQuery-backend ingest path

Now the backend recognises the new wire variant.

#### D.1 Vendored proto regeneration

`asap-common/dependencies/rs/asap_otel_proto/`: regenerate the
vendored proto. The tonic build script will produce a new
`Data::Foosketch` variant on the `Metric.data` oneof automatically.

#### D.2 Ingest router

`asap-query-engine/src/drivers/ingest/otel.rs`:

```rust
enum SketchKind {
    CountMin, CountSketch, Kll, DdSketch, Hll,
    Foo,    // <-- new
}

// In route_modified_otlp_sketches_to_precompute:
Some(Data::Foosketch(f)) => f.data_points.iter().map(|dp| ModifiedOtlpSketchDp {
    kind: SketchKind::Foo,
    attrs: merge_point_attributes(&base_labels, &dp.attributes),
    time_unix_nano: dp.time_unix_nano,
    sketch: dp.sketch.clone(),
    encoding: dp.encoding,
}).collect(),

// In decode_modified_otlp_sketch_bytes:
SketchKind::Foo => Ok(Box::new(
    FooSketchAccumulator::from_sketchlib_proto_bytes(bytes)?,
)),
```

#### D.3 Concrete accumulator

`asap-query-engine/src/precompute_operators/foo_sketch_accumulator.rs`:

```rust
pub struct FooSketchAccumulator {
    inner: sketchlib::Foo,
}

impl FooSketchAccumulator {
    pub fn from_sketchlib_proto_bytes(buffer: &[u8]) -> Result<Self, …>
    pub fn from_msgpack_bytes(buffer: &[u8]) -> Result<Self, …>
    pub fn update(&mut self, value: f64) { self.inner.insert(value) }
    pub fn estimate(&self, …) -> f64 { self.inner.estimate(…) }
}

impl AggregateCore for FooSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> { … }
    fn type_name(&self) -> &'static str { "FooSketchAccumulator" }
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn merge_with(&self, other: &dyn AggregateCore) -> Result<…> { … }
    fn get_accumulator_type(&self) -> AggregationType { AggregationType::Foo }
    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> { None }
    fn query_statistic(&self, statistic, key, kwargs) -> Result<f64> { … }
    fn approx_memory_bytes(&self) -> usize { … }

    /// IMPORTANT (sketch DB design Phase 1, §5.1, §6.4):
    /// surface count / sum / min / max via aux columns when the
    /// underlying sketch tracks them. Don't default to empty.
    fn aux_stats(&self) -> AuxStats {
        AuxStats {
            count: Some(self.inner.count()),
            sum: …,    // None if the sketch doesn't track it
            min: …,
            max: …,
        }
    }
}

impl SerializableToSink for FooSketchAccumulator { … }
```

Plus an `AccumulatorUpdater` impl in
`precompute_engine/accumulator_factory.rs`:

```rust
fn create_accumulator_updater(config: &AggregationConfig) -> Box<dyn AccumulatorUpdater> {
    match config.aggregation_type {
        // existing arms...
        AggregationType::Foo => {
            let p = foo_params(&config.parameters);
            Box::new(FooAccumulatorUpdater::new(p))
        }
    }
}

fn foo_params(parameters: &HashMap<String, Value>) -> FooParams {
    FooParams {
        param1: parameters.get("param1").and_then(|v| v.as_u64()).unwrap_or(default_param1()),
        // ...
    }
}
```

---

### Phase E. Type system

`asap-common/dependencies/rs/promql_utilities/src/query_logics/enums.rs`:

```rust
pub enum AggregationType {
    Sum, Increase, MinMax, DatasketchesKLL,
    // existing...
    Foo,    // <-- new variant
}

impl AggregationType {
    pub fn as_str(self) -> &'static str {
        match self {
            // existing...
            AggregationType::Foo => "Foo",
        }
    }
}

impl FromStr for AggregationType {
    fn from_str(s: &str) -> Result<Self, …> {
        match s {
            // existing...
            "Foo" => Ok(AggregationType::Foo),
            _ => Err(…),
        }
    }
}
```

After this, `StreamingConfig` YAML files can carry
`aggregationType: Foo`.

---

### Phase F. Capability matching

`asap-common/dependencies/rs/asap_types/src/capability_matching.rs`:

```rust
fn compatible_agg_types(stat: &Statistic) -> Vec<AggregationType> {
    match stat {
        Statistic::WhateverFooAnswers => {
            vec![AggregationType::Foo, /* alternative existing types */]
        }
        // existing...
    }
}
```

Without this, even if the controller plans a Foo sketch,
SimpleEngine's capability matcher won't route queries to it.

---

### Phase G. Sketch DB integration (assumes sketch DB Phase 6+ has shipped)

#### G.1 Accuracy profile

`AccuracyProfile::for_sketch_type(SketchType::Foo, params)` (sketch
DB design §6.4) must return a real `ErrorBound` derived from
`Foo`'s theoretical formula. Add the formula to §19.9 of the
design doc.

#### G.2 Profiler catalogue

If you completed Phase A.5, the controller already has cost
numbers. Verify the `/api/v1/db/cost_estimate` endpoint returns
sensible numbers for `Foo` configs.

#### G.3 Controller decision logic

`DataCollector/controller/src/analyzer.rs` and `planner.rs` decide
which sketch type to pick for a given query. Add `Foo` to the
candidate set for the relevant query intents. The cost model
(driven by the profiler catalogue) handles the actual selection.

---

### Phase H. Tests

Minimum bar before merging:

| Test | Repo | What it proves |
|---|---|---|
| Algorithm correctness | sketchlib-rust + go | property-based tests, accuracy within theoretical bound |
| Merge associativity | sketchlib-rust + go | merge(a, merge(b, c)) == merge(merge(a, b), c) within tolerance |
| Cross-language round-trip | shared CI | Go produce → Rust consume, statistic within bound |
| Processor unit test | DataCollector | batch + window modes emit the right `*SketchDataPoint` shape |
| OTLP wire decode | ASAPQuery-backend | end-to-end: Go processor → bytes on wire → Rust accumulator → query returns expected value |
| Aux-column round-trip | ASAPQuery-backend | `aux_stats()` returns the right scalar without deserialising the sketch (sketch DB Phase 1) |
| Capability match | ASAPQuery-backend | a query naming the relevant statistic actually picks `Foo` |
| Profiler regression | shared CI | cost numbers within 10% of the committed catalogue baseline |

---

### Phase I. Documentation

Without these, your sketch is invisible to operators and to the
next person trying to add another one:

| Doc | What to add |
|---|---|
| `DataCollector docs/pipeline-query-catalog.md` | A new row in the §3 query catalog: query class → OTel op → backend accumulator → PromQL → accuracy property |
| `ASAPQuery-backend docs/design-sketch-db.md §19.9` | The theoretical accuracy bound formula for `Foo` |
| `ASAPQuery-backend docs/design-sketch-db.md §19.10` | The merge propagation rule for `Foo` |
| Shared design doc for `Foo` | A short rationale: what query class, why this sketch over alternatives, what the trade-off is |
| `DataCollector controller/docs/query-to-sketch-translation.md` | Where in the five-layer plan `Foo` shows up as a candidate |

---

## Anti-patterns to avoid

These are mistakes contributors have made or will make:

1. **Duplicating proto field numbers**. Each new variant on
   `Metric.data` and on `SketchEnvelope.sketch_state` MUST get a
   fresh tag. Reusing a tag silently corrupts all wire interop.
2. **Skipping cross-language round-trip tests** because "the proto
   schema is shared so it must work." It doesn't. Endianness,
   floating-point representation, ordering of sub-fields, packed
   vs unpacked encoding — every one of these has bitten Rust/Go
   interop in the past. The CI test is the only protection.
3. **Returning `AuxStats::empty()`** because writing the override
   "looks like work." Then every Count / Sum / Min / Max query on
   this sketch pays sketch deserialisation cost forever (sketch DB
   design §5.1). At minimum, populate `count` from the sketch's
   sample counter.
4. **Hand-deriving CPU / memory numbers** for the controller's
   cost model. The Sketch Profiler Library (sketch DB design §20)
   exists exactly to prevent this. Always commit a calibration
   run with the new sketch.
5. **Adding the sketch only to one tier.** The sketch DB has
   Tier 1 (PromSketch, in-memory EH) and Tier 2 (precompute + LSM
   parts). If `Foo` only makes sense in Tier 2, that's fine —
   document it. If it could serve Tier 1, implement both.
6. **Deferring the documentation**. The design doc and the query
   catalog are how operators decide whether to use the sketch and
   how the controller's planner reasons about it. Undocumented
   sketch types in the codebase lead to operators picking
   suboptimal aggregations because they don't know the new option
   exists.

---

## Rough effort estimate

| Phase | LoC (approx) | Time |
|---|---|---|
| A. sketchlib (Rust + Go + cross-lang test) | 1,500–3,000 | 1–2 weeks |
| B. DataCollector proto + pmetric | 800–1,500 | 3–5 days |
| C. DataCollector processor | 700–1,000 | 4–6 days |
| D. ASAPQuery-backend ingest + accumulator | 800–1,200 | 3–5 days |
| E. Type system update | 50–100 | 1 day |
| F. Capability matching | 50–100 | 1 day |
| G. Sketch DB integration | 200–400 | 2–3 days |
| H. Tests across all repos | 500–1,000 | 3–5 days |
| I. Documentation | 100–200 | 1–2 days |
| **Total** | **~4,800–8,500 LoC** | **~4–6 weeks** for a single contributor |

If multiple contributors can work in parallel (one on sketchlib,
one on DataCollector, one on backend), this can compress to ~2–3
weeks of wall time, but the coordination overhead means it rarely
goes below that.

---

## Quick reference: which existing sketch did each step pattern-match?

When in doubt, look at the most recent precedent for the same step:

| Phase | Best precedent to copy |
|---|---|
| sketchlib-rust algorithm | the `KLL` family if you need quantile-shaped behaviour, `HLL` for set/cardinality, `CountMin` for frequency |
| sketchlib-go matching impl | same family on the Go side |
| Modified OTLP proto variant | `KLLSketch` or `CountMinSketch` (both have all four encoding variants) |
| pmetric typed accessors | `CountMinSketch` accessor commit history (PR #157 era) |
| OTel processor | `kllprocessor` for window-mode, `countminsketchprocessor` for batch+heap |
| Backend accumulator | `DatasketchesKLLAccumulator` (cleanest current example) |
| Cross-language wire test | the existing `e2e_modified_otlp_sketch_path.rs` test pattern in ASAPQuery-backend |
