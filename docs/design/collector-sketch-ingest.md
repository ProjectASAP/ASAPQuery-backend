# Design: leaf sketch views precomputed in the collector

Status: proposal, for review. Decisions are recorded at the end.

## Audience and context

This document is for developers of the backend and of ASAPCollector-public,
the collector built on OTAP Dataflow (OpenTelemetry Arrow's dataflow engine)
whose `asap_sketches` processor builds windowed sketches at the edge. It
decides how the collector builds some of a plan's leaf views and how the
backend ingests the resulting states, so that the backend stores and answers
them as if its own precompute had built them.

Today every sample travels raw to the backend: the collector (or the
application) sends OTLP or Remote Write, and the backend's precompute builds
every view. A leaf view such as "a DDSketch per series per 5 s tumbling
window" folds the samples of one series and one window into one state, and
the collector can build exactly that state before the network. For a series
sampled at 10 Hz, 50 samples become one state.

Terms, as in ASAPPlanner:

- **View**: a materialized node of the plan, identified by `ViewId { plan,
  node }`.
- **Leaf view**: a view whose input is a raw `Scan`.
- **Tumbling window**: a window of fixed width `w` on multiples of `w`; the
  window ending at `e` covers `(e − w, e]`.
- **State**: the summary of one series (or group) in one window; a sketch
  state is `asap_sketchlib`'s ASAPv1 bytes.
- **Chain**: the views and query-time merges that build a longer range from
  a leaf's windows (`SummaryTumblingWindowChain`, then `TimeRange` and
  `SummaryMerge`).
- **Pushed view**: a leaf view the collector builds; the backend only stores
  and reads it.

Scope:

- ASAPPlanner does not change. Which views are pushed is a deployment
  decision the backend's control plane makes after planning (decision 1).
- The backend changes in four places: a new ingest adapter, a new `common`
  type, the view store's write path, and the control plane.
- The collector changes in one place, its window boundary and filter
  configuration, in its own repository.

## Components

```text
                         ┌──────────────────────────── collector ────────────────────────────┐
 application ── OTLP ──▶ │ receiver ─┬─▶ asap_sketches (one per pushed view) ─▶ OTLP exporter │──┐ sketch path
                         │           └─▶ raw forward ─────────────────────────▶ OTLP exporter │──┤ raw path
                         └───────────────────────────────────────────────────────────────────┘  │
                                                  ▲ collector config (TOML)                     │
                                                  │                                              ▼
 workload ─▶ control-plane ─▶ InstallablePlan ────┼──────────────▶ data-plane  POST /v1/metrics
              │  (asap-planner, unchanged)        │                 ├─ record without sketch.envelope ─▶ otlp ─▶ IngestEngine (raw path)
              └─ emit_collector_config ───────────┘                 └─ record with sketch.envelope ─▶ asap-sketch-otlp ─▶ ViewStateBatch
                 + PushedViews file ───────────────▶ data-plane              ─▶ precompute committer ─▶ storage (view store, persistence)
```

| Component | Crate or repository | Role |
|---|---|---|
| Push-down rule | `crates/control-plane` | Picks the pushable leaf views of the installed plan (rule below) |
| Collector config emitter | `crates/control-plane` (`control_plane --emit-collector-config`) | Writes one `asap_sketches` processor block per pushed view, and the `PushedViews` file the data plane reads |
| `asap_sketches` processor | ASAPCollector-public | Builds each pushed view's per-series states over tumbling windows and exports them as OTLP summary points carrying `sketch.envelope` |
| `asap-sketch-otlp` | `crates/adapters/ingest/asap-sketch-otlp` (new) | Decodes those OTLP records into a `common::ViewStateBatch`; never reads the sketch bytes |
| `ViewStateBatch` | `crates/common` (new type) | Pushed states between the adapter and the ingest engine |
| `/v1/metrics` route | `crates/data-plane` | One route for both paths: a data point with `sketch.envelope` goes to the sketch path, any other to the raw path |
| Pushed-state ingest | `crates/precompute` | Maps `agg_id` to the view, checks the state against the plan, stages it until the window commits, deduplicates, merges late states |
| Late-state merge | `crates/storage` | Replaces a committed window's rows with merged rows and persists them |

The raw path stays (decision 3). The raw samples of a metric still go to the
backend when a view that is not pushed reads that metric, when a query-time
`Scan` reads it, and for the fallback: when a pushed view misses a window
(the collector was down), the query engine reports `NotReady` and the
dispatcher forwards the query, as for any view that is not ready.

### Dependencies

Following the layout rules (adapters see no engine and no store):

```text
crate              depends on
-----------------  ------------------------------------------------------
asap-sketch-otlp   common (ViewStateBatch), otlp (its protobuf types)
precompute         + decoding ASAPv1 through storage's existing codec (persist.rs decode_sketch)
control-plane      unchanged dependencies (asap-planner, common)
```

The adapter does not decode ASAPv1: it hands the bytes on. Decoding needs
the executor's kernels, and `storage` already rebuilds every kernel from
ASAPv1 for persistence (`decode_sketch`), so the ingest engine reuses that.

## Algorithm

### 1. Push-down rule (control plane)

The first version is rule based, with no cost comparison (decision 6). A
view of the installed plan is pushed if and only if all of these hold:

1. **Leaf on raw input.** It is a `SummaryTumblingWindowChain` whose child is
   a `Scan` of a `TimeSeries` source, with no node in between.
2. **Per series.** Its reduction is `PerEntity`: one state per series and
   window. A view that reduces across series (`Reduce [...]`) is not pushed,
   because one collector does not see every series of a group.
3. **Tumbling window.** It has a `tumbling_window_width_ms`; the collector's
   `window_size` is set to it and `window_slide` is left out.
4. **Supported sketch and parameters.** Its summary type and parameters map
   to an `asap_sketches` sketch type with the same parameters:

   | Plan summary | Plan parameters | `sketch_type` | `sketch_params` |
   |---|---|---|---|
   | `Sketch: DDSketch` | `alpha` | `ddsketch` | `relative_accuracy = alpha` |
   | `Sketch: KLL` | `k` | `kll` | `k` |
   | `Sketch: HLL` | precision 14 only | `hll` | none (fixed at 14) |
   | `Sketch: CountSketch` | rows × columns | `countsketch` | `depth`, `width` |
   | `Sketch: CountMin` | rows × columns | `countminsketch` | `depth`, `width` |
   | `Sketch: CountMinHeap` | rows × columns, heap size | `countminsketch_heap` | `rows`, `columns`, `k` |

   The input must also match what the collector inserts: DDSketch and KLL
   insert each sample's value (the plan's `input` is `item: null, weight:
   SampleValue`); the frequency and cardinality sketches insert a key, and
   are pushed only when the plan's item is a label the collector keys by
   (`per_attribute`). Any other summary (exact aggregates, UnivMon, Hydra,
   KMV, an HLL of another precision) is not pushed.
5. **Expressible filter.** Its `Scan` has a metric name, and its predicates
   and the view's `filter` are a conjunction of label equalities and
   inequalities (`=`, `!=`), which the collector's `LabelMatcher` evaluates.
   A regular-expression matcher is not pushed. The collector exposes
   matchers in its configuration only after the collector change below.

**Exact aggregates stay in the backend.** The collector's `asap_sketches`
processor builds only the six sketches above. Its `Exact` type
(`EXv1`, the exact baseline window holding every value) is a baseline for
comparisons, not a `sum`, `max` or counter state, and has no
`sketch_type` spelling. Views of `ExactAggregate` (`Sum`, `Max`, `Min`,
`Rate`, `Increase`, …) are therefore not pushed. They become pushable when
the collector builds those states, with counter-reset handling for `Rate`
and `Increase`.

**Known limitation: sparse series.** The rule ignores size. A series with few
samples per window can produce a sketch state larger than its raw samples
(a DDSketch with one sample still carries its header, mapping and bins,
against about 16 bytes for the raw sample). Step 4 measures where this
happens. Push-down that compares costs is future work; it needs a general
planner capability (costing where a view is built), to be discussed later.

### 2. Collector configuration (control plane)

For each pushed view the control plane writes one processor block (worked
example below). The fields are fixed by the view:

| Field | Value | Why |
|---|---|---|
| `id` | `asap_view_<node>` | One processor per view |
| `sketch_type`, `sketch_params` | from the table in step 1 | The state must decode as the view's kernel with the plan's parameters |
| `window_size` | `tumbling_window_width_ms` | Same windows as the view |
| `output_metric_name` | the `Scan`'s metric name | Series identity: `__name__` must equal the raw series' name |
| `agg_id` | the view id, encoded as below | Maps a state back to its view |
| `matchers` | the metric name, then each `=` / `!=` predicate | The processor would otherwise fold every metric it receives (its series key has no metric name) |
| `transmit_sketch` | `true` | Full ASAPv1 states (decision 2) |
| `aggregate_by` | `[]` | Per series: every label is part of the key |
| `omit_resource_attrs`, `global_aggregation`, `emit_window_stats` | `false` | Any of them changes the series' labels, and with them its identity |

`agg_id` is a `u64` in the collector and an `IntValue` (signed 64-bit) on the
OTLP wire. It encodes the view: the low 32 bits are the node id; the high 31
bits are the plan's fingerprint (the hash of the plan file that the view
files' segment headers already name), truncated. A state built under another
plan's configuration then never lands in this plan's view of the same node
id.

The same command writes the `PushedViews` file the data plane takes with
`--pushed-views`: per pushed view, its node, its `agg_id`, its kernel and
parameters, and the metric. The data plane does not change the
`InstallablePlan`; it reads the plan and this file.

The precompute engine skips the pushed views: at install it compiles each
refresh group without the pushed leaf views (and without a `Scan` only they
read). The query engine is unchanged; it reads a pushed view from the store
like any other.

### 3. Window semantics: Prometheus's `(t − range, t]`

Both sides use Prometheus's boundary (decision 4). Prometheus 3.x range
selectors are left-open and right-closed. Its querying documentation
(Range Vector Selectors) says: "The range is a left-open and right-closed
interval, i.e. samples with timestamps coinciding with the left boundary of
the range are excluded from the selection, while samples coinciding with the
right boundary of the range are included in the selection." Its 3.0
migration guide ("Range selectors and lookback exclude samples coinciding
with the left boundary") records the change from the earlier left-closed
form. OTLP's metrics data model uses the same interval for a delta point,
`(start, end]` ("The time interval is inclusive of the end time").

- **Backend: already matches.** `storage::store::window_end` puts a sample at
  `t` in the window ending at `⌈t / w⌉ · w`, so windows are `(e − w, e]`, and
  `ViewInstanceKey::window_ms` is `(start, end]`.
- **Collector: must change.** `WindowState::init_window` and
  `advance_window` put a sample at `t` in `[⌊t / w⌋ · w, ⌊t / w⌋ · w + w)`,
  and `SketchEnvelope` documents `window_start_ms` as inclusive and
  `window_end_ms` as exclusive. At 10 Hz with 5 s windows every 50th sample
  lies on a boundary, and the two sides put it in different windows, so a
  pushed view and a backend-built view of the same data differ.

The collector change, in its own repository and PR (step 5 of the plan):

1. A sample at `t` belongs to the window ending at `⌈t / w⌉ · w`; a window
   closes when a sample later than its end arrives (`t > end`, not `≥`), or
   on the idle timer as today.
2. A sample is placed by its own timestamp, not into the active window:
   today a sample older than the active window (out of order, from another
   source) lands in the active window. It goes to its own window while that
   window is open, and is dropped and counted once it is closed.
3. `window_start_ms` is exclusive and `window_end_ms` inclusive, in the
   envelope, the `sketch.window_*_ms` attributes and the OTLP point times.
4. `matchers` (name, `=` or `!=`, value; `__name__` for the metric) in
   `AsapSketchesUserConfig`, passed to `PrecomputeConfig::matchers`, which
   the runtime already evaluates but the TOML does not expose.

### 4. The sketch path in the data plane

`POST /v1/metrics` decodes the request once. Each summary data point that
carries a `sketch.envelope` bytes attribute is a pushed state; every other
point is a raw sample, as today. A request may carry both.

The adapter (`asap-sketch-otlp`) reads, per pushed state:

| Field | Where on the wire | Check |
|---|---|---|
| `agg_id` | Resource attribute `sketch.agg_id` (int) | present |
| sketch type | Resource attribute `sketch.type` (`DDSketch`, `KLLSketch`, `HLLSketch`, `CountSketch`, `CountMinSketch`, `CountMinSketchWithHeap`) | known |
| schema version | Resource attribute `sketch.schema_version` | ≤ the highest supported |
| labels | Resource attributes, then Scope (`asap.series`) attributes, then point attributes, without any `sketch.*` key; `__name__` = metric name | the merge order the raw OTLP adapter uses |
| window | point attributes `sketch.window_start_ms`, `sketch.window_end_ms` | equal to the point's `start_time_unix_nano` / `time_unix_nano` in ms; `start < end` |
| state | point attribute `sketch.envelope` (bytes) | non-empty |
| sample count | point `count` | none |

`sketch.series_id` is ignored: it is the collector's local counter and
restarts with it; the labels are the identity. `sketch.size` is ignored: the
parameters are checked from the decoded state.

The ingest engine then, per state:

1. **View.** Looks up `agg_id` in the `PushedViews` map. Unknown → rejected
   (`reason="unknown_view"`).
2. **Window.** Requires `end − start` = the view's width and `end` a multiple
   of it. Otherwise rejected (`reason="window"`).
3. **Parameters.** Decodes the ASAPv1 bytes with `storage`'s codec into the
   view's kernel and compares the decoded sketch's parameters (DDSketch
   `alpha`, KLL `k`, rows × columns, heap size, HLL precision, hash seed)
   with the plan's, exactly (decision 5). A state that does not decode, or
   decodes with other parameters, is rejected (`reason="decode"`,
   `reason="params"`).
4. **Series.** Interns the label set into the store's series table, as the
   raw path does, so the view row's `$promql_series_identity` is the same
   canonical JSON a raw sample of that series gets.
5. **Stage, dedup, commit.** See below.

Every rejection increments
`asap_pushed_states_rejected_total{view, reason}` and logs once per view and
reason per minute; the request still returns 200 with the rejected count, as
the raw OTLP route reports `rejected`.

### 5. Staging, deduplication, late states

The view store appends a view's windows in order and each window once, and
other refresh groups may read a pushed view (a wider level of its chain), so
a pushed window is committed once and whole. The engine stages pushed states
per `(view, window)` and commits through the precompute committer, the same
path that commits a window it built (readers, `RefreshTaskGraph` ordering,
persistence and the memory budget all apply).

- **Commit time.** No raw samples of a pushed view's metric need reach the
  backend, so the raw watermark cannot close its windows. A pushed window
  commits when the wall clock passes `end + push_grace_ms` (default 10 s:
  the collector's 5 s idle-close grace plus an export interval).
- **Duplicates.** A state for a `(view, series, window)` that already has a
  state with the same bytes (compared by a 64-bit hash of the envelope, kept
  per key for the view's retention) is ignored and counted
  (`asap_pushed_states_duplicate_total{view}`). This covers exporter
  retries.
- **Second states.** A state for a `(view, series, window)` that already has
  a state with other bytes is merged into it (sketch merge). This covers two
  partial states: a collector that restarted inside a window, or two
  collectors that each saw part of one series. Merging is safe because the
  states are full, not deltas, and each partial covers different samples.
- **Late states.** A state that arrives after its window committed:
  - within the view's retention, it is merged into the committed window
    (deduplicated as above), through `merge_late_rows` below, which writes
    the merged window to memory and to its view file;
    `asap_pushed_states_late_merged_total{view}`;
  - beyond the retention, it is dropped;
    `asap_pushed_states_late_dropped_total{view}`.

A late merge changes a window a query may already have read. The result
cache keeps final answers per evaluation time, so the merge drops the cached
answers whose range covers the merged window (open question 6).

## Code interface

### `crates/common`

```rust
/// States the collector built for pushed views: what the sketch-path ingest
/// adapter hands the ingest engine. Never data between operators.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewStateBatch {
    pub states: Vec<PushedViewState>,
}

/// One series' state of one pushed view in one window.
#[derive(Debug, Clone, PartialEq)]
pub struct PushedViewState {
    /// The view, as the collector config encodes it (see `AggId`).
    pub agg_id: AggId,
    /// Which sketch the bytes hold, as the record declares it.
    pub sketch: PushedSketch,
    /// ASAPv1 envelope bytes from `asap_sketchlib`: the full state.
    pub asapv1: Vec<u8>,
    /// The series' labels, `__name__` included.
    pub labels: LabelSet,
    /// The window `(start_ms, end_ms]`.
    pub window_ms: (i64, i64),
    /// Samples folded into the state, as the collector counted them.
    pub sample_count: u64,
}

/// A pushed view's id on the wire: plan fingerprint (high 31 bits) and
/// node id (low 32 bits).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AggId(pub u64);

/// The sketches the collector builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PushedSketch { DDSketch, Kll, Hll, CountSketch, CountMin, CountMinHeap }

/// What happened to a batch's states.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ViewStateIngestReport {
    pub staged: u64,
    pub duplicate: u64,
    pub late_merged: u64,
    pub late_dropped: u64,
    /// Rejected states by reason (`unknown_view`, `window`, `decode`, `params`).
    pub rejected: BTreeMap<&'static str, u64>,
}

/// Takes states the collector built for pushed views. Implemented by the
/// ingest engine, next to `IngestEngine`.
#[async_trait]
pub trait ViewStateIngest: Send + Sync {
    /// Checks, stages or merges each state; waits under backpressure as
    /// `IngestEngine::ingest` does.
    async fn ingest_view_states(&self, batch: ViewStateBatch)
        -> Result<ViewStateIngestReport, IngestError>;
}
```

`ViewStore::put`'s contract ("each key is put once and never rewritten")
gains one exception, for late states:

```rust
pub trait ViewStore: Send + Sync {
    // put, get, instances, evict_before, timestamp_range_per_view: unchanged

    /// Replaces the committed instance under `key` with `instance` (the old
    /// one merged with late states). `false` when `key` is not stored
    /// (evicted, or never committed).
    fn replace(&self, key: ViewInstanceKey, instance: ViewInstance) -> bool;
}
```

and the backend's own store, which the engines use directly:

```rust
// crates/storage/src/store.rs
impl ViewStore {
    /// Replaces the rows of `view`'s committed window `time_range_ms` with
    /// `batches` (copy on write: runs reading the old rows keep them); the
    /// window is re-queued for its view file. `Err` when the window is not
    /// stored.
    pub fn merge_late_rows(
        &mut self,
        view: &MaterializedView,
        time_range_ms: (i64, i64),
        batches: Vec<Batch>,
    ) -> Result<(), Error>;
}
```

### `crates/adapters/ingest/asap-sketch-otlp`

```rust
/// Why a request's pushed states could not be read (the whole request is
/// refused with 400; per-state problems are counted, not errors).
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SketchOtlpError {
    #[error("sketch record without {0}")]
    MissingAttribute(&'static str),
    #[error("unknown sketch.type {0:?}")]
    UnknownSketchType(String),
    #[error("sketch.schema_version {0} is newer than supported")]
    SchemaVersion(u64),
    #[error("window attributes ({start_ms}, {end_ms}] disagree with the point's times")]
    Window { start_ms: i64, end_ms: i64 },
}

/// Whether an OTLP data point carries a pushed state (a `sketch.envelope`
/// bytes attribute): the route's test for the sketch path.
pub fn is_sketch_point(attributes: &[KeyValue]) -> bool;

/// The pushed states of a decoded OTLP metrics request; points without
/// `sketch.envelope` are left to the raw `otlp` decoder.
pub fn decode_view_states(
    request: &ExportMetricsServiceRequest,
) -> Result<ViewStateBatch, SketchOtlpError>;
```

The raw `otlp` decoder already skips points that carry a sketch payload; it
learns `sketch.envelope` as one more such attribute, so one request body is
decoded by both and each point is read once.

### `crates/control-plane`

```rust
/// A leaf view the collector builds, and how.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PushedView {
    pub node: PhysicalASAPNodeId,
    pub agg_id: u64,
    pub metric: String,
    /// Label matchers: the metric name, then the Scan's predicates.
    pub matchers: Vec<CollectorMatcher>,
    pub sketch_type: &'static str,
    pub sketch_params: BTreeMap<&'static str, f64>,
    pub window_ms: u64,
}

/// Why a leaf view is not pushed, for the command's report.
#[derive(Debug, Clone, PartialEq)]
pub enum NotPushed {
    NotOnRawScan,
    NotPerSeries,
    NotTumbling,
    UnsupportedSummary(String),
    UnsupportedParameters(String),
    FilterNotExpressible(String),
}

/// The installed plan's pushed views, by the rule of this design, and why
/// each other leaf view is not pushed.
pub fn pushed_views(plan: &InstallablePlan)
    -> (Vec<PushedView>, Vec<(PhysicalASAPNodeId, NotPushed)>);

/// The collector TOML: one `asap_sketches` processor block per pushed view.
pub fn emit_collector_config(views: &[PushedView]) -> String;
```

CLI: `control_plane --plan <plan.json> --emit-collector-config
<collector.toml> --pushed-views <pushed-views.json>` reads a saved plan and
writes both files; the data plane takes `--pushed-views
<pushed-views.json>`.

## Worked example: q01

The query, from the `smoke` workload: `quantile_over_time(0.99, data[1m])`,
evaluated every 5 s, 10 samples/s per series. The plan the control plane
selects has, for q01:

```text
node 0  Scan data                                   (ts, value, $promql_series_identity)
node 1  SummaryTumblingWindowChain                  DDSketch α 0.05, PerEntity, 5 000 ms tumbling,
        (view, ingestion time, retention 120 s)    filter none
node 2  TimeRange 60 s          (query time)        reads node 1's windows in (T − 60 s, T]
node 3  SummaryMerge            (query time)        merges them per series
node 4  SummaryEstimate q = 0.99
node 5  Project                 (root)              (ts, value, $promql_series_identity)
```

Node 1 passes the rule: its child is a `Scan` of `data`, it is `PerEntity`,
tumbling at 5 s, DDSketch with α 0.05 maps to `relative_accuracy = 0.05`,
and it has no predicate or filter. The command writes the block below
(for illustration, a plan whose fingerprint truncates to `0x2acbeb01`, so
`agg_id` = `0x2acbeb01_00000001`):

```toml
# Pushed view: node 1 (SummaryTumblingWindowChain DDSketch, alpha 0.05,
# PerEntity, 5000 ms tumbling) of the installed plan.
[[pipelines.metrics.processors]]
id     = "asap_view_1"
plugin = "asap_sketches"

  [pipelines.metrics.processors.config]
  sketch_type         = "ddsketch"
  window_size         = "5s"
  output_metric_name  = "data"
  agg_id              = 3083816759392731137
  transmit_sketch     = true
  aggregate_by        = []
  omit_resource_attrs = false
  global_aggregation  = false
  emit_window_stats   = false
  # Requires the collector's matchers option (collector change, item 4).
  matchers = [{ name = "__name__", op = "=", value = "data" }]

  [pipelines.metrics.processors.config.sketch_params]
  relative_accuracy = 0.05
```

and `pushed-views.json`:

```json
[{"node": 1, "agg_id": 3083816759392731137, "metric": "data",
  "matchers": [{"name": "__name__", "op": "=", "value": "data"}],
  "sketch_type": "ddsketch", "sketch_params": {"relative_accuracy": 0.05},
  "window_ms": 5000}]
```

For one series `data{region="r0",service="s0",job="j0",label_0="l0"}`, the
collector folds the 50 samples with timestamps in `(T − 5 s, T]` into one
DDSketch and exports one summary point: Resource `sketch.agg_id =
3083816759392731137`, `sketch.type = "DDSketch"`; Scope `asap.series` with
the four labels; metric `data`; point `sketch.window_start_ms = T − 5000`,
`sketch.window_end_ms = T`, `sketch.envelope = <ASAPv1 bytes>`, `count = 50`.

The backend keeps exactly what its own precompute keeps for node 1: per
series, one DDSketch state per 5 s window, for the view's 120 s retention
(24 windows). At evaluation time `T` (a multiple of 5 s), q01 runs at query
time as before: node 2 selects the 12 windows ending in `(T − 60 s, T]`,
node 3 merges the 12 states of each series into one, and node 4 reads the
0.99 quantile. That merge chain, the result cache and the fallback do not
change. The backend no longer runs node 1, and `data`'s raw samples still
arrive because node 6 (`max_over_time(data[30s])`) reads them.

## The benchmark queries

The five dashboard queries (`smoke` widths; q05 is q01 as a range panel):

| Query | Leaf view | Pushed | Why |
|---|---|---|---|
| q01 `quantile_over_time(0.99, data[1m])` | DDSketch α 0.05, per series, 5 s tumbling, on `Scan data` | yes | passes every rule |
| q02 `topk(10, sum by (region, service, job) (rate(data_total[1m])))` | `ExactAggregate Rate`, per series, 5 s tumbling | no | exact counter state: the collector builds none |
| q03 `max_over_time(data[30s])` | `ExactAggregate Max`, per series, 5 s tumbling | no | exact aggregate: the collector builds none |
| q04 `sum by (region, service) (increase(data_total[1m]))` | `ExactAggregate Increase`, per series, 5 s tumbling | no | exact counter state |
| q05 q01 as a range panel (60 s span, 5 s step) | the same view as q01 | yes | shares q01's leaf; earlier steps come from the result cache |

So for this workload the collector builds one view, the backend still builds
three, and both metrics' raw samples still reach the backend. Step 4
measures what that saves; a workload whose metric is read only by pushed
views saves the raw samples too.

## Plan

Separate PRs, each reviewed on its own:

1. This document.
2. `ViewStateBatch` and `ViewStateIngest` in `common`; the
   `asap-sketch-otlp` adapter; the `/v1/metrics` split; staging, dedup,
   late merge and `ViewStore::merge_late_rows`. Test data: real ASAPv1
   records the collector emits (captured OTLP bodies, checked in with the
   collector commit that wrote them), for each of the six sketch types,
   including one with mismatched parameters, one duplicate and one late
   state.
3. The control plane's push-down rule, `--emit-collector-config` and
   `--pushed-views`; the precompute engine skips pushed views. Tests: the
   `smoke` plan emits exactly the q01 block above; a plan with a regex
   matcher, a `Reduce` leaf or a sliding window pushes nothing.
4. End-to-end comparison, collector + backend against backend only, on the
   same feed: answers equal (exact views) or within the plan's error
   (sketch views) at every evaluation; backend ingest CPU, network bytes
   between collector and backend, and collector CPU; including a sparse
   series case for the limitation above.
5. In the collector repository: the window boundary and filter change
   (algorithm step 3). It lands before step 4, which needs equal windows.

## Decisions

1. **ASAPPlanner does not change.** Which views the collector builds is a
   deployment decision the backend's control plane makes on the installed
   plan; the plan and its `InstallablePlan` format are unchanged.
2. **The collector is ASAPCollector-public (OTAP), sending full ASAPv1
   states.** No deltas: each record holds the whole state of one series in
   one window.
3. **Two paths.** The raw path (raw OTLP or Remote Write to the backend)
   stays, for views that are not pushed, query-time `Scan`s and the
   fallback. The sketch path carries pushed states on the same
   `/v1/metrics` route; a record with `sketch.envelope` takes the sketch
   path.
4. **Prometheus window semantics on both sides**: `(t − range, t]`, as
   Prometheus 3.x range selectors. The backend already uses it; the
   collector changes in its own PR.
5. **Parameters match the plan exactly**; the backend rejects a state whose
   decoded parameters differ, and counts it. A state delivered twice for the
   same view, series and window is ignored the second time, and counted.
   A late state within retention is merged into the stored state; beyond
   retention it is dropped and counted.
6. **Rule-based push-down first**: a view is pushed iff its leaf reads raw
   input directly, is per series, uses a tumbling window, its sketch type
   and parameters are among the collector's (DDSketch, KLL, HLL,
   CountSketch, Count-Min, Count-Min-heap), and its filter is expressible in
   the collector's configuration. No cost comparison. Exact aggregates stay
   in the backend, since the collector builds none. Cost-based push-down is
   future work and needs a general planner capability.
7. **`agg_id` is the view id**, and the backend's precompute skips pushed
   views.
