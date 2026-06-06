# Edge Aggregation Scope (`mode`) and Sparse HLL (`hll_sparse`)

This note documents how the control plane maps query/workload signals to two
per-metric knobs it emits into the edge agent's fused `asap_edge` processor
config (`processors.asap_edge.metrics[]`):

1. **`mode`** — the aggregation **SCOPE**: `per_series` (default) vs
   `whole_stream`.
2. **`hll_sparse`** — opt-in to the in-memory **sparse HLL** base (bool,
   default `false` = dense).

The edge side of both knobs landed in ASAPCollector:

- **#471** added `MetricFamily.mode` (operator-facing string `per_series` /
  `whole_stream`), plumbed into `precompute.PrecomputeConfig.Scope`
  (`AggMode` = `ModePerSeries` / `ModeWholeStream`). #471 *folded the legacy
  `GlobalAggregation` bool INTO this scope*: `whole_stream` is the unified
  `effectiveScope`, semantically identical to the old empty-`aggregate_by` →
  `GlobalAggregation` behaviour.
- **#472** added the sparse-HLL base (`NewHLLWrapperSparse`), selected per-HLL
  by `hll_sparse`. The sparse base is *lossless*: it auto-promotes to dense
  once enough registers are set, and its serialized output is byte-identical to
  dense for the same inputs. Default `false` (dense) = today's behaviour.

Both knobs are emitted from a single site: `control_plane/src/emit/stage_config.rs`,
in `emit_edge_yaml_asap_edge`'s sketch-family loop (the `e: Mapping` per-metric
entry assembly). The legacy single-processor emitter in `emit/agent.rs`
(`build_processor_block`) does **not** emit a `metrics[]` family map — its
`mode` field is the unrelated `ProcessorMode` (Window/Batch windowing strategy),
not this aggregation scope — so it needs **no** change.

## `mode` (aggregation scope)

### The signal: effective grouping × family

At the emit site each sketch entry already computes its **effective
`aggregate_by`** = the metric's `metric_to_grouping_labels` minus its
`metric_to_item_label` (the inner heavy-hitter dimension is the sketch
*subject*, not a grouping key). Call this `effective_by`.

- `effective_by` **non-empty** ⇒ the edge keys ONE sketch per group (e.g. per
  `zone`). This is `per_series` (per-group) scope.
- `effective_by` **empty** ⇒ the edge factory collapses grouping into a single
  attr-less sketch. Per #471 that *is* `whole_stream`.

But "empty grouping" alone is **not** sufficient: the per-series quantile
families (DDSketch / KLL) also carry no grouping (a `quantile_over_time` reduces
*within* a single series), and an empty grouping there means "no extra keying",
**not** "collapse the whole stream". Emitting `whole_stream` for them would be
wrong.

So the rule combines grouping **and family role**:

```
whole_stream  ⇔  effective_by.is_empty()  AND  family ∈ {HLL, CMS, CountSketch}
```

The item-counting / frequency families (HLL = distinct-count, CMS = frequency,
CountSketch = top-k / frequency) with an empty effective grouping are exactly
the *genuinely global* aggregates the planner emits for `count(distinct …)`
(no `by`), global top-k, and global frequency — #471's own `WholeStream`
examples ("distinct-count / global-quantile / grand-total / global-top-k").

### What we emit

- **whole_stream**: emit `mode: whole_stream`.
- **per_series**: emit **nothing**. `per_series` is the edge default (an
  omitted/empty `mode` parses to `ModePerSeries`), so omitting keeps the YAML
  byte-identical to the pre-#471 emit for every metric that is not a genuine
  whole-stream global aggregate. This is the deliberate choice: explicit
  `whole_stream` where it matters, implicit `per_series` everywhere else.

### Back-compat / safety

`whole_stream` ≡ the folded `GlobalAggregation` (#471's `effectiveScope`), so
emitting it for an HLL/CMS/CountSketch that **today** already produces an empty
`aggregate_by` is semantically identical to current behaviour — no wire-level
change, just an explicit name for the existing collapse.

Crucially we emit `whole_stream` **only** where `effective_by` is empty. A
heap-bearing CountSketch (warm top-k) or any item-counting family that *does*
carry per-group keying (non-empty `effective_by`) stays `per_series`, so we
never newly collapse a metric that needs per-group sid minting at the backend
(the failure mode the long comment in `emit_edge_yaml_asap_edge` warns about:
empty `aggregate_by` + GlobalAggregation breaks backend sid minting for
attr-less series). We do not *introduce* empty-grouping; we only *name* the
scope the existing emit already produces.

## `hll_sparse` (sparse HLL base)

Emitted **only** on HLL-family entries (`SketchKind::Hll`); never on any other
family.

### The rule (scope-driven)

```
hll_sparse  =  !whole_stream     (i.e. true for per_series HLL, false for whole_stream HLL)
```

- **whole_stream HLL** → a single high-cardinality instance per metric (distinct
  values over the whole stream). It promotes to dense almost immediately, so the
  sparse base buys nothing and only adds promotion churn → `hll_sparse: false`
  (dense).
- **per_series HLL** → one HLL per group; most groups are low-cardinality (e.g.
  distinct `user_id`s per `region`), where the sparse base is a large memory win
  and auto-promotes the few hot groups → `hll_sparse: true`.

Because the sparse base is lossless and auto-promoting, the scope-based default
carries **zero** accuracy or wire risk.

### Cardinality hint (deferred follow-up)

The ideal refinement is a per-metric cardinality hint:
`WorkloadCharacteristics.distinct_keys_per_window: Option<u64>`
(`control_plane/src/types.rs:45`). When present and **large** — above the
dense-crossover (~4096 non-zero registers ≈ the in-memory promotion point) — even
a `per_series` HLL should go dense (`hll_sparse: false`) to skip the
sparse→dense promotion. When small or absent, sparse.

**This hint is not reachable at the emit site today.** The only input to
`emit_edge_yaml_asap_edge` is `EdgeStageConfig`
(`control_plane/src/physical/colored_dag/emitter.rs`), which carries no
`WorkloadCharacteristics` and no per-metric cardinality map. Plumbing one
through the L5 stage config is a follow-up. Until then we use the scope-based
default above, which is safe (sparse is lossless). Per the default-OFF safety
rule, we never make a metric sparse when scope can't be determined — but for the
HLL family scope is always determinable here (empty vs non-empty
`effective_by`), so HLL entries always carry an explicit `hll_sparse`.

## Worked examples

| Query | Family | grouping_labels | item_label | `effective_by` | `mode` emitted | `aggregate_by` | `hll_sparse` |
|---|---|---|---|---|---|---|---|
| `count(distinct user_id)` (no `by`) | HLL | — | `user_id` | empty | `whole_stream` | (omitted) | `false` (dense) |
| `count by (region)(distinct user_id)` | HLL | `[region]` | `user_id` | `[region]` | (omitted ⇒ per_series) | `[region]` | `true` (sparse) |
| `quantile_over_time(0.99, latency[5m])` (per-series) | DDSketch / KLL | — | — | empty | (omitted ⇒ per_series) | (omitted) | n/a (not HLL) |
| `quantile by (zone)(latency)` | DDSketch | `[zone]` | — | `[zone]` | (omitted ⇒ per_series) | `[zone]` | n/a |
| `topk(10, endpoint_qps)` (global) | CountSketch | — | `endpoint` | empty | `whole_stream` | (omitted) | n/a |
| `topk(10, sum by (zone)(...))` | CountSketch + heap | `[zone]` | `endpoint` | `[zone]` | (omitted ⇒ per_series) | `[zone]` | n/a |
| global frequency `endpoint_request_freq` | CMS | — | `endpoint` | empty | `whole_stream` | (omitted) | n/a |
| `sum by (zone)(http_requests_total)` | Sum (not a sketch) | `[zone]` | — | `[zone]` | (no mode on sum entries) | `[zone]` | n/a |

Note the byte-stability column: every row that emits no `mode` (the per-series
quantile and per-group cases) keeps its `metrics[]` entry byte-identical to the
pre-#471 emit; only the genuine whole-stream globals gain a `mode:` key, and
only HLL entries gain `hll_sparse:`.

## Tests

In `control_plane/src/emit/stage_config.rs`:

- `fused_asap_edge_emits_single_pipeline_and_metrics_list` (extended): asserts
  the global item-counting families (HLL / CountSketch / CMS, no grouping) emit
  `mode: whole_stream` and no `aggregate_by`; the per-series quantile families
  (DDSketch / KLL) and the sum entry emit no `mode`; the whole-stream HLL emits
  `hll_sparse: false`; no non-HLL family carries `hll_sparse`.
- `fused_asap_edge_per_group_hll_is_per_series_and_sparse`: a per-group HLL
  (`grouping=[region]`, `item_label=user_id`) emits `aggregate_by: [region]`,
  no `mode` (per_series), and `hll_sparse: true`.
- `fused_asap_edge_quantile_only_omits_mode_and_sparse`: byte-stability guard —
  a quantile-only (DDSketch + KLL) plan emits neither `mode:` nor `hll_sparse`.
- `fused_asap_edge_keys_are_a_subset_of_asapedgeprocessor_config_go`: extended
  to allow `mode` and `hll_sparse` as known `MetricFamily` keys.
