# Analyzer parity matrix — α catalog

**Companion to** `data_plane/src/query_engines/asap_query_engine/engine.rs::analyzer_parity_tests`.

Two PromQL → asap-tier analyzers live in this workspace today and the
analyzer-unification chain (α→β→γ→δ→ε) is going to collapse them. This
document is the **human-readable freeze** of how each one answers an
18-query corpus; the inline golden-master test pins the same data
byte-for-byte. β/γ MUST preserve every `engine` row in the table below
— that is the parity contract. δ may change them **only** when the
matching `ctrl` row already matches.

## The two analyzers

| | **Control plane (canonical)** | **Engine (duplicate)** |
|---|---|---|
| Entry | `control_plane::asap_tier_analysis::analyze_promql_for_asap_tier(metricsql: &str) -> ASAPTierAnalysis` | `ASAPQueryEngine::parse_and_match_promql` + `build_query_requirements_promql` |
| Pipeline | `query_parser::parse_query` → `intent_algebra::lower::lower_parsed_query` → walk `QueryExpr` → `sketch_algebra::capability::capability_for(&AggIntent)` | `promql_parser::parser::parse` → match against `controller_patterns: HashMap<QueryPatternType, Vec<PromQLPattern>>` built in `new_with_hot_reload` |
| Output value | `ASAPTierAnalysis { candidates: Vec<ASAPTierCandidate>, unsupported: Option<UnsupportedReason> }` | `(QueryPatternType, PromQLMatchResult)` + `QueryRequirements { metric, statistics, data_range_ms, grouping_labels, spatial_filter_normalized }` |
| Vocabulary | `Capability` + `AggIntent` (L3 — semantic) | `Statistic` + `QueryPatternType` (physical — sketch-storage table) |
| File | `control_plane/src/asap_tier_analysis.rs` | `data_plane/src/query_engines/asap_query_engine/engine.rs` (~1100 LOC of analyzer-shaped code spread across the file) |

## How to read each row

```
─── q01: <promql>
    ctrl   {OK [cand,...] | MISS(reason)}
    engine {OK pattern=… stats=[…] metric=… fn=… agg_op=… range_s=… range_ms=… spatial=… grouping=… | MISS(NoPattern)}
```

- `ctrl` rows speak `Capability` + `AggIntent`. `MISS(UnsupportedAggIntent("X"))` means `capability_for` returned `None` for `AggIntent::X` at the default accuracy; `MISS(NoCallNodeFound)` means the lowerer produced no call shape (bare selector); `MISS(UnparseableMetricsql(...))` means `query_parser::parse_query` rejected the input.
- `engine` rows speak `Statistic` + `QueryPatternType`. `MISS(NoPattern)` means none of the entries in `controller_patterns` matched the AST.

## The 18-query corpus

| id  | query | ctrl | engine | parity? |
|-----|-------|------|--------|--------|
| q01 | `quantile_over_time(0.99, http_latency_ms[5m])` | OK · `QuantileApprox(Any)` · range=300s | OK · `only_temporal/quantile` · range=300s | ✅ |
| q02 | `quantile_over_time(0.5, m[30s])` | OK · range=30s | OK · range=30s | ✅ |
| q03 | `quantile_over_time(0.99, m[2h])` | OK · range=7200s | OK · range=7200s | ✅ |
| q04 | `sum by (zone) (http_requests_total)` | MISS · `UnsupportedAggIntent("sum")` | OK · `only_spatial/sum` · grouping=`[zone]` | **D1** |
| q05 | `sum by (zone, region) (http_requests_total)` | MISS · same | OK · grouping=`[region, zone]` (sorted) | **D1** |
| q06 | `topk(5, http_requests_total)` | MISS · `UnsupportedAggIntent("topk")` | OK · `only_spatial/topk` | **D2** |
| q07 | `topk(10, sum by (svc) (m))` | MISS · `UnsupportedAggIntent("topk")` | MISS · `NoPattern` | ✅ |
| q08 | `count_over_time(http_requests_total[5m])` | MISS · `UnsupportedAggIntent("count_over_time")` | OK · `only_temporal/count` · range=300s | **D3** |
| q09 | `count by (zone) (count_over_time(http_requests_total[5m]))` | OK · `CardinalityApprox` · gbk=`[zone]` · range=300s | OK · `one_temporal_one_spatial/count` · grouping=`[zone]` · range=300s | ✅ |
| q10 | `histogram_quantile(0.99, sum by (le) (rate(http_latency_bucket[5m])))` | MISS · `UnparseableMetricsql` (γ5 substitution rejects non-matrix arg) | MISS · `NoPattern` (no `histogram_quantile` pattern) | ✅ |
| q11 | `histogram_quantile(0.99, http_latency_bucket)` | MISS · `UnparseableMetricsql` (same) | MISS · `NoPattern` | ✅ |
| q12 | `http_requests_total` | MISS · `NoCallNodeFound` | MISS · `NoPattern` | ✅ |
| q13 | `http_requests_total{zone="z0"}` | MISS · `NoCallNodeFound` | MISS · `NoPattern` | ✅ |
| q14 | `rate(http_requests_total[5m])` | MISS · `UnsupportedAggIntent("rate")` | OK · `only_temporal/rate` | **D4** |
| q15 | `irate(http_requests_total[5m])` | MISS · `UnsupportedAggIntent("irate")` | MISS · `NoPattern` (no `irate` in engine pattern list) | ✅ |
| q16 | `increase(http_requests_total[5m])` | MISS · `UnsupportedAggIntent("increase")` | OK · `only_temporal/increase` | **D4** |
| q17 | `sum(rate(http_requests_total[5m]))` | MISS · `UnsupportedAggIntent("sum")` | OK · `one_temporal_one_spatial/rate` · agg_op=sum | **D5** |
| q18 | `@@@ not promql @@@` | MISS · `UnparseableMetricsql("PromQL parse error: invalid promql query")` | MISS · `NoPattern` | ✅ |

## Divergences

Each divergence has a code; γ implementations refer to it.

### D1 — `sum by (...)(m)` control-plane-reject vs engine-accept

`sum`, `min`, `max`, `count` over a bare metric never become an
`AggIntent::Sum`-with-capability — `capability_for(Sum)` returns
`None` because sum is exact (no sketch needed), the ASAP tier doesn't
materialise the raw counter values that a `sum` would reduce. The
engine, by contrast, registers `Statistic::Sum` in `OnlySpatial` and
routes to a stored aggregation that exposes the sum.

This is **semantically correct on both sides**: the control plane
refuses because the ASAP-tier sketch can't satisfy the query *as
written*; engine accepts because the ASAP tier might still own a
*precomputed* sum-by-zone aggregation that does. γ's adapter must keep
the engine path answerable when a precompute exists — see
`find_compatible_aggregation_with_miss_notify`.

### D2 — `topk(k, m)` control-plane-reject vs engine-accept

Same shape as D1: the control plane treats `topk` as a non-sketchable
exact AggIntent (`UnsupportedAggIntent("topk")`); engine accepts as
`Statistic::Topk` and answers from a precomputed CMS-with-heap when
the indexed sketch carries `Capability::FrequencyTopk(...)`. γ must
preserve the engine path's `topk(k, bare_metric)` shape.

### D3 — `count_over_time(m[r])` control-plane-reject vs engine-accept

`count_over_time` lowers to `AggIntent::Count` in the control plane,
and `capability_for(Count)` at the default accuracy is `None` (count
is exact). Engine emits `Statistic::Count` and routes to a counter
agg. This is the textbook example of "the engine has a precompute
table the control plane doesn't model" — γ must preserve the engine
row.

### D4 — `rate(m[r])` and `increase(m[r])` control-plane-reject vs engine-accept

The control plane's analyzer rejects rate/increase as
`UnsupportedAggIntent("rate" / "increase")`. The engine has them in
the temporal generic pattern set and emits `Statistic::Rate` or
`Statistic::Increase`. Note that **`irate` is NOT in the engine
pattern list** (q15), so `irate` is one of the few queries both
analyzers reject.

### D5 — `sum(rate(m[r]))` rejected by the control plane, accepted by engine as `OneTemporalOneSpatial/rate` + `agg_op=sum`

The composition of D1 (sum) and D4 (rate). Engine has the
`spatial_of_temporal_pattern` cross-product wired in
`new_with_hot_reload` (the engine's `OneTemporalOneSpatial` table);
the control plane's lowerer hits the outer `sum` first and bails. γ
must preserve the composed engine output.

## Implementation notes

1. **`histogram_quantile` parser-level substitution (γ5 / PR #144)** —
   the control plane substitutes `histogram_quantile(phi, m)` to
   `AggIntent::Quantile { q: phi }` at the parser site. In the
   current corpus (q10, q11) this substitution is rejecting both
   shapes with `UnparseableMetricsql("expected MatrixSelector, got
   Discriminant(...)")` — the substitution path requires
   `histogram_quantile(phi, MatrixSelector)` directly, and the
   composed `histogram_quantile(phi, sum_by(le, rate(m[r])))` is not
   accepted. This may be an intentional restriction (γ5 only handles
   the canonical Prometheus shape), but is worth re-examining when
   ε expands the corpus. **Do NOT reintroduce
   `AggIntent::HistogramQuantile`** — this is the hard cross-cutting
   invariant from `controller_todo_may_12.md` §6.

2. **`KeyByLabelNames::new` sorts** — q05's grouping comes back as
   `[region, zone]` not `[zone, region]`. The engine summary already
   reflects this; the control plane's `BTreeSet<String>` ordering
   aligns.

3. **`QuantileApprox(Any)` vs `QuantileApprox(DDSketch)`** — the
   TODO doc references `QuantileApprox(DDSketch)`, but post-Step 2a
   (PR #129) `capability_for` returns the family-wildcard `Any` form.
   Don't be surprised by the diff — `Capability::is_satisfied_by`
   does the family match at the indexed-side.

4. **Why no `Capability::is_satisfied_by` tests in α** — those live
   in `control_plane/src/sketch_algebra/capability.rs::tests` and the
   data plane's `index::classify` tests. α is strictly about *what
   each analyzer says it requires*; satisfaction is downstream.

## Where to look for each pipeline

### Control plane path
- Parse: `control_plane/src/query_parser/promql.rs` (γ5 lives here)
- Lower: `control_plane/src/intent_algebra/lower.rs`
- Capability map: `control_plane/src/sketch_algebra/capability.rs::capability_for`
- Public entry: `control_plane/src/asap_tier_analysis.rs::analyze_promql_for_asap_tier`

### Engine path
- Pattern set: `data_plane/src/query_engines/asap_query_engine/engine.rs:316–460` (inside `new_with_hot_reload`)
- AST match: `parse_and_match_promql` (`:2219`)
- Requirements build: `build_query_requirements_promql` (`:1690`)
- Free helpers (also to be deleted in δ): `crates/promql_utilities/src/query_logics/parsing.rs::{get_metric_and_spatial_filter, get_statistics_to_compute, get_spatial_aggregation_output_labels}`

## When to update this file

- **β** (add `physical_projection` + `analyze_promql_full`) — extend
  the control plane column when it newly covers a previously-MISS row
  (D1/D2/D3/D4/D5). The engine column must NOT move.
- **γ** (rewrite engine analyzer as a shim) — the engine column MUST
  match the embedded golden exactly. If it doesn't, γ has dropped
  parity.
- **δ** (delete the engine analyzer) — engine column becomes
  derivable from the control plane column; merge the two into a single
  `unified` column and re-run α.
- **ε** (expand corpus to 35 queries) — append new rows; never edit
  q01–q18 without an accompanying β/γ semantic change explaining
  the drift.
