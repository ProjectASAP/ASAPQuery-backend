//! Per-Capability sketch reducer (ASAP-tier query evaluator).
//!
//! Caller has already classified all candidate sids as `Hit`
//! against the [`SketchStore`] (see PR #122's classify hook in
//! `simple/engine.rs::QueryEngine::execute`). This module:
//!
//! 1. Resolves each sid's [`Capability`] + [`SketchKindHandle`] +
//!    [`SketchConfig`] from `SketchStore::instance`.
//! 2. Validates that the user's PromQL function is answerable by
//!    that capability — `quantile_over_time` only on
//!    `QuantileApprox`, `topk` only on `FrequencyTopk`,
//!    `count_distinct_over_time` only on `CardinalityApprox`.
//! 3. For each sid, calls `SketchStore::query_range` to fetch all
//!    `SketchTimeSeries` (one per distinct group-by VALUES vector)
//!    for the request window.
//! 4. For each window's sketch state:
//!    - Decode bytes via the encoding-specific deserialize
//!      (`from_sketchlib_proto_bytes` for `ProtoFull`,
//!      `from_msgpack_bytes` for `MsgpackFull`; `*Delta` encodings
//!      surface as `DeserializeFailure` because applying a delta
//!      requires the prior base, which the ASAP-tier query path
//!      doesn't carry today).
//!    - Run the canonical sketch query (`quantile`, `estimate`).
//! 5. Return per-series, per-window scalars in [`ASAPTierResult`].
//!
//! The deserialize + query primitives are the **same** library
//! calls that `precompute_operators::*_accumulator.rs` uses — so
//! a query answered through this reducer matches what the
//! precompute path would have produced from the same bytes.
//!
//! ## What's deferred
//!
//! - **Per-window merge for window queries**: today
//!   `quantile_over_time` returns one quantile per
//!   `window_end_unix_ms` rather than merging windows in the
//!   request range and returning a single quantile. This matches
//!   how the ASAP-tier columnar store carries one sketch per
//!   `(start, end)` window; the request-range merge can be added
//!   as a post-process when the simple engine's range-query
//!   pipeline is wired to call this reducer.
//! - **Hybrid stitch** (`[t0..t1']` from warm + `[t1'..t1]` from
//!   archive) — `QueryResult` doesn't carry timestamp-coverage
//!   metadata yet, so we materialize the full ASAP-tier answer
//!   and let the engine router decide.
//! - **Top-k items**: top-k requires CMS-with-heap (the heap
//!   structure carries the actual heavy hitters); the
//!   `Capability::FrequencyTopk(SketchKindHandle::CountMin)`
//!   variant in PR #122 doesn't yet plumb the per-key list
//!   through the wire format. We surface `FrequencyTopk` queries
//!   as `UnsupportedCapability` for now and document the gap.

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_sketchlib::CountMinSketch;
use asap_sketchlib::CountSketch;
use asap_sketchlib::DdSketch;
use asap_sketchlib::HllSketch;
use asap_sketchlib::KllSketch;
use asap_sketchlib::MessagePackCodec;

use crate::storage_engines::sketch_db::query::decoders::{
    decode_cms_from_msgpack, decode_cms_from_proto, decode_cms_with_heap_from_msgpack,
    decode_cs_from_msgpack, decode_cs_from_proto,
};
use crate::storage_engines::sketch_db::query::delta_apply::{
    cumulative_evaluate, per_window_evaluate, DeltaSketchKind,
};
use crate::storage_engines::sketch_db::index::{
    AggregationType, Capability, SketchEncoding, SketchStore, SketchInstanceMetadata,
    SketchKindHandle, SketchSampleState,
};
use promql_utilities::query_logics::enums::Statistic;

/// Reducer wrapping a `&SketchStore`. Constructed per-query; cheap.
pub struct SketchReducer<'a> {
    pub index: &'a SketchStore,
}

/// Distinct failure modes the engine maps onto the routing layer.
///
/// `UnsupportedFunction` / `UnsupportedCapability` → "the ASAP tier
/// can't answer this; archive can". `DeserializeFailure` → "the
/// ASAP-tier state didn't decode; defensive fallback". `NoData` →
/// "the sketch index has no samples in `[t0, t1]`; archive may have
/// older history". `MissingHeap` → "the sid is FrequencyTopk-classed
/// but the underlying sketch family carries no heap (vanilla
/// CountSketch / CountMinSketch without `CmsWithHeap`), so the
/// reducer can't materialize top-k items without an external item
/// universe".
#[derive(Debug)]
pub enum ASAPTierError {
    UnsupportedFunction(String),
    UnsupportedCapability {
        function: String,
        capability: Capability,
    },
    /// Top-k requested against a `FrequencyTopk(CountMin)` or
    /// `FrequencyTopk(CountSketch)` sid (i.e. the sketch shape
    /// supports point queries but not heavy-hitter enumeration). The
    /// router falls over to archive — an archive scan can materialize
    /// the full item universe and compute the true top-k.
    MissingHeap {
        sid: u64,
        sketch_kind: SketchKindHandle,
    },
    DeserializeFailure {
        sid: u64,
        encoding: SketchEncoding,
        reason: String,
    },
    NoData {
        metric_name: String,
    },
}

impl std::fmt::Display for ASAPTierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ASAPTierError::UnsupportedFunction(name) => {
                write!(f, "ASAP-tier reducer does not support function `{name}`")
            }
            ASAPTierError::UnsupportedCapability {
                function,
                capability,
            } => write!(
                f,
                "ASAP-tier reducer cannot answer `{function}` against capability {capability:?}"
            ),
            ASAPTierError::MissingHeap { sid, sketch_kind } => write!(
                f,
                "ASAP-tier reducer cannot enumerate top-k for sid {sid}: \
                 sketch kind {sketch_kind:?} carries no top-k heap \
                 (CountMin / CountSketch only support point-frequency queries; \
                 use CmsWithHeap for top-k)"
            ),
            ASAPTierError::DeserializeFailure {
                sid,
                encoding,
                reason,
            } => write!(
                f,
                "ASAP-tier sketch decode failure for sid {sid} \
                 (encoding={encoding:?}): {reason}"
            ),
            ASAPTierError::NoData { metric_name } => write!(
                f,
                "ASAP-tier index has no samples for metric `{metric_name}` in window"
            ),
        }
    }
}

impl std::error::Error for ASAPTierError {}

/// Per-series, per-window scalar results.
///
/// `coverage` is the actual `(min_window_start_ms, max_window_end_ms)`
/// the reducer covered. `None` when the reducer didn't observe any
/// in-range window (defensive default). The caller (`ASAPQueryEngine`)
/// compares `coverage` against the requested `[t0, t1]` and, on a
/// partial hit (`cov_lo > t0 || cov_hi < t1`), falls over to archive
/// for the missing range and stitches the two answers. See TODO 3 in
/// the ASAP-tier follow-up PR.
#[derive(Debug, Clone, Default)]
pub struct ASAPTierResult {
    /// `(label_values, samples)` where `samples` is
    /// `(window_end_unix_ms, value)`.
    pub series: Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>,
    /// Effective coverage `(min_window_start_ms, max_window_end_ms)`.
    /// Set whenever the reducer observed at least one window; left
    /// `None` when `series` is empty.
    pub coverage: Option<(u64, u64)>,
}

impl ASAPTierResult {
    pub fn is_empty(&self) -> bool {
        self.series.iter().all(|(_, s)| s.is_empty())
    }
}

/// Family of sketch query the user's function maps onto. Determined
/// once per call so the per-sid loop doesn't re-string-match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryFamily {
    Quantile,
    Cardinality,
    /// Heap-BEARING heavy-hitter top-k. Requires `CmsWithHeap` or
    /// `CountSketchWithHeap` to enumerate items.
    FrequencyTopk,
    /// Heap-LESS bare frequency point query. Answered by `CountMin` /
    /// `CountSketch` (and ALSO by `CmsWithHeap` / `CountSketchWithHeap`,
    /// since the heap is additional info layered over the matrix).
    FrequencyEstimate,
}

impl<'a> SketchReducer<'a> {
    pub fn new(index: &'a SketchStore) -> Self {
        Self { index }
    }

    /// Map a PromQL function name to the ASAP-tier query family it
    /// addresses. After the Step 2a refactor the canonical dispatch is
    /// off the analyzer's `Capability` (see [`capability_to_family`]);
    /// this string-based fallback exists ONLY for the
    /// `SketchReducer::evaluate` entry point's `function_name: &str`
    /// API surface, which is preserved for the existing call sites.
    /// Unrecognised names route to the canonical family via the
    /// downstream `require_capability` check.
    fn function_to_family(function_name: &str) -> Result<QueryFamily, ASAPTierError> {
        match function_name {
            "quantile_over_time" | "histogram_quantile" | "quantile" => Ok(QueryFamily::Quantile),
            // `distinct_over_time` (MetricsQL) is the canonical
            // distinct-count-over-window name. `cardinality_estimate`
            // and `count_distinct_over_time` are accepted as historical
            // aliases for back-compat with PR #128's reducer tests.
            // PromQL's plain `count` is also a distinct-counting
            // operator per spec (counts the number of label sets in
            // the result vector — equivalent to cardinality for an
            // ASAP-tier HLL); the analyzer's `walk_aggregate_qe`
            // produces `function: "count"` for the outer aggregator
            // and we route that to the same Cardinality family. New
            // callers should prefer the analyzer's
            // `required_capability` and dispatch via
            // [`capability_to_family`] instead.
            "distinct_over_time"
            | "count_distinct_over_time"
            | "cardinality_estimate"
            | "count_distinct"
            | "count" => Ok(QueryFamily::Cardinality),
            "topk" | "topk_over_time" | "bottomk" => Ok(QueryFamily::FrequencyTopk),
            // Bare frequency point queries — the MetricsQL surface for
            // `sum by (item) (rate(m[r]))` with epsilon accuracy. The
            // reducer answers these by decoding the CMS / CountSketch
            // matrix directly (no heap needed). `frequency` is the
            // canonical name; `count_over_time` is the PromQL surface
            // (per-series sample count over a range window — exactly
            // what a CMS / CountSketch estimates without distinct-set
            // tracking).
            "frequency" | "frequency_estimate" | "count_over_time" => {
                Ok(QueryFamily::FrequencyEstimate)
            }
            other => Err(ASAPTierError::UnsupportedFunction(other.to_string())),
        }
    }

    /// Map a [`Capability`] to a [`QueryFamily`]. This is the canonical
    /// dispatch path after Step 2a: the control plane's analyzer hands
    /// each `ASAPTierCandidate` a `required_capability`, and the
    /// reducer picks a family without ever matching on the PromQL
    /// function-name string.
    ///
    /// Returns `None` for `Capability::ExactAgg(_)` — the ASAP-tier
    /// sketch reducer only handles sketch-backed sids. Exact-aggregation
    /// state is read through `SketchStore::query_precomputes_by_agg`
    /// (a parallel code path), so an ExactAgg capability has no
    /// `QueryFamily` mapping here.
    #[allow(dead_code)]
    pub(crate) fn capability_to_family(cap: &Capability) -> Option<QueryFamily> {
        match cap {
            Capability::QuantileApprox(_) => Some(QueryFamily::Quantile),
            Capability::CardinalityApprox => Some(QueryFamily::Cardinality),
            Capability::FrequencyTopk(_) => Some(QueryFamily::FrequencyTopk),
            Capability::FrequencyEstimate(_) => Some(QueryFamily::FrequencyEstimate),
            // ExactAgg sids are served by the precompute query path, not
            // the sketch reducer. Callers that hand ExactAgg to this
            // helper should branch to the precompute path instead.
            Capability::ExactAgg(_) => None,
        }
    }

    /// Validate that a sid's capability is compatible with the
    /// requested query family. Returns the `Capability` on match,
    /// `Err(UnsupportedCapability)` on mismatch.
    fn require_capability(
        function_name: &str,
        family: QueryFamily,
        meta: &SketchInstanceMetadata,
    ) -> Result<Capability, ASAPTierError> {
        // The ASAP-tier reducer only ever runs on sketch-backed sids
        // (the analyzer's `instances_matching` filters on `Capability`,
        // which is `None` for precompute-backed sids). A `None` here
        // means upstream classification broke — surface as a missing
        // capability rather than panicking the request path.
        let Some(cap) = meta.capability.as_ref() else {
            return Err(ASAPTierError::UnsupportedCapability {
                function: function_name.to_string(),
                capability: Capability::CardinalityApprox,
            });
        };
        match (family, cap) {
            (QueryFamily::Quantile, Capability::QuantileApprox(_))
            | (QueryFamily::Cardinality, Capability::CardinalityApprox)
            | (QueryFamily::FrequencyTopk, Capability::FrequencyTopk(_))
            | (QueryFamily::FrequencyEstimate, Capability::FrequencyEstimate(_))
            // A heap-bearing `FrequencyTopk` sid ALSO answers bare frequency
            // point queries — the heap is additional info layered over the
            // sketch matrix, so the underlying CMS / CountSketch matrix can
            // be queried point-wise without consulting it.
            | (QueryFamily::FrequencyEstimate, Capability::FrequencyTopk(_)) => {
                Ok(cap.clone())
            }
            (_, other) => Err(ASAPTierError::UnsupportedCapability {
                function: function_name.to_string(),
                capability: other.clone(),
            }),
        }
    }

    /// Evaluate a PromQL query against the ASAP tier.
    ///
    /// Caller invariant: every sid in `sids` has already been
    /// verified to classify as `Hit` against `self.index`. We
    /// re-resolve metadata (via `instance(sid)`) but don't
    /// re-classify.
    ///
    /// ## Delta-stitching modes
    ///
    /// For DD / KLL / HLL the reducer walks per-window samples in
    /// time order via [`delta_apply`](super::delta_apply). The function
    /// name decides between:
    /// - **per-window**: `quantile`, `histogram_quantile`,
    ///   `cardinality_estimate` — one scalar per window-end.
    /// - **cumulative**: `quantile_over_time`,
    ///   `count_distinct_over_time` — single scalar covering the
    ///   full `[t0, t1]` range (Full + every subsequent Delta merged).
    ///
    /// ## Top-k mode
    ///
    /// For `topk` / `topk_over_time` against a CmsWithHeap sid, the
    /// reducer reads the heap directly from the most-recent window's
    /// `CountMinSketchWithHeap` state and emits one
    /// `(label_values={"item": <key>}, [(window_end, count)])` entry
    /// per top-k item, truncated to the user's `k`. CountMin /
    /// CountSketch (no heap) surface as `MissingHeap`.
    pub fn evaluate(
        &self,
        sids: &[u64],
        function_name: &str,
        function_args: &[f64],
        t0_ms: u64,
        t1_ms: u64,
    ) -> Result<ASAPTierResult, ASAPTierError> {
        let family = Self::function_to_family(function_name)?;
        let is_cumulative = matches!(
            function_name,
            "quantile_over_time" | "count_distinct_over_time" | "topk_over_time"
        );

        // Per-(sid, label-values) → time-stamped scalar values.
        let mut out_series: Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)> = Vec::new();
        let mut metric_name_for_err = String::new();
        let mut any_window = false;
        let mut cov_lo: u64 = u64::MAX;
        let mut cov_hi: u64 = 0;

        for &sid in sids {
            let meta = match self.index.instance(sid) {
                Some(m) => m,
                None => continue, // defensive — sid was Hit, but instance gone
            };
            metric_name_for_err = meta.metric_name.clone();
            let _capability = Self::require_capability(function_name, family, &meta)?;

            let series_list = self.index.query_range(sid, t0_ms, t1_ms);
            if series_list.is_empty() {
                continue;
            }

            // Bare frequency point query — heap-LESS dispatch. Decode
            // each window's CMS / CountSketch (or the underlying matrix
            // of a heap-bearing sid) and emit one (window_end, total_count)
            // sample per window. The CMS / CountSketch substrate carries
            // ALL items inserted via `bulk_insert`, so the per-window
            // total count is the sum-of-all-rates contribution from that
            // window — the natural answer to bare `sum by (item)
            // (rate(m[r]))` when no specific item key is supplied.
            //
            // Per-item lookup (estimate(key)) is a follow-up — it requires
            // plumbing a string-keyed `function_arg` through the reducer
            // entry point, which the current `&[f64]` signature can't carry.
            if family == QueryFamily::FrequencyEstimate {
                for ts in series_list {
                    let mut samples_out: Vec<(i64, f64)> = Vec::with_capacity(ts.samples.len());
                    for (w_end, state) in ts.samples.iter() {
                        any_window = true;
                        let w = if *w_end >= 0 { *w_end as u64 } else { 0 };
                        if w < cov_lo {
                            cov_lo = w;
                        }
                        if w > cov_hi {
                            cov_hi = w;
                        }
                        let total = decode_frequency_total(sid, meta.sketch_kind().expect("ASAP-tier reducer only handles sketch-backed sids"), state)?;
                        samples_out.push((*w_end, total));
                    }
                    out_series.push((ts.series_label_values, samples_out));
                }
                continue;
            }

            // Top-k is a different shape — one entry per top-k item.
            if family == QueryFamily::FrequencyTopk {
                let k = function_args
                    .first()
                    .copied()
                    .filter(|k| *k > 0.0)
                    .map(|k| k as usize)
                    .unwrap_or(10);
                for ts in series_list {
                    // Find latest window's CMS-with-heap state.
                    let Some((window_end, state)) = ts.samples.iter().next_back() else {
                        continue;
                    };
                    any_window = true;
                    let w_end_u64 = if *window_end >= 0 {
                        *window_end as u64
                    } else {
                        0
                    };
                    if w_end_u64 < cov_lo {
                        cov_lo = w_end_u64;
                    }
                    if w_end_u64 > cov_hi {
                        cov_hi = w_end_u64;
                    }
                    let cms_heap = match meta.sketch_kind().expect("ASAP-tier reducer only handles sketch-backed sids") {
                        SketchKindHandle::CmsWithHeap | SketchKindHandle::CountSketchWithHeap => {
                            // Both heap-bearing variants serialize the
                            // outer `CountMinSketchWithHeap` envelope via
                            // msgpack (`CountSketchWithHeap` reuses the
                            // same wire shape since the heap is the
                            // distinguishing payload).
                            decode_cms_with_heap_from_msgpack(&state.bytes).map_err(|e| {
                                ASAPTierError::DeserializeFailure {
                                    sid,
                                    encoding: state.encoding,
                                    reason: e,
                                }
                            })?
                        }
                        SketchKindHandle::CountMin | SketchKindHandle::CountSketch => {
                            // Heap-LESS variants can't enumerate top-k —
                            // they support point-frequency only (which
                            // routes through QueryFamily::FrequencyEstimate).
                            return Err(ASAPTierError::MissingHeap {
                                sid,
                                sketch_kind: meta.sketch_kind().expect("ASAP-tier reducer only handles sketch-backed sids"),
                            });
                        }
                        other => {
                            return Err(ASAPTierError::UnsupportedCapability {
                                function: function_name.to_string(),
                                capability: Capability::FrequencyTopk(other),
                            });
                        }
                    };
                    let mut items = cms_heap.topk_heap_items();
                    // Sort descending by estimated count.
                    items.sort_by(|a, b| {
                        b.value
                            .partial_cmp(&a.value)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    for item in items.into_iter().take(k) {
                        let mut lv = ts.series_label_values.clone();
                        lv.insert("item".to_string(), item.key);
                        out_series.push((lv, vec![(*window_end, item.value)]));
                    }
                }
                continue;
            }

            // Quantile / Cardinality with delta stitching.
            let delta_kind = match (family, meta.sketch_kind().expect("ASAP-tier reducer only handles sketch-backed sids")) {
                (QueryFamily::Quantile, SketchKindHandle::DDSketch) => DeltaSketchKind::DDSketch,
                (QueryFamily::Quantile, SketchKindHandle::Kll) => DeltaSketchKind::Kll,
                (QueryFamily::Cardinality, SketchKindHandle::Hll) => DeltaSketchKind::Hll,
                _ => {
                    return Err(ASAPTierError::UnsupportedCapability {
                        function: function_name.to_string(),
                        capability: meta
                            .capability
                            .clone()
                            .unwrap_or(Capability::CardinalityApprox),
                    });
                }
            };
            let q = function_args
                .first()
                .copied()
                .filter(|q| (0.0..=1.0).contains(q))
                .unwrap_or(0.99);
            let evaluator: Box<dyn Fn(&super::delta_apply::RollingState) -> f64> = match family {
                QueryFamily::Quantile => Box::new(move |rs| rs.quantile(q)),
                QueryFamily::Cardinality => Box::new(|rs| rs.cardinality()),
                _ => unreachable!(),
            };

            for ts in series_list {
                // Build sorted-by-window-end slice of refs.
                let samples_vec: Vec<(i64, &SketchSampleState)> =
                    ts.samples.iter().map(|(t, s)| (*t, s)).collect();
                // BTreeMap iteration is already sorted by key; the
                // collect preserves order. Track coverage from raw
                // window-end timestamps before delta evaluation
                // (skipped leading deltas still count toward the
                // covered range).
                for (w_end, _) in &samples_vec {
                    any_window = true;
                    let w = if *w_end >= 0 { *w_end as u64 } else { 0 };
                    if w < cov_lo {
                        cov_lo = w;
                    }
                    if w > cov_hi {
                        cov_hi = w;
                    }
                }

                let samples_out: Vec<(i64, f64)> = if is_cumulative {
                    let (one, _skipped) = cumulative_evaluate(&samples_vec, delta_kind, &evaluator)
                        .map_err(|e| ASAPTierError::DeserializeFailure {
                            sid,
                            encoding: SketchEncoding::ProtoFull,
                            reason: e,
                        })?;
                    match one {
                        Some(s) => vec![s],
                        None => Vec::new(),
                    }
                } else {
                    let (per_win, _skipped) =
                        per_window_evaluate(&samples_vec, delta_kind, &evaluator).map_err(|e| {
                            ASAPTierError::DeserializeFailure {
                                sid,
                                encoding: SketchEncoding::ProtoFull,
                                reason: e,
                            }
                        })?;
                    // Drop carry-in base windows: `SketchStore::query_range`
                    // may splice in a Full snapshot ending BEFORE `t0_ms`
                    // so the delta-apply walk can establish a rolling base
                    // for a delta-only window. That base must not surface
                    // as an output sample in the requested `[t0, t1]`
                    // range. Cumulative mode emits a single scalar at the
                    // latest in-window end so it's unaffected; per-window
                    // mode emits one sample per window, so filter here.
                    let lo = t0_ms as i64;
                    per_win
                        .into_iter()
                        .filter(|(w_end, _)| *w_end >= lo)
                        .collect()
                };
                out_series.push((ts.series_label_values, samples_out));
            }
        }

        if !any_window {
            return Err(ASAPTierError::NoData {
                metric_name: metric_name_for_err,
            });
        }

        let coverage = if cov_lo <= cov_hi {
            Some((cov_lo, cov_hi))
        } else {
            None
        };
        Ok(ASAPTierResult {
            series: out_series,
            coverage,
        })
    }

    /// ExactAgg dispatch — sister of [`Self::evaluate`] for sids whose
    /// `Capability` is `ExactAgg(_)`. The sketch-backed `evaluate`
    /// path can't answer these because they carry `Box<dyn
    /// AggregateCore>` payloads (per-window `SumAccumulator` /
    /// `IncreaseAccumulator` / `MinMaxAccumulator` etc.) rather than
    /// opaque sketch bytes.
    ///
    /// PromQL `sum by (group_by_keys) (metric)` lowers (via the
    /// control plane's `analyze_promql_for_asap_tier`) to a candidate
    /// with `required_capability = ExactAgg(Sum)` and
    /// `group_by_keys = {requested labels}`. This method walks every
    /// hit sid's exact-aggregation state, projects each window's
    /// label map onto `group_by_keys` (so a sid registered with
    /// `[zone, rack]` answering a `by (zone)` query collapses across
    /// rack values), and emits one `(label_values, [(window_end,
    /// scalar)])` series per distinct projected group.
    ///
    /// For each (group, window_end) pair we MERGE all matching
    /// accumulators via `AggregateCore::merge_with` and then read the
    /// `Statistic` the agg_type implies — `Sum`/`Increase` →
    /// `Statistic::Sum`, `MinMax` → currently UnsupportedCapability
    /// (min vs max disambiguation needs the outer function name; deferred
    /// to a follow-up). Both `SumAccumulator` and `IncreaseAccumulator`
    /// answer `Statistic::Sum` from their `query_statistic` (the latter
    /// returns the accumulated increase, which is what a PromQL `sum`
    /// over rate/increase wants).
    ///
    /// `group_by_keys` empty (i.e. `sum(metric)` without `by (...)`)
    /// collapses every series to a single grouping with empty label
    /// map — the natural PromQL semantics.
    ///
    /// `accumulate_windows` (issue #301) controls the per-group output
    /// shape:
    /// - `false` (range-query / matrix surface): emit ONE sample per
    ///   `(group, window_end)` — the per-window delta timeseries. The
    ///   range-query wire format wants a matrix with a point per window.
    /// - `true` (instant `sum(counter)` / `increase(counter[r])`): SUM
    ///   every in-range window's value into ONE cumulative number per
    ///   group, timestamped at `t1_ms`. This is the PromQL-correct
    ///   semantic for instant counter sums (cumulative-since-storage)
    ///   and `increase` (Σ deltas in `[t-r,t]`). Without this the
    ///   instant path's `.last()` projection (engine
    ///   `asap_tier_result_to_query_result`) returned only the MOST
    ///   RECENT window's delta — the Layer-3 bug from #301.
    pub fn evaluate_exact_agg(
        &self,
        sids: &[u64],
        agg_type: AggregationType,
        group_by_keys: &std::collections::BTreeSet<String>,
        t0_ms: u64,
        t1_ms: u64,
        accumulate_windows: bool,
    ) -> Result<ASAPTierResult, ASAPTierError> {
        // Pick the Statistic answer this agg_type implies. PromQL
        // `sum by (...)` against an ExactAgg sid is the standard
        // counter rollup — every additive type answers via Sum.
        let stat = match agg_type {
            AggregationType::Sum
            | AggregationType::MultipleSum
            | AggregationType::Increase
            | AggregationType::MultipleIncrease => Statistic::Sum,
            // MinMax disambiguation requires the outer PromQL function
            // name (min vs max); deferred until the engine threads it
            // through. Today min/max queries are not produced by the
            // analyzer's ExactAgg(MinMax) capability path for `sum by`
            // queries, so this branch is defensive.
            other => {
                return Err(ASAPTierError::UnsupportedCapability {
                    function: format!("sum_by_for_{other:?}"),
                    capability: Capability::ExactAgg(other),
                });
            }
        };

        // (projected_group_map, window_end_ms) -> Vec<accumulator>
        // BTreeMap so window_ends sort naturally for output and the
        // group map key is a Vec<(k,v)> tuple sorted by key (BTreeMap
        // iteration is key-sorted, so collecting yields a canonical
        // order).
        type GroupKey = Vec<(String, String)>;
        let mut grouped: BTreeMap<
            (GroupKey, i64),
            Vec<Arc<dyn crate::storage_engines::types::AggregateCore>>,
        > = BTreeMap::new();
        let mut metric_name_for_err = String::new();
        let mut cov_lo: u64 = u64::MAX;
        let mut cov_hi: u64 = 0;
        let mut any_window = false;

        for &sid in sids {
            let meta = match self.index.instance(sid) {
                Some(m) => m,
                None => continue,
            };
            metric_name_for_err = meta.metric_name.clone();

            // Pull every (label_map, samples) tuple this sid carries
            // in window. Sketch-backed sids (or sids with no in-window
            // exact-agg state) return empty.
            let series_list = self.index.query_exact_agg_range(sid, t0_ms, t1_ms);
            for (label_map, samples) in series_list {
                // Project label_map onto group_by_keys. Missing keys are
                // dropped (the user didn't ask for them); requested keys
                // absent from the sid's label_map become empty-string
                // values so a sid registered with a subset of the
                // requested keys still groups deterministically.
                let projected: GroupKey = if group_by_keys.is_empty() {
                    Vec::new()
                } else {
                    group_by_keys
                        .iter()
                        .map(|k| {
                            let v = label_map.get(k).cloned().unwrap_or_default();
                            (k.clone(), v)
                        })
                        .collect()
                };

                for (window_end, acc) in samples {
                    any_window = true;
                    let w = if window_end >= 0 { window_end as u64 } else { 0 };
                    if w < cov_lo {
                        cov_lo = w;
                    }
                    if w > cov_hi {
                        cov_hi = w;
                    }
                    grouped
                        .entry((projected.clone(), window_end))
                        .or_default()
                        .push(acc);
                }
            }
        }

        if !any_window {
            return Err(ASAPTierError::NoData {
                metric_name: metric_name_for_err,
            });
        }

        // Fold per-(group, window) accumulator lists into a single
        // scalar via `merge_with` (additive across the list) and
        // `query_statistic`. Re-bucket by group so each group emits
        // ONE series with the full per-window timeseries.
        let mut by_group: BTreeMap<GroupKey, Vec<(i64, f64)>> = BTreeMap::new();
        for ((group, w_end), accs) in grouped {
            // Merge all accumulators landing in (group, window). For
            // a single ExactAgg sid covering one group there's
            // typically one entry; multiple entries come from multiple
            // sids that share the projected group (e.g. several
            // (zone=z0, rack=*) sids collapsing to a single zone=z0
            // group).
            let mut iter = accs.into_iter();
            let head = match iter.next() {
                Some(h) => h,
                None => continue,
            };
            let mut merged: Box<dyn crate::storage_engines::types::AggregateCore> =
                head.clone_boxed_core();
            for next in iter {
                match merged.merge_with(next.as_ref()) {
                    Ok(m) => merged = m,
                    Err(e) => {
                        return Err(ASAPTierError::DeserializeFailure {
                            sid: 0,
                            encoding: SketchEncoding::ProtoFull,
                            reason: format!("exact-agg merge failed: {e}"),
                        });
                    }
                }
            }
            let value = match merged.query_statistic(
                stat,
                &None,
                &std::collections::HashMap::new(),
            ) {
                Ok(v) => v,
                Err(e) => {
                    return Err(ASAPTierError::DeserializeFailure {
                        sid: 0,
                        encoding: SketchEncoding::ProtoFull,
                        reason: format!("exact-agg query_statistic({stat:?}) failed: {e}"),
                    });
                }
            };
            by_group.entry(group).or_default().push((w_end, value));
        }

        // Build the series. BTreeMap iteration is already sorted, so
        // each series's samples vec is in window-end order.
        //
        // When `accumulate_windows` is set, collapse each group's
        // per-window deltas into ONE cumulative sample (Σ of values),
        // timestamped at `t1_ms`. This is the PromQL semantic for an
        // instant counter `sum` (cumulative-since-storage) and for
        // `increase(counter[r])` (Σ deltas in the `[t0,t1]` clip).
        // Otherwise keep the per-window timeseries for the matrix
        // (range-query) surface.
        let sample_ts = if t1_ms <= i64::MAX as u64 {
            t1_ms as i64
        } else {
            i64::MAX
        };
        let mut out_series: Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)> = Vec::new();
        for (group, samples) in by_group {
            let label_map: BTreeMap<String, String> = group.into_iter().collect();
            if accumulate_windows {
                let total: f64 = samples.iter().map(|(_, v)| *v).sum();
                out_series.push((label_map, vec![(sample_ts, total)]));
            } else {
                out_series.push((label_map, samples));
            }
        }

        let coverage = if cov_lo <= cov_hi {
            Some((cov_lo, cov_hi))
        } else {
            None
        };
        Ok(ASAPTierResult {
            series: out_series,
            coverage,
        })
    }

    /// ExactAgg-rate dispatch — sister of [`Self::evaluate_exact_agg`]
    /// for `rate(metric[r])` / `irate(metric[r])` (plus the composed
    /// shape `sum by (gbk) (rate(metric[r]))`) over `ExactAgg(Sum)` /
    /// `ExactAgg(Increase)` sids.
    ///
    /// Semantics: for an ExactAgg(Sum) sid, the per-window accumulator
    /// carries the count of events in that window. PromQL's
    /// `rate(metric[r])` at instant `t` is "events per second in
    /// `[t-r, t]`" — for our sub-window-sized sids that's:
    ///
    /// ```text
    /// rate(t) = (Σ over windows w ⊆ [t-r, t] of Sum[w])  /  divisor
    /// divisor = min(r_seconds, actual_coverage_span_seconds)
    /// ```
    ///
    /// Differs from `evaluate_exact_agg` in two ways:
    /// 1. Folds EVERY window's accumulator (in `[t0_ms, t1_ms]`) into
    ///    ONE merged accumulator per group rather than keeping per-window
    ///    samples. For an instant rate query that's the correct shape:
    ///    one number per series, where the number is "events per second
    ///    in the lookback".
    /// 2. Divides the merged `Statistic::Sum` by `min(range_seconds,
    ///    coverage_span)` to produce the rate (events/sec). Issue #301
    ///    Layer 4: dividing by the NOMINAL `range_seconds` (300 for
    ///    `[5m]`) when the producer has only run for a fraction of that
    ///    span systematically UNDER-reports the rate (the smoke test's
    ///    64% rate rel-err). `coverage_span` = `(max_window_end −
    ///    min_window_start)/1000` across the contributing windows, read
    ///    from `SketchStore::exact_agg_coverage_bounds`. Clamped to
    ///    `range_seconds` so a query whose window genuinely spans the
    ///    full `[r]` still divides by `r`. `range_seconds == 0` would
    ///    indicate a non-range query routing through this path by
    ///    mistake — defensive, surface as `UnsupportedCapability` so
    ///    the engine falls over rather than divide-by-zero.
    ///
    /// Emits ONE sample per group, timestamped at `t1_ms` (the right
    /// edge of the request window), matching PromQL's "evaluate rate
    /// at time `t` over the trailing window" semantics.
    ///
    /// `group_by_keys` empty (i.e. `rate(metric[r])` without any outer
    /// aggregation) collapses every series to the natural per-(sid's
    /// own full label map) grouping — same as `evaluate_exact_agg`,
    /// preserving full per-series rate values.
    pub fn evaluate_exact_agg_rate(
        &self,
        sids: &[u64],
        agg_type: AggregationType,
        group_by_keys: &std::collections::BTreeSet<String>,
        range_seconds: u64,
        t0_ms: u64,
        t1_ms: u64,
    ) -> Result<ASAPTierResult, ASAPTierError> {
        // Only additive types answer Statistic::Sum (same restriction as
        // `evaluate_exact_agg`).
        let stat = match agg_type {
            AggregationType::Sum
            | AggregationType::MultipleSum
            | AggregationType::Increase
            | AggregationType::MultipleIncrease => Statistic::Sum,
            other => {
                return Err(ASAPTierError::UnsupportedCapability {
                    function: format!("rate_for_{other:?}"),
                    capability: Capability::ExactAgg(other),
                });
            }
        };

        if range_seconds == 0 {
            // Defensive — the engine should only route here when a
            // matrix selector was present.
            return Err(ASAPTierError::UnsupportedCapability {
                function: "rate_with_zero_range".to_string(),
                capability: Capability::ExactAgg(agg_type),
            });
        }

        // Coverage-aware divisor (issue #301 Layer 4). The merged Sum is
        // "events in `[t0,t1] ∩ stored windows`". Dividing by the
        // NOMINAL `range_seconds` (e.g. 300 for `[5m]`) when the producer
        // has only run for part of that span under-reports the rate.
        // Use the ACTUAL covered span = (max_window_end −
        // min_window_start)/1000 across all contributing sids, clamped to
        // `[1, range_seconds]`. Clamping to `range_seconds` keeps a
        // full-window query dividing by `r`; the lower bound of 1s guards
        // against divide-by-zero when only a single sub-second window
        // exists. When no bounds are available (no in-range exact-agg
        // windows on any sid) the per-sid loop below produces NoData
        // anyway, so the divisor fallback to `range_seconds` is moot.
        let mut span_lo: u64 = u64::MAX;
        let mut span_hi: u64 = 0;
        for &sid in sids {
            if let Some((start, end)) =
                self.index.exact_agg_coverage_bounds(sid, t0_ms, t1_ms)
            {
                if start < span_lo {
                    span_lo = start;
                }
                if end > span_hi {
                    span_hi = end;
                }
            }
        }
        let coverage_seconds: u64 = if span_lo <= span_hi {
            span_hi.saturating_sub(span_lo) / 1000
        } else {
            range_seconds
        };
        let divisor = range_seconds.min(coverage_seconds).max(1) as f64;

        // Choice of grouping mirrors `evaluate_exact_agg`:
        // * `group_by_keys` empty → preserve each sid's own full
        //   label map (one rate series per natural series).
        // * `group_by_keys` non-empty → project each sid's label map
        //   onto that subset, merging across subgroups.
        type GroupKey = Vec<(String, String)>;
        let mut by_group: BTreeMap<
            GroupKey,
            Option<Box<dyn crate::storage_engines::types::AggregateCore>>,
        > = BTreeMap::new();
        // Remember the natural label_map for each group_key so we can
        // emit it on the output side (only relevant when
        // `group_by_keys` is empty; otherwise the key IS the label
        // map). For the projected case the BTreeMap from GroupKey is
        // fine.
        let mut natural_label_map: BTreeMap<GroupKey, BTreeMap<String, String>> = BTreeMap::new();

        let mut metric_name_for_err = String::new();
        let mut cov_lo: u64 = u64::MAX;
        let mut cov_hi: u64 = 0;
        let mut any_window = false;

        for &sid in sids {
            let meta = match self.index.instance(sid) {
                Some(m) => m,
                None => continue,
            };
            metric_name_for_err = meta.metric_name.clone();

            let series_list = self.index.query_exact_agg_range(sid, t0_ms, t1_ms);
            for (label_map, samples) in series_list {
                let projected: GroupKey = if group_by_keys.is_empty() {
                    // Use the natural label map as the grouping key so
                    // distinct series stay separated. Sort the (k,v)
                    // pairs by key for canonical ordering — BTreeMap
                    // iteration is already key-sorted, so collecting
                    // is enough.
                    label_map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                } else {
                    group_by_keys
                        .iter()
                        .map(|k| {
                            let v = label_map.get(k).cloned().unwrap_or_default();
                            (k.clone(), v)
                        })
                        .collect()
                };
                natural_label_map
                    .entry(projected.clone())
                    .or_insert_with(|| projected.iter().cloned().collect());

                for (window_end, acc) in samples {
                    any_window = true;
                    let w = if window_end >= 0 { window_end as u64 } else { 0 };
                    if w < cov_lo {
                        cov_lo = w;
                    }
                    if w > cov_hi {
                        cov_hi = w;
                    }
                    let slot = by_group.entry(projected.clone()).or_insert(None);
                    match slot.take() {
                        None => {
                            *slot = Some(acc.clone_boxed_core());
                        }
                        Some(prev) => {
                            match prev.merge_with(acc.as_ref()) {
                                Ok(m) => *slot = Some(m),
                                Err(e) => {
                                    return Err(ASAPTierError::DeserializeFailure {
                                        sid,
                                        encoding: SketchEncoding::ProtoFull,
                                        reason: format!(
                                            "exact-agg rate merge failed: {e}"
                                        ),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        if !any_window {
            return Err(ASAPTierError::NoData {
                metric_name: metric_name_for_err,
            });
        }

        // Emit one sample per group, timestamped at t1_ms (instant-rate
        // semantics: the rate is "at time t over the trailing window").
        let sample_ts = if t1_ms <= i64::MAX as u64 {
            t1_ms as i64
        } else {
            i64::MAX
        };
        let mut out_series: Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)> = Vec::new();
        for (group, slot) in by_group {
            let merged = match slot {
                Some(m) => m,
                None => continue,
            };
            let raw = match merged.query_statistic(
                stat,
                &None,
                &std::collections::HashMap::new(),
            ) {
                Ok(v) => v,
                Err(e) => {
                    return Err(ASAPTierError::DeserializeFailure {
                        sid: 0,
                        encoding: SketchEncoding::ProtoFull,
                        reason: format!(
                            "exact-agg rate query_statistic({stat:?}) failed: {e}"
                        ),
                    });
                }
            };
            let rate_value = raw / divisor;
            let label_map: BTreeMap<String, String> = natural_label_map
                .remove(&group)
                .unwrap_or_else(|| group.into_iter().collect());
            out_series.push((label_map, vec![(sample_ts, rate_value)]));
        }

        let coverage = if cov_lo <= cov_hi {
            Some((cov_lo, cov_hi))
        } else {
            None
        };
        Ok(ASAPTierResult {
            series: out_series,
            coverage,
        })
    }

    /// Decode one window's sketch state and run the family-appropriate
    /// reduction.
    ///
    /// Retained as `#[allow(dead_code)]` after the delta-stitching
    /// follow-up moved the per-window decode-then-evaluate flow into
    /// [`super::delta_apply`]. Callers that want a one-shot evaluate
    /// without delta-state plumbing can still reach this entry point;
    /// the ASAP-tier reducer's main loop now goes through
    /// [`per_window_evaluate`] / [`cumulative_evaluate`].
    #[allow(dead_code)]
    fn evaluate_one_state(
        &self,
        sid: u64,
        family: QueryFamily,
        sketch_kind: SketchKindHandle,
        function_args: &[f64],
        state: &SketchSampleState,
    ) -> Result<f64, ASAPTierError> {
        match family {
            QueryFamily::Quantile => {
                let q = function_args
                    .first()
                    .copied()
                    .filter(|q| (0.0..=1.0).contains(q))
                    .unwrap_or(0.99);
                self.evaluate_quantile(sid, sketch_kind, q, state)
            }
            QueryFamily::Cardinality => self.evaluate_cardinality(sid, sketch_kind, state),
            QueryFamily::FrequencyTopk => {
                // Top-k materialization is handled in-line by the main
                // `evaluate` loop via `decode_cms_with_heap_from_msgpack`;
                // this legacy one-shot entry never participates in the
                // top-k path. Surface as `UnsupportedCapability` so a
                // stray caller falls over to archive.
                Err(ASAPTierError::UnsupportedCapability {
                    function: "topk".to_string(),
                    capability: Capability::FrequencyTopk(sketch_kind),
                })
            }
            QueryFamily::FrequencyEstimate => {
                // Bare frequency point query — handled in-line by the
                // main `evaluate` loop via `decode_frequency_total`.
                // This legacy one-shot entry doesn't drive the
                // FrequencyEstimate path; surface as a defensive
                // `UnsupportedCapability` so a stray caller falls over
                // to archive rather than silently misroutes.
                Err(ASAPTierError::UnsupportedCapability {
                    function: "frequency".to_string(),
                    capability: Capability::FrequencyEstimate(sketch_kind),
                })
            }
        }
    }

    #[allow(dead_code)]
    fn evaluate_quantile(
        &self,
        sid: u64,
        sketch_kind: SketchKindHandle,
        q: f64,
        state: &SketchSampleState,
    ) -> Result<f64, ASAPTierError> {
        match sketch_kind {
            SketchKindHandle::DDSketch => {
                let sk = decode_ddsketch(sid, state)?;
                Ok(sk.quantile(q).unwrap_or(0.0))
            }
            SketchKindHandle::Kll => {
                let sk = decode_kll(sid, state)?;
                Ok(sk.quantile(q))
            }
            other => Err(ASAPTierError::UnsupportedCapability {
                function: "quantile".to_string(),
                capability: Capability::QuantileApprox(other),
            }),
        }
    }

    #[allow(dead_code)]
    fn evaluate_cardinality(
        &self,
        sid: u64,
        sketch_kind: SketchKindHandle,
        state: &SketchSampleState,
    ) -> Result<f64, ASAPTierError> {
        match sketch_kind {
            SketchKindHandle::Hll => {
                let sk = decode_hll(sid, state)?;
                Ok(sk.estimate())
            }
            other => Err(ASAPTierError::UnsupportedCapability {
                function: "cardinality".to_string(),
                capability: Capability::QuantileApprox(other),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Per-sketch-kind decoders. Mirror the precompute_operators/*.rs paths so the
// behavior matches what the precompute (ingest-side) accumulator would have
// done for the same bytes — including which encodings round-trip and which
// surface as decode failure.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn decode_ddsketch(sid: u64, state: &SketchSampleState) -> Result<DdSketch, ASAPTierError> {
    match state.encoding {
        SketchEncoding::ProtoFull => {
            DdSketch_from_sketchlib_proto_bytes(&state.bytes).map_err(|e| {
                ASAPTierError::DeserializeFailure {
                    sid,
                    encoding: state.encoding,
                    reason: e.to_string(),
                }
            })
        }
        SketchEncoding::MsgpackFull => DdSketch::from_msgpack(&state.bytes).map_err(|e| {
            ASAPTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: e.to_string(),
            }
        }),
        SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
            Err(ASAPTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: "delta encodings require base sketch state \
                         (ASAP-tier reducer doesn't yet stitch delta + base \
                         within query_range)"
                    .to_string(),
            })
        }
    }
}

#[allow(dead_code)]
fn decode_kll(sid: u64, state: &SketchSampleState) -> Result<KllSketch, ASAPTierError> {
    match state.encoding {
        SketchEncoding::ProtoFull => {
            KllSketch_from_sketchlib_proto_bytes(&state.bytes).map_err(|e| {
                ASAPTierError::DeserializeFailure {
                    sid,
                    encoding: state.encoding,
                    reason: e.to_string(),
                }
            })
        }
        SketchEncoding::MsgpackFull => KllSketch::from_msgpack(&state.bytes).map_err(|e| {
            ASAPTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: e.to_string(),
            }
        }),
        SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
            Err(ASAPTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: "KLL delta encodings not implemented in ASAP-tier reducer".to_string(),
            })
        }
    }
}

#[allow(dead_code)]
fn decode_hll(sid: u64, state: &SketchSampleState) -> Result<HllSketch, ASAPTierError> {
    match state.encoding {
        SketchEncoding::ProtoFull => {
            HllSketch_from_sketchlib_proto_bytes(&state.bytes).map_err(|e| {
                ASAPTierError::DeserializeFailure {
                    sid,
                    encoding: state.encoding,
                    reason: e.to_string(),
                }
            })
        }
        SketchEncoding::MsgpackFull => HllSketch::from_msgpack(&state.bytes).map_err(|e| {
            ASAPTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: e.to_string(),
            }
        }),
        SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
            Err(ASAPTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: "HLL delta encodings not implemented in ASAP-tier reducer".to_string(),
            })
        }
    }
}

// Decoders inlined from `precompute_operators/*_accumulator.rs`. They
// don't live as methods on the sketchlib types directly because the
// proto envelope wrapping (from DataCollector's `*processor`) is a
// product of the OTLP wire layer, not the sketch library.

#[allow(non_snake_case, dead_code)]
fn DdSketch_from_sketchlib_proto_bytes(buffer: &[u8]) -> Result<DdSketch, String> {
    use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
    use prost::Message;
    let state = match SketchEnvelope::decode(buffer) {
        Ok(env) => match env.sketch_state {
            Some(sketch_envelope::SketchState::Ddsketch(st)) => st,
            Some(_) => return Err("SketchEnvelope contains non-DDSketch sketch".to_string()),
            None => {
                DdSketchState::decode(buffer).map_err(|e| format!("decode DDSketchState: {e}"))?
            }
        },
        Err(_) => {
            DdSketchState::decode(buffer).map_err(|e| format!("decode DDSketchState: {e}"))?
        }
    };
    if !(state.alpha > 0.0 && state.alpha < 1.0) {
        return Err(format!(
            "DDSketchState alpha {} out of range (expected 0 < alpha < 1)",
            state.alpha
        ));
    }
    Ok(DdSketch::from_raw(
        state.alpha,
        state.store_counts.clone(),
        state.store_offset,
        state.count,
        state.sum,
        state.min,
        state.max,
    ))
}

#[allow(non_snake_case, dead_code)]
fn KllSketch_from_sketchlib_proto_bytes(buffer: &[u8]) -> Result<KllSketch, String> {
    use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
    use prost::Message;
    let state = match SketchEnvelope::decode(buffer) {
        Ok(env) => match env.sketch_state {
            Some(sketch_envelope::SketchState::Kll(st)) => st,
            Some(_) => return Err("SketchEnvelope contains non-KLL sketch".to_string()),
            None => KllState::decode(buffer).map_err(|e| format!("decode KllState: {e}"))?,
        },
        Err(_) => KllState::decode(buffer).map_err(|e| format!("decode KllState: {e}"))?,
    };
    if state.k < 8 {
        return Err(format!("KllState.k must be >= 8 (got {})", state.k));
    }
    if state.k > u16::MAX as u32 {
        return Err(format!(
            "KllState.k does not fit in u16 (got {}, max {})",
            state.k,
            u16::MAX
        ));
    }
    let k = state.k as u16;
    let mut sk = KllSketch::new(k);
    for item in &state.items {
        sk.update(*item);
    }
    Ok(sk)
}

#[allow(non_snake_case, dead_code)]
fn HllSketch_from_sketchlib_proto_bytes(buffer: &[u8]) -> Result<HllSketch, String> {
    use asap_sketchlib::proto::sketchlib::{
        sketch_envelope, HllVariant as ProtoVariant, HyperLogLogState, SketchEnvelope,
    };
    use asap_sketchlib::HllVariant;
    use prost::Message;
    let state = match SketchEnvelope::decode(buffer) {
        Ok(env) => match env.sketch_state {
            Some(sketch_envelope::SketchState::Hll(st)) => st,
            Some(_) => return Err("SketchEnvelope contains non-HLL sketch".to_string()),
            None => HyperLogLogState::decode(buffer)
                .map_err(|e| format!("decode HyperLogLogState: {e}"))?,
        },
        Err(_) => {
            HyperLogLogState::decode(buffer).map_err(|e| format!("decode HyperLogLogState: {e}"))?
        }
    };
    if state.precision == 0 || state.precision > 20 {
        return Err(format!(
            "HyperLogLogState precision {} out of range (expected 1..=20)",
            state.precision
        ));
    }
    let expected_len = 1usize << state.precision;
    if state.registers.len() != expected_len {
        return Err(format!(
            "HyperLogLogState registers has {} bytes, expected 2^precision = {}",
            state.registers.len(),
            expected_len
        ));
    }
    let proto_variant = ProtoVariant::try_from(state.variant)
        .map_err(|_| format!("HyperLogLogState has unknown variant tag {}", state.variant))?;
    let variant = match proto_variant {
        ProtoVariant::Unspecified => HllVariant::Unspecified,
        ProtoVariant::Regular => HllVariant::Regular,
        ProtoVariant::ErtlMle => HllVariant::Datafusion,
        ProtoVariant::Hip => HllVariant::Hip,
    };
    Ok(HllSketch::from_raw(
        variant,
        state.precision,
        state.registers.clone(),
        state.hip_kxq0,
        state.hip_kxq1,
        state.hip_est,
    ))
}

// CMS / CountSketch decoders are wired through the reducer's
// `FrequencyEstimate` dispatch arm (bare-frequency point queries).
// `FrequencyTopk` continues to require a heap-bearing variant (handled
// inline in the FrequencyTopk branch via `decode_cms_with_heap_from_msgpack`).
#[allow(dead_code)]
fn _unused_cms_kept_for_future_topk(buffer: &[u8]) -> Option<CountMinSketch> {
    CountMinSketch::from_msgpack(buffer).ok()
}
#[allow(dead_code)]
fn _unused_count_sketch_kept_for_future_topk(buffer: &[u8]) -> Option<CountSketch> {
    CountSketch::from_msgpack(buffer).ok()
}

/// Decode a sid's per-window frequency sketch and emit a per-window
/// total-count summary. The CMS / CountSketch matrix sums row 0 (the
/// first hash row); for a CMS, row r's column-wise sum equals the total
/// weighted insert volume into that row (each insert contributes once
/// per row), so row 0's sum is the natural per-window total-frequency
/// scalar.
///
/// Heap-bearing variants (`CmsWithHeap` / `CountSketchWithHeap`) are
/// decoded via the same wrapper and the underlying CMS matrix is used.
///
/// Returns `ASAPTierError::DeserializeFailure` if the bytes don't decode
/// against the sid's declared sketch kind. Heap-less CMS / CountSketch
/// are NOT a `MissingHeap` error here — bare frequency is exactly what
/// heap-less variants are designed to answer.
fn decode_frequency_total(
    sid: u64,
    sketch_kind: SketchKindHandle,
    state: &SketchSampleState,
) -> Result<f64, ASAPTierError> {
    let to_err = |e: String, encoding: SketchEncoding| ASAPTierError::DeserializeFailure {
        sid,
        encoding,
        reason: e,
    };
    match sketch_kind {
        SketchKindHandle::CountMin => {
            let cms =
                match state.encoding {
                    SketchEncoding::ProtoFull => decode_cms_from_proto(&state.bytes)
                        .map_err(|e| to_err(e, state.encoding))?,
                    SketchEncoding::MsgpackFull => decode_cms_from_msgpack(&state.bytes)
                        .map_err(|e| to_err(e, state.encoding))?,
                    SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
                        return Err(to_err(
                            "CMS delta encodings not implemented in ASAP-tier reducer".to_string(),
                            state.encoding,
                        ));
                    }
                };
            Ok(row0_sum_cms(&cms))
        }
        SketchKindHandle::CountSketch => {
            let cs = match state.encoding {
                SketchEncoding::ProtoFull => {
                    decode_cs_from_proto(&state.bytes).map_err(|e| to_err(e, state.encoding))?
                }
                SketchEncoding::MsgpackFull => {
                    decode_cs_from_msgpack(&state.bytes).map_err(|e| to_err(e, state.encoding))?
                }
                SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
                    return Err(to_err(
                        "CountSketch delta encodings not implemented in ASAP-tier reducer"
                            .to_string(),
                        state.encoding,
                    ));
                }
            };
            Ok(row0_sum_cs(&cs))
        }
        // Heap-bearing variants: decode via the CMS-with-heap envelope
        // and read the underlying CMS matrix the same way.
        SketchKindHandle::CmsWithHeap | SketchKindHandle::CountSketchWithHeap => {
            let heap = decode_cms_with_heap_from_msgpack(&state.bytes)
                .map_err(|e| to_err(e, state.encoding))?;
            let matrix = heap.sketch_matrix();
            Ok(row0_sum_from_matrix(&matrix))
        }
        // Quantile / cardinality handles can't answer frequency — caller
        // should have rejected at `require_capability`. Defensive arm.
        other => Err(ASAPTierError::UnsupportedCapability {
            function: "frequency".to_string(),
            capability: Capability::FrequencyEstimate(other),
        }),
    }
}

fn row0_sum_cms(cms: &CountMinSketch) -> f64 {
    let matrix = cms.sketch();
    row0_sum_from_matrix(&matrix)
}

fn row0_sum_cs(cs: &CountSketch) -> f64 {
    let matrix = cs.sketch();
    row0_sum_from_matrix(matrix)
}

fn row0_sum_from_matrix(matrix: &[Vec<f64>]) -> f64 {
    matrix
        .first()
        .map(|row| row.iter().copied().sum::<f64>())
        .unwrap_or(0.0)
}
