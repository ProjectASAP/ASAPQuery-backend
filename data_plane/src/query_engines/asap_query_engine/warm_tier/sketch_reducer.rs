//! Per-Capability sketch reducer (warm-tier query evaluator).
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
//!      requires the prior base, which the warm-tier query path
//!      doesn't carry today).
//!    - Run the canonical sketch query (`quantile`, `estimate`).
//! 5. Return per-series, per-window scalars in [`WarmTierResult`].
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
//!   how the warm-tier columnar store carries one sketch per
//!   `(start, end)` window; the request-range merge can be added
//!   as a post-process when the simple engine's range-query
//!   pipeline is wired to call this reducer.
//! - **Hybrid stitch** (`[t0..t1']` from warm + `[t1'..t1]` from
//!   archive) — `QueryResult` doesn't carry timestamp-coverage
//!   metadata yet, so we materialize the full warm-tier answer
//!   and let the engine router decide.
//! - **Top-k items**: top-k requires CMS-with-heap (the heap
//!   structure carries the actual heavy hitters); the
//!   `Capability::FrequencyTopk(SketchKindHandle::CountMin)`
//!   variant in PR #122 doesn't yet plumb the per-key list
//!   through the wire format. We surface `FrequencyTopk` queries
//!   as `UnsupportedCapability` for now and document the gap.

use std::collections::BTreeMap;

use asap_sketchlib::sketches::countminsketch::CountMinSketch;
use asap_sketchlib::sketches::countsketch::CountSketch;
use asap_sketchlib::sketches::ddsketch::DdSketch;
use asap_sketchlib::sketches::hll::HllSketch;
use asap_sketchlib::sketches::kll::KllSketch;

use crate::query_engines::asap_query_engine::warm_tier::decoders::{
    decode_cms_from_msgpack, decode_cms_from_proto, decode_cms_with_heap_from_msgpack,
    decode_cs_from_msgpack, decode_cs_from_proto,
};
use crate::query_engines::asap_query_engine::warm_tier::delta_apply::{
    cumulative_evaluate, per_window_evaluate, DeltaSketchKind,
};
use crate::stores::sketch_db::index::{
    Capability, SketchEncoding, SketchStore, SketchInstanceMetadata, SketchKindHandle,
    SketchSampleState,
};

/// Reducer wrapping a `&SketchStore`. Constructed per-query; cheap.
pub struct SketchReducer<'a> {
    pub index: &'a SketchStore,
}

/// Distinct failure modes the engine maps onto the routing layer.
///
/// `UnsupportedFunction` / `UnsupportedCapability` → "the warm tier
/// can't answer this; archive can". `DeserializeFailure` → "the
/// warm-tier state didn't decode; defensive fallback". `NoData` →
/// "the sketch index has no samples in `[t0, t1]`; archive may have
/// older history". `MissingHeap` → "the sid is FrequencyTopk-classed
/// but the underlying sketch family carries no heap (vanilla
/// CountSketch / CountMinSketch without `CmsWithHeap`), so the
/// reducer can't materialize top-k items without an external item
/// universe".
#[derive(Debug)]
pub enum WarmTierError {
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

impl std::fmt::Display for WarmTierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WarmTierError::UnsupportedFunction(name) => {
                write!(f, "warm-tier reducer does not support function `{name}`")
            }
            WarmTierError::UnsupportedCapability {
                function,
                capability,
            } => write!(
                f,
                "warm-tier reducer cannot answer `{function}` against capability {capability:?}"
            ),
            WarmTierError::MissingHeap { sid, sketch_kind } => write!(
                f,
                "warm-tier reducer cannot enumerate top-k for sid {sid}: \
                 sketch kind {sketch_kind:?} carries no top-k heap \
                 (CountMin / CountSketch only support point-frequency queries; \
                 use CmsWithHeap for top-k)"
            ),
            WarmTierError::DeserializeFailure {
                sid,
                encoding,
                reason,
            } => write!(
                f,
                "warm-tier sketch decode failure for sid {sid} \
                 (encoding={encoding:?}): {reason}"
            ),
            WarmTierError::NoData { metric_name } => write!(
                f,
                "warm-tier index has no samples for metric `{metric_name}` in window"
            ),
        }
    }
}

impl std::error::Error for WarmTierError {}

/// Per-series, per-window scalar results.
///
/// `coverage` is the actual `(min_window_start_ms, max_window_end_ms)`
/// the reducer covered. `None` when the reducer didn't observe any
/// in-range window (defensive default). The caller (`ASAPQueryEngine`)
/// compares `coverage` against the requested `[t0, t1]` and, on a
/// partial hit (`cov_lo > t0 || cov_hi < t1`), falls over to archive
/// for the missing range and stitches the two answers. See TODO 3 in
/// the warm-tier follow-up PR.
#[derive(Debug, Clone, Default)]
pub struct WarmTierResult {
    /// `(label_values, samples)` where `samples` is
    /// `(window_end_unix_ms, value)`.
    pub series: Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>,
    /// Effective coverage `(min_window_start_ms, max_window_end_ms)`.
    /// Set whenever the reducer observed at least one window; left
    /// `None` when `series` is empty.
    pub coverage: Option<(u64, u64)>,
}

impl WarmTierResult {
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

    /// Map a PromQL function name to the warm-tier query family it
    /// addresses. After the Step 2a refactor the canonical dispatch is
    /// off the analyzer's `Capability` (see [`capability_to_family`]);
    /// this string-based fallback exists ONLY for the
    /// `SketchReducer::evaluate` entry point's `function_name: &str`
    /// API surface, which is preserved for the existing call sites.
    /// Unrecognised names route to the canonical family via the
    /// downstream `require_capability` check.
    fn function_to_family(function_name: &str) -> Result<QueryFamily, WarmTierError> {
        match function_name {
            "quantile_over_time" | "histogram_quantile" | "quantile" => Ok(QueryFamily::Quantile),
            // `distinct_over_time` (MetricsQL) is the canonical
            // distinct-count-over-window name. `cardinality_estimate`
            // and `count_distinct_over_time` are accepted as historical
            // aliases for back-compat with PR #128's reducer tests; new
            // callers should pass the analyzer's `required_capability`
            // and dispatch via [`capability_to_family`] instead.
            "distinct_over_time"
            | "count_distinct_over_time"
            | "cardinality_estimate"
            | "count_distinct" => Ok(QueryFamily::Cardinality),
            "topk" | "topk_over_time" | "bottomk" => Ok(QueryFamily::FrequencyTopk),
            // Bare frequency point queries — the MetricsQL surface for
            // `sum by (item) (rate(m[r]))` with epsilon accuracy. The
            // reducer answers these by decoding the CMS / CountSketch
            // matrix directly (no heap needed). `frequency` is the
            // canonical name; `count_over_time` is accepted as an alias
            // for back-compat with PromQL counter-style point queries.
            "frequency" | "frequency_estimate" => Ok(QueryFamily::FrequencyEstimate),
            other => Err(WarmTierError::UnsupportedFunction(other.to_string())),
        }
    }

    /// Map a [`Capability`] to a [`QueryFamily`]. This is the canonical
    /// dispatch path after Step 2a: the controller's analyzer hands
    /// each `WarmTierCandidate` a `required_capability`, and the
    /// reducer picks a family without ever matching on the PromQL
    /// function-name string.
    #[allow(dead_code)]
    pub(crate) fn capability_to_family(cap: &Capability) -> QueryFamily {
        match cap {
            Capability::QuantileApprox(_) => QueryFamily::Quantile,
            Capability::CardinalityApprox => QueryFamily::Cardinality,
            Capability::FrequencyTopk(_) => QueryFamily::FrequencyTopk,
            Capability::FrequencyEstimate(_) => QueryFamily::FrequencyEstimate,
        }
    }

    /// Validate that a sid's capability is compatible with the
    /// requested query family. Returns the `Capability` on match,
    /// `Err(UnsupportedCapability)` on mismatch.
    fn require_capability(
        function_name: &str,
        family: QueryFamily,
        meta: &SketchInstanceMetadata,
    ) -> Result<Capability, WarmTierError> {
        // The warm-tier reducer only ever runs on sketch-backed sids
        // (the analyzer's `instances_matching` filters on `Capability`,
        // which is `None` for precompute-backed sids). A `None` here
        // means upstream classification broke — surface as a missing
        // capability rather than panicking the request path.
        let Some(cap) = meta.capability.as_ref() else {
            return Err(WarmTierError::UnsupportedCapability {
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
            (_, other) => Err(WarmTierError::UnsupportedCapability {
                function: function_name.to_string(),
                capability: other.clone(),
            }),
        }
    }

    /// Evaluate a PromQL query against the warm tier.
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
    ) -> Result<WarmTierResult, WarmTierError> {
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
                        let total = decode_frequency_total(sid, meta.sketch_kind().expect("warm-tier reducer only handles sketch-backed sids"), state)?;
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
                    let cms_heap = match meta.sketch_kind().expect("warm-tier reducer only handles sketch-backed sids") {
                        SketchKindHandle::CmsWithHeap | SketchKindHandle::CountSketchWithHeap => {
                            // Both heap-bearing variants serialize the
                            // outer `CountMinSketchWithHeap` envelope via
                            // msgpack (`CountSketchWithHeap` reuses the
                            // same wire shape since the heap is the
                            // distinguishing payload).
                            decode_cms_with_heap_from_msgpack(&state.bytes).map_err(|e| {
                                WarmTierError::DeserializeFailure {
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
                            return Err(WarmTierError::MissingHeap {
                                sid,
                                sketch_kind: meta.sketch_kind().expect("warm-tier reducer only handles sketch-backed sids"),
                            });
                        }
                        other => {
                            return Err(WarmTierError::UnsupportedCapability {
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
            let delta_kind = match (family, meta.sketch_kind().expect("warm-tier reducer only handles sketch-backed sids")) {
                (QueryFamily::Quantile, SketchKindHandle::DDSketch) => DeltaSketchKind::DDSketch,
                (QueryFamily::Quantile, SketchKindHandle::Kll) => DeltaSketchKind::Kll,
                (QueryFamily::Cardinality, SketchKindHandle::Hll) => DeltaSketchKind::Hll,
                _ => {
                    return Err(WarmTierError::UnsupportedCapability {
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
                        .map_err(|e| WarmTierError::DeserializeFailure {
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
                            WarmTierError::DeserializeFailure {
                                sid,
                                encoding: SketchEncoding::ProtoFull,
                                reason: e,
                            }
                        })?;
                    per_win
                };
                out_series.push((ts.series_label_values, samples_out));
            }
        }

        if !any_window {
            return Err(WarmTierError::NoData {
                metric_name: metric_name_for_err,
            });
        }

        let coverage = if cov_lo <= cov_hi {
            Some((cov_lo, cov_hi))
        } else {
            None
        };
        Ok(WarmTierResult {
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
    /// the warm-tier reducer's main loop now goes through
    /// [`per_window_evaluate`] / [`cumulative_evaluate`].
    #[allow(dead_code)]
    fn evaluate_one_state(
        &self,
        sid: u64,
        family: QueryFamily,
        sketch_kind: SketchKindHandle,
        function_args: &[f64],
        state: &SketchSampleState,
    ) -> Result<f64, WarmTierError> {
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
                Err(WarmTierError::UnsupportedCapability {
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
                Err(WarmTierError::UnsupportedCapability {
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
    ) -> Result<f64, WarmTierError> {
        match sketch_kind {
            SketchKindHandle::DDSketch => {
                let sk = decode_ddsketch(sid, state)?;
                Ok(sk.quantile(q).unwrap_or(0.0))
            }
            SketchKindHandle::Kll => {
                let sk = decode_kll(sid, state)?;
                Ok(sk.quantile(q))
            }
            other => Err(WarmTierError::UnsupportedCapability {
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
    ) -> Result<f64, WarmTierError> {
        match sketch_kind {
            SketchKindHandle::Hll => {
                let sk = decode_hll(sid, state)?;
                Ok(sk.estimate())
            }
            other => Err(WarmTierError::UnsupportedCapability {
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
fn decode_ddsketch(sid: u64, state: &SketchSampleState) -> Result<DdSketch, WarmTierError> {
    match state.encoding {
        SketchEncoding::ProtoFull => {
            DdSketch_from_sketchlib_proto_bytes(&state.bytes).map_err(|e| {
                WarmTierError::DeserializeFailure {
                    sid,
                    encoding: state.encoding,
                    reason: e.to_string(),
                }
            })
        }
        SketchEncoding::MsgpackFull => DdSketch::deserialize_msgpack(&state.bytes).map_err(|e| {
            WarmTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: e.to_string(),
            }
        }),
        SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
            Err(WarmTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: "delta encodings require base sketch state \
                         (warm-tier reducer doesn't yet stitch delta + base \
                         within query_range)"
                    .to_string(),
            })
        }
    }
}

#[allow(dead_code)]
fn decode_kll(sid: u64, state: &SketchSampleState) -> Result<KllSketch, WarmTierError> {
    match state.encoding {
        SketchEncoding::ProtoFull => {
            KllSketch_from_sketchlib_proto_bytes(&state.bytes).map_err(|e| {
                WarmTierError::DeserializeFailure {
                    sid,
                    encoding: state.encoding,
                    reason: e.to_string(),
                }
            })
        }
        SketchEncoding::MsgpackFull => KllSketch::deserialize_msgpack(&state.bytes).map_err(|e| {
            WarmTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: e.to_string(),
            }
        }),
        SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
            Err(WarmTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: "KLL delta encodings not implemented in warm-tier reducer".to_string(),
            })
        }
    }
}

#[allow(dead_code)]
fn decode_hll(sid: u64, state: &SketchSampleState) -> Result<HllSketch, WarmTierError> {
    match state.encoding {
        SketchEncoding::ProtoFull => {
            HllSketch_from_sketchlib_proto_bytes(&state.bytes).map_err(|e| {
                WarmTierError::DeserializeFailure {
                    sid,
                    encoding: state.encoding,
                    reason: e.to_string(),
                }
            })
        }
        SketchEncoding::MsgpackFull => HllSketch::deserialize_msgpack(&state.bytes).map_err(|e| {
            WarmTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: e.to_string(),
            }
        }),
        SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
            Err(WarmTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: "HLL delta encodings not implemented in warm-tier reducer".to_string(),
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
    use asap_sketchlib::sketches::hll::HllVariant;
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
    CountMinSketch::deserialize_msgpack(buffer).ok()
}
#[allow(dead_code)]
fn _unused_count_sketch_kept_for_future_topk(buffer: &[u8]) -> Option<CountSketch> {
    CountSketch::deserialize_msgpack(buffer).ok()
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
/// Returns `WarmTierError::DeserializeFailure` if the bytes don't decode
/// against the sid's declared sketch kind. Heap-less CMS / CountSketch
/// are NOT a `MissingHeap` error here — bare frequency is exactly what
/// heap-less variants are designed to answer.
fn decode_frequency_total(
    sid: u64,
    sketch_kind: SketchKindHandle,
    state: &SketchSampleState,
) -> Result<f64, WarmTierError> {
    let to_err = |e: String, encoding: SketchEncoding| WarmTierError::DeserializeFailure {
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
                            "CMS delta encodings not implemented in warm-tier reducer".to_string(),
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
                        "CountSketch delta encodings not implemented in warm-tier reducer"
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
        other => Err(WarmTierError::UnsupportedCapability {
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
