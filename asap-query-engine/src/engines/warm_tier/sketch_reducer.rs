//! Per-Capability sketch reducer (warm-tier query evaluator).
//!
//! Caller has already classified all candidate sids as `Hit`
//! against the [`SketchIndex`] (see PR #122's classify hook in
//! `simple/engine.rs::QueryEngine::execute`). This module:
//!
//! 1. Resolves each sid's [`Capability`] + [`SketchKindHandle`] +
//!    [`SketchConfig`] from `SketchIndex::instance`.
//! 2. Validates that the user's PromQL function is answerable by
//!    that capability — `quantile_over_time` only on
//!    `QuantileApprox`, `topk` only on `FrequencyTopk`,
//!    `count_distinct_over_time` only on `CardinalityApprox`.
//! 3. For each sid, calls `SketchIndex::query_range` to fetch all
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

use asap_sketchlib::sketches::ddsketch::DdSketch;
use asap_sketchlib::sketches::hll::HllSketch;
use asap_sketchlib::sketches::kll::KllSketch;
use asap_sketchlib::sketches::countminsketch::CountMinSketch;
use asap_sketchlib::sketches::countsketch::CountSketch;

use crate::engines::warm_tier::decoders::decode_cms_with_heap_from_msgpack;
use crate::engines::warm_tier::delta_apply::{
    cumulative_evaluate, per_window_evaluate, DeltaSketchKind,
};
use crate::stores::sketch_db::sketch_index::{
    Capability, SketchEncoding, SketchIndex, SketchInstanceMetadata, SketchKindHandle,
    SketchSampleState,
};

/// Reducer wrapping a `&SketchIndex`. Constructed per-query; cheap.
pub struct SketchReducer<'a> {
    pub index: &'a SketchIndex,
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
/// in-range window (defensive default). The caller (`SimpleEngine`)
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
    FrequencyTopk,
}

impl<'a> SketchReducer<'a> {
    pub fn new(index: &'a SketchIndex) -> Self {
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
        match (family, &meta.capability) {
            (QueryFamily::Quantile, Capability::QuantileApprox(_))
            | (QueryFamily::Cardinality, Capability::CardinalityApprox)
            | (QueryFamily::FrequencyTopk, Capability::FrequencyTopk(_)) => {
                Ok(meta.capability.clone())
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
                    let w_end_u64 = if *window_end >= 0 { *window_end as u64 } else { 0 };
                    if w_end_u64 < cov_lo {
                        cov_lo = w_end_u64;
                    }
                    if w_end_u64 > cov_hi {
                        cov_hi = w_end_u64;
                    }
                    let cms_heap = match meta.sketch_kind {
                        SketchKindHandle::CmsWithHeap => {
                            decode_cms_with_heap_from_msgpack(&state.bytes).map_err(|e| {
                                WarmTierError::DeserializeFailure {
                                    sid,
                                    encoding: state.encoding,
                                    reason: e,
                                }
                            })?
                        }
                        SketchKindHandle::CountMin | SketchKindHandle::CountSketch => {
                            return Err(WarmTierError::MissingHeap {
                                sid,
                                sketch_kind: meta.sketch_kind,
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
            let delta_kind = match (family, meta.sketch_kind) {
                (QueryFamily::Quantile, SketchKindHandle::DDSketch) => DeltaSketchKind::DDSketch,
                (QueryFamily::Quantile, SketchKindHandle::Kll) => DeltaSketchKind::Kll,
                (QueryFamily::Cardinality, SketchKindHandle::Hll) => DeltaSketchKind::Hll,
                _ => {
                    return Err(WarmTierError::UnsupportedCapability {
                        function: function_name.to_string(),
                        capability: meta.capability.clone(),
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
                    let (one, _skipped) =
                        cumulative_evaluate(&samples_vec, delta_kind, &evaluator)
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
                        per_window_evaluate(&samples_vec, delta_kind, &evaluator)
                            .map_err(|e| WarmTierError::DeserializeFailure {
                                sid,
                                encoding: SketchEncoding::ProtoFull,
                                reason: e,
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
                // CMS / CountSketch frequency point query needs a
                // key. The PromQL `topk(k, foo)` shape doesn't
                // pass an explicit key — the canonical answer
                // would draw from a CMS-with-heap (heavy-hitter
                // sketch). PR #122's `Capability::FrequencyTopk`
                // doesn't yet wire the heap through, so surface
                // as `UnsupportedCapability` and let the router
                // fall through to archive. This is a documented
                // follow-up: once `SketchKindHandle` carries a
                // CmsWithHeap variant, route to a heap-walking
                // estimator that returns the top-k items.
                Err(WarmTierError::UnsupportedCapability {
                    function: "topk".to_string(),
                    capability: Capability::FrequencyTopk(sketch_kind),
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
fn decode_ddsketch(
    sid: u64,
    state: &SketchSampleState,
) -> Result<DdSketch, WarmTierError> {
    match state.encoding {
        SketchEncoding::ProtoFull => DdSketch_from_sketchlib_proto_bytes(&state.bytes).map_err(|e| {
            WarmTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: e.to_string(),
            }
        }),
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
fn decode_kll(
    sid: u64,
    state: &SketchSampleState,
) -> Result<KllSketch, WarmTierError> {
    match state.encoding {
        SketchEncoding::ProtoFull => KllSketch_from_sketchlib_proto_bytes(&state.bytes).map_err(|e| {
            WarmTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: e.to_string(),
            }
        }),
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
                reason: "KLL delta encodings not implemented in warm-tier reducer"
                    .to_string(),
            })
        }
    }
}

#[allow(dead_code)]
fn decode_hll(
    sid: u64,
    state: &SketchSampleState,
) -> Result<HllSketch, WarmTierError> {
    match state.encoding {
        SketchEncoding::ProtoFull => HllSketch_from_sketchlib_proto_bytes(&state.bytes).map_err(|e| {
            WarmTierError::DeserializeFailure {
                sid,
                encoding: state.encoding,
                reason: e.to_string(),
            }
        }),
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
                reason: "HLL delta encodings not implemented in warm-tier reducer"
                    .to_string(),
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
            None => DdSketchState::decode(buffer)
                .map_err(|e| format!("decode DDSketchState: {e}"))?,
        },
        Err(_) => DdSketchState::decode(buffer)
            .map_err(|e| format!("decode DDSketchState: {e}"))?,
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
        Err(_) => HyperLogLogState::decode(buffer)
            .map_err(|e| format!("decode HyperLogLogState: {e}"))?,
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

// CMS / CountSketch decoders are not yet wired through the reducer
// because the warm-tier `topk` capability requires CMS-with-heap
// (see `evaluate_one_state`'s FrequencyTopk arm). The decoders
// themselves exist on `precompute_operators::{count_min_sketch,
// count_sketch}_accumulator.rs::from_sketchlib_proto_bytes` and are
// trivially liftable when the heap variant lands.
#[allow(dead_code)]
fn _unused_cms_kept_for_future_topk(buffer: &[u8]) -> Option<CountMinSketch> {
    CountMinSketch::deserialize_msgpack(buffer).ok()
}
#[allow(dead_code)]
fn _unused_count_sketch_kept_for_future_topk(buffer: &[u8]) -> Option<CountSketch> {
    CountSketch::deserialize_msgpack(buffer).ok()
}
