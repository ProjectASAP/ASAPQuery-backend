//! Reconstruction of stored window states from full and delta frames.
use asap_physical_operators::summary_kernels as k;
use asap_physical_operators::summary_kernels::weighted_frequency::{
    FrequencyAlgorithm, WeightedFrequency,
};
use asap_physical_operators::AggregateCore;
use asap_sketchlib::{
    CountMinSketch, CountMinSketchWithHeap, CountSketch, CountSketchWithHeap, DdSketch, HllSketch,
    HllVariant, KllSketch,
};

use super::decoders as d;
use super::{SketchEncoding, SketchSampleState};
use asap_physical_operators::summary_kernels::univmon::UnivMonAccumulator;

/// Which sketch family a candidate is, and the parameters needed to
/// *bootstrap an empty state* — required by the per-window-reset (PWR)
/// delta model where a window's FIRST frame is a delta-from-empty (no
/// carry-in Full).
#[derive(Debug, Clone, Copy)]
pub enum DeltaSketchKind {
    UnivMon {
        heap_size: u32,
        sketch_rows: u32,
        sketch_cols: u32,
        layers: u8,
    },
    DDSketch {
        alpha: f64,
    },
    Hll {
        precision: u32,
    },
    Kll {
        k: u32,
    },
    Cms {
        rows: usize,
        cols: usize,
    },
    CountSketch {
        rows: usize,
        cols: usize,
    },
    /// Count-Min (min-over-rows) and Count Sketch (median-of-signed-rows)
    /// heaps share a storage shape but are different algorithms, so they are
    /// separate kinds and never merge into each other.
    CmsWithHeap {
        rows: usize,
        cols: usize,
        heap_size: usize,
    },
    CountSketchWithHeap {
        rows: usize,
        cols: usize,
        heap_size: usize,
    },
}

impl DeltaSketchKind {
    /// An EMPTY state for this kind, seeding a window whose first frame is a
    /// delta-from-empty (PWR): empty ⊕ delta = that window's state.
    fn bootstrap_empty(&self) -> SummaryState {
        match *self {
            Self::UnivMon {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            } => SummaryState::UnivMon(
                UnivMonAccumulator::new(
                    heap_size as usize,
                    sketch_rows as usize,
                    sketch_cols as usize,
                    layers as usize,
                )
                .expect("validated UnivMon catalog dimensions"),
            ),
            Self::DDSketch { alpha } => SummaryState::Dd(k::DDSketchAccumulator::new(alpha)),
            Self::Kll { k } => SummaryState::Kll(k::DatasketchesKLLAccumulator::new(k as u16)),
            Self::Hll { precision } => {
                SummaryState::Hll(k::HllSketchAccumulator::new(HllVariant::Regular, precision))
            }
            Self::Cms { rows, cols } => {
                SummaryState::Cms(k::CountMinSketchAccumulator::new(rows, cols))
            }
            Self::CountSketch { rows, cols } => {
                SummaryState::CountSketch(k::CountSketchAccumulator::new(rows, cols))
            }
            Self::CmsWithHeap {
                rows,
                cols,
                heap_size,
            } => SummaryState::CmsWithHeap(k::CountMinSketchWithHeapAccumulator::new(
                rows, cols, heap_size,
            )),
            Self::CountSketchWithHeap {
                rows,
                cols,
                heap_size,
            } => SummaryState::CountSketchWithHeap(k::CountSketchWithHeapAccumulator::new(
                rows, cols, heap_size,
            )),
        }
    }
}

/// Decode a "full" frame (used by both per-window and cumulative modes when
/// the encoding is `*Full`).
fn decode_full(
    kind: &DeltaSketchKind,
    bytes: &[u8],
    encoding: SketchEncoding,
) -> Result<SummaryState, String> {
    use SketchEncoding::{MsgpackFull, ProtoFull, WeightedFrequencyV1};
    Ok(match (kind, encoding) {
        (
            DeltaSketchKind::UnivMon {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            },
            MsgpackFull,
        ) => {
            let state = asap_sketchlib::UnivMon::deserialize_from_bytes(bytes)
                .map_err(|e| e.to_string())
                .and_then(|sketch| {
                    UnivMonAccumulator::from_sketch(sketch).map_err(|e| e.to_string())
                })
                .map_err(|e| e.to_string())?;
            if state.dimensions()
                != (
                    *heap_size as usize,
                    *sketch_rows as usize,
                    *sketch_cols as usize,
                    *layers as usize,
                )
            {
                return Err("UnivMon payload dimensions differ from installed catalog".into());
            }
            SummaryState::UnivMon(state)
        }
        (DeltaSketchKind::DDSketch { .. }, ProtoFull) => SummaryState::Dd(
            k::DDSketchAccumulator::from_sketch(
                d::ddsketch_from_proto(bytes)?,
                d::sample_probability(bytes)?,
            )
            .map_err(|e| e.to_string())?,
        ),
        (DeltaSketchKind::DDSketch { .. }, MsgpackFull) => dd(d::ddsketch_from_msgpack(bytes)?),
        (DeltaSketchKind::Hll { .. }, ProtoFull) => SummaryState::Hll(
            k::HllSketchAccumulator::from_sketch(
                d::hll_from_proto(bytes)?,
                d::sample_probability(bytes)?,
            )
            .map_err(|e| e.to_string())?,
        ),
        (DeltaSketchKind::Hll { .. }, MsgpackFull) => hll(d::hll_from_msgpack(bytes)?),
        (DeltaSketchKind::Kll { .. }, ProtoFull) => kll(d::kll_from_proto(bytes)?),
        (DeltaSketchKind::Kll { .. }, MsgpackFull) => kll(d::kll_from_msgpack(bytes)?),
        (DeltaSketchKind::Cms { .. }, ProtoFull) => SummaryState::Cms(
            k::CountMinSketchAccumulator::from_sketch(
                d::cms_from_proto(bytes)?,
                d::sample_probability(bytes)?,
            )
            .map_err(|e| e.to_string())?,
        ),
        (DeltaSketchKind::Cms { .. }, MsgpackFull) => cms(d::cms_from_msgpack(bytes)?),
        (DeltaSketchKind::CountSketch { .. }, ProtoFull) => SummaryState::CountSketch(
            k::CountSketchAccumulator::from_sketch(
                d::cs_from_proto(bytes)?,
                d::sample_probability(bytes)?,
            )
            .map_err(|e| e.to_string())?,
        ),
        (DeltaSketchKind::CountSketch { .. }, MsgpackFull) => cs(d::cs_from_msgpack(bytes)?),
        // The legacy heap wire format is msgpack-only.
        (DeltaSketchKind::CmsWithHeap { .. }, ProtoFull | MsgpackFull) => {
            cms_heap_state(d::cms_with_heap_from_msgpack(bytes)?)
        }
        (DeltaSketchKind::CountSketchWithHeap { .. }, ProtoFull | MsgpackFull) => {
            cs_heap_state(d::cs_with_heap_from_msgpack(bytes)?)
        }
        (
            DeltaSketchKind::CmsWithHeap { rows, cols, .. }
            | DeltaSketchKind::CountSketchWithHeap { rows, cols, .. },
            WeightedFrequencyV1,
        ) => {
            let expected = match kind {
                DeltaSketchKind::CmsWithHeap { .. } => FrequencyAlgorithm::Cms,
                _ => FrequencyAlgorithm::CountSketch,
            };
            let state = super::codec::frequency_state(bytes).map_err(|e| e.to_string())?;
            let kernel = &state;
            // The catalog's heap size is not carried here; matrix shape is.
            let (width, depth, _) = kernel.shape();
            if kernel.algorithm() != expected || (width, depth) != (*cols, *rows) {
                return Err("weighted frequency shape differs from installed catalog".into());
            }
            SummaryState::WeightedFrequency(state)
        }
        (_, e) => return Err(format!("decode_full called with non-Full encoding {e:?}")),
    })
}

fn dd(inner: DdSketch) -> SummaryState {
    SummaryState::Dd(k::DDSketchAccumulator::from_sketch(inner, 1.0).expect("unsampled sketch"))
}
fn hll(inner: HllSketch) -> SummaryState {
    SummaryState::Hll(k::HllSketchAccumulator::from_sketch(inner, 1.0).expect("unsampled sketch"))
}
fn kll(inner: KllSketch) -> SummaryState {
    SummaryState::Kll(k::DatasketchesKLLAccumulator { inner })
}
fn cms(inner: CountMinSketch) -> SummaryState {
    SummaryState::Cms(
        k::CountMinSketchAccumulator::from_sketch(inner, 1.0).expect("unsampled sketch"),
    )
}
fn cs(inner: CountSketch) -> SummaryState {
    SummaryState::CountSketch(
        k::CountSketchAccumulator::from_sketch(inner, 1.0).expect("unsampled sketch"),
    )
}
fn cms_heap_state(inner: CountMinSketchWithHeap) -> SummaryState {
    SummaryState::CmsWithHeap(k::CountMinSketchWithHeapAccumulator { inner })
}
fn cs_heap_state(inner: CountSketchWithHeap) -> SummaryState {
    SummaryState::CountSketchWithHeap(k::CountSketchWithHeapAccumulator { inner })
}

/// Render a ranked heap item as the legacy heap key: item parts joined by
/// `;`, with a canonical series identity (it names `__name__`) shown as its
/// series key. A grouped heap's identity omits the labels it is partitioned
/// by; those are stored as the heap's group labels and restored here.
fn heap_item_key(
    items: &[asap_physical_operators::values::Value],
    group: &std::collections::BTreeMap<String, String>,
) -> String {
    use asap_physical_operators::values::Value;
    items
        .iter()
        .map(|item| match item {
            Value::Utf8(text) => {
                asap_physical_operators::physical_planner::promql_rows::decode_series_identity(text)
                    .ok()
                    .filter(|labels| labels.contains_key("__name__"))
                    .map(|mut labels| {
                        // An empty group value is a series without that label.
                        for (name, value) in group.iter().filter(|(_, v)| !v.is_empty()) {
                            labels.entry(name.clone()).or_insert_with(|| value.clone());
                        }
                        series_key(&labels)
                    })
                    .unwrap_or_else(|| text.to_string())
            }
            Value::Null => String::new(),
            Value::Float64(value) => value.to_string(),
            Value::Int64(value) => value.to_string(),
            Value::Bool(value) => value.to_string(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join(";")
}

/// `name{k="v",...}` with labels sorted and values escaped, as series keys are.
fn series_key(labels: &std::collections::BTreeMap<String, String>) -> String {
    let name = labels
        .get("__name__")
        .map(String::as_str)
        .unwrap_or_default();
    let pairs = labels
        .iter()
        .filter(|(k, _)| k.as_str() != "__name__")
        .map(|(k, v)| {
            let escaped = v
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            format!("{k}=\"{escaped}\"")
        })
        .collect::<Vec<_>>();
    if pairs.is_empty() {
        name.to_owned()
    } else {
        format!("{name}{{{}}}", pairs.join(","))
    }
}

/// The reconstructed Planner kernel state one candidate sid contributes —
/// either folded across a window (or several) via delta application, or
/// merged in from another sid's own reconstruction.
pub enum SummaryState {
    UnivMon(UnivMonAccumulator),
    Dd(k::DDSketchAccumulator),
    Hll(k::HllSketchAccumulator),
    Kll(k::DatasketchesKLLAccumulator),
    Cms(k::CountMinSketchAccumulator),
    CountSketch(k::CountSketchAccumulator),
    /// See `DeltaSketchKind::CmsWithHeap` for why the two heaps differ.
    CmsWithHeap(k::CountMinSketchWithHeapAccumulator),
    CountSketchWithHeap(k::CountSketchWithHeapAccumulator),
    /// Planner's weighted-frequency heap, read through its ranked rows.
    WeightedFrequency(WeightedFrequency),
}

impl SummaryState {
    /// The Planner kernel this state holds.
    pub fn kernel(&self) -> &dyn AggregateCore {
        match self {
            Self::UnivMon(s) => s,
            Self::Dd(s) => s,
            Self::Hll(s) => s,
            Self::Kll(s) => s,
            Self::Cms(s) => s,
            Self::CountSketch(s) => s,
            Self::CmsWithHeap(s) => s,
            Self::CountSketchWithHeap(s) => s,
            Self::WeightedFrequency(s) => s,
        }
    }

    /// Apply a delta-encoded frame. DDSketch bucket deltas and HLL register
    /// deltas apply in place; every other frame decodes independently and
    /// merges in.
    pub fn apply_delta_bytes(
        &mut self,
        bytes: &[u8],
        encoding: SketchEncoding,
    ) -> Result<(), String> {
        use SketchEncoding::{MsgpackDelta, MsgpackFull, ProtoDelta, ProtoFull};
        if !matches!(encoding, ProtoDelta | MsgpackDelta) {
            return Err(format!(
                "apply_delta_bytes called with non-Delta encoding {encoding:?}"
            ));
        }
        let fragment = |kind: DeltaSketchKind, full| decode_full(&kind, bytes, full);
        match (&mut *self, encoding) {
            (Self::UnivMon(_), _) => Err("UnivMon requires full pane snapshots".into()),
            (Self::WeightedFrequency(_), _) => {
                Err("weighted frequency frames are complete states, not deltas".into())
            }
            // Two shapes arrive on the DDSketch proto-delta channel: a full
            // envelope fragment (merged) and the common bucket-index delta.
            (Self::Dd(sketch), ProtoDelta) => {
                if d::carries_sketch_state(bytes) {
                    let other = fragment(DeltaSketchKind::DDSketch { alpha: 0.0 }, ProtoFull)?;
                    self.merge_same_family(&other)
                } else {
                    sketch.merge_sample_p(1.0).map_err(|e| e.to_string())?;
                    d::apply_ddsketch_proto_delta(&mut sketch.inner, bytes)
                        .map_err(|e| format!("apply DDSketch proto bucket-delta: {e}"))
                }
            }
            (Self::Dd(_), _) => {
                let other = fragment(DeltaSketchKind::DDSketch { alpha: 0.0 }, MsgpackFull)?;
                self.merge_same_family(&other)
            }
            (Self::Hll(sketch), ProtoDelta) => {
                sketch.merge_sample_p(1.0).map_err(|e| e.to_string())?;
                d::apply_hll_proto_delta(&mut sketch.inner, bytes)
            }
            (Self::Hll(_), _) => {
                let other = fragment(DeltaSketchKind::Hll { precision: 0 }, MsgpackFull)?;
                self.merge_same_family(&other)
            }
            (Self::Kll(_), _) => {
                let full = if encoding == ProtoDelta {
                    ProtoFull
                } else {
                    MsgpackFull
                };
                let other = fragment(DeltaSketchKind::Kll { k: 0 }, full)?;
                self.merge_same_family(&other)
            }
            (Self::Cms(_), ProtoDelta) => {
                let other = cms(d::cms_from_proto_delta(bytes)?);
                self.merge_same_family(&other)
            }
            (Self::CountSketch(_), ProtoDelta) => {
                let other = cs(d::cs_from_proto_delta(bytes)?);
                self.merge_same_family(&other)
            }
            (Self::Cms(_) | Self::CountSketch(_), _) => Err(
                "heap-less CountMin/CountSketch MSGPACK_DELTA is not a valid producer encoding \
                 (msgpack-delta is the heap-bearing form)"
                    .into(),
            ),
            // A heap delta frame decodes to a standalone window state and
            // merges in; heap proto "deltas" are full msgpack states.
            (Self::CmsWithHeap(_), _) => {
                let other = cms_heap_state(if encoding == MsgpackDelta {
                    d::cms_with_heap_from_msgpack_delta(bytes)?
                } else {
                    d::cms_with_heap_from_msgpack(bytes)?
                });
                self.merge_same_family(&other)
            }
            (Self::CountSketchWithHeap(_), _) => {
                let other = cs_heap_state(if encoding == MsgpackDelta {
                    d::cs_with_heap_from_msgpack_delta(bytes)?
                } else {
                    d::cs_with_heap_from_msgpack(bytes)?
                });
                self.merge_same_family(&other)
            }
        }
    }

    pub fn quantile(&self, q: f64) -> f64 {
        use planner_types::post_asap::SketchQuery;
        match self {
            Self::Dd(_) | Self::Kll(_) => self
                .kernel()
                .estimate(&SketchQuery::Quantile { q })
                .unwrap_or(0.0),
            _ => 0.0,
        }
    }

    pub fn cardinality(&self) -> f64 {
        use planner_types::post_asap::SketchQuery;
        match self {
            Self::Hll(s) => s.estimate(&SketchQuery::Cardinality).unwrap_or(0.0),
            _ => 0.0,
        }
    }

    /// Total update mass where the Planner kernel supports an unkeyed count.
    pub fn total(&self) -> Option<f64> {
        use planner_types::{post_asap::SketchQuery, pre_asap::ColumnRef};
        match self {
            Self::Cms(c) => Some(c.total()),
            Self::CmsWithHeap(c) => Some(c.total()),
            Self::Dd(c) => c
                .estimate(&SketchQuery::PointCount {
                    key: ColumnRef::SampleValue,
                    value: None,
                })
                .ok(),
            // Signed CountSketch projections do not preserve total update mass.
            _ => None,
        }
    }

    /// Per-item point estimate; `None` for families without an item universe.
    pub fn estimate(&self, key: &str) -> Option<f64> {
        match self {
            Self::Cms(c) => {
                Some(c.query_key(&crate::KeyByLabelValues::new_with_labels(vec![key.into()])))
            }
            Self::CountSketch(c) => {
                Some(c.query_key(&crate::KeyByLabelValues::new_with_labels(vec![key.into()])))
            }
            Self::CmsWithHeap(h) => {
                Some(h.query_key(&crate::KeyByLabelValues::new_with_labels(vec![key.into()])))
            }
            Self::CountSketchWithHeap(h) => {
                Some(h.query_key(&crate::KeyByLabelValues::new_with_labels(vec![key.into()])))
            }
            _ => None,
        }
    }

    /// Heap `(key, value)` pairs, unordered; `None` for a heap-less state.
    /// `group` is the stored group the state belongs to.
    pub fn topk_items(
        &self,
        group: &std::collections::BTreeMap<String, String>,
    ) -> Option<Vec<(String, f64)>> {
        match self {
            Self::CmsWithHeap(h) => Some(
                h.inner
                    .topk_heap_items()
                    .into_iter()
                    .map(|item| (item.key, item.value))
                    .collect(),
            ),
            Self::CountSketchWithHeap(h) => Some(
                h.inner
                    .topk_heap_items()
                    .into_iter()
                    .map(|item| (item.key, item.value))
                    .collect(),
            ),
            Self::WeightedFrequency(h) => Some(
                h.rows(usize::MAX)
                    .into_iter()
                    .filter_map(|mut row| {
                        let asap_physical_operators::values::Value::Float64(score) = row.pop()?
                        else {
                            return None;
                        };
                        Some((heap_item_key(&row, group), score))
                    })
                    .collect(),
            ),
            _ => None,
        }
    }

    /// Merge `other` into `self` with the Planner kernel's merge; both must be
    /// the same family and shape.
    pub fn merge_same_family(&mut self, other: &SummaryState) -> Result<(), String> {
        let mut merged = self
            .kernel()
            .merge_with(other.kernel())
            .map_err(|e| format!("merge {} state: {e}", self.family_name()))?;
        // Move the merged value into the existing variant without cloning its sketch.
        macro_rules! replace {
            ($state:expr, $ty:ty) => {
                std::mem::swap(
                    $state,
                    merged
                        .as_any_mut()
                        .downcast_mut::<$ty>()
                        .ok_or("Planner merge changed the state family")?,
                )
            };
        }
        match self {
            Self::UnivMon(s) => replace!(s, UnivMonAccumulator),
            Self::Dd(s) => replace!(s, k::DDSketchAccumulator),
            Self::Hll(s) => replace!(s, k::HllSketchAccumulator),
            Self::Kll(s) => replace!(s, k::DatasketchesKLLAccumulator),
            Self::Cms(s) => replace!(s, k::CountMinSketchAccumulator),
            Self::CountSketch(s) => replace!(s, k::CountSketchAccumulator),
            Self::CmsWithHeap(s) => replace!(s, k::CountMinSketchWithHeapAccumulator),
            Self::CountSketchWithHeap(s) => replace!(s, k::CountSketchWithHeapAccumulator),
            Self::WeightedFrequency(s) => replace!(s, WeightedFrequency),
        }
        Ok(())
    }

    /// Diagnostic family name for error messages — not used for dispatch.
    fn family_name(&self) -> &'static str {
        match self {
            Self::UnivMon(_) => "UnivMon",
            Self::Dd(_) => "DDSketch",
            Self::Hll(_) => "Hll",
            Self::Kll(_) => "Kll",
            Self::Cms(_) => "Cms",
            Self::CountSketch(_) => "CountSketch",
            Self::CmsWithHeap(_) => "CmsWithHeap",
            Self::CountSketchWithHeap(_) => "CountSketchWithHeap",
            Self::WeightedFrequency(_) => "WeightedFrequency",
        }
    }
}

/// Fold every in-range window's frames for ONE series into a single
/// merged `SummaryState` (cumulative over `[t0, t1]`), returning `None`
/// if no Full frame ever landed (every sample was a leading delta). The
/// per-sid building block for a cross-sid answer: reconstruct each
/// candidate sid's state this way, then merge them (`merge_same_family`)
/// before reading out a quantile/cardinality over the combined data.
pub fn cumulative_summary_state(
    samples: &[(i64, &SketchSampleState)],
    kind: DeltaSketchKind,
) -> Result<Option<SummaryState>, String> {
    let mut rolling: Option<SummaryState> = None;
    visit_window_summary_states(samples, kind, |_, state| {
        if let Some(acc) = rolling.as_mut() {
            acc.merge_same_family(&state)?;
        } else {
            rolling = Some(state);
        }
        Ok(())
    })?;
    Ok(rolling)
}

#[cfg(test)]
/// Walk a sorted-by-window-end slice of samples in time order and
/// produce ONE per-window scalar `(window_end_ms, scalar)`.
///
/// ## Per-window-reset (PWR) delta model
///
/// The edge emits frames grouped by window (all frames of one window
/// share the same `window_end` key; the key changes across windows).
/// The edge RESETS its snapshot base at each window boundary, so each
/// window's state is built *from empty*:
///
/// * Within a window, frames accumulate to the window total. The first
///   frame may be a `Full` (window 1, or a periodic re-snapshot) or a
///   `Delta`-from-empty (windows 2+ under PWR); subsequent frames are
///   `Delta` INCREMENTS applied onto the window's running base.
/// * Across windows, the base MUST reset — a new `window_end` discards
///   the previous window's rolling state and starts from empty. Never
///   carry one window's state into the next (that would inflate via
///   cross-window accumulation).
///
/// Concretely this fixes two bugs in the old "single rolling Option that
/// only ever resets on a Full" walk:
///   1. A query range whose Full lives only in window 1 (or out of
///      range) left windows 2+ as deltas with `rolling=None`, all
///      skipped → empty result.
///   2. A window 2+ delta applied onto window 1's leftover rolling state
///      → cross-window inflation.
///
/// For a `Delta` that is the window's FIRST frame (the PWR delta-from-
/// empty case), we bootstrap an EMPTY rolling state of `kind` and apply
/// the delta onto it (delta-from-empty ⊕ empty = that window's state).
///
/// The delta-OFF path (exactly one `Full` per window) still produces one
/// correct value per window: the window opens with a Full, has no
/// further frames, and emits that Full's scalar.
///
/// `eval` reads a scalar from the rolling state (`quantile(q)` /
/// `cardinality()`). `skipped` counts frames that could not contribute
/// (a delta we genuinely couldn't bootstrap from — should be rare).
///
/// Returns `Ok(per_window_samples, skipped)`.
pub fn per_window_evaluate<E>(
    samples: &[(i64, &SketchSampleState)],
    kind: DeltaSketchKind,
    eval: E,
) -> Result<(Vec<(i64, f64)>, usize), String>
where
    E: Fn(&SummaryState) -> f64,
{
    let (states, skipped) = per_window_summary_states(samples, kind)?;
    Ok((
        states.into_iter().map(|(w, rs)| (w, eval(&rs))).collect(),
        skipped,
    ))
}

/// Walk a sorted-by-window-end slice of samples in time order and
/// reconstruct ONE sid's per-window `SummaryState` (same per-window-reset
/// walk as [`per_window_evaluate`], generalized to return the
/// reconstructed state itself instead of an already-evaluated scalar).
/// The per-sid building block for cross-sid per-window merging (unlike
/// [`cumulative_summary_state`], which folds a whole `[t0, t1]` range
/// into one answer, this keeps each window separate so a caller can
/// merge same-window states across several sids before evaluating --
/// needed for a matrix/range-query answer, where each output point is
/// itself a cross-sid merge for that one window).
///
/// Returns `Ok((per_window_states, skipped))`.
pub fn per_window_summary_states(
    samples: &[(i64, &SketchSampleState)],
    kind: DeltaSketchKind,
) -> Result<(Vec<(i64, SummaryState)>, usize), String> {
    let mut out: Vec<(i64, SummaryState)> = Vec::new();
    let skipped = visit_window_summary_states(samples, kind, |end, state| {
        out.push((end, state));
        Ok(())
    })?;
    Ok((out, skipped))
}

// Both readout modes must reconstruct the same final pane population. The
// visitor lets cumulative merging stream panes without retaining every state.
fn visit_window_summary_states(
    samples: &[(i64, &SketchSampleState)],
    kind: DeltaSketchKind,
    mut emit: impl FnMut(i64, SummaryState) -> Result<(), String>,
) -> Result<usize, String> {
    let mut skipped = 0usize;

    // Rolling state for the CURRENT window only. Reset to None whenever
    // `window_end` changes (a new window establishes its own base from
    // empty). `cur_end` tracks which window `rolling` belongs to.
    let mut rolling: Option<SummaryState> = None;
    let mut cur_end: Option<i64> = None;

    for (window_end, state) in samples {
        // Window boundary: flush the previous window's final accumulated
        // state, then reset the base so this window starts from empty.
        if cur_end != Some(*window_end) {
            if let (Some(prev_end), Some(rs)) = (cur_end, rolling.take()) {
                emit(prev_end, rs)?;
            }
            cur_end = Some(*window_end);
        }

        match state.encoding {
            SketchEncoding::NativeBatchV1 => {
                return Err("native physical outputs require the bound native batch decoder".into())
            }
            SketchEncoding::ProtoFull
            | SketchEncoding::MsgpackFull
            | SketchEncoding::WeightedFrequencyV1 => {
                // A Full (re)sets this window's base.
                rolling = Some(decode_full(&kind, &state.bytes, state.encoding)?);
            }
            SketchEncoding::ProtoDelta | SketchEncoding::MsgpackDelta => {
                // Apply onto this window's running base. If this is the
                // window's first frame (PWR delta-from-empty), bootstrap
                // an empty base and apply onto it.
                if rolling.is_none() {
                    rolling = Some(kind.bootstrap_empty());
                }
                match rolling.as_mut() {
                    Some(rs) => rs.apply_delta_bytes(&state.bytes, state.encoding)?,
                    None => skipped += 1,
                }
            }
        }
    }

    // Flush the final window.
    if let (Some(prev_end), Some(rs)) = (cur_end, rolling.take()) {
        emit(prev_end, rs)?;
    }

    Ok(skipped)
}

#[cfg(test)]
mod tests {
    //! P2-3 / P2-4 regression tests for the consolidated single-decoder
    //! path. These exercise the family proto decoders that now delegate
    //! to the precompute accumulators (the single source of truth), so a
    //! divergence between the warm read path and the ingest path —
    //! notably the SPARSE-register HLL handling the deleted dead decoder
    //! got wrong — fails the build.
    use super::*;

    // A stored Planner heap window decodes only for its catalog family and shape, merges
    // with another window, and ranks items under their series keys.
    #[test]
    fn weighted_frequency_frames_rank_items_by_series_key() {
        use asap_physical_operators::values::Value;
        let frame = |weight| {
            let mut state = WeightedFrequency::new(FrequencyAlgorithm::Cms, 64, 3, 8).unwrap();
            let identity = r#"{"__name__":"m","endpoint":"a\"b"}"#;
            state
                .update(&[Value::Utf8(identity.into())], weight)
                .unwrap();
            state.update(&[Value::Utf8("plain".into())], 1.0).unwrap();
            SketchSampleState {
                bytes: state.to_bytes(),
                encoding: SketchEncoding::WeightedFrequencyV1,
            }
        };
        let (first, second) = (frame(3.0), frame(4.0));
        let heap = DeltaSketchKind::CmsWithHeap {
            rows: 3,
            cols: 64,
            heap_size: 8,
        };
        let state = cumulative_summary_state(&[(1000, &first), (2000, &second)], heap)
            .unwrap()
            .unwrap();
        let mut items = state.topk_items(&Default::default()).unwrap();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            items,
            vec![
                (r#"m{endpoint="a\"b"}"#.to_string(), 7.0),
                ("plain".to_string(), 2.0)
            ]
        );
        let other = DeltaSketchKind::CountSketchWithHeap {
            rows: 3,
            cols: 64,
            heap_size: 8,
        };
        assert!(cumulative_summary_state(&[(1000, &first)], other).is_err());
        let narrower = DeltaSketchKind::CmsWithHeap {
            rows: 3,
            cols: 32,
            heap_size: 8,
        };
        assert!(cumulative_summary_state(&[(1000, &first)], narrower).is_err());
    }

    // A grouped heap's item identity omits its partition labels; readout
    // restores non-empty ones from the stored group and keeps identity values.
    #[test]
    fn weighted_frequency_items_restore_group_labels() {
        use asap_physical_operators::values::Value;
        let mut state = WeightedFrequency::new(FrequencyAlgorithm::Cms, 64, 3, 8).unwrap();
        let identity = r#"{"__name__":"m","endpoint":"a"}"#;
        state.update(&[Value::Utf8(identity.into())], 2.0).unwrap();
        let state = SummaryState::WeightedFrequency(state);
        let group = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        assert_eq!(
            state.topk_items(&group(&[("job", "j")])).unwrap(),
            vec![(r#"m{endpoint="a",job="j"}"#.to_string(), 2.0)]
        );
        assert_eq!(
            state
                .topk_items(&group(&[("job", ""), ("endpoint", "other")]))
                .unwrap(),
            vec![(r#"m{endpoint="a"}"#.to_string(), 2.0)]
        );
    }
    use asap_sketchlib::HllVariant;

    fn encode_dd(sk: &DdSketch) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
        use prost::Message;
        let state = DdSketchState {
            alpha: sk.alpha,
            store_counts: sk.store_counts.clone(),
            store_offset: sk.store_offset,
            ..Default::default()
        };
        SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Ddsketch(state)),
            ..Default::default()
        }
        .encode_to_vec()
    }

    /// Build a SPARSE HLL proto frame: dense `registers` left empty,
    /// `registers_sparse.packed` = varint (index_delta, value) pairs.
    /// This is exactly the wire form a low-cardinality producer emits
    /// (sketchlib-go below its dense/sparse crossover) — the frame the
    /// DELETED `HllSketch_from_sketchlib_proto_bytes` hard-rejected with
    /// "registers has 0 bytes".
    fn encode_hll_sparse(precision: u32, nonzero: &[(u64, u8)]) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, HllSparseRegisters, HllVariant as ProtoVariant, HyperLogLogState,
            SketchEnvelope,
        };
        use prost::Message;
        // Varint-pack (index_delta, value), ascending index order.
        let mut packed: Vec<u8> = Vec::new();
        let mut prev: u64 = 0;
        let put_uvarint = |buf: &mut Vec<u8>, mut v: u64| loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                buf.push(b | 0x80);
            } else {
                buf.push(b);
                break;
            }
        };
        let mut sorted = nonzero.to_vec();
        sorted.sort_by_key(|(i, _)| *i);
        for (idx, val) in &sorted {
            put_uvarint(&mut packed, idx - prev);
            put_uvarint(&mut packed, *val as u64);
            prev = *idx;
        }
        let state = HyperLogLogState {
            variant: ProtoVariant::Regular as i32,
            precision,
            registers: Vec::new(), // dense field empty → sparse path
            hip_kxq0: 0.0,
            hip_kxq1: 0.0,
            hip_est: 0.0,
            // `num_registers` is informational — the decoder expands
            // against `expected_len` from precision, not this field.
            registers_sparse: Some(HllSparseRegisters {
                num_registers: 1u32 << precision,
                packed,
            }),
        };
        SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Hll(state)),
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[test]
    fn hll_from_proto_accepts_sparse_frame() {
        // The consolidated decoder must accept the sparse wire form (the
        // deleted dead decoder rejected it). Build a sparse frame setting
        // a handful of registers, decode it, and confirm those register
        // slots came back set in the dense array.
        let precision = 12u32;
        let nonzero = [(3u64, 5u8), (100, 2), (4000, 7)];
        let bytes = encode_hll_sparse(precision, &nonzero);
        let sk = d::hll_from_proto(&bytes).expect("sparse HLL frame must decode (P2-3 regression)");
        assert_eq!(sk.registers.len(), 1usize << precision);
        for (idx, val) in nonzero {
            assert_eq!(
                sk.registers[idx as usize], val,
                "sparse register {idx} expanded to wrong value"
            );
        }
    }

    // -----------------------------------------------------------------
    // Per-window-reset (PWR) delta-apply regression tests.
    //
    // The edge resets its snapshot base at every window boundary, so a
    // window's first frame is either a Full (window 1 / re-snapshot) or
    // a Delta-from-empty (windows 2+). The query-side walk must:
    //   * reset the rolling base when `window_end` changes,
    //   * bootstrap an empty base for a window's leading Delta,
    //   * emit ONE value per window (the window's final accumulated
    //     state), never per-frame and never cross-window-accumulated.
    // -----------------------------------------------------------------

    fn full(bytes: Vec<u8>) -> SketchSampleState {
        SketchSampleState {
            bytes,
            encoding: SketchEncoding::ProtoFull,
        }
    }
    fn delta(bytes: Vec<u8>) -> SketchSampleState {
        SketchSampleState {
            bytes,
            encoding: SketchEncoding::ProtoDelta,
        }
    }

    fn dd_over(alpha: f64, vals: &[f64]) -> DdSketch {
        let mut sk = DdSketch::new(alpha);
        for &v in vals {
            sk.update(v);
        }
        sk
    }

    // Sampled envelope deltas preserve their scaled count.
    #[test]
    fn sampled_ddsketch_envelope_on_delta_channel_scales_count() {
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, SketchEnvelope};
        use prost::Message;
        let sketch = dd_over(0.01, &[1., 2.]);
        let state = asap_sketch_codec::ddsketch_state(&encode_dd(&sketch))
            .unwrap()
            .0;
        let sampled = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Ddsketch(state)),
            sample_p: 0.5,
            ..Default::default()
        }
        .encode_to_vec();
        let mut rolling = DeltaSketchKind::DDSketch { alpha: 0.01 }.bootstrap_empty();
        rolling
            .apply_delta_bytes(&sampled, SketchEncoding::ProtoDelta)
            .unwrap();
        assert_eq!(
            rolling
                .kernel()
                .estimate(&planner_types::post_asap::SketchQuery::PointCount {
                    key: planner_types::pre_asap::ColumnRef::SampleValue,
                    value: None,
                })
                .unwrap(),
            4.0
        );
    }

    /// A full re-snapshot replaces its pane's earlier frames; cumulative
    /// readout must merge the finalized panes without counting updates twice.
    #[test]
    fn cumulative_readout_counts_resnapshot_population_once() {
        let first = full(encode_dd(&dd_over(0.01, &[1., 2.])));
        let updated = full(encode_dd(&dd_over(0.01, &[1., 2., 3.])));
        let next = delta(encode_dd(&dd_over(0.01, &[9.])));
        let samples = [(1000, &first), (1000, &updated), (2000, &next)];
        let state = cumulative_summary_state(&samples, DeltaSketchKind::DDSketch { alpha: 0.01 })
            .unwrap()
            .unwrap();
        let SummaryState::Dd(state) = state else {
            panic!("expected DDSketch state");
        };
        assert_eq!(state.inner.store_counts.iter().sum::<u64>(), 4);
    }

    /// PWR across 3 windows: window 1 is `[Full]`, windows 2 & 3 are
    /// `[Delta-from-empty]` (NO Full carry-in). Each window must
    /// reconstruct its OWN distribution's median — not empty (the old
    /// "skip delta with no base" bug) and not cross-window-inflated.
    #[test]
    fn pwr_ddsketch_three_windows_delta_from_empty() {
        let alpha = 0.01;
        let w1 = dd_over(alpha, &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let w2 = dd_over(alpha, &[10.0, 20.0, 30.0, 40.0, 50.0]);
        let w3 = dd_over(alpha, &[100.0, 200.0, 300.0, 400.0, 500.0]);

        // window 1 ships a Full; windows 2+ ship a delta-from-empty.
        let s1 = full(encode_dd(&w1));
        let s2 = delta(encode_dd(&w2));
        let s3 = delta(encode_dd(&w3));
        let samples = vec![(1000_i64, &s1), (2000, &s2), (3000, &s3)];

        let kind = DeltaSketchKind::DDSketch { alpha };
        let (out, skipped) =
            per_window_evaluate(&samples, kind, |rs| rs.quantile(0.5)).expect("pwr eval");
        assert_eq!(skipped, 0, "PWR must not skip delta-from-empty frames");
        assert_eq!(out.len(), 3, "one value per window");

        // Each window's median ≈ that window's own distribution median,
        // independent of the others (no carry-in inflation).
        let truth = [
            w1.quantile(0.5).unwrap(),
            w2.quantile(0.5).unwrap(),
            w3.quantile(0.5).unwrap(),
        ];
        for (i, (w_end, est)) in out.iter().enumerate() {
            assert_eq!(*w_end, (i as i64 + 1) * 1000);
            let rel = (est - truth[i]).abs() / truth[i].max(1e-9);
            assert!(
                rel < 0.05,
                "window {i}: est={est} truth={} rel={rel}",
                truth[i]
            );
        }
        // Cross-window-inflation guard: window 2's median must NOT have
        // absorbed window 1 (would pull it well below 30).
        assert!(
            out[1].1 > 20.0,
            "window 2 median {} suggests cross-window accumulation",
            out[1].1
        );
    }

    /// Sub-window producer: a SINGLE window carries multiple frames
    /// `[Full, Delta, Delta]`, where each later delta is an increment
    /// since the previous emit in that window. The walk must COLLAPSE
    /// them to ONE value = the window's running total, not emit three.
    #[test]
    fn pwr_ddsketch_subwindow_frames_collapse_to_window_total() {
        let alpha = 0.01;
        // Three sub-window increments that together cover 1..=15.
        let a = dd_over(alpha, &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let b = dd_over(alpha, &[6.0, 7.0, 8.0, 9.0, 10.0]);
        let c = dd_over(alpha, &[11.0, 12.0, 13.0, 14.0, 15.0]);
        let s_a = full(encode_dd(&a));
        let s_b = delta(encode_dd(&b));
        let s_c = delta(encode_dd(&c));
        // All three share the same window_end (one window, sub-window frames).
        let samples = vec![(5000_i64, &s_a), (5000, &s_b), (5000, &s_c)];

        let kind = DeltaSketchKind::DDSketch { alpha };
        let (out, skipped) =
            per_window_evaluate(&samples, kind, |rs| rs.quantile(0.5)).expect("subwindow eval");
        assert_eq!(skipped, 0);
        assert_eq!(out.len(), 1, "sub-window frames collapse to ONE value");
        assert_eq!(out[0].0, 5000);

        let truth = dd_over(alpha, &(1..=15).map(|v| v as f64).collect::<Vec<_>>())
            .quantile(0.5)
            .unwrap();
        let rel = (out[0].1 - truth).abs() / truth.max(1e-9);
        assert!(rel < 0.05, "window total est={} truth={truth}", out[0].1);
    }

    /// Same sub-window collapse, but the window's FIRST frame is a
    /// Delta-from-empty (PWR window 2+ with sub-window frames):
    /// `[Delta-from-empty, Delta, Delta]`.
    #[test]
    fn pwr_ddsketch_subwindow_first_frame_delta_from_empty() {
        let alpha = 0.01;
        let a = dd_over(alpha, &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let b = dd_over(alpha, &[6.0, 7.0, 8.0, 9.0, 10.0]);
        let c = dd_over(alpha, &[11.0, 12.0, 13.0, 14.0, 15.0]);
        let s_a = delta(encode_dd(&a)); // first frame is delta-from-empty
        let s_b = delta(encode_dd(&b));
        let s_c = delta(encode_dd(&c));
        let samples = vec![(9000_i64, &s_a), (9000, &s_b), (9000, &s_c)];

        let kind = DeltaSketchKind::DDSketch { alpha };
        let (out, skipped) =
            per_window_evaluate(&samples, kind, |rs| rs.quantile(0.5)).expect("eval");
        assert_eq!(skipped, 0);
        assert_eq!(out.len(), 1);
        let truth = dd_over(alpha, &(1..=15).map(|v| v as f64).collect::<Vec<_>>())
            .quantile(0.5)
            .unwrap();
        let rel = (out[0].1 - truth).abs() / truth.max(1e-9);
        assert!(rel < 0.05, "est={} truth={truth}", out[0].1);
    }

    /// PWR for HLL across 3 windows, each a Delta-from-empty (sparse
    /// register delta). Bootstrapping an EMPTY HLL of the right precision
    /// is required (register deltas index into a pre-sized array). Each
    /// window's cardinality must reflect its OWN item set.
    #[test]
    fn pwr_hll_three_windows_delta_from_empty() {
        let precision = 12u32;
        // Build per-window HLLs, then encode each as a register-delta
        // against an EMPTY sketch (= that window's full register state,
        // the PWR delta-from-empty wire form).
        let empty = HllSketch::new(HllVariant::Regular, precision);
        let mut frames = Vec::new();
        let truths = [200usize, 800, 1500];
        for (w, &n) in truths.iter().enumerate() {
            let mut sk = HllSketch::new(HllVariant::Regular, precision);
            let base = (w as u64) * 100_000; // disjoint item sets per window
            for i in 0..n as u64 {
                sk.update(format!("u-{}", base + i).as_bytes());
            }
            let bytes = sk.compute_delta(&empty, 0);
            frames.push((((w as u64) + 1) * 1000, delta(bytes)));
        }
        let samples: Vec<(i64, &SketchSampleState)> =
            frames.iter().map(|(t, s)| (*t as i64, s)).collect();

        let kind = DeltaSketchKind::Hll { precision };
        let (out, skipped) =
            per_window_evaluate(&samples, kind, |rs| rs.cardinality()).expect("hll pwr eval");
        assert_eq!(skipped, 0, "HLL delta-from-empty must bootstrap, not skip");
        assert_eq!(out.len(), 3);
        for (i, (_w_end, est)) in out.iter().enumerate() {
            let n = truths[i] as f64;
            let rel = (est - n).abs() / n;
            assert!(
                rel < 0.15,
                "window {i}: HLL est={est} truth={n} rel={rel} (each window independent)"
            );
        }
    }

    /// `CmsWithHeap` (min-over-rows estimator, `CountMinSketchWithHeap`)
    /// and `CountSketchWithHeap` (median-of-signed-rows estimator, the
    /// distinct `CountSketchWithHeap` type) are different sketch
    /// algorithms that merely happen to share a storage shape — merging
    /// one into the other must be rejected as a family mismatch, the
    /// same as merging a `Cms` into a `Kll` would be. Since the two
    /// `SummaryState` variants now hold genuinely different Rust types,
    /// this is also enforced at compile time — there is no arm in
    /// `merge_same_family` that type-checks a mixed pair together.
    #[test]
    fn cms_with_heap_and_count_sketch_with_heap_are_not_the_same_family() {
        use asap_sketchlib::{CountMinSketchWithHeap, CountSketchWithHeap, MessagePackCodec};

        let mut cms_heap = CountMinSketchWithHeap::new(4, 256, 10);
        cms_heap.update("a", 1.0);
        let mut cs_heap = CountSketchWithHeap::new(4, 256, 10);
        cs_heap.update("b", 1.0);

        let mut a = cms_heap_state(
            CountMinSketchWithHeap::from_msgpack(&cms_heap.to_msgpack().unwrap()).unwrap(),
        );
        let b = cs_heap_state(
            CountSketchWithHeap::from_msgpack(&cs_heap.to_msgpack().unwrap()).unwrap(),
        );
        assert!(
            a.merge_same_family(&b).is_err(),
            "CmsWithHeap must not merge with CountSketchWithHeap"
        );
    }

    fn encode_delta_heap(
        rows: u32,
        cols: u32,
        cells: &[(u32, u32, i64)],
        heap: &[(&str, f64)],
        heap_size: u64,
    ) -> Vec<u8> {
        #[derive(serde::Serialize)]
        struct W<'a>(
            bool,
            (u32, u32, &'a [(u32, u32, i64)]),
            Vec<(String, f64)>,
            u64,
        );
        let heap_owned: Vec<(String, f64)> =
            heap.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        let w = W(true, (rows, cols, cells), heap_owned, heap_size);
        rmp_serde::to_vec(&w).expect("encode delta-heap")
    }

    /// `SummaryState::CountSketchWithHeap` must decode both FULL and
    /// DELTA-HEAP msgpack frames through the genuine
    /// `asap_sketchlib::CountSketchWithHeap` (median-of-signed-rows
    /// estimator) rather than the CMS-family `CountMinSketchWithHeap`
    /// (min-over-rows estimator) it used to alias — the bug this split
    /// fixed. Built via real `update()` calls (not a hand-crafted matrix)
    /// so the sign-hashed row semantics are genuinely exercised, then
    /// checks both decode paths reproduce the same matrix and the same
    /// `estimate()` as the in-memory sketch they were encoded from.
    #[test]
    fn count_sketch_with_heap_full_and_delta_decode_via_new_asap_sketchlib_type() {
        use asap_sketchlib::{CountSketchWithHeap, MessagePackCodec};

        let mut built = CountSketchWithHeap::new(4, 64, 10);
        for _ in 0..50 {
            built.update("k", 1.0);
        }
        let expected_matrix = built.sketch_matrix();
        let expected_estimate = built.estimate("k");

        // FULL path.
        let full_bytes = built.to_msgpack().expect("encode full CountSketchWithHeap");
        let full_state = decode_full(
            &DeltaSketchKind::CountSketchWithHeap {
                rows: 4,
                cols: 64,
                heap_size: 10,
            },
            &full_bytes,
            SketchEncoding::MsgpackFull,
        )
        .expect("decode_full CountSketchWithHeap");
        match full_state {
            SummaryState::CountSketchWithHeap(inner) => {
                assert_eq!(inner.inner.sketch_matrix(), expected_matrix);
                assert_eq!(inner.inner.estimate("k"), expected_estimate);
            }
            other => panic!(
                "expected CountSketchWithHeap state, got {}",
                other.family_name()
            ),
        }

        // DELTA-HEAP path: same cells + heap against an empty base (PWR
        // contract), encoded the way the Go producer does.
        let cells: Vec<(u32, u32, i64)> = expected_matrix
            .iter()
            .enumerate()
            .flat_map(|(r, row)| {
                row.iter().enumerate().filter_map(move |(c, v)| {
                    if *v != 0.0 {
                        Some((r as u32, c as u32, *v as i64))
                    } else {
                        None
                    }
                })
            })
            .collect();
        let heap_pairs: Vec<(String, f64)> = built
            .topk_heap_items()
            .into_iter()
            .map(|item| (item.key, item.value))
            .collect();
        assert!(!heap_pairs.is_empty(), "expected \"k\" in the top-k heap");
        let heap_refs: Vec<(&str, f64)> =
            heap_pairs.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        let delta_bytes = encode_delta_heap(4, 64, &cells, &heap_refs, 10);

        let mut rolling = DeltaSketchKind::CountSketchWithHeap {
            rows: 4,
            cols: 64,
            heap_size: 10,
        }
        .bootstrap_empty();
        rolling
            .apply_delta_bytes(&delta_bytes, SketchEncoding::MsgpackDelta)
            .expect("apply CountSketchWithHeap delta");
        match rolling {
            SummaryState::CountSketchWithHeap(inner) => {
                assert_eq!(
                    inner.inner.sketch_matrix(),
                    expected_matrix,
                    "delta path must reconstruct the identical matrix"
                );
                assert_eq!(inner.inner.estimate("k"), expected_estimate);
            }
            other => panic!(
                "expected CountSketchWithHeap state, got {}",
                other.family_name()
            ),
        }
    }
}
