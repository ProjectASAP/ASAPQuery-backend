//! `data_plane`'s implementation of `asap_sketch::exec::SummaryExecutor`
//! — the serving-time interface that resolves an `L4Node` plan tree
//! against whatever is actually materialized right now. See
//! `data_plane/docs/l4node-plan-executor-design.md` for the surrounding
//! design.
//!
//! ## Scope
//!
//! Covers **quantile/cardinality queries** (DDSketch/Kll/Hll) **and the
//! Frequency family** — both the bare total (`count`/`sum` with no
//! specific item key) and a per-item point lookup (`count(cms_metric
//! {item="x"})`, `SketchQuery::PointCount{key: Named(_), value: Some(_)}`
//! — `value` is where the filter's actual value lives; see
//! `asap_sketch::SketchQuery::PointCount`'s doc for why `readout` can't
//! resolve it itself from a `Filter` predicate) — for CMS/CountSketch/
//! CMS-with-heap/CountSketch-with-heap, both cumulative (instant) and
//! per-window (matrix/range). All modes do real cross-sid merging via
//! `delta_apply::SummaryState`: reconstruct each candidate sid's own
//! state over the range (or per window), then merge same-window/
//! same-range states *across* sids before reading out one answer per
//! group (or per group per window).
//!
//! `Self::Value` (`SummaryValue`) carries two shapes: `Points` (one
//! scalar per timestamp — everything but `TopK`) and `TopK` (one ranked
//! `(item, count)` list per timestamp). Both cumulative and per-window
//! readout merge cross-sid the same way regardless of which shape the
//! query needs — top-k merge reuses `SummaryState::topk_items` plus the
//! same `merge_same_family` pipeline (`asap_sketchlib`'s heap `merge`
//! already re-reconciles the heap against the merged matrix), no separate
//! merge logic.
//!
//! Not covered, and reported as an explicit `Unsupported` error rather
//! than silently mishandled:
//! - `SketchQuery::TopK` against a heap-less family (`Dd`/`Hll`/`Kll`/
//!   `Cms`/`CountSketch`) — a family limitation (no item universe to
//!   rank), not an unimplemented-query limitation; see `topk_ranked`.
//! - `SketchQuery::PointCount` against a heap-less-*and*-quantile/
//!   cardinality family (`Dd`/`Hll`/`Kll` have no item universe at all)
//!   — a family limitation, not an unimplemented-query limitation.
//! - A `PointCount` whose `key`/`value` combination isn't one of the two
//!   expected shapes (`SampleValue` + `None`, or `Named`/`Qualified` +
//!   `Some(_)`) — reported rather than silently guessed at.
//!
//! `find_candidates`/`fetch_state`/`merge_states` ALSO recognize
//! `AggKind::ExactAgg` sids for `SummaryKind::{Sum, Increase}` (see
//! `exact_agg_kind_match`'s doc for why `MinMax`/`Count`/`Rate` aren't
//! matched) — one sid is one aggregation, read out directly, with no
//! special-casing of exact-vs-approximate at the `find_candidates`/merge
//! level. But `readout`/`SketchQuery` NEVER see these: `asap_plan::bind`
//! never wraps an `ExactAccumulator` implementation in a
//! `SummaryEstimate` (`estimate = false` in `bind_summary_agg`), so
//! `execute()` on such a tree returns `ExecOutcome::State` at the root
//! rather than calling `readout`. `GroupState::exact_value` is the
//! ExactAgg analog of `readout_cumulative` — a future caller reads the
//! final value out of `ExecOutcome::State`'s `GroupState` by calling it
//! directly, not through this trait.
//!
//! Not covered here either: stacking an outer statistic (avg/stddev/...)
//! on top of a sketch or exact-agg readout ("outer-agg-fold") — out of
//! scope by explicit design choice, not an oversight.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

use asap_ir::intent_algebra::{ColumnId, ColumnRef, QueryExpr, Reduction, Source};
use asap_sketch::exec::SummaryExecutor;
use asap_sketch::{L4Node, SketchQuery, SummaryExpr, SummaryKind, SummaryParams};

use control_plane::sketch_algebra::capability::SketchKindHandle;

use crate::storage_engines::sketch_db::data::{AggKind, SketchConfig, SketchTimeSeries};
use crate::storage_engines::sketch_db::index::{SketchSampleState, SketchStore};
use crate::storage_engines::sketch_db::query::delta_apply::{
    cumulative_summary_state, per_window_summary_states, DeltaSketchKind, SummaryState,
};
use crate::storage_engines::types::{AggregateCore, AggregationType, KeyByLabelValues};

/// Per-query, per-call execution context — constructed fresh for each
/// incoming query (never shared across concurrent queries, never
/// mutated after construction). This is what carries the time range and
/// cumulative-vs-per-window mode: `SummaryExecutor`'s trait methods take
/// no such parameters, and `ASAPQueryEngine` itself is called
/// concurrently (`Arc<dyn QueryEngine>`), so threading the range through
/// shared mutable state on the engine would be a race — a fresh,
/// stack-local context per call is the safe alternative.
pub struct QueryExecutionContext<'a> {
    pub index: &'a SketchStore,
    pub t0_ms: u64,
    pub t1_ms: u64,
    /// `true` for `quantile_over_time`/`count_distinct_over_time`-shaped
    /// instant queries (fold the whole range into one answer, via
    /// `readout_cumulative`); `false` for a per-window matrix (one merged
    /// answer per window, via `readout_per_window`).
    pub is_cumulative: bool,
}

/// One candidate sid, already carrying its `[t0, t1]` data and decode
/// parameters. `find_candidates` fetches this once per candidate (it
/// needs the sid's label values to build the group key anyway); folding
/// it into the handle means `fetch_state`/`readout` reuse it instead of
/// re-querying the same `(sid, t0, t1)` range a second time. `Rc` keeps
/// clones of the handle cheap (a refcount bump, not a re-fetch or a
/// re-clone of the sample bytes).
///
/// `ExactAgg` mirrors `Sketch` structurally (one sid's already-decoded
/// `[t0, t1]` data, `Rc`-shared) rather than getting a special exact-vs-
/// approximate treatment — see this module's doc. Its payload is already
/// `Arc<dyn AggregateCore>` per window (from
/// `SketchStore::query_exact_agg_range`, which decodes eagerly, unlike
/// the sketch path's lazy raw-bytes-until-readout), so there's no
/// decode-parameter analog of `DeltaSketchKind` to carry.
#[derive(Clone)]
pub enum SidHandle {
    Sketch {
        series: Rc<SketchTimeSeries>,
        kind: DeltaSketchKind,
    },
    ExactAgg {
        windows: Rc<BTreeMap<i64, Arc<dyn AggregateCore>>>,
        agg_type: AggregationType,
    },
}

// Manual `Debug` -- `dyn AggregateCore` doesn't implement it, so
// `#[derive(Debug)]` can't reach through `ExactAgg`'s `windows` field.
impl std::fmt::Debug for SidHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SidHandle::Sketch { series, kind } => f
                .debug_struct("SidHandle::Sketch")
                .field("series", series)
                .field("kind", kind)
                .finish(),
            SidHandle::ExactAgg { windows, agg_type } => f
                .debug_struct("SidHandle::ExactAgg")
                .field("window_count", &windows.len())
                .field("agg_type", agg_type)
                .finish(),
        }
    }
}

/// One group's accumulated candidates. `Sketch` entries all share one
/// `DeltaSketchKind`; `ExactAgg` entries all share one `AggregationType`
/// (both guaranteed by `find_candidates`'s exact-match contract) — a
/// group is never a mix of the two (`merge_states` errors if it somehow
/// were).
#[derive(Clone)]
pub enum GroupState {
    Sketch {
        entries: Vec<Rc<SketchTimeSeries>>,
        kind: DeltaSketchKind,
    },
    ExactAgg {
        entries: Vec<Rc<BTreeMap<i64, Arc<dyn AggregateCore>>>>,
        agg_type: AggregationType,
    },
}

impl std::fmt::Debug for GroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupState::Sketch { entries, kind } => f
                .debug_struct("GroupState::Sketch")
                .field("entry_count", &entries.len())
                .field("kind", kind)
                .finish(),
            GroupState::ExactAgg { entries, agg_type } => f
                .debug_struct("GroupState::ExactAgg")
                .field("entry_count", &entries.len())
                .field("agg_type", agg_type)
                .finish(),
        }
    }
}

impl GroupState {
    /// Merge this group's ExactAgg accumulators — across every sid AND
    /// every window in `[t0, t1]` (there's no per-window concept exposed
    /// here; a future caller that needs one issues a narrower query) —
    /// into one combined value, then read out the statistic `agg_type`
    /// implies. The ExactAgg analog of `readout_cumulative`, but called
    /// directly by a future caller reading `ExecOutcome::State` (see this
    /// module's doc for why `readout()`/`SketchQuery` never see these).
    ///
    /// `None` for a `Sketch` state, a group with no windows in range, or
    /// a merge/query failure. `AggregationType::MinMax` (and any other
    /// type `exact_agg_kind_match` doesn't match) can't reach a
    /// `GroupState::ExactAgg` via `find_candidates` in the first place —
    /// the fallback arm here is defensive, not a real path.
    pub fn exact_value(&self, key: &Option<KeyByLabelValues>) -> Option<f64> {
        let GroupState::ExactAgg { entries, agg_type } = self else {
            return None;
        };
        let stat = match agg_type {
            AggregationType::Sum
            | AggregationType::MultipleSum
            | AggregationType::Increase
            | AggregationType::MultipleIncrease => asap_types::Statistic::Sum,
            _ => return None,
        };
        let mut merged: Option<Box<dyn AggregateCore>> = None;
        for windows in entries {
            for acc in windows.values() {
                merged = Some(match merged.take() {
                    None => acc.clone_boxed_core(),
                    Some(m) => m.merge_with(acc.as_ref()).ok()?,
                });
            }
        }
        merged?
            .query_statistic(stat, key, &std::collections::HashMap::new())
            .ok()
    }

    /// Coverage analog of `exact_value` — folds `(min_window_end_ms,
    /// max_window_end_ms)` from every window observed across the group's
    /// entries, via the same `fold_coverage` helper
    /// `readout_cumulative`/`readout_per_window` already use for the
    /// sketch family (same window-end-only caveat — see `SummaryValue`'s
    /// doc). Without this, a caller reading an `ExactAgg` group's value
    /// via `exact_value` would have no coverage signal at all to decide
    /// whether an archive tier also needs to be consulted — unlike
    /// `SummaryValue::coverage()` on the sketch side. `None` for a
    /// `Sketch` state or a group with no windows in range.
    pub fn exact_coverage(&self) -> Option<(u64, u64)> {
        let GroupState::ExactAgg { entries, .. } = self else {
            return None;
        };
        let mut coverage: Option<(u64, u64)> = None;
        for windows in entries {
            for &w_end in windows.keys() {
                fold_coverage(&mut coverage, w_end);
            }
        }
        coverage
    }
}

#[derive(Debug)]
pub enum SummaryExecutorError {
    /// No sid in the catalog matches the requested `(metric, by,
    /// SummaryKind, SummaryParams)` — mirrors today's `CapabilityMiss`
    /// contract; the caller fails over to archive.
    NoCandidates,
    /// Couldn't recover a metric name by walking the `SummaryAgg`'s
    /// child subtree — an unsupported/CSE-`Ref`-shaped `QueryExpr` this
    /// executor doesn't walk through.
    NoMetricFound,
    /// A requested `by` `ColumnId` doesn't resolve to a name against the
    /// child's schema.
    UnresolvedColumn(ColumnId),
    /// A candidate sid claims a `SummaryKind` this executor doesn't
    /// implement cross-sid merge for, or the sid's on-disk
    /// `SketchConfig` didn't decode into a `DeltaSketchKind`.
    UnsupportedFamily,
    /// Decode/merge failure surfaced from `delta_apply`/`asap_sketchlib`.
    Decode(String),
    /// A `SummaryExpr::Logical` node — nothing committed at L4. Same
    /// meaning as today's "no candidate bound"; the caller fails over.
    Logical,
    /// A query shape not covered by this executor — see the module doc.
    Unsupported(&'static str),
}

/// A query answer: one scalar per point (`Points` — everything but
/// `TopK`), or one ranked `(item, count)` list per point (`TopK` only).
/// Which variant a query produces is determined entirely by the
/// `SketchQuery` issued (`TopK` vs everything else); callers match on
/// their own query to know which variant to expect.
///
/// Both variants carry a trailing `coverage: Option<(u64, u64)>` — this
/// GROUP's own `(min_window_end_ms, max_window_end_ms)` observed across
/// the windows its readout actually walked. `None` only when a group
/// somehow produced a value with zero windows observed (shouldn't
/// happen in practice: `NoCandidates` fires first).
///
/// Mirrors `ASAPTierResult.coverage`'s ACTUAL semantics (see
/// `sketch_reducer.rs`'s doc and `evaluate_core`/`evaluate_cardinality_global`):
/// despite that field's doc naming it "min_window_start_ms", the sketch
/// path stores no window-start at all (`SketchTimeSeries::samples` is
/// keyed by window-END only — see that struct's doc), so both bounds
/// are folded from window-END timestamps. Also, like the legacy
/// per-window path, this is computed from RAW window-ends *before* the
/// `w_end < t0_ms` carry-in-base filter — a carry-in Full spliced in by
/// `SketchStore::query_range` to seed a leading delta never appears as
/// an output point, but its window-end still legitimately extends this
/// group's covered range further back than the first in-range point.
#[derive(Debug, Clone)]
pub enum SummaryValue {
    Points(Vec<(i64, f64)>, Option<(u64, u64)>),
    TopK(Vec<(i64, Vec<(String, f64)>)>, Option<(u64, u64)>),
}

impl SummaryValue {
    pub fn coverage(&self) -> Option<(u64, u64)> {
        match self {
            SummaryValue::Points(_, coverage) | SummaryValue::TopK(_, coverage) => *coverage,
        }
    }
}

/// Fold `w_end` (a raw window-end timestamp, may be negative pre-epoch
/// in principle) into a running `(min, max)` coverage accumulator —
/// shared by `readout_cumulative`/`readout_per_window` so both compute
/// coverage identically. Mirrors `sketch_reducer.rs`'s repeated
/// `cov_lo`/`cov_hi` fold (see `SummaryValue`'s doc).
fn fold_coverage(coverage: &mut Option<(u64, u64)>, w_end: i64) {
    let w = if w_end >= 0 { w_end as u64 } else { 0 };
    *coverage = Some(match *coverage {
        Some((lo, hi)) => (lo.min(w), hi.max(w)),
        None => (w, w),
    });
}

impl<'a> SummaryExecutor for QueryExecutionContext<'a> {
    type Handle = SidHandle;
    type State = GroupState;
    type Value = SummaryValue;
    type Error = SummaryExecutorError;
    type GroupKey = BTreeMap<String, String>;

    fn find_candidates(
        &self,
        sketch: &SummaryKind,
        params: &SummaryParams,
        _col: &ColumnRef,
        reduction: &Reduction,
        child: &L4Node,
    ) -> Result<Vec<(Self::GroupKey, Self::Handle)>, Self::Error> {
        let metric = find_metric(child).ok_or(SummaryExecutorError::NoMetricFound)?;

        let by: &[ColumnId] = reduction.group_keys().map(|k| k.keys()).unwrap_or(&[]);
        let mut by_names: Vec<String> = Vec::with_capacity(by.len());
        for &col_id in by {
            let name = child
                .schema
                .fields
                .get(col_id)
                .map(|f| f.name.clone())
                .ok_or(SummaryExecutorError::UnresolvedColumn(col_id))?;
            by_names.push(name);
        }
        let required_keys: BTreeSet<String> = by_names.iter().cloned().collect();

        // Which family a matching sid resolved to, carrying just enough
        // to build its `SidHandle` -- kept local to this function since
        // nothing outside needs a candidate BEFORE it's turned into a
        // handle.
        enum Candidate {
            Sketch(DeltaSketchKind),
            ExactAgg(AggregationType),
        }

        let candidate_sids = self.index.instances_matching(&metric, &required_keys);
        let mut out = Vec::new();
        for sid in candidate_sids {
            let candidate = self
                .index
                .with_instance(sid, |m| match &m.agg_kind {
                    AggKind::Sketch { kind, config, .. } => {
                        summary_params_match(sketch, params, *kind, config)
                            .then(|| to_delta_kind(*kind, config))
                            .flatten()
                            .map(Candidate::Sketch)
                    }
                    AggKind::ExactAgg { agg_type, .. } => {
                        exact_agg_kind_match(sketch, params, *agg_type)
                            .then_some(Candidate::ExactAgg(*agg_type))
                    }
                })
                .flatten();
            let Some(candidate) = candidate else {
                continue;
            };

            match candidate {
                Candidate::Sketch(candidate_kind) => {
                    // Fetching the series here (rather than just checking
                    // membership) is what lets `fetch_state`/`readout` skip a
                    // second identical `query_range` call later -- see
                    // `SidHandle`'s doc. The label values it carries are also
                    // the only place a group's actual values live (metadata
                    // only has the group-by KEY names, not values).
                    let series = self.index.query_range(sid, self.t0_ms, self.t1_ms);
                    let Some(series) = series.into_iter().next() else {
                        continue;
                    };
                    let group_key = resolve_group_key(reduction, &by_names, &series.series_label_values);
                    out.push((
                        group_key,
                        SidHandle::Sketch {
                            series: Rc::new(series),
                            kind: candidate_kind,
                        },
                    ));
                }
                Candidate::ExactAgg(agg_type) => {
                    // `query_exact_agg_range` decodes eagerly (returns
                    // `Arc<dyn AggregateCore>` per window already merged
                    // in-memory + disk) -- no raw-bytes/delta-frame
                    // handling analogous to the sketch path is needed.
                    let series_list = self
                        .index
                        .query_exact_agg_range(sid, self.t0_ms, self.t1_ms);
                    let Some((label_values, windows)) = series_list.into_iter().next() else {
                        continue;
                    };
                    let group_key = resolve_group_key(reduction, &by_names, &label_values);
                    out.push((
                        group_key,
                        SidHandle::ExactAgg {
                            windows: Rc::new(windows),
                            agg_type,
                        },
                    ));
                }
            }
        }
        // Empty is NOT an error here -- `asap_sketch::exec::execute()`
        // itself checks `find_candidates`'s result for emptiness and
        // raises the canonical `ExecError::NoCandidates`; erroring here
        // too would just wrap that in `ExecError::Executor(..)` instead,
        // losing the distinction callers match on.
        Ok(out)
    }

    fn fetch_state(&self, handle: &Self::Handle) -> Result<Self::State, Self::Error> {
        Ok(match handle {
            SidHandle::Sketch { series, kind } => GroupState::Sketch {
                kind: *kind,
                entries: vec![series.clone()],
            },
            SidHandle::ExactAgg { windows, agg_type } => GroupState::ExactAgg {
                agg_type: *agg_type,
                entries: vec![windows.clone()],
            },
        })
    }

    fn merge_states(&self, states: Vec<Self::State>) -> Result<Self::State, Self::Error> {
        // The actual decode/merge math (`cumulative_summary_state`/
        // `merge_same_family`) happens in `readout`, not here: it needs
        // to distinguish cumulative vs. per-window mode
        // (`self.is_cumulative`), which only `readout` is positioned to
        // do generically for both callers. `merge_states` and
        // `fetch_state` just assemble the group's full candidate list.
        // ExactAgg has no such split (`GroupState::exact_value` always
        // folds the whole range), but still just accumulates entries here.
        let mut states = states.into_iter();
        let mut acc = states.next().ok_or(SummaryExecutorError::NoCandidates)?;
        for s in states {
            match (&mut acc, s) {
                (GroupState::Sketch { entries, .. }, GroupState::Sketch { entries: more, .. }) => {
                    entries.extend(more);
                }
                (
                    GroupState::ExactAgg { entries, .. },
                    GroupState::ExactAgg { entries: more, .. },
                ) => {
                    entries.extend(more);
                }
                // `find_candidates`'s exact-match contract never produces a
                // mixed group (a `(SummaryKind, SummaryParams)` query
                // matches either sketch-family sids or ExactAgg sids, never
                // both) -- defensive, not a real path.
                _ => return Err(SummaryExecutorError::UnsupportedFamily),
            }
        }
        Ok(acc)
    }

    fn readout(
        &self,
        state: &Self::State,
        query: &SketchQuery,
    ) -> Result<Self::Value, Self::Error> {
        // `ExactAgg` states never reach here in practice -- see this
        // module's doc (`asap_plan::bind` never wraps an ExactAccumulator
        // in a `SummaryEstimate`, so `execute()` stops at
        // `ExecOutcome::State` before ever calling `readout`). Defensive,
        // not a real path.
        let GroupState::Sketch { entries, kind } = state else {
            return Err(SummaryExecutorError::UnsupportedFamily);
        };
        if self.is_cumulative {
            readout_cumulative(entries, *kind, query, self.t1_ms as i64)
        } else {
            readout_per_window(entries, *kind, query, self.t0_ms as i64)
        }
    }

    fn logical(&self, _expr: &QueryExpr) -> Result<Self::Value, Self::Error> {
        Err(SummaryExecutorError::Logical)
    }
}

/// Fold a group's whole `[t0, t1]` into one merged state and read out one
/// scalar -- `quantile_over_time`/`count_distinct_over_time`-shaped
/// instant queries.
fn readout_cumulative(
    entries: &[Rc<SketchTimeSeries>],
    kind: DeltaSketchKind,
    query: &SketchQuery,
    t1_ms: i64,
) -> Result<SummaryValue, SummaryExecutorError> {
    let mut merged: Option<SummaryState> = None;
    let mut latest_window_end: Option<i64> = None;
    let mut coverage: Option<(u64, u64)> = None;
    for entry in entries {
        let samples_vec: Vec<(i64, &SketchSampleState)> = entry
            .samples
            .iter()
            .flat_map(|(t, frames)| frames.iter().map(move |s| (*t, s)))
            .collect();
        if let Some((w, _)) = samples_vec.last() {
            latest_window_end = Some(latest_window_end.map_or(*w, |prev| prev.max(*w)));
        }
        for (w, _) in &samples_vec {
            fold_coverage(&mut coverage, *w);
        }
        let rs =
            cumulative_summary_state(&samples_vec, kind).map_err(SummaryExecutorError::Decode)?;
        if let Some(rs) = rs {
            merged = Some(match merged.take() {
                None => rs,
                Some(mut acc) => {
                    acc.merge_same_family(&rs)
                        .map_err(SummaryExecutorError::Decode)?;
                    acc
                }
            });
        }
    }
    let Some(merged) = merged else {
        return Err(SummaryExecutorError::NoCandidates);
    };
    let w_end = latest_window_end.unwrap_or(t1_ms);
    if let SketchQuery::TopK { k } = query {
        Ok(SummaryValue::TopK(
            vec![(w_end, topk_ranked(&merged, *k)?)],
            coverage,
        ))
    } else {
        Ok(SummaryValue::Points(
            vec![(w_end, sketch_query_value(&merged, query)?)],
            coverage,
        ))
    }
}

/// Per-window matrix/range-query readout: reconstruct each of the
/// group's sids' own per-window states, then merge same-window states
/// *across* sids before evaluating each window -- one merged answer per
/// window, not one merged answer for the whole range. Windows are
/// unioned across sids: a sid that's missing a particular window just
/// doesn't contribute to it, rather than the whole window being dropped.
fn readout_per_window(
    entries: &[Rc<SketchTimeSeries>],
    kind: DeltaSketchKind,
    query: &SketchQuery,
    t0_ms: i64,
) -> Result<SummaryValue, SummaryExecutorError> {
    let mut by_window: BTreeMap<i64, SummaryState> = BTreeMap::new();
    // Tracked from RAW window-ends, before the `w_end < t0_ms` carry-in
    // filter below -- see `SummaryValue`'s doc for why.
    let mut coverage: Option<(u64, u64)> = None;
    for entry in entries {
        let samples_vec: Vec<(i64, &SketchSampleState)> = entry
            .samples
            .iter()
            .flat_map(|(t, frames)| frames.iter().map(move |s| (*t, s)))
            .collect();
        for (w, _) in &samples_vec {
            fold_coverage(&mut coverage, *w);
        }
        let (per_window, _skipped) =
            per_window_summary_states(&samples_vec, kind).map_err(SummaryExecutorError::Decode)?;
        for (w_end, rs) in per_window {
            // `SketchStore::query_range` may splice in a carry-in Full
            // snapshot ending before the requested range so the
            // delta-apply walk can establish a rolling base for a
            // delta-only leading window; that base must not surface as
            // an output point.
            if w_end < t0_ms {
                continue;
            }
            match by_window.get_mut(&w_end) {
                Some(acc) => acc
                    .merge_same_family(&rs)
                    .map_err(SummaryExecutorError::Decode)?,
                None => {
                    by_window.insert(w_end, rs);
                }
            }
        }
    }
    if by_window.is_empty() {
        return Err(SummaryExecutorError::NoCandidates);
    }
    if let SketchQuery::TopK { k } = query {
        let points = by_window
            .into_iter()
            .map(|(w_end, rs)| topk_ranked(&rs, *k).map(|items| (w_end, items)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SummaryValue::TopK(points, coverage))
    } else {
        let points = by_window
            .into_iter()
            .map(|(w_end, rs)| sketch_query_value(&rs, query).map(|v| (w_end, v)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SummaryValue::Points(points, coverage))
    }
}

/// Read one scalar out of a merged `SummaryState` for the requested
/// `SketchQuery` -- shared by both the cumulative and per-window readout
/// paths.
fn sketch_query_value(rs: &SummaryState, query: &SketchQuery) -> Result<f64, SummaryExecutorError> {
    match query {
        SketchQuery::Quantile { q } => Ok(rs.quantile(*q)),
        SketchQuery::Cardinality => Ok(rs.cardinality()),
        // `key: ColumnRef::SampleValue, value: None` means "no specific
        // item" -- the bare bucket total. `key: Named(_), value: Some(v)`
        // is a per-item point lookup (e.g. `count(cms_metric{item="x"})`)
        // -- `value` is where the filter's actual value lives (see
        // `asap_sketch::SketchQuery::PointCount`'s doc for why `readout`
        // can't resolve it itself). Any other combination (e.g. a `Named`
        // key with no value, or `SampleValue` with a value) is a shape
        // this executor doesn't expect to see and reports rather than
        // silently misreading.
        SketchQuery::PointCount {
            key: ColumnRef::SampleValue,
            value: None,
        } => Ok(rs.total()),
        SketchQuery::PointCount {
            key: ColumnRef::Named(_) | ColumnRef::Qualified { .. },
            value: Some(v),
        } => rs.estimate(v).ok_or(SummaryExecutorError::Unsupported(
            "PointCount by key requires a Frequency-family sketch (Cms/CountSketch/..WithHeap)",
        )),
        SketchQuery::PointCount { .. } => Err(SummaryExecutorError::Unsupported(
            "unrecognized PointCount shape (key/value combination not expected)",
        )),
        // Both readout callers branch on `TopK` before ever calling this
        // function (see `readout_cumulative`/`readout_per_window`), so
        // this arm is unreachable in practice; kept for match
        // exhaustiveness (`SketchQuery` has no `#[non_exhaustive]`) and to
        // fail loudly rather than panic if that invariant is ever broken.
        SketchQuery::TopK { .. } => Err(SummaryExecutorError::Unsupported(
            "TopK must be read out via topk_ranked, not sketch_query_value",
        )),
    }
}

/// Rank a merged `SummaryState`'s top-k heap items descending by value and
/// cap at the requested `k`. The sort is load-bearing, not defensive
/// polish: `SummaryState::topk_items` reads back a bounded min-heap's
/// backing array as-is (`HHHeap::heap()`, asap_sketchlib) -- it does NOT
/// actually guarantee order despite its own doc wording. Errors for a
/// heap-less family (`Dd`/`Hll`/`Kll`/`Cms`/`CountSketch` -- no item
/// universe to rank), not for an empty heap (a heap-bearing family that
/// simply never received any updates yields `Ok(vec![])`, not an error).
fn topk_ranked(rs: &SummaryState, k: usize) -> Result<Vec<(String, f64)>, SummaryExecutorError> {
    let mut items = rs.topk_items().ok_or(SummaryExecutorError::Unsupported(
        "TopK requires a heap-bearing family (CmsWithHeap/CountSketchWithHeap) -- \
         this state's family carries no item universe to rank",
    ))?;
    items.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0)) // deterministic tie-break for equal counts
    });
    items.truncate(k);
    Ok(items)
}

/// Exact `(SummaryKind, SummaryParams)` match against a sid's own
/// `(SketchKindHandle, SketchConfig)` -- the check `find_candidates`'s
/// trait contract requires (not the looser family-only
/// `Capability::is_satisfied_by` check the legacy analyzer path uses),
/// so a `SummaryMerge`'s precondition (every child agrees on kind AND
/// params) is guaranteed by construction for anything routed through
/// this executor.
fn summary_params_match(
    sketch: &SummaryKind,
    params: &SummaryParams,
    kind: SketchKindHandle,
    config: &SketchConfig,
) -> bool {
    // `SummaryParams::{Cms,CmsWithHeap,CountSketch,CountSketchWithHeap}`
    // use width=cols/depth=rows (matches the control-plane wire
    // convention -- see `sketch_config_to_json`'s comment). `SketchConfig`
    // has no `heap_size` field at all (heap-bearing kinds reuse their
    // heap-less base's config shape for identity -- see
    // `base_sketch_kind_handle`'s doc in `drivers/ingest/otel.rs`), so
    // heap_size can't be part of this match; width/depth are.
    match (sketch, params, kind, config) {
        (
            SummaryKind::DDSketch,
            SummaryParams::DDSketch { alpha },
            SketchKindHandle::DDSketch,
            SketchConfig::DDSketch { relative_accuracy },
        ) => alpha == relative_accuracy,
        (
            SummaryKind::Kll,
            SummaryParams::Kll { k },
            SketchKindHandle::Kll,
            SketchConfig::Kll { k: sid_k },
        ) => k == sid_k,
        (
            SummaryKind::Hll,
            SummaryParams::Hll { precision },
            SketchKindHandle::Hll,
            SketchConfig::Hll { precision: sid_p },
        ) => u32::from(*precision) == *sid_p,
        (
            SummaryKind::Cms,
            SummaryParams::Cms { width, depth },
            SketchKindHandle::CountMin,
            SketchConfig::CountMin { rows, cols },
        ) => *depth as i32 == *rows && *width as i32 == *cols,
        (
            SummaryKind::CountSketch,
            SummaryParams::CountSketch { width, depth },
            SketchKindHandle::CountSketch,
            SketchConfig::CountSketch { rows, cols },
        ) => *depth as i32 == *rows && *width as i32 == *cols,
        (
            SummaryKind::CmsWithHeap,
            SummaryParams::CmsWithHeap { width, depth, .. },
            SketchKindHandle::CmsWithHeap,
            SketchConfig::CountMin { rows, cols },
        ) => *depth as i32 == *rows && *width as i32 == *cols,
        (
            SummaryKind::CountSketchWithHeap,
            SummaryParams::CountSketchWithHeap { width, depth, .. },
            SketchKindHandle::CountSketchWithHeap,
            SketchConfig::CountSketch { rows, cols },
        ) => *depth as i32 == *rows && *width as i32 == *cols,
        _ => false,
    }
}

/// Exact-agg analog of `summary_params_match`, for `AggKind::ExactAgg`
/// sids. `SummaryParams::{Sum, Count, MinMax, Increase, Rate}` are unit
/// variants (no tuning parameters — see `asap-sketch`'s `SummaryParams`
/// doc), so this is a pure kind-identity check against the sid's
/// `AggregationType`, mirroring the canonical `AggregationType ->
/// SummaryKind` mapping `asap_types::accumulator_spec` uses on the write
/// side (`Sum|MultipleSum -> SummaryKind::Sum`, `Increase|MultipleIncrease
/// -> SummaryKind::Increase` — confirmed against that module's own
/// dispatch table rather than invented here).
///
/// `SummaryKind::MinMax` is deliberately NOT matched: `AggregationType`
/// carries no min-vs-max DIRECTION (that lives in the write-side
/// `AggregationConfig::aggregation_sub_type` string, which this sid's
/// `AggKind::ExactAgg` metadata doesn't retain), so there's no honest way
/// for `GroupState::exact_value` to know which statistic to compute --
/// matching it here would force a later caller to silently guess a
/// direction. `SummaryKind::Count`/`Rate` are ALSO not matched: no
/// `AggregationType` variant resolves to either today (mirrors
/// `sketch_reducer.rs::evaluate_exact_agg`'s own `stat` mapping, which
/// only handles `Sum`/`Increase` for the same reason); `Rate` in
/// particular is "outer-agg-fold" territory the user has explicitly
/// deferred pending a design conversation with ASAPController.
fn exact_agg_kind_match(
    sketch: &SummaryKind,
    params: &SummaryParams,
    agg_type: AggregationType,
) -> bool {
    matches!(
        (sketch, params, agg_type),
        (
            SummaryKind::Sum,
            SummaryParams::Sum,
            AggregationType::Sum | AggregationType::MultipleSum,
        ) | (
            SummaryKind::Increase,
            SummaryParams::Increase,
            AggregationType::Increase | AggregationType::MultipleIncrease,
        )
    )
}

/// Project a full label-values map down to the requested `by` columns --
/// used by the `ExactAgg` branch of `find_candidates`. Missing keys
/// become empty strings so a sid registered with a subset of the
/// requested keys still groups deterministically (mirrors
/// `sketch_reducer.rs::evaluate_exact_agg`'s identical projection,
/// including its `by=[]` behavior: Sum/Increase are additive PromQL
/// aggregation operators, so an empty `by` legitimately means "reduce
/// fully" -- every matching sid collapses to ONE group and gets summed
/// together, which is the correct `sum(metric)`/`increase(metric[r])`
/// answer, not a bug to route around).
fn project_group_key(
    by_names: &[String],
    label_values: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    by_names
        .iter()
        .map(|k| (k.clone(), label_values.get(k).cloned().unwrap_or_default()))
        .collect()
}

/// Group-key construction for `find_candidates`, shared by both the
/// `Sketch` and `ExactAgg` branches -- driven directly by L3/L4's own
/// `Reduction` (ASAPController#163/#164/#165), not inferred from whether
/// `by` happens to be empty.
///
/// This replaces the old family-specific split (`sketch_group_key` vs.
/// `project_group_key` used bare): before `Reduction` existed on
/// `SummaryAgg`, an empty `by: Vec<ColumnId>` was genuinely ambiguous --
/// it could mean either "no explicit grouping was even resolvable" (a
/// bare per-series range function like `quantile_over_time(0.99,
/// http_latency_ms[10s])`, where L3/L4 planning has no reference to any
/// label column at all) or "a real cross-series reduction with zero
/// grouping columns" (`count(hll_metric)`, `sum(...)`-shaped). Those two
/// cases need OPPOSITE group-key behavior and the old `by: &[ColumnId]`
/// signature could not tell them apart -- `sketch_group_key`'s heuristic
/// (treat empty `by` as "keep every series distinct" for the Sketch
/// family only) fixed the first case but could not fix the second, since
/// by the time `find_candidates` saw a bare `[]`, the distinction was
/// already lost.
///
/// `Reduction` restores it directly:
/// - `PerEntity`: no grouping concept at all -- use the sid's own FULL
///   label map, matching the legacy `sketch_reducer.rs::evaluate_core`
///   path's behavior exactly (it passes `series_label_values` straight
///   through, unconditionally), so distinct series always stay distinct
///   rows. Applies uniformly to both families now (previously
///   `ExactAgg`'s `project_group_key` had no equivalent, since `Sum`/
///   `Increase`-shaped exact aggregations only ever reach an unqualified
///   PromQL aggregation operator, which is never `PerEntity`).
/// - `Reduce(by)`: a genuine reduction. Project onto `by_names` as
///   before -- when `by_names` is empty this naturally returns the SAME
///   `{}` key for every matching candidate, correctly merging them into
///   one group (the fix for the `count(hll_metric)`-style case the old
///   `by: &[ColumnId]` signature couldn't resolve). When non-empty, an
///   explicit grouping was resolvable from the query (e.g. `quantile by
///   (zone) (...)`), so project onto it as requested.
fn resolve_group_key(
    reduction: &Reduction,
    by_names: &[String],
    label_values: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    match reduction {
        Reduction::PerEntity => label_values.clone(),
        Reduction::Reduce(_) => project_group_key(by_names, label_values),
    }
}

/// `SketchConfig` (data_plane's per-sid stored params) -> `DeltaSketchKind`
/// (`delta_apply`'s decode/merge parameter carrier).
fn to_delta_kind(kind: SketchKindHandle, config: &SketchConfig) -> Option<DeltaSketchKind> {
    // Default heap_size when bootstrapping an empty Heap state for a
    // delta-from-empty leading window -- `SketchConfig` carries no
    // heap_size (see `summary_params_match`'s doc), so this only matters
    // transiently: `CountMinSketchWithHeap::merge` takes `min(self,
    // other)`, so it converges to the real decoded value as soon as any
    // actual frame merges in. Matches this codebase's existing
    // heap_size-absent default (`accuracy.rs`).
    const DEFAULT_HEAP_SIZE: usize = 100;
    match (kind, config) {
        (SketchKindHandle::DDSketch, SketchConfig::DDSketch { relative_accuracy }) => {
            Some(DeltaSketchKind::DDSketch {
                alpha: *relative_accuracy,
            })
        }
        (SketchKindHandle::Kll, SketchConfig::Kll { k }) => Some(DeltaSketchKind::Kll { k: *k }),
        (SketchKindHandle::Hll, SketchConfig::Hll { precision }) => Some(DeltaSketchKind::Hll {
            precision: *precision,
        }),
        (SketchKindHandle::CountMin, SketchConfig::CountMin { rows, cols }) => {
            Some(DeltaSketchKind::Cms {
                rows: *rows as usize,
                cols: *cols as usize,
            })
        }
        (SketchKindHandle::CountSketch, SketchConfig::CountSketch { rows, cols }) => {
            Some(DeltaSketchKind::CountSketch {
                rows: *rows as usize,
                cols: *cols as usize,
            })
        }
        (SketchKindHandle::CmsWithHeap, SketchConfig::CountMin { rows, cols }) => {
            Some(DeltaSketchKind::CmsWithHeap {
                rows: *rows as usize,
                cols: *cols as usize,
                heap_size: DEFAULT_HEAP_SIZE,
            })
        }
        (SketchKindHandle::CountSketchWithHeap, SketchConfig::CountSketch { rows, cols }) => {
            Some(DeltaSketchKind::CountSketchWithHeap {
                rows: *rows as usize,
                cols: *cols as usize,
                heap_size: DEFAULT_HEAP_SIZE,
            })
        }
        _ => None,
    }
}

/// Walk an `L4Node`'s `SummaryAgg`/`SummaryEstimate`/`SummaryMerge`
/// spine down to a `Logical` leaf, then walk that leaf's `QueryExpr`
/// down to a `Scan { source: Source::TimeSeries { metric }, .. }` to
/// recover the metric name -- `SummaryAgg` itself carries no
/// metric/source field (see `SummaryExecutor::find_candidates`'s trait
/// doc). Mirrors `control_plane::asap_tier_implement::collect_aggregate_roots`'s
/// exhaustive-variant recursion style (same `QueryExpr` type), swapping
/// "collect Aggregate roots" for "find the first Scan".
fn find_metric(node: &L4Node) -> Option<String> {
    match &node.expr {
        SummaryExpr::Logical(qe) => find_metric_in_query_expr(qe),
        SummaryExpr::SummaryAgg { child, .. } => find_metric(child),
        SummaryExpr::SummaryEstimate { summary_input, .. } => find_metric(summary_input),
        SummaryExpr::SummaryMerge { children } => children.first().and_then(|c| find_metric(c)),
        _ => None,
    }
}

/// Walk a canonical `QueryExpr` down to its first `Scan {
/// source: Source::TimeSeries { metric }, .. }` to recover the target
/// metric name. Shared with `l4_lowering.rs`'s observed-family lookup
/// (serving time must know which metric to check the `SketchStore`
/// against BEFORE binding — see that module's docs).
pub(crate) fn find_metric_in_query_expr(qe: &QueryExpr) -> Option<String> {
    match qe {
        QueryExpr::Scan {
            source: Source::TimeSeries { metric },
            ..
        } => Some(metric.clone()),
        QueryExpr::Window { child, .. }
        | QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Aggregate { child, .. }
        | QueryExpr::Distinct { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::Subquery { child, .. }
        | QueryExpr::TimeRange { child, .. }
        | QueryExpr::TimeShift { child, .. }
        | QueryExpr::WindowFunc { child, .. } => find_metric_in_query_expr(child),
        QueryExpr::LetBinding { expr, child, .. } => {
            find_metric_in_query_expr(expr).or_else(|| find_metric_in_query_expr(child))
        }
        QueryExpr::Merge { children } => children.iter().find_map(find_metric_in_query_expr),
        QueryExpr::Join { left, .. } | QueryExpr::SetOp { left, .. } => {
            find_metric_in_query_expr(left)
        }
        QueryExpr::BinaryOp { lhs, .. } => find_metric_in_query_expr(lhs),
        // The PromQL-surface superset (Scan's siblings: Ref/Scalar/
        // EvalTime/VectorFromScalar/ScalarFromVector/Relabel/InfoJoin/
        // Sample) isn't constructed by this parser today -- mirrors
        // `collect_aggregate_roots`'s same no-op default.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchInstanceMetadata, SketchSampleState, SketchStore,
    };
    use asap_ir::intent_algebra::{Column, DataType, Schema};
    use asap_sketch::exec::{execute, ExecOutcome};
    use asap_sketch::schema::{L4DataType, L4Field, L4Schema};
    use std::rc::Rc;

    fn scan_node(metric: &str, group_by_field: Option<&str>) -> Rc<L4Node> {
        let qe = QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: metric.to_string(),
            },
            predicates: vec![],
            schema: Schema::with_time_index(
                vec![
                    Column::new("ts", DataType::Timestamp, false),
                    Column::new("value", DataType::Float64, false),
                ],
                0,
                vec![],
            ),
        };
        let mut fields = vec![L4Field {
            name: "value".into(),
            dtype: L4DataType::Primitive(DataType::Float64),
            nullable: false,
        }];
        if let Some(name) = group_by_field {
            fields.push(L4Field {
                name: name.into(),
                dtype: L4DataType::Primitive(DataType::Utf8),
                nullable: false,
            });
        }
        Rc::new(L4Node {
            expr: SummaryExpr::Logical(Box::new(qe)),
            schema: L4Schema {
                fields,
                time_index: None,
            },
        })
    }

    fn kll_agg_node(child: Rc<L4Node>, reduction: Reduction) -> Rc<L4Node> {
        Rc::new(L4Node {
            expr: SummaryExpr::SummaryAgg {
                child,
                summary: SummaryKind::Kll,
                params: SummaryParams::Kll { k: 200 },
                col: ColumnRef::SampleValue,
                reduction,
            },
            schema: L4Schema {
                fields: vec![],
                time_index: None,
            },
        })
    }

    fn hll_agg_node(child: Rc<L4Node>) -> Rc<L4Node> {
        hll_agg_node_with(child, Reduction::by(vec![]))
    }

    fn hll_agg_node_with(child: Rc<L4Node>, reduction: Reduction) -> Rc<L4Node> {
        Rc::new(L4Node {
            expr: SummaryExpr::SummaryAgg {
                child,
                summary: SummaryKind::Hll,
                params: SummaryParams::Hll { precision: 10 },
                col: ColumnRef::SampleValue,
                reduction,
            },
            schema: L4Schema {
                fields: vec![],
                time_index: None,
            },
        })
    }

    fn estimate_node(summary_input: Rc<L4Node>, query: SketchQuery) -> Rc<L4Node> {
        Rc::new(L4Node {
            expr: SummaryExpr::SummaryEstimate {
                summary_input,
                query,
            },
            schema: L4Schema {
                fields: vec![],
                time_index: None,
            },
        })
    }

    fn kll_meta(sid: u64, metric: &str, group_by: &[&str]) -> SketchInstanceMetadata {
        let cfg = SketchConfig::Kll { k: 200 };
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: group_by
                .iter()
                .map(|s| s.to_string())
                .collect::<BTreeSet<_>>(),
            capability: Some(Capability::QuantileApprox(SketchKindHandle::Kll)),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                kind: SketchKindHandle::Kll,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    fn hll_meta(sid: u64, metric: &str) -> SketchInstanceMetadata {
        let cfg = SketchConfig::Hll { precision: 10 };
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::CardinalityApprox),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                kind: SketchKindHandle::Hll,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    fn encode_kll_items_proto(k: u16, items: &[f64]) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
        use prost::Message;
        let state = KllState {
            k: k as u32,
            items: items.to_vec(),
            levels: vec![],
            num_levels: 0,
            ..Default::default()
        };
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Kll(state)),
            ..Default::default()
        };
        env.encode_to_vec()
    }

    fn encode_hll_from_items(precision: u32, items: &[&str]) -> Vec<u8> {
        use asap_sketchlib::{HllSketch, HllVariant, MessagePackCodec};
        let mut sk = HllSketch::new(HllVariant::Regular, precision);
        for item in items {
            sk.update(item.as_bytes());
        }
        sk.to_msgpack().expect("encode HLL msgpack")
    }

    fn cms_meta(sid: u64, metric: &str) -> SketchInstanceMetadata {
        let cfg = SketchConfig::CountMin { rows: 4, cols: 256 };
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::FrequencyEstimate(SketchKindHandle::CountMin)),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                kind: SketchKindHandle::CountMin,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    /// Encode a CMS msgpack frame whose row-0 total is `total_weight`
    /// (`update`s a single synthetic key `total_weight` times -- row 0's
    /// sum equals the number of insertions regardless of hashing, since
    /// every insertion touches every row including row 0).
    fn encode_cms_with_total(rows: usize, cols: usize, total_weight: usize) -> Vec<u8> {
        use asap_sketchlib::{CountMinSketch, MessagePackCodec};
        let mut sk = CountMinSketch::new(rows, cols);
        for _ in 0..total_weight {
            sk.update("k", 1.0);
        }
        sk.to_msgpack().expect("encode CountMinSketch msgpack")
    }

    /// Encode a CMS msgpack frame with one `update` of `weight` for a
    /// single named `key` -- unlike `encode_cms_with_total`'s synthetic
    /// "k", this lets a test control which key a `PointCount` query looks
    /// up.
    fn encode_cms_with_item(rows: usize, cols: usize, key: &str, weight: f64) -> Vec<u8> {
        use asap_sketchlib::{CountMinSketch, MessagePackCodec};
        let mut sk = CountMinSketch::new(rows, cols);
        sk.update(key, weight);
        sk.to_msgpack().expect("encode CountMinSketch msgpack")
    }

    fn cms_agg_node(child: Rc<L4Node>) -> Rc<L4Node> {
        Rc::new(L4Node {
            expr: SummaryExpr::SummaryAgg {
                child,
                summary: SummaryKind::Cms,
                params: SummaryParams::Cms {
                    width: 256,
                    depth: 4,
                },
                col: ColumnRef::SampleValue,
                reduction: Reduction::by(vec![]),
            },
            schema: L4Schema {
                fields: vec![],
                time_index: None,
            },
        })
    }

    fn cms_with_heap_meta(sid: u64, metric: &str) -> SketchInstanceMetadata {
        let cfg = SketchConfig::CountMin { rows: 4, cols: 256 };
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap)),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                kind: SketchKindHandle::CmsWithHeap,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    /// Encode a `CmsWithHeap` msgpack frame with one `update` per
    /// `(key, weight)` pair.
    fn encode_cms_with_heap_items(
        rows: usize,
        cols: usize,
        heap_size: usize,
        items: &[(&str, f64)],
    ) -> Vec<u8> {
        use asap_sketchlib::{CountMinSketchWithHeap, MessagePackCodec};
        let mut sk = CountMinSketchWithHeap::new(rows, cols, heap_size);
        for (key, weight) in items {
            sk.update(key, *weight);
        }
        sk.to_msgpack()
            .expect("encode CountMinSketchWithHeap msgpack")
    }

    fn cms_with_heap_agg_node(child: Rc<L4Node>) -> Rc<L4Node> {
        Rc::new(L4Node {
            expr: SummaryExpr::SummaryAgg {
                child,
                summary: SummaryKind::CmsWithHeap,
                params: SummaryParams::CmsWithHeap {
                    width: 256,
                    depth: 4,
                    heap_size: 10,
                },
                col: ColumnRef::SampleValue,
                reduction: Reduction::by(vec![]),
            },
            schema: L4Schema {
                fields: vec![],
                time_index: None,
            },
        })
    }

    // ── ExactAgg (Sum) fixtures ────────────────────────────────────────

    fn sum_exact_agg_meta(sid: u64, metric: &str, group_by: &[&str]) -> SketchInstanceMetadata {
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: group_by
                .iter()
                .map(|s| s.to_string())
                .collect::<BTreeSet<_>>(),
            capability: Some(Capability::ExactAgg(asap_types::AggregationType::Sum)),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::ExactAgg {
                agg_type: asap_types::AggregationType::Sum,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: None,
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    fn sum_agg_node(child: Rc<L4Node>, by: Vec<ColumnId>) -> Rc<L4Node> {
        Rc::new(L4Node {
            expr: SummaryExpr::SummaryAgg {
                child,
                summary: SummaryKind::Sum,
                params: SummaryParams::Sum,
                col: ColumnRef::SampleValue,
                // Sum is a genuine PromQL aggregation operator -- an empty
                // `by` always means "reduce fully," never `PerEntity` (see
                // `resolve_group_key`'s doc).
                reduction: Reduction::by(by),
            },
            schema: L4Schema {
                fields: vec![],
                time_index: None,
            },
        })
    }

    const T0: u64 = 1_000_000;
    const T1: u64 = 2_000_000;

    fn ctx(index: &SketchStore) -> QueryExecutionContext<'_> {
        QueryExecutionContext {
            index,
            t0_ms: T0,
            t1_ms: T1,
            is_cumulative: true,
        }
    }

    fn matrix_ctx(index: &SketchStore) -> QueryExecutionContext<'_> {
        QueryExecutionContext {
            index,
            t0_ms: T0,
            t1_ms: T1,
            is_cumulative: false,
        }
    }

    #[test]
    fn single_kll_sid_quantile_readout() {
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(kll_meta(sid, "latency_ms", &[]));
        let items: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        let child = scan_node("latency_ms", None);
        let tree = estimate_node(
            kll_agg_node(child, Reduction::by(vec![])),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        assert_eq!(v.len(), 1, "ungrouped query produces exactly one group");
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples, coverage) = value else {
            panic!("expected Points, got {value:?}");
        };
        assert_eq!(
            *coverage,
            Some((T0 + 1000, T0 + 1000)),
            "cumulative readout's coverage must reflect the single window observed"
        );
        let (_ts, median) = samples[0];
        // Median of 1..=100 is ~50.
        assert!(
            (45.0..=55.0).contains(&median),
            "median {median} out of range"
        );
    }

    #[test]
    fn two_sids_same_group_actually_merge_not_just_first() {
        // Two sids covering the SAME group must be MERGED into one
        // combined answer, not silently duplicated or one-of-them-dropped.
        let idx = SketchStore::new();
        idx.register(kll_meta(1, "latency_ms", &[]));
        idx.register(kll_meta(2, "latency_ms", &[]));
        // sid 1: values 1..=50 (median ~25); sid 2: values 51..=100 (median ~75).
        // Merged, the combined median should be ~50 -- NOT ~25 (if merge
        // silently dropped sid 2) and NOT ~75 (if it dropped sid 1).
        let items1: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        let items2: Vec<f64> = (51..=100).map(|i| i as f64).collect();
        idx.append_sample(
            1,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items1),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        idx.append_sample(
            2,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items2),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        let child = scan_node("latency_ms", None);
        let tree = estimate_node(
            kll_agg_node(child, Reduction::by(vec![])),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        assert_eq!(v.len(), 1);
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples, _coverage) = value else {
            panic!("expected Points, got {value:?}");
        };
        let (_ts, median) = samples[0];
        assert!(
            (40.0..=60.0).contains(&median),
            "merged median {median} should be ~50 (both sids' data combined), \
             not ~25 or ~75 (one sid dropped)"
        );
    }

    #[test]
    fn two_sids_different_groups_produce_two_series_not_one_merged_blob() {
        // `quantile by (zone) (...)` must produce one output series per
        // zone, not one series merging both zones together.
        let idx = SketchStore::new();
        idx.register(kll_meta(1, "latency_ms", &["zone"]));
        idx.register(kll_meta(2, "latency_ms", &["zone"]));
        let items1: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        let items2: Vec<f64> = (51..=100).map(|i| i as f64).collect();
        let mut labels_east = BTreeMap::new();
        labels_east.insert("zone".to_string(), "us-east".to_string());
        let mut labels_west = BTreeMap::new();
        labels_west.insert("zone".to_string(), "us-west".to_string());
        idx.append_sample(
            1,
            labels_east,
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items1),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        idx.append_sample(
            2,
            labels_west,
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items2),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        let child = scan_node("latency_ms", Some("zone"));
        // "zone" is field index 1 in `scan_node`'s schema (0 = value).
        let tree = estimate_node(
            kll_agg_node(child, Reduction::by(vec![1])),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(mut v) = execute(&tree, &exec).expect("execute should succeed")
        else {
            panic!("expected a value");
        };
        assert_eq!(
            v.len(),
            2,
            "two zones must produce two output series, not one merged blob"
        );
        v.sort_by(|a, b| a.0.get("zone").cmp(&b.0.get("zone")));
        let (east_group, east_value) = &v[0];
        let (west_group, west_value) = &v[1];
        assert_eq!(east_group.get("zone").map(String::as_str), Some("us-east"));
        assert_eq!(west_group.get("zone").map(String::as_str), Some("us-west"));
        let SummaryValue::Points(east_samples, _coverage) = east_value else {
            panic!("expected Points, got {east_value:?}");
        };
        let SummaryValue::Points(west_samples, _coverage) = west_value else {
            panic!("expected Points, got {west_value:?}");
        };
        let east_median = east_samples[0].1;
        let west_median = west_samples[0].1;
        assert!(
            (20.0..=30.0).contains(&east_median),
            "us-east median {east_median} should reflect only sid 1's data (~25)"
        );
        assert!(
            (70.0..=80.0).contains(&west_median),
            "us-west median {west_median} should reflect only sid 2's data (~75)"
        );
    }

    #[test]
    fn bare_per_series_query_keeps_distinct_series_separate_even_with_no_by() {
        // The `Reduction::PerEntity` half of the empty-`by` ambiguity
        // (ASAPController#163/#164/#165), confirmed against a real e2e
        // shadow-mode run for `quantile_over_time(m[r])` (a bare per-series
        // range function -- no PromQL `by(...)`, no label selector, so
        // there's no grouping concept for the query to express at all,
        // NOT a request to merge everything). Two sids with DIFFERENT real
        // labels ("zone") under a `PerEntity` query must still produce TWO
        // separate output series -- naively projecting onto an empty `by`
        // would collapse both sids' group keys to the SAME `{}` and
        // silently merge two unrelated distributions into one wrong
        // answer. See `genuine_full_reduction_merges_distinct_series_unlike_per_entity`
        // for the opposite (`Reduce([])`) case, which MUST merge.
        let idx = SketchStore::new();
        idx.register(kll_meta(1, "latency_ms", &["zone"]));
        idx.register(kll_meta(2, "latency_ms", &["zone"]));
        let items1: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        let items2: Vec<f64> = (51..=100).map(|i| i as f64).collect();
        let mut labels_east = BTreeMap::new();
        labels_east.insert("zone".to_string(), "us-east".to_string());
        let mut labels_west = BTreeMap::new();
        labels_west.insert("zone".to_string(), "us-west".to_string());
        idx.append_sample(
            1,
            labels_east,
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items1),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        idx.append_sample(
            2,
            labels_west,
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items2),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        // No `by` requested at all -- mirrors `quantile_over_time(0.99,
        // latency_ms[r])` with no `by(...)`/label selector.
        let child = scan_node("latency_ms", Some("zone"));
        let tree = estimate_node(
            kll_agg_node(child, Reduction::PerEntity),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(mut v) = execute(&tree, &exec).expect("execute should succeed")
        else {
            panic!("expected a value");
        };
        assert_eq!(
            v.len(),
            2,
            "two distinct series must stay separate even with no explicit `by` -- \
             got {v:?}"
        );
        v.sort_by(|a, b| a.0.get("zone").cmp(&b.0.get("zone")));
        let (east_group, east_value) = &v[0];
        let (west_group, west_value) = &v[1];
        assert_eq!(east_group.get("zone").map(String::as_str), Some("us-east"));
        assert_eq!(west_group.get("zone").map(String::as_str), Some("us-west"));
        let SummaryValue::Points(east_samples, _coverage) = east_value else {
            panic!("expected Points, got {east_value:?}");
        };
        let SummaryValue::Points(west_samples, _coverage) = west_value else {
            panic!("expected Points, got {west_value:?}");
        };
        assert!(
            (20.0..=30.0).contains(&east_samples[0].1),
            "us-east median {} should reflect only sid 1's data (~25), not a merge with sid 2",
            east_samples[0].1
        );
        assert!(
            (70.0..=80.0).contains(&west_samples[0].1),
            "us-west median {} should reflect only sid 2's data (~75), not a merge with sid 1",
            west_samples[0].1
        );
    }

    #[test]
    fn hll_cardinality_readout() {
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(hll_meta(sid, "unique_users"));
        let items: Vec<&str> = vec!["a", "b", "c", "d", "e"];
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_hll_from_items(10, &items),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );

        let child = scan_node("unique_users", None);
        let tree = estimate_node(hll_agg_node(child), SketchQuery::Cardinality);

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples, _coverage) = value else {
            panic!("expected Points, got {value:?}");
        };
        let (_ts, card) = samples[0];
        assert!(
            (3.0..=7.0).contains(&card),
            "cardinality {card} should be ~5"
        );
    }

    #[test]
    fn genuine_full_reduction_merges_distinct_series_unlike_per_entity() {
        // The other half of the empty-`by` ambiguity `Reduction` resolves
        // (ASAPController#163/#164/#165): a bare per-series range function
        // like `quantile_over_time(m[r])` (Reduction::PerEntity, see
        // `bare_per_series_query_keeps_distinct_series_separate_even_with_no_by`
        // above) must NOT merge distinct series, but a genuine
        // cross-series reduction with zero grouping columns --
        // `count(hll_metric)`-shaped, Reduction::Reduce(GroupKeys::by([]))
        // -- legitimately MUST merge them into one combined answer. Before
        // `find_candidates` took `&Reduction` instead of `&[ColumnId]`,
        // these two cases were indistinguishable from an empty `by` alone
        // (the old `sketch_group_key` heuristic could only ever pick ONE
        // of the two behaviors for every empty-`by` query).
        //
        // Two HLL sids with DIFFERENT real "zone" labels (same shape as
        // the PerEntity test above) and DISJOINT item sets: under a
        // genuine `Reduce([])`, they must merge into ONE group whose
        // cardinality reflects BOTH sids' items combined (~10), not two
        // separate ~5-item answers.
        let idx = SketchStore::new();
        idx.register(hll_meta(1, "unique_users"));
        idx.register(hll_meta(2, "unique_users"));
        let items1: Vec<&str> = vec!["a", "b", "c", "d", "e"];
        let items2: Vec<&str> = vec!["f", "g", "h", "i", "j"];
        let mut labels_east = BTreeMap::new();
        labels_east.insert("zone".to_string(), "us-east".to_string());
        let mut labels_west = BTreeMap::new();
        labels_west.insert("zone".to_string(), "us-west".to_string());
        idx.append_sample(
            1,
            labels_east,
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_hll_from_items(10, &items1),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
        idx.append_sample(
            2,
            labels_west,
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_hll_from_items(10, &items2),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );

        let child = scan_node("unique_users", Some("zone"));
        let tree = estimate_node(
            hll_agg_node_with(child, Reduction::by(vec![])),
            SketchQuery::Cardinality,
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        assert_eq!(
            v.len(),
            1,
            "a genuine full reduction must merge both sids into one group, got {v:?}"
        );
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples, _coverage) = value else {
            panic!("expected Points, got {value:?}");
        };
        let (_ts, card) = samples[0];
        assert!(
            (8.0..=12.0).contains(&card),
            "merged cardinality {card} should be ~10 (both sids' disjoint item sets combined), \
             not ~5 (one sid dropped or kept separate)"
        );
    }

    #[test]
    fn single_cms_sid_total_readout() {
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(cms_meta(sid, "requests_total"));
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_cms_with_total(4, 256, 42),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );

        let child = scan_node("requests_total", None);
        let tree = estimate_node(
            cms_agg_node(child),
            SketchQuery::PointCount {
                key: ColumnRef::SampleValue,
                value: None,
            },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples, _coverage) = value else {
            panic!("expected Points, got {value:?}");
        };
        let (_ts, total) = samples[0];
        assert_eq!(
            total, 42.0,
            "bare total must equal the number of insertions"
        );
    }

    #[test]
    fn two_cms_sids_same_group_totals_actually_merge() {
        // Cross-sid merge for the Frequency family: two sids' totals must
        // ADD (matrix merge then row-0 sum), not just report one of them.
        let idx = SketchStore::new();
        idx.register(cms_meta(1, "requests_total"));
        idx.register(cms_meta(2, "requests_total"));
        idx.append_sample(
            1,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_cms_with_total(4, 256, 30),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
        idx.append_sample(
            2,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_cms_with_total(4, 256, 12),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );

        let child = scan_node("requests_total", None);
        let tree = estimate_node(
            cms_agg_node(child),
            SketchQuery::PointCount {
                key: ColumnRef::SampleValue,
                value: None,
            },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples, _coverage) = value else {
            panic!("expected Points, got {value:?}");
        };
        let (_ts, total) = samples[0];
        assert_eq!(
            total, 42.0,
            "merged total must be the SUM of both sids (30 + 12)"
        );
    }

    #[test]
    fn single_cms_sid_named_key_point_estimate() {
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(cms_meta(sid, "requests_by_route"));
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_cms_with_item(4, 256, "checkout", 17.0),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );

        let child = scan_node("requests_by_route", None);
        let tree = estimate_node(
            cms_agg_node(child),
            SketchQuery::PointCount {
                key: ColumnRef::Named("item".to_string()),
                value: Some("checkout".to_string()),
            },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples, _coverage) = value else {
            panic!("expected Points, got {value:?}");
        };
        let (_ts, estimate) = samples[0];
        assert_eq!(
            estimate, 17.0,
            "named-key point estimate must equal that key's own insertions"
        );
    }

    #[test]
    fn two_cms_sids_same_group_named_key_estimate_merges_cross_sid() {
        // Cross-sid merge for a NAMED-KEY point lookup: the same key's
        // weight contributed by two different sids must ADD (matrix merge
        // then keyed estimate), not just report one sid's contribution --
        // the point-lookup analog of `two_cms_sids_same_group_totals_actually_merge`.
        let idx = SketchStore::new();
        idx.register(cms_meta(1, "requests_by_route"));
        idx.register(cms_meta(2, "requests_by_route"));
        idx.append_sample(
            1,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_cms_with_item(4, 256, "checkout", 30.0),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
        idx.append_sample(
            2,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_cms_with_item(4, 256, "checkout", 12.0),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );

        let child = scan_node("requests_by_route", None);
        let tree = estimate_node(
            cms_agg_node(child),
            SketchQuery::PointCount {
                key: ColumnRef::Named("item".to_string()),
                value: Some("checkout".to_string()),
            },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples, _coverage) = value else {
            panic!("expected Points, got {value:?}");
        };
        let (_ts, estimate) = samples[0];
        assert_eq!(
            estimate, 42.0,
            "merged named-key estimate must be the SUM of both sids (30 + 12)"
        );
    }

    #[test]
    fn topk_query_against_non_heap_sketch_is_unsupported() {
        // A heap-less family (Cms/CountSketch/Dd/Hll/Kll) carries no item
        // universe to rank -- TopK must still error for it. This is a
        // FAMILY limitation (see `topk_ranked`), not "TopK is
        // unimplemented" -- contrast with the heap-bearing TopK tests
        // below, which succeed.
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(cms_meta(sid, "requests_total"));
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_cms_with_total(4, 256, 1),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
        let child = scan_node("requests_total", None);
        let tree = estimate_node(cms_agg_node(child), SketchQuery::TopK { k: 5 });
        let exec = ctx(&idx);
        match execute(&tree, &exec) {
            Err(asap_sketch::exec::ExecError::Executor(SummaryExecutorError::Unsupported(_))) => {}
            other => panic!("expected Unsupported, got {}", other.is_ok()),
        }
    }

    #[test]
    fn single_cms_with_heap_sid_topk_readout_sorted_and_capped() {
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(cms_with_heap_meta(sid, "requests_by_route"));
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_cms_with_heap_items(
                    4,
                    256,
                    10,
                    &[
                        ("a", 10.0),
                        ("b", 20.0),
                        ("c", 30.0),
                        ("d", 40.0),
                        ("e", 50.0),
                    ],
                ),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );

        let child = scan_node("requests_by_route", None);
        let tree = estimate_node(cms_with_heap_agg_node(child), SketchQuery::TopK { k: 3 });

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = &v[0];
        let SummaryValue::TopK(points, _coverage) = value else {
            panic!("expected TopK, got {value:?}");
        };
        assert_eq!(points.len(), 1, "cumulative query produces one point");
        let (_ts, items) = &points[0];
        assert_eq!(items.len(), 3, "must be capped at k=3");
        let keys: Vec<&str> = items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            vec!["e", "d", "c"],
            "must be sorted descending by value (50, 40, 30), not heap-insertion order"
        );
    }

    #[test]
    fn two_cms_with_heap_sids_same_group_topk_merges_cross_sid() {
        // Two sids in the same group with DISJOINT key sets -- the merged
        // top-k must contain BOTH sids' keys, proving the readout actually
        // merges cross-sid (via `merge_same_family`/`asap_sketchlib`'s heap
        // reconciliation) rather than just reading out one sid's heap.
        let idx = SketchStore::new();
        idx.register(cms_with_heap_meta(1, "requests_by_route"));
        idx.register(cms_with_heap_meta(2, "requests_by_route"));
        idx.append_sample(
            1,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_cms_with_heap_items(4, 256, 10, &[("a", 30.0)]),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
        idx.append_sample(
            2,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_cms_with_heap_items(4, 256, 10, &[("b", 40.0)]),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );

        let child = scan_node("requests_by_route", None);
        let tree = estimate_node(cms_with_heap_agg_node(child), SketchQuery::TopK { k: 5 });

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = &v[0];
        let SummaryValue::TopK(points, _coverage) = value else {
            panic!("expected TopK, got {value:?}");
        };
        let (_ts, items) = &points[0];
        let keys: std::collections::BTreeSet<&str> =
            items.iter().map(|(k, _)| k.as_str()).collect();
        assert!(
            keys.contains("a") && keys.contains("b"),
            "merged top-k must contain both sids' keys, got {items:?}"
        );
    }

    #[test]
    fn per_window_matrix_topk_produces_per_window_ranked_lists() {
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(cms_with_heap_meta(sid, "requests_by_route"));
        let w1_end = T0 + 100_000;
        let w2_end = T0 + 200_000;
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0, w1_end),
            SketchSampleState {
                bytes: encode_cms_with_heap_items(4, 256, 10, &[("x", 100.0)]),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (w1_end, w2_end),
            SketchSampleState {
                bytes: encode_cms_with_heap_items(4, 256, 10, &[("y", 200.0)]),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );

        let child = scan_node("requests_by_route", None);
        let tree = estimate_node(cms_with_heap_agg_node(child), SketchQuery::TopK { k: 2 });

        let exec = matrix_ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = &v[0];
        let SummaryValue::TopK(mut points, _coverage) = value.clone() else {
            panic!("expected TopK, got {value:?}");
        };
        points.sort_by_key(|(ts, _)| *ts);
        assert_eq!(
            points.len(),
            2,
            "two distinct windows must produce two independently-ranked points"
        );
        assert_eq!(points[0].0, w1_end as i64);
        assert_eq!(points[1].0, w2_end as i64);
        assert_eq!(points[0].1[0].0, "x", "window 1's top key is x");
        assert_eq!(points[1].1[0].0, "y", "window 2's top key is y");
    }

    #[test]
    fn no_matching_sid_is_no_candidates() {
        let idx = SketchStore::new();
        let child = scan_node("nonexistent_metric", None);
        let tree = estimate_node(
            kll_agg_node(child, Reduction::by(vec![])),
            SketchQuery::Quantile { q: 0.5 },
        );
        let exec = ctx(&idx);
        match execute(&tree, &exec) {
            Err(asap_sketch::exec::ExecError::NoCandidates) => {}
            other => panic!("expected NoCandidates, got {}", other.is_ok()),
        }
    }

    #[test]
    fn mismatched_params_does_not_match() {
        // sid is Kll{k: 200}; query wants Kll{k: 500} -- must NOT match,
        // even though both are "Kll".
        let idx = SketchStore::new();
        idx.register(kll_meta(1, "latency_ms", &[]));
        idx.append_sample(
            1,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &[1.0, 2.0, 3.0]),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        let child = scan_node("latency_ms", None);
        let mismatched = Rc::new(L4Node {
            expr: SummaryExpr::SummaryAgg {
                child,
                summary: SummaryKind::Kll,
                params: SummaryParams::Kll { k: 500 },
                col: ColumnRef::SampleValue,
                reduction: Reduction::by(vec![]),
            },
            schema: L4Schema {
                fields: vec![],
                time_index: None,
            },
        });
        let tree = estimate_node(mismatched, SketchQuery::Quantile { q: 0.5 });
        let exec = ctx(&idx);
        match execute(&tree, &exec) {
            Err(asap_sketch::exec::ExecError::NoCandidates) => {}
            other => panic!(
                "expected NoCandidates (param mismatch), got {}",
                other.is_ok()
            ),
        }
    }

    #[test]
    fn per_window_matrix_produces_multiple_points_for_one_sid() {
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(kll_meta(sid, "latency_ms", &[]));
        let w1_end = T0 + 100_000;
        let w2_end = T0 + 200_000;
        let items_w1: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        let items_w2: Vec<f64> = (901..=1000).map(|i| i as f64).collect();
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0, w1_end),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items_w1),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (w1_end, w2_end),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items_w2),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        let child = scan_node("latency_ms", None);
        let tree = estimate_node(
            kll_agg_node(child, Reduction::by(vec![])),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = matrix_ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        assert_eq!(v.len(), 1, "ungrouped query produces exactly one group");
        let (_group, value) = v.into_iter().next().unwrap();
        let SummaryValue::Points(mut samples, coverage) = value else {
            panic!("expected Points");
        };
        assert_eq!(
            coverage,
            Some((w1_end, w2_end)),
            "fully-covered group's coverage must bracket every window-end observed"
        );
        samples.sort_by_key(|(ts, _)| *ts);
        assert_eq!(
            samples.len(),
            2,
            "two distinct windows must produce two output points, not one collapsed answer"
        );
        assert_eq!(samples[0].0, w1_end as i64);
        assert_eq!(samples[1].0, w2_end as i64);
        assert!(
            (20.0..=30.0).contains(&samples[0].1),
            "window 1 median {} should be ~25 (items 1..=50)",
            samples[0].1
        );
        assert!(
            (940.0..=960.0).contains(&samples[1].1),
            "window 2 median {} should be ~950 (items 901..=1000)",
            samples[1].1
        );
    }

    #[test]
    fn per_window_matrix_merges_across_sids_per_window() {
        // Two sids in the SAME group, both contributing a frame to the
        // SAME window_end -- the per-window answer for that window must
        // reflect BOTH sids merged, not just one (the cross-sid analog of
        // `two_sids_same_group_actually_merge_not_just_first`, but for
        // one window instead of the whole cumulative range).
        let idx = SketchStore::new();
        idx.register(kll_meta(1, "latency_ms", &[]));
        idx.register(kll_meta(2, "latency_ms", &[]));
        let w_end = T0 + 100_000;
        let items1: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        let items2: Vec<f64> = (51..=100).map(|i| i as f64).collect();
        idx.append_sample(
            1,
            BTreeMap::new(),
            (T0, w_end),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items1),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        idx.append_sample(
            2,
            BTreeMap::new(),
            (T0, w_end),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items2),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        let child = scan_node("latency_ms", None);
        let tree = estimate_node(
            kll_agg_node(child, Reduction::by(vec![])),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = matrix_ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = v.into_iter().next().unwrap();
        let SummaryValue::Points(samples, _coverage) = value else {
            panic!("expected Points");
        };
        assert_eq!(
            samples.len(),
            1,
            "both sids share one window_end -> one output point"
        );
        let (ts, median) = samples[0];
        assert_eq!(ts, w_end as i64);
        assert!(
            (40.0..=60.0).contains(&median),
            "merged per-window median {median} should be ~50 (both sids' data combined)"
        );
    }

    #[test]
    fn per_window_matrix_drops_carry_in_base_before_t0() {
        // `SketchStore::query_range` may splice in a carry-in Full ending
        // BEFORE `t0_ms` to seed the delta-apply walk. That base must not
        // surface as an output point.
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(kll_meta(sid, "latency_ms", &[]));
        let carry_in_end = T0 - 50_000; // before t0
        let in_range_end = T0 + 100_000;
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0 - 100_000, carry_in_end),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &[1.0, 2.0, 3.0]),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (carry_in_end, in_range_end),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &[10.0, 20.0, 30.0]),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        let child = scan_node("latency_ms", None);
        let tree = estimate_node(
            kll_agg_node(child, Reduction::by(vec![])),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = matrix_ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = v.into_iter().next().unwrap();
        let SummaryValue::Points(samples, _coverage) = value else {
            panic!("expected Points");
        };
        assert_eq!(
            samples.len(),
            1,
            "the carry-in-base window (ending before t0) must not appear in output"
        );
        assert_eq!(samples[0].0, in_range_end as i64);
    }

    #[test]
    fn per_window_matrix_coverage_includes_carry_in_base_before_t0() {
        // A GENUINE carry-in splice (unlike the test above, whose earlier
        // window is Full-encoded and simply falls outside `query_range`'s
        // overlap scan entirely): `SketchStore::query_range` splices in the
        // most-recent Full snapshot ending before `t0_ms` when the
        // earliest IN-WINDOW frame is a Delta, so the delta-apply walk has
        // a rolling base to apply onto (see `query_range`'s doc). That
        // splice must not surface as its own output point (existing
        // behavior), but its window-end DOES extend this group's observed
        // coverage further back than the first in-range point -- coverage
        // is folded from RAW window-ends before the `w_end < t0_ms` output
        // filter (see `SummaryValue`'s doc).
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(cms_with_heap_meta(sid, "requests_by_route"));

        let carry_in_end = T0 - 50_000; // strictly before t0 -- Full base
        let delta_window_end = T0 + 50_000; // in-range -- Delta, needs the base

        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0 - 100_000, carry_in_end),
            SketchSampleState {
                bytes: encode_cms_with_heap_items(4, 256, 10, &[("a", 5.0)]),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
        // Real DELTA-HEAP wire shape (`(is_delta, (rows, cols, cells),
        // heap, heap_size)`, see `delta_apply.rs`'s `encode_delta_heap`
        // test helper) -- zero matrix cells (no change), heap replaces
        // wholesale with `{b: 3.0}`.
        #[derive(serde::Serialize)]
        struct DeltaHeapFrame<'a>(
            bool,
            (u32, u32, &'a [(u32, u32, i64)]),
            Vec<(String, f64)>,
            u64,
        );
        let cells: Vec<(u32, u32, i64)> = vec![];
        let delta_bytes = rmp_serde::to_vec(&DeltaHeapFrame(
            true,
            (4, 256, &cells),
            vec![("b".to_string(), 3.0)],
            10,
        ))
        .expect("encode delta-heap frame");
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (carry_in_end, delta_window_end),
            SketchSampleState {
                bytes: delta_bytes,
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackDelta,
            },
        );

        let child = scan_node("requests_by_route", None);
        let tree = estimate_node(cms_with_heap_agg_node(child), SketchQuery::TopK { k: 5 });

        let exec = matrix_ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = v.into_iter().next().unwrap();
        let SummaryValue::TopK(points, coverage) = value else {
            panic!("expected TopK, got {value:?}");
        };
        assert_eq!(
            points.len(),
            1,
            "the carry-in base must not appear as its own output point"
        );
        assert_eq!(points[0].0, delta_window_end as i64);
        assert_eq!(
            coverage,
            Some((carry_in_end, delta_window_end)),
            "coverage must extend back to the carry-in base's window-end, even \
             though it never appears as an output point itself"
        );
    }

    // ── ExactAgg (Sum) support ──────────────────────────────────────────

    #[test]
    fn single_sum_exactagg_sid_exact_value_merges_windows() {
        // A bare `SummaryAgg` over an ExactAgg(Sum) sid resolves to
        // `ExecOutcome::State` directly (never `Value` -- see this
        // module's doc), and `GroupState::exact_value` must fold every
        // window in range into one combined sum.
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(sum_exact_agg_meta(sid, "bytes_total", &[]));
        idx.append_precompute(
            sid,
            BTreeMap::new(),
            (T0, T0 + 1000),
            Box::new(crate::precompute_engine::operators::SumAccumulator::with_sum(10.0)),
        );
        idx.append_precompute(
            sid,
            BTreeMap::new(),
            (T0 + 1000, T0 + 2000),
            Box::new(crate::precompute_engine::operators::SumAccumulator::with_sum(15.0)),
        );

        let child = scan_node("bytes_total", None);
        let tree = sum_agg_node(child, vec![]);

        let exec = ctx(&idx);
        let ExecOutcome::State(groups) = execute(&tree, &exec).expect("execute should succeed")
        else {
            panic!("expected State, not Value -- ExactAgg never reaches readout");
        };
        assert_eq!(
            groups.len(),
            1,
            "ungrouped query produces exactly one group"
        );
        let (_key, state, kind, params) = &groups[0];
        assert_eq!(*kind, SummaryKind::Sum);
        assert_eq!(*params, SummaryParams::Sum);
        assert_eq!(
            state.exact_value(&None),
            Some(25.0),
            "exact_value must merge both windows' sums (10 + 15)"
        );
        assert_eq!(
            state.exact_coverage(),
            Some((T0 + 1000, T0 + 2000)),
            "exact_coverage must bracket both windows' end timestamps, mirroring \
             SummaryValue::coverage()'s sketch-family behavior"
        );
    }

    #[test]
    fn exact_coverage_is_none_for_a_sketch_state() {
        // `exact_coverage` is the `ExactAgg`-only counterpart to
        // `SummaryValue::coverage()` -- must not silently return something
        // for a `Sketch` state.
        let idx = SketchStore::new();
        let sid = 1u64;
        idx.register(kll_meta(sid, "latency_ms", &[]));
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (T0, T0 + 1000),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &[1.0, 2.0, 3.0]),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        let child = scan_node("latency_ms", None);
        let tree = estimate_node(
            kll_agg_node(child, Reduction::by(vec![])),
            SketchQuery::Quantile { q: 0.5 },
        );
        let exec = ctx(&idx);
        let ExecOutcome::Value(_) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        // Build the Sketch GroupState directly via fetch_state to exercise
        // exact_coverage's defensive None arm (readout() already proved
        // this tree resolves to a real Sketch Value above).
        let handles = exec
            .find_candidates(
                &SummaryKind::Kll,
                &SummaryParams::Kll { k: 200 },
                &ColumnRef::SampleValue,
                &Reduction::by(vec![]),
                &scan_node("latency_ms", None),
            )
            .expect("find_candidates should succeed");
        let (_key, handle) = &handles[0];
        let state = exec
            .fetch_state(handle)
            .expect("fetch_state should succeed");
        assert_eq!(
            state.exact_coverage(),
            None,
            "exact_coverage must be None for a Sketch state, not silently Some"
        );
    }

    #[test]
    fn two_sum_exactagg_sids_same_group_merge_cross_sid() {
        // Cross-sid merge for ExactAgg: two sids in the same (ungrouped)
        // group must ADD, not just report one of them -- the ExactAgg
        // analog of `two_cms_sids_same_group_totals_actually_merge`.
        let idx = SketchStore::new();
        idx.register(sum_exact_agg_meta(1, "bytes_total", &[]));
        idx.register(sum_exact_agg_meta(2, "bytes_total", &[]));
        idx.append_precompute(
            1,
            BTreeMap::new(),
            (T0, T0 + 1000),
            Box::new(crate::precompute_engine::operators::SumAccumulator::with_sum(30.0)),
        );
        idx.append_precompute(
            2,
            BTreeMap::new(),
            (T0, T0 + 1000),
            Box::new(crate::precompute_engine::operators::SumAccumulator::with_sum(12.0)),
        );

        let child = scan_node("bytes_total", None);
        let tree = sum_agg_node(child, vec![]);

        let exec = ctx(&idx);
        let ExecOutcome::State(groups) = execute(&tree, &exec).expect("execute should succeed")
        else {
            panic!("expected State, not Value -- ExactAgg never reaches readout");
        };
        assert_eq!(groups.len(), 1);
        let (_key, state, ..) = &groups[0];
        assert_eq!(
            state.exact_value(&None),
            Some(42.0),
            "merged exact_value must be the SUM of both sids (30 + 12)"
        );
    }

    #[test]
    fn minmax_exactagg_sid_is_not_matched() {
        // `SummaryKind::MinMax` is deliberately NOT matched against
        // ExactAgg sids (see `exact_agg_kind_match`'s doc: no direction
        // info survives to `AggKind::ExactAgg`) -- must fail over as
        // NoCandidates, not silently guess a direction.
        let idx = SketchStore::new();
        let sid = 1u64;
        let mut meta = sum_exact_agg_meta(sid, "latency_max_ms", &[]);
        meta.agg_kind = crate::storage_engines::sketch_db::index::AggKind::ExactAgg {
            agg_type: asap_types::AggregationType::MinMax,
            parameters_canonical: String::new(),
            spatial_filter_canonical: String::new(),
        };
        meta.capability = Some(Capability::ExactAgg(asap_types::AggregationType::MinMax));
        idx.register(meta);
        idx.append_precompute(
            sid,
            BTreeMap::new(),
            (T0, T0 + 1000),
            Box::new(crate::precompute_engine::operators::MinMaxAccumulator::new_min()),
        );

        let child = scan_node("latency_max_ms", None);
        let tree = Rc::new(L4Node {
            expr: SummaryExpr::SummaryAgg {
                child,
                summary: SummaryKind::MinMax,
                params: SummaryParams::MinMax,
                col: ColumnRef::SampleValue,
                reduction: Reduction::by(vec![]),
            },
            schema: L4Schema {
                fields: vec![],
                time_index: None,
            },
        });
        let exec = ctx(&idx);
        match execute(&tree, &exec) {
            Err(asap_sketch::exec::ExecError::NoCandidates) => {}
            other => panic!("expected NoCandidates, got {}", other.is_ok()),
        }
    }
}
