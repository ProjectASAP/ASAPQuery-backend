//! `data_plane`'s implementation of `asap_sketch::exec::SummaryExecutor`
//! — the serving-time interface that resolves an `L4Node` plan tree
//! against whatever is actually materialized right now. See
//! `data_plane/docs/l4node-plan-executor-design.md` for the surrounding
//! design.
//!
//! ## Scope
//!
//! Covers **quantile/cardinality queries** (DDSketch/Kll/Hll) **and the
//! Frequency family's bare total** (CMS/CountSketch/CMS-with-heap/
//! CountSketch-with-heap, `count`/`sum` with no specific item key), both
//! cumulative (instant) and per-window (matrix/range). All modes do real
//! cross-sid merging via `delta_apply::SummaryState`: reconstruct each
//! candidate sid's own state over the range (or per window), then merge
//! same-window/same-range states *across* sids before reading out one
//! answer per group (or per group per window).
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
//! - `SketchQuery::PointCount` with a *named* item key (a point lookup
//!   for one specific item, e.g. `count(cms_metric{item="x"})`). The
//!   *value* to look up isn't carried by `SketchQuery` or available in
//!   `readout`'s signature — `PointCount{key: ColumnRef}` names which
//!   *column* is being queried, not the value to filter for, which would
//!   come from a `Filter` predicate elsewhere in the tree. Resolving
//!   that is a separate problem from this trait's scope.
//!   `PointCount{key: ColumnRef::SampleValue}` (no specific item — the
//!   bare bucket total) is covered.
//! - `ExactAgg` intents (`Sum`/`Rate`/`Increase`/`MinMax`/exact `Count`).
//!   These don't reach `readout` at all — `asap_plan::bind` never wraps
//!   an `ExactAccumulator` implementation in a `SummaryEstimate`
//!   (`estimate = false` in `bind_summary_agg`), so `execute()` on such a
//!   tree returns `ExecOutcome::State` at the root; the caller must read
//!   the final value out of that `State` itself, not through this trait.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use asap_ir::intent_algebra::{ColumnId, ColumnRef, QueryExpr, Source};
use asap_sketch::exec::SummaryExecutor;
use asap_sketch::{L4Node, SketchQuery, SummaryExpr, SummaryKind, SummaryParams};

use control_plane::sketch_algebra::capability::SketchKindHandle;

use crate::storage_engines::sketch_db::data::{SketchConfig, SketchTimeSeries};
use crate::storage_engines::sketch_db::index::{SketchSampleState, SketchStore};
use crate::storage_engines::sketch_db::query::delta_apply::{
    cumulative_summary_state, per_window_summary_states, DeltaSketchKind, SummaryState,
};

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
#[derive(Debug, Clone)]
pub struct SidHandle {
    series: Rc<SketchTimeSeries>,
    kind: DeltaSketchKind,
}

/// One group's accumulated candidates, all sharing one `DeltaSketchKind`
/// (guaranteed by `find_candidates`'s exact-match contract).
#[derive(Debug, Clone)]
pub struct GroupState {
    entries: Vec<SidHandle>,
    kind: DeltaSketchKind,
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
#[derive(Debug, Clone)]
pub enum SummaryValue {
    Points(Vec<(i64, f64)>),
    TopK(Vec<(i64, Vec<(String, f64)>)>),
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
        by: &[ColumnId],
        child: &L4Node,
    ) -> Result<Vec<(Self::GroupKey, Self::Handle)>, Self::Error> {
        let metric = find_metric(child).ok_or(SummaryExecutorError::NoMetricFound)?;

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

        let candidate_sids = self.index.instances_matching(&metric, &required_keys);
        let mut out = Vec::new();
        for sid in candidate_sids {
            let candidate_kind = self.index.with_instance(sid, |m| {
                let kind = m.sketch_kind()?;
                let config = m.sketch_config()?;
                summary_params_match(sketch, params, kind, config)
                    .then(|| to_delta_kind(kind, config))
                    .flatten()
            });
            let Some(candidate_kind) = candidate_kind.flatten() else {
                continue;
            };

            // Fetching the series here (rather than just checking
            // membership) is what lets `fetch_state`/`readout` skip a
            // second identical `query_range` call later -- see
            // `SidHandle`'s doc. The label values it carries are also
            // the only place a group's actual values live (metadata only
            // has the group-by KEY names, not values).
            let series = self.index.query_range(sid, self.t0_ms, self.t1_ms);
            let Some(series) = series.into_iter().next() else {
                continue;
            };
            let group_key: BTreeMap<String, String> = by_names
                .iter()
                .map(|k| {
                    let v = series
                        .series_label_values
                        .get(k)
                        .cloned()
                        .unwrap_or_default();
                    (k.clone(), v)
                })
                .collect();
            out.push((
                group_key,
                SidHandle {
                    series: Rc::new(series),
                    kind: candidate_kind,
                },
            ));
        }
        // Empty is NOT an error here -- `asap_sketch::exec::execute()`
        // itself checks `find_candidates`'s result for emptiness and
        // raises the canonical `ExecError::NoCandidates`; erroring here
        // too would just wrap that in `ExecError::Executor(..)` instead,
        // losing the distinction callers match on.
        Ok(out)
    }

    fn fetch_state(&self, handle: &Self::Handle) -> Result<Self::State, Self::Error> {
        Ok(GroupState {
            kind: handle.kind,
            entries: vec![handle.clone()],
        })
    }

    fn merge_states(&self, states: Vec<Self::State>) -> Result<Self::State, Self::Error> {
        // The actual decode/merge math (`cumulative_summary_state`/
        // `merge_same_family`) happens in `readout`, not here: it needs
        // to distinguish cumulative vs. per-window mode
        // (`self.is_cumulative`), which only `readout` is positioned to
        // do generically for both callers. `merge_states` and
        // `fetch_state` just assemble the group's full candidate list.
        let mut states = states.into_iter();
        let mut acc = states.next().ok_or(SummaryExecutorError::NoCandidates)?;
        for s in states {
            acc.entries.extend(s.entries);
        }
        Ok(acc)
    }

    fn readout(
        &self,
        state: &Self::State,
        query: &SketchQuery,
    ) -> Result<Self::Value, Self::Error> {
        if self.is_cumulative {
            readout_cumulative(state, query, self.t1_ms as i64)
        } else {
            readout_per_window(state, query, self.t0_ms as i64)
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
    state: &GroupState,
    query: &SketchQuery,
    t1_ms: i64,
) -> Result<SummaryValue, SummaryExecutorError> {
    let mut merged: Option<SummaryState> = None;
    let mut latest_window_end: Option<i64> = None;
    for entry in &state.entries {
        let samples_vec: Vec<(i64, &SketchSampleState)> = entry
            .series
            .samples
            .iter()
            .flat_map(|(t, frames)| frames.iter().map(move |s| (*t, s)))
            .collect();
        if let Some((w, _)) = samples_vec.last() {
            latest_window_end = Some(latest_window_end.map_or(*w, |prev| prev.max(*w)));
        }
        let rs = cumulative_summary_state(&samples_vec, state.kind)
            .map_err(SummaryExecutorError::Decode)?;
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
        Ok(SummaryValue::TopK(vec![(w_end, topk_ranked(&merged, *k)?)]))
    } else {
        Ok(SummaryValue::Points(vec![(
            w_end,
            sketch_query_value(&merged, query)?,
        )]))
    }
}

/// Per-window matrix/range-query readout: reconstruct each of the
/// group's sids' own per-window states, then merge same-window states
/// *across* sids before evaluating each window -- one merged answer per
/// window, not one merged answer for the whole range. Windows are
/// unioned across sids: a sid that's missing a particular window just
/// doesn't contribute to it, rather than the whole window being dropped.
fn readout_per_window(
    state: &GroupState,
    query: &SketchQuery,
    t0_ms: i64,
) -> Result<SummaryValue, SummaryExecutorError> {
    let mut by_window: BTreeMap<i64, SummaryState> = BTreeMap::new();
    for entry in &state.entries {
        let samples_vec: Vec<(i64, &SketchSampleState)> = entry
            .series
            .samples
            .iter()
            .flat_map(|(t, frames)| frames.iter().map(move |s| (*t, s)))
            .collect();
        let (per_window, _skipped) = per_window_summary_states(&samples_vec, state.kind)
            .map_err(SummaryExecutorError::Decode)?;
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
        Ok(SummaryValue::TopK(points))
    } else {
        let points = by_window
            .into_iter()
            .map(|(w_end, rs)| sketch_query_value(&rs, query).map(|v| (w_end, v)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SummaryValue::Points(points))
    }
}

/// Read one scalar out of a merged `SummaryState` for the requested
/// `SketchQuery` -- shared by both the cumulative and per-window readout
/// paths.
fn sketch_query_value(rs: &SummaryState, query: &SketchQuery) -> Result<f64, SummaryExecutorError> {
    match query {
        SketchQuery::Quantile { q } => Ok(rs.quantile(*q)),
        SketchQuery::Cardinality => Ok(rs.cardinality()),
        // `key: ColumnRef::SampleValue` means "no specific item" -- the
        // bare bucket total. Any other column names an item to look up
        // by VALUE, which isn't carried by `SketchQuery` -- see the
        // module doc.
        SketchQuery::PointCount {
            key: ColumnRef::SampleValue,
        } => Ok(rs.total()),
        SketchQuery::PointCount { .. } => Err(SummaryExecutorError::Unsupported(
            "PointCount for a named item key needs a filter value this trait doesn't carry",
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
        SummaryExpr::SummaryEstimate { sketch_input, .. } => find_metric(sketch_input),
        SummaryExpr::SummaryMerge { children } => children.first().and_then(|c| find_metric(c)),
        _ => None,
    }
}

fn find_metric_in_query_expr(qe: &QueryExpr) -> Option<String> {
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

    fn kll_agg_node(child: Rc<L4Node>, by: Vec<ColumnId>) -> Rc<L4Node> {
        Rc::new(L4Node {
            expr: SummaryExpr::SummaryAgg {
                child,
                sketch: SummaryKind::Kll,
                params: SummaryParams::Kll { k: 200 },
                col: ColumnRef::SampleValue,
                by,
            },
            schema: L4Schema {
                fields: vec![],
                time_index: None,
            },
        })
    }

    fn hll_agg_node(child: Rc<L4Node>) -> Rc<L4Node> {
        Rc::new(L4Node {
            expr: SummaryExpr::SummaryAgg {
                child,
                sketch: SummaryKind::Hll,
                params: SummaryParams::Hll { precision: 10 },
                col: ColumnRef::SampleValue,
                by: vec![],
            },
            schema: L4Schema {
                fields: vec![],
                time_index: None,
            },
        })
    }

    fn estimate_node(sketch_input: Rc<L4Node>, query: SketchQuery) -> Rc<L4Node> {
        Rc::new(L4Node {
            expr: SummaryExpr::SummaryEstimate {
                sketch_input,
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

    fn cms_agg_node(child: Rc<L4Node>) -> Rc<L4Node> {
        Rc::new(L4Node {
            expr: SummaryExpr::SummaryAgg {
                child,
                sketch: SummaryKind::Cms,
                params: SummaryParams::Cms {
                    width: 256,
                    depth: 4,
                },
                col: ColumnRef::SampleValue,
                by: vec![],
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
                sketch: SummaryKind::CmsWithHeap,
                params: SummaryParams::CmsWithHeap {
                    width: 256,
                    depth: 4,
                    heap_size: 10,
                },
                col: ColumnRef::SampleValue,
                by: vec![],
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
            kll_agg_node(child, vec![]),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        assert_eq!(v.len(), 1, "ungrouped query produces exactly one group");
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples) = value else {
            panic!("expected Points, got {value:?}");
        };
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
            kll_agg_node(child, vec![]),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        assert_eq!(v.len(), 1);
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples) = value else {
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
            kll_agg_node(child, vec![1]),
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
        let SummaryValue::Points(east_samples) = east_value else {
            panic!("expected Points, got {east_value:?}");
        };
        let SummaryValue::Points(west_samples) = west_value else {
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
        let SummaryValue::Points(samples) = value else {
            panic!("expected Points, got {value:?}");
        };
        let (_ts, card) = samples[0];
        assert!(
            (3.0..=7.0).contains(&card),
            "cardinality {card} should be ~5"
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
            },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples) = value else {
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
            },
        );

        let exec = ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = &v[0];
        let SummaryValue::Points(samples) = value else {
            panic!("expected Points, got {value:?}");
        };
        let (_ts, total) = samples[0];
        assert_eq!(
            total, 42.0,
            "merged total must be the SUM of both sids (30 + 12)"
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
        let SummaryValue::TopK(points) = value else {
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
        let SummaryValue::TopK(points) = value else {
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
        let SummaryValue::TopK(mut points) = value.clone() else {
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
            kll_agg_node(child, vec![]),
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
                sketch: SummaryKind::Kll,
                params: SummaryParams::Kll { k: 500 },
                col: ColumnRef::SampleValue,
                by: vec![],
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
            kll_agg_node(child, vec![]),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = matrix_ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        assert_eq!(v.len(), 1, "ungrouped query produces exactly one group");
        let (_group, value) = v.into_iter().next().unwrap();
        let SummaryValue::Points(mut samples) = value else {
            panic!("expected Points");
        };
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
            kll_agg_node(child, vec![]),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = matrix_ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = v.into_iter().next().unwrap();
        let SummaryValue::Points(samples) = value else {
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
            kll_agg_node(child, vec![]),
            SketchQuery::Quantile { q: 0.5 },
        );

        let exec = matrix_ctx(&idx);
        let ExecOutcome::Value(v) = execute(&tree, &exec).expect("execute should succeed") else {
            panic!("expected a value");
        };
        let (_group, value) = v.into_iter().next().unwrap();
        let SummaryValue::Points(samples) = value else {
            panic!("expected Points");
        };
        assert_eq!(
            samples.len(),
            1,
            "the carry-in-base window (ending before t0) must not appear in output"
        );
        assert_eq!(samples[0].0, in_range_end as i64);
    }
}
