//! `data_plane`'s `asap_sketch::exec::SummaryExecutor` implementation —
//! Step C of the plan-shaped-serving migration
//! (`data_plane/docs/l4node-plan-executor-design.md`).
//!
//! ## Scope of this first cut
//!
//! Covers **quantile/cardinality queries, both cumulative (instant) and
//! per-window (matrix/range)** — the `DdSketch`/`Kll`/`Hll` families
//! `RollingState` (`storage_engines::sketch_db::query::delta_apply`)
//! already covers. Both modes do real cross-sid merging:
//! - Cumulative: `delta_apply::cumulative_rolling_state` folds a
//!   group's whole `[t0, t1]` into one answer (generalized from the
//!   HLL-only global-cardinality rollup for this).
//! - Per-window: `delta_apply::per_window_rolling_states`
//!   reconstructs each sid's own per-window states, then this module
//!   merges same-window states *across* the group's sids before
//!   evaluating each window — one merged answer per window, not one
//!   merged answer for the whole range.
//!
//! Explicitly **not** covered yet, and left as follow-up rather than
//! silently mishandled — `readout` returns
//! [`SummaryExecutorError::Unsupported`] for all of these:
//! - The Frequency family (`TopK`/`PointCount`, i.e. CMS/CountSketch) —
//!   `RollingState` doesn't cover these; they decode via a different path
//!   (`sketch_reducer.rs`'s `decode_frequency_total`/
//!   `decode_cms_with_heap_from_msgpack` etc.) that would need its own
//!   analogous cross-sid-merge generalization.
//! - `ExactAgg` intents (`Sum`/`Rate`/`Increase`/`MinMax`/exact `Count`).
//!   These don't reach `readout` at all — `asap_plan::bind` never wraps
//!   an `ExactAccumulator` implementation in a `SummaryEstimate`
//!   (`estimate = false` in `bind_summary_agg`), so `execute()` on such a
//!   tree returns `ExecOutcome::State` at the root; the caller must read
//!   the final value out of that `State` itself, not through this trait.

use std::collections::{BTreeMap, BTreeSet};

use asap_ir::intent_algebra::{ColumnId, ColumnRef, QueryExpr, Source};
use asap_sketch::exec::SummaryExecutor;
use asap_sketch::{L4Node, SketchQuery, SummaryExpr, SummaryKind, SummaryParams};

use control_plane::sketch_algebra::capability::SketchKindHandle;

use crate::storage_engines::sketch_db::data::SketchConfig;
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::sketch_db::query::delta_apply::{
    cumulative_rolling_state, per_window_rolling_states, DeltaSketchKind, RollingState,
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

/// One group's accumulated candidate sids, plus enough to reconstruct
/// each sid's `RollingState` at readout time — `readout` only receives
/// `&Self::State`, not the `SummaryKind`/`SummaryParams` that produced
/// it (per the trait), so the state has to self-describe.
#[derive(Debug, Clone)]
pub struct GroupState {
    sids: Vec<u64>,
    delta_kind: DeltaSketchKind,
}

#[derive(Debug)]
pub enum SummaryExecutorError {
    /// No sid in the catalog matches the requested `(metric, by,
    /// SummaryKind, SummaryParams)` — mirrors today's `CapabilityMiss`
    /// contract; the caller fails over to archive.
    NoCandidates,
    /// Couldn't recover a metric name by walking the `SummaryAgg`'s
    /// child subtree (an unsupported/CSE-`Ref`-shaped `QueryExpr` this
    /// first cut doesn't walk through).
    NoMetricFound,
    /// A requested `by` `ColumnId` doesn't resolve to a name against the
    /// child's schema.
    UnresolvedColumn(ColumnId),
    /// A candidate sid claims a `SummaryKind` this executor doesn't
    /// implement cross-sid merge for yet (Frequency family) or the sid's
    /// on-disk `SketchConfig` didn't decode into a `DeltaSketchKind`.
    UnsupportedFamily,
    /// Decode/merge failure surfaced from `delta_apply`/`asap_sketchlib`.
    Decode(String),
    /// A `SummaryExpr::Logical` node — nothing committed at L4. Same
    /// meaning as today's "no candidate bound"; the caller fails over.
    Logical,
    /// Scoped out of this first cut — see the module doc.
    Unsupported(&'static str),
}

impl<'a> SummaryExecutor for QueryExecutionContext<'a> {
    type Handle = u64;
    type State = GroupState;
    type Value = Vec<(i64, f64)>;
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
            let matched = self.index.with_instance(sid, |m| {
                let kind = m.sketch_kind()?;
                let config = m.sketch_config()?;
                summary_params_match(sketch, params, kind, config).then_some(())
            });
            if matched.flatten().is_none() {
                continue;
            }

            // Project this sid's actual label values onto `by` for the
            // group key. Needs `query_range` (the only place per-sid
            // label values live) rather than `SketchInstanceMetadata`
            // alone (which only carries the group-by KEY names, not
            // values) -- a real per-candidate cost worth optimizing
            // later (this is exactly what
            // design-backend-plan-wire-format.md's RoutingIndex Tier-2
            // columnar index is for), not attempted in this first cut.
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
            out.push((group_key, sid));
        }
        // Empty is NOT an error here -- `asap_sketch::exec::execute()`
        // itself checks `find_candidates`'s result for emptiness and
        // raises the canonical `ExecError::NoCandidates`; erroring here
        // too would just wrap that in `ExecError::Executor(..)` instead,
        // losing the distinction callers match on.
        Ok(out)
    }

    fn fetch_state(&self, handle: &Self::Handle) -> Result<Self::State, Self::Error> {
        let sid = *handle;
        let delta_kind = self
            .index
            .with_instance(sid, |m| match (m.sketch_kind(), m.sketch_config()) {
                (Some(kind), Some(config)) => to_delta_kind(kind, config),
                _ => None,
            })
            .flatten()
            .ok_or(SummaryExecutorError::UnsupportedFamily)?;
        Ok(GroupState {
            sids: vec![sid],
            delta_kind,
        })
    }

    fn merge_states(&self, states: Vec<Self::State>) -> Result<Self::State, Self::Error> {
        // Deliberately lazy: concatenate sid lists rather than eagerly
        // decoding+merging here. The real merge math needs the query's
        // time range (`self.t0_ms`/`t1_ms`), which `merge_states` isn't
        // given -- `readout` is the first point in the trait that has
        // both the state and (via `self`) the range, so that's where
        // the actual `cumulative_rolling_state`/`merge_same_family` work
        // happens. `merge_states` and `fetch_state` together just build
        // up "the list of sids this group's answer must be built from."
        let mut states = states.into_iter();
        let mut acc = states.next().ok_or(SummaryExecutorError::NoCandidates)?;
        for s in states {
            acc.sids.extend(s.sids);
        }
        Ok(acc)
    }

    fn readout(
        &self,
        state: &Self::State,
        query: &SketchQuery,
    ) -> Result<Self::Value, Self::Error> {
        if self.is_cumulative {
            self.readout_cumulative(state, query)
        } else {
            self.readout_per_window(state, query)
        }
    }

    fn logical(&self, _expr: &QueryExpr) -> Result<Self::Value, Self::Error> {
        Err(SummaryExecutorError::Logical)
    }
}

impl<'a> QueryExecutionContext<'a> {
    /// Fold a group's whole `[t0, t1]` into one merged state and read out
    /// one scalar -- `quantile_over_time`/`count_distinct_over_time`-
    /// shaped instant queries.
    fn readout_cumulative(
        &self,
        state: &GroupState,
        query: &SketchQuery,
    ) -> Result<Vec<(i64, f64)>, SummaryExecutorError> {
        let mut merged: Option<RollingState> = None;
        let mut latest_window_end: i64 = self.t1_ms as i64;
        for &sid in &state.sids {
            for ts in self.index.query_range(sid, self.t0_ms, self.t1_ms) {
                let samples_vec: Vec<(
                    i64,
                    &crate::storage_engines::sketch_db::index::SketchSampleState,
                )> = ts
                    .samples
                    .iter()
                    .flat_map(|(t, frames)| frames.iter().map(move |s| (*t, s)))
                    .collect();
                if let Some((w, _)) = samples_vec.last() {
                    latest_window_end = *w;
                }
                let rs = cumulative_rolling_state(&samples_vec, state.delta_kind)
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
        }
        let Some(merged) = merged else {
            return Err(SummaryExecutorError::NoCandidates);
        };
        let value = sketch_query_value(&merged, query)?;
        Ok(vec![(latest_window_end, value)])
    }

    /// Per-window matrix/range-query readout: reconstruct each of the
    /// group's sids' own per-window states
    /// (`delta_apply::per_window_rolling_states`), then merge same-window
    /// states *across* sids before evaluating each window -- one merged
    /// answer per window, not one merged answer for the whole range.
    /// Windows are unioned across sids (mirrors `SummaryMerge`'s "fold
    /// whatever's present" semantics from ASAPController#161 -- a sid
    /// that's missing a particular window just doesn't contribute to it,
    /// rather than the whole window being dropped).
    fn readout_per_window(
        &self,
        state: &GroupState,
        query: &SketchQuery,
    ) -> Result<Vec<(i64, f64)>, SummaryExecutorError> {
        let mut by_window: BTreeMap<i64, RollingState> = BTreeMap::new();
        // `SketchStore::query_range` may splice in a carry-in Full
        // snapshot ending BEFORE `t0_ms` so the delta-apply walk can
        // establish a rolling base for a delta-only leading window (see
        // `delta_apply.rs`'s module doc). That base must not surface as
        // an output point in the requested `[t0, t1]` range -- mirrors
        // `sketch_reducer.rs`'s `evaluate_core`'s identical filter on the
        // legacy path.
        let lo = self.t0_ms as i64;
        for &sid in &state.sids {
            for ts in self.index.query_range(sid, self.t0_ms, self.t1_ms) {
                let samples_vec: Vec<(
                    i64,
                    &crate::storage_engines::sketch_db::index::SketchSampleState,
                )> = ts
                    .samples
                    .iter()
                    .flat_map(|(t, frames)| frames.iter().map(move |s| (*t, s)))
                    .collect();
                let (per_window, _skipped) =
                    per_window_rolling_states(&samples_vec, state.delta_kind)
                        .map_err(SummaryExecutorError::Decode)?;
                for (w_end, rs) in per_window {
                    if w_end < lo {
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
        }
        if by_window.is_empty() {
            return Err(SummaryExecutorError::NoCandidates);
        }
        by_window
            .into_iter()
            .map(|(w_end, rs)| sketch_query_value(&rs, query).map(|v| (w_end, v)))
            .collect()
    }
}

/// Read one scalar out of a merged `RollingState` for the requested
/// `SketchQuery` -- shared by both the cumulative and per-window readout
/// paths.
fn sketch_query_value(rs: &RollingState, query: &SketchQuery) -> Result<f64, SummaryExecutorError> {
    match query {
        SketchQuery::Quantile { q } => Ok(rs.quantile(*q)),
        SketchQuery::Cardinality => Ok(rs.cardinality()),
        SketchQuery::PointCount { .. } | SketchQuery::TopK { .. } => {
            Err(SummaryExecutorError::Unsupported(
                "Frequency-family (PointCount/TopK) readout not yet implemented",
            ))
        }
    }
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
        _ => false,
    }
}

/// `SketchConfig` (data_plane's per-sid stored params) -> `DeltaSketchKind`
/// (`delta_apply`'s decode/merge parameter carrier) for the three
/// families `RollingState` covers. `None` for CMS/CountSketch (the
/// Frequency family -- not yet supported by this executor, see the
/// module doc).
fn to_delta_kind(kind: SketchKindHandle, config: &SketchConfig) -> Option<DeltaSketchKind> {
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
        let (_group, samples) = &v[0];
        let (_ts, median) = samples[0];
        // Median of 1..=100 is ~50.
        assert!(
            (45.0..=55.0).contains(&median),
            "median {median} out of range"
        );
    }

    #[test]
    fn two_sids_same_group_actually_merge_not_just_first() {
        // The gap #409 flagged: two sids covering the SAME group must be
        // MERGED (one combined answer), not silently duplicated /
        // one-of-them-dropped.
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
        let (_group, samples) = &v[0];
        let (_ts, median) = samples[0];
        assert!(
            (40.0..=60.0).contains(&median),
            "merged median {median} should be ~50 (both sids' data combined), \
             not ~25 or ~75 (one sid dropped)"
        );
    }

    #[test]
    fn two_sids_different_groups_produce_two_series_not_one_merged_blob() {
        // ASAPController#159: `quantile by (zone) (...)` must produce one
        // output series per zone, not one series merging both zones.
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
        let (east_group, east_samples) = &v[0];
        let (west_group, west_samples) = &v[1];
        assert_eq!(east_group.get("zone").map(String::as_str), Some("us-east"));
        assert_eq!(west_group.get("zone").map(String::as_str), Some("us-west"));
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
        let (_group, samples) = &v[0];
        let (_ts, card) = samples[0];
        assert!(
            (3.0..=7.0).contains(&card),
            "cardinality {card} should be ~5"
        );
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
        let (_group, mut samples) = v.into_iter().next().unwrap();
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
        let (_group, samples) = v.into_iter().next().unwrap();
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
        let (_group, samples) = v.into_iter().next().unwrap();
        assert_eq!(
            samples.len(),
            1,
            "the carry-in-base window (ending before t0) must not appear in output"
        );
        assert_eq!(samples[0].0, in_range_end as i64);
    }
}
