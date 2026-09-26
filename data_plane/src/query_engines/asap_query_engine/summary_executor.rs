//! Deployment adapters for resolving, decoding, and reading installed materializations.
use asap_physical_operators::summary_kernels::{MaxAccumulator, MinAccumulator};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

use planner_types::post_asap::{
    ExactKind, SketchAlgorithm, SketchParams, SketchQuery, SummaryFamilyType,
};
use planner_types::pre_asap::{QueryExpr, Source};

use crate::storage_engines::sketch_db::data::{AggKind, SketchConfig, SketchTimeSeries};
use crate::storage_engines::sketch_db::index::{SketchSampleState, SketchStore};
use crate::storage_engines::sketch_db::query::delta_apply::{
    cumulative_summary_state, per_window_summary_states, DeltaSketchKind, SummaryState,
};
use crate::storage_engines::types::{AggregateCore, AggregationType, KeyByLabelValues};

/// Per-query, per-call execution context — constructed fresh for each
/// incoming query (never shared across concurrent queries, never
/// mutated after construction). This is what carries the time range and
/// cumulative-vs-per-window mode. `ASAPQueryEngine` itself is called
/// concurrently (`Arc<dyn QueryEngine>`), so threading the range through
/// shared mutable state on the engine would be a race — a fresh,
/// each call receives its own stack-local context.
pub struct QueryExecutionContext<'a> {
    pub index: &'a SketchStore,
    pub t0_ms: u64,
    pub t1_ms: u64,
    /// `true` for `quantile_over_time`/`count_distinct_over_time`-shaped
    /// instant queries (fold the whole range into one answer, via
    /// `readout_cumulative`); `false` for a per-window matrix (one merged
    /// answer per window, via `readout_per_window`).
    pub is_cumulative: bool,
    /// Materializations authorized by the installed QueryPlan bindings.
    /// `None` is the explicit dynamic-test mode; `Some` fails closed and
    /// excludes stale or unrelated SIDs even when their metric/family match.
    pub allowed_materializations: Option<BTreeSet<asap_types::PolicyFingerprint>>,
}

/// One group's accumulated candidates. `Sketch` entries all share one
/// `DeltaSketchKind`; `ExactAgg` entries all share one `AggregationType`
/// (validated by the deployment binding) — a
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
    /// Finalize the group's exact state using its declared family.
    /// Typed query execution uses `exact_value_for` to preserve readout errors.
    pub fn exact_value(&self, key: &Option<KeyByLabelValues>) -> Option<f64> {
        let GroupState::ExactAgg { entries, agg_type } = self else {
            return None;
        };
        let stat = match agg_type.planner_exact_family()? {
            SummaryFamilyType::ExactAggregate(ExactKind::Sum, _) => asap_types::Statistic::Sum,
            SummaryFamilyType::ExactAggregate(ExactKind::Count, _) => asap_types::Statistic::Count,
            SummaryFamilyType::ExactAggregate(ExactKind::Increase, _) => {
                asap_types::Statistic::Increase
            }
            SummaryFamilyType::ExactAggregate(ExactKind::Rate, _) => asap_types::Statistic::Rate,
            _ => return None,
        };
        asap_physical_operators::stored_state::readout::exact_readout(
            entries.iter().flat_map(|windows| windows.values().cloned()),
            stat,
            key,
            &std::collections::HashMap::new(),
        )
        .ok()
    }

    /// Finalize the Planner-declared exact family with its matching readout.
    pub fn exact_value_for(
        &self,
        readout: asap_types::query_plan::ExactReadout,
        key: &Option<KeyByLabelValues>,
        range_start_ms: u64,
        range_end_ms: u64,
    ) -> Result<Option<f64>, String> {
        let parameters = std::collections::HashMap::from([
            ("range_start_ms".to_string(), range_start_ms.to_string()),
            ("range_end_ms".to_string(), range_end_ms.to_string()),
        ]);
        self.exact_value_with_parameters(readout, key, &parameters)
    }

    pub(crate) fn exact_value_with_parameters(
        &self,
        readout: asap_types::query_plan::ExactReadout,
        key: &Option<KeyByLabelValues>,
        parameters: &std::collections::HashMap<String, String>,
    ) -> Result<Option<f64>, String> {
        let GroupState::ExactAgg { entries, agg_type } = self else {
            return Err("exact readout requires matching exact state".into());
        };
        if agg_type.planner_exact_family().as_ref() != Some(&readout.planner_family()) {
            return Err("exact readout requires matching exact state".into());
        }
        let stat = match readout {
            asap_types::query_plan::ExactReadout::Count => asap_types::Statistic::Count,
            asap_types::query_plan::ExactReadout::Sum => asap_types::Statistic::Sum,
            asap_types::query_plan::ExactReadout::Increase => asap_types::Statistic::Increase,
            asap_types::query_plan::ExactReadout::Rate => asap_types::Statistic::Rate,
            asap_types::query_plan::ExactReadout::Min => asap_types::Statistic::Min,
            asap_types::query_plan::ExactReadout::Max => asap_types::Statistic::Max,
        };

        asap_physical_operators::stored_state::readout::exact_readout_optional(
            entries.iter().flat_map(|windows| windows.values().cloned()),
            stat,
            key,
            parameters,
        )
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
    /// SummaryFamilyType)` — mirrors today's `CapabilityMiss`
    /// contract; the caller fails over to archive.
    NoCandidates,
    /// A candidate sid claims a `SummaryFamilyType` this executor doesn't
    /// implement cross-sid merge for, or the sid's on-disk
    /// `SketchConfig` didn't decode into a `DeltaSketchKind`.
    UnsupportedFamily,
    /// Decode/merge failure surfaced from `delta_apply`/`asap_sketchlib`.
    Decode(String),
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

#[cfg(test)]
fn validate_binding_phase(
    binding: &asap_types::query_plan::MaterializationBinding,
    evaluation_ms: u64,
) -> Result<(), SummaryExecutorError> {
    let start = evaluation_ms.checked_sub(binding.readout_lookback_ms.unwrap_or(binding.window_ms));
    if start.is_some_and(|start| binding.covers_range(start, evaluation_ms)) {
        Ok(())
    } else {
        Err(SummaryExecutorError::Unsupported(
            "query range does not match materialized window boundaries",
        ))
    }
}

impl QueryExecutionContext<'_> {
    /// Resolve exactly one compiler-bound materialization. This is the formal
    /// QueryPlan path: the validated stored output resolves to its definition's
    /// generation-scoped SID index. Metadata checks never broaden that set.
    pub fn read_bound_materialization(
        &self,
        binding: &asap_types::query_plan::MaterializationBinding,
    ) -> Result<Vec<(BTreeMap<String, String>, GroupState)>, SummaryExecutorError> {
        use asap_types::query_plan::PhysicalGrouping;

        if binding.stored_output_reference.validate().is_err()
            || binding.stored_output_reference.stored_output_id != binding.materialization
        {
            return Err(SummaryExecutorError::Unsupported(
                "read binding has invalid stored output",
            ));
        }

        if self
            .allowed_materializations
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&binding.materialization.fingerprint()))
        {
            return Err(SummaryExecutorError::Unsupported(
                "materialization is not authorized by the installed query",
            ));
        }
        let catalog =
            self.index
                .summary_catalog_snapshot()
                .ok_or(SummaryExecutorError::Unsupported(
                    "bound read requires installed SDS definitions",
                ))?;
        let expected = catalog
            .output_reference(binding.materialization)
            .map_err(|_| SummaryExecutorError::Unsupported("stored output is not installed"))?;
        if binding.stored_output_reference != expected {
            return Err(SummaryExecutorError::Unsupported(
                "bound read semantic identity differs from installed output",
            ));
        }

        let inventory_revision = self.index.summary_update_revision();
        let query_range = asap_types::sds::HalfOpenTimeRange {
            start_ms: i64::try_from(self.t0_ms).map_err(|_| {
                SummaryExecutorError::Unsupported("query start exceeds signed event time")
            })?,
            end_ms: i64::try_from(self.t1_ms).map_err(|_| {
                SummaryExecutorError::Unsupported("query end exceeds signed event time")
            })?,
        };
        if self.index.has_pending_summary_updates(
            binding.materialization,
            query_range,
            binding.full_window_slide_ms.is_some(),
        ) {
            return Err(SummaryExecutorError::Unsupported(
                "materialization population has unpublished input",
            ));
        }
        if !binding.covers_range(self.t0_ms, self.t1_ms) {
            return Err(SummaryExecutorError::Unsupported(
                "query range does not match materialized window boundaries",
            ));
        }

        enum Candidate {
            Sketch(DeltaSketchKind),
            ExactAgg(AggregationType),
        }

        // A missing pane can mean delayed ingestion, not an empty interval.
        // Until explicit empty-pane completion exists, multi-pane reads require
        // every pane for each stored series, before any cross-series merge.
        let check_panes = |ends: Vec<i64>| -> Result<(), SummaryExecutorError> {
            let width = binding.window_ms;
            if width == 0 {
                return Err(SummaryExecutorError::Unsupported("zero pane width"));
            }
            if self.t1_ms.saturating_sub(self.t0_ms) <= width {
                return Ok(());
            }
            let mut expected = self.t0_ms.checked_add(width);
            for end in ends {
                let Ok(end) = u64::try_from(end) else {
                    continue;
                };
                if end <= self.t0_ms {
                    continue;
                } // delta decoding carry-in is not an answer pane
                if Some(end) != expected {
                    return Err(SummaryExecutorError::Unsupported(
                        "missing materialized pane",
                    ));
                }
                expected = end.checked_add(width);
            }
            if expected != self.t1_ms.checked_add(width) {
                return Err(SummaryExecutorError::Unsupported(
                    "incomplete materialized panes",
                ));
            }
            Ok(())
        };
        let mut sids = self
            .index
            .storage_handles_for_output(&binding.stored_output_reference);
        sids.sort_unstable();
        sids.dedup();
        let mut matched_metadata = 0usize;
        let mut by_group: BTreeMap<BTreeMap<String, String>, Vec<GroupState>> = BTreeMap::new();
        let mut source_order = BTreeMap::new();

        for sid in sids.iter().copied() {
            if self
                .index
                .summary_window_known_empty(binding.materialization, sid, query_range)
            {
                continue;
            }
            let candidate = self
                .index
                .with_instance(sid, |meta| {
                    if meta.policy_fp != binding.materialization.fingerprint() {
                        return None;
                    }
                    match &meta.agg_kind {
                        AggKind::Sketch {
                            algorithm, config, ..
                        } => to_delta_kind(algorithm.clone(), config).map(Candidate::Sketch),
                        AggKind::ExactAgg { agg_type, .. } => Some(Candidate::ExactAgg(*agg_type)),
                    }
                })
                .flatten();
            let Some(candidate) = candidate else { continue };
            matched_metadata += 1;
            match candidate {
                Candidate::Sketch(kind) => {
                    let Some(mut series) = self
                        .index
                        .query_range(sid, self.t0_ms, self.t1_ms)
                        .into_iter()
                        .next()
                    else {
                        check_panes(Vec::new())?;
                        continue;
                    };
                    if binding.full_window_slide_ms.is_some() {
                        // Overlap lookup also returns neighboring complete windows.
                        // They overlap the answer and must never be merged into it.
                        series.samples.retain(|end, _| *end == self.t1_ms as i64);
                        if series.samples.is_empty() {
                            // A series that reported nothing in this window has no
                            // full-window snapshot. The generic `known_empty` skip at
                            // the top of the loop is layout-blind and lets such a sid
                            // through; when the layout-aware check proves it empty,
                            // skip it rather than discarding the sids already
                            // accumulated and dropping the query to the exact path.
                            if self.index.full_summary_window_known_empty(
                                binding.materialization,
                                sid,
                                query_range,
                            ) {
                                continue;
                            }
                            return Err(SummaryExecutorError::NoCandidates);
                        }
                    }
                    check_panes(series.samples.keys().copied().collect())?;
                    let key = match &binding.output_grouping {
                        PhysicalGrouping::PerEntity => series.series_label_values.clone(),
                        PhysicalGrouping::Reduce(keys) => {
                            project_group_key(keys, &series.series_label_values)
                        }
                    };
                    source_order.entry(key.clone()).or_insert(sid);
                    by_group.entry(key).or_default().push(GroupState::Sketch {
                        entries: vec![Rc::new(series)],
                        kind,
                    });
                }
                Candidate::ExactAgg(agg_type) => {
                    let exact_family = agg_type.planner_exact_family();
                    if matches!(
                        exact_family.as_ref(),
                        Some(SummaryFamilyType::ExactAggregate(
                            ExactKind::Increase | ExactKind::Rate,
                            _
                        ))
                    ) {
                        // Sparse counters may have empty edge panes. Every
                        // stored pane must still lie wholly within the requested
                        // range and on its declared grid; cutting a pane would
                        // require raw boundary samples that this state lacks.
                        let coverage = self
                            .index
                            .exact_agg_coverage_bounds(sid, self.t0_ms, self.t1_ms);
                        let Some((start, end)) = coverage else {
                            continue;
                        };
                        if start < self.t0_ms
                            || end > self.t1_ms
                            || !binding.covers_range(start, end)
                        {
                            return Err(SummaryExecutorError::Unsupported(
                                "counter SDS requires full-pane query coverage",
                            ));
                        }
                    }
                    if let Some((reduction, is_min)) = match exact_family.as_ref() {
                        Some(SummaryFamilyType::ExactAggregate(ExactKind::Min, _)) => Some((
                            crate::storage_engines::sketch_db::index::RollupReduction::Min,
                            true,
                        )),
                        Some(SummaryFamilyType::ExactAggregate(ExactKind::Max, _)) => Some((
                            crate::storage_engines::sketch_db::index::RollupReduction::Max,
                            false,
                        )),
                        _ => None,
                    } {
                        if let Some(series) = binding
                            .full_window_slide_ms
                            .is_none()
                            .then(|| {
                                self.index
                                    .query_rollup_range(reduction, sid, self.t0_ms, self.t1_ms)
                            })
                            .flatten()
                        {
                            for (labels, value) in series {
                                let key = match &binding.output_grouping {
                                    PhysicalGrouping::PerEntity => labels,
                                    PhysicalGrouping::Reduce(keys) => {
                                        project_group_key(keys, &labels)
                                    }
                                };
                                let accumulator: Arc<dyn AggregateCore> = if is_min {
                                    Arc::new(MinAccumulator::with_value(value))
                                } else {
                                    Arc::new(MaxAccumulator::with_value(value))
                                };
                                source_order.entry(key.clone()).or_insert(sid);
                                by_group.entry(key).or_default().push(GroupState::ExactAgg {
                                    entries: vec![Rc::new(BTreeMap::from([(
                                        self.t1_ms as i64,
                                        accumulator,
                                    )]))],
                                    agg_type,
                                });
                            }
                            continue;
                        }
                    }
                    let Some((labels, windows)) = self
                        .index
                        .query_exact_agg_range(sid, self.t0_ms, self.t1_ms)
                        .into_iter()
                        .next()
                    else {
                        continue;
                    };
                    // Exact accumulators carry their own first/last event
                    // timestamps. Empty panes need no stored identity and
                    // gaps between sampled panes are therefore valid for
                    // counter and extrema state. Additive pane summaries must
                    // remain contiguous because a missing pane is not zero.
                    if matches!(
                        exact_family.as_ref(),
                        Some(SummaryFamilyType::ExactAggregate(
                            ExactKind::Sum | ExactKind::Count,
                            _
                        ))
                    ) {
                        check_panes(windows.keys().copied().collect())?;
                    }
                    let key = match &binding.output_grouping {
                        PhysicalGrouping::PerEntity => labels,
                        PhysicalGrouping::Reduce(keys) => project_group_key(keys, &labels),
                    };
                    source_order.entry(key.clone()).or_insert(sid);
                    by_group.entry(key).or_default().push(GroupState::ExactAgg {
                        entries: vec![Rc::new(windows)],
                        agg_type,
                    });
                }
            }
        }
        if by_group.is_empty() {
            tracing::debug!(
                materialization = %binding.materialization.as_u64(),
                ?sids,
                matched_metadata,
                t0_ms = self.t0_ms,
                t1_ms = self.t1_ms,
                "bound materialization produced no readable state"
            );
            return Err(SummaryExecutorError::NoCandidates);
        }
        let mut result: Vec<_> = by_group
            .into_iter()
            .map(|(key, states)| self.merge_states(states).map(|state| (key, state)))
            .collect::<Result<_, _>>()?;
        if binding.output_grouping == PhysicalGrouping::PerEntity {
            // Stable native ranking retains the first source on equal values.
            // Grouping by labels must not replace the source's series order.
            result.sort_by_key(|(key, _)| source_order[key]);
        }
        if !inventory_revision.matches(self.index.summary_update_revision()) {
            return Err(SummaryExecutorError::Unsupported(
                "summary input changed during read",
            ));
        }
        Ok(result)
    }

    pub fn readout_bound(
        &self,
        state: &GroupState,
        query: &SketchQuery,
    ) -> Result<SummaryValue, SummaryExecutorError> {
        self.readout(state, query)
    }

    pub fn merge_bound_states(
        &self,
        states: Vec<GroupState>,
    ) -> Result<GroupState, SummaryExecutorError> {
        self.merge_states(states)
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

impl QueryExecutionContext<'_> {
    fn merge_states(&self, states: Vec<GroupState>) -> Result<GroupState, SummaryExecutorError> {
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
                    GroupState::ExactAgg { entries, agg_type },
                    GroupState::ExactAgg {
                        entries: more,
                        agg_type: incoming,
                    },
                ) => {
                    if agg_type.planner_exact_family() != incoming.planner_exact_family() {
                        return Err(SummaryExecutorError::UnsupportedFamily);
                    }
                    entries.extend(more);
                }
                // `find_candidates`'s exact-match contract never produces a
                // mixed group (a `(SummaryFamilyType)` query
                // matches either sketch-family sids or ExactAgg sids, never
                // both) -- defensive, not a real path.
                _ => return Err(SummaryExecutorError::UnsupportedFamily),
            }
        }
        Ok(acc)
    }

    fn readout(
        &self,
        state: &GroupState,
        query: &SketchQuery,
    ) -> Result<SummaryValue, SummaryExecutorError> {
        let GroupState::Sketch { entries, kind } = state else {
            return Err(SummaryExecutorError::UnsupportedFamily);
        };
        if self.is_cumulative {
            readout_cumulative(entries, *kind, query, self.t1_ms as i64)
        } else {
            readout_per_window(entries, *kind, query, self.t0_ms as i64)
        }
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
fn sketch_query_value(
    state: &SummaryState,
    query: &SketchQuery,
) -> Result<f64, SummaryExecutorError> {
    asap_physical_operators::stored_state::readout::sketch_query_value(state, query).map_err(
        |asap_physical_operators::stored_state::readout::Error::Unsupported(reason)| {
            SummaryExecutorError::Unsupported(reason)
        },
    )
}
fn topk_ranked(state: &SummaryState, k: usize) -> Result<Vec<(String, f64)>, SummaryExecutorError> {
    asap_physical_operators::stored_state::readout::topk_ranked(state, k).map_err(
        |asap_physical_operators::stored_state::readout::Error::Unsupported(reason)| {
            SummaryExecutorError::Unsupported(reason)
        },
    )
}

/// Exact canonical `SketchKind` match against a sid's own
/// `(SketchAlgorithm, SketchConfig)` -- the check `find_candidates`'s
/// trait contract requires (not the looser family-only
/// `Capability::is_satisfied_by` check the legacy analyzer path uses),
/// so a `SummaryMerge`'s precondition (every child agrees on kind AND
/// params) is guaranteed by construction for anything routed through
/// this executor.
fn summary_family_matches_sketch(
    family: &SummaryFamilyType,
    kind: SketchAlgorithm,
    config: &SketchConfig,
) -> bool {
    // `SketchParams::{Cms,CmsWithHeap,CountSketch,CountSketchWithHeap}`
    // use width=cols/depth=rows (matches the control-plane wire
    // convention -- see `sketch_config_to_json`'s comment). `SketchConfig`
    // has no `heap_size` field at all (heap-bearing kinds reuse their
    // heap-less base's config shape for identity -- see
    // `base_sketch_algorithm`'s doc in `drivers/ingest/otel.rs`), so
    // heap_size can't be part of this match; width/depth are.
    let SummaryFamilyType::Sketch(sketch, _) = family else {
        return false;
    };
    match (sketch.algorithm(), sketch.params(), kind, config) {
        (
            SketchAlgorithm::UnivMon,
            SketchParams::UnivMon {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            },
            SketchAlgorithm::UnivMon,
            SketchConfig::UnivMon {
                heap_size: h,
                sketch_rows: r,
                sketch_cols: c,
                layers: l,
            },
        ) => heap_size == h && sketch_rows == r && sketch_cols == c && layers == l,
        (
            SketchAlgorithm::DDSketch,
            SketchParams::DDSketch { alpha },
            SketchAlgorithm::DDSketch,
            SketchConfig::DDSketch { relative_accuracy },
        ) => alpha == relative_accuracy,
        (
            SketchAlgorithm::Kll,
            SketchParams::Kll { k },
            SketchAlgorithm::Kll,
            SketchConfig::Kll { k: sid_k },
        ) => k == sid_k,
        (
            SketchAlgorithm::Hll,
            SketchParams::Hll { precision },
            SketchAlgorithm::Hll,
            SketchConfig::Hll { precision: sid_p },
        ) => u32::from(*precision) == *sid_p,
        (
            SketchAlgorithm::Cms,
            SketchParams::Cms { width, depth },
            SketchAlgorithm::Cms,
            SketchConfig::CountMin { rows, cols },
        ) => *depth as i32 == *rows && *width as i32 == *cols,
        (
            SketchAlgorithm::CountSketch,
            SketchParams::CountSketch { width, depth },
            SketchAlgorithm::CountSketch,
            SketchConfig::CountSketch { rows, cols },
        ) => *depth as i32 == *rows && *width as i32 == *cols,
        (
            SketchAlgorithm::CmsWithHeap,
            SketchParams::CmsWithHeap { width, depth, .. },
            SketchAlgorithm::CmsWithHeap,
            SketchConfig::CountMin { rows, cols },
        ) => *depth as i32 == *rows && *width as i32 == *cols,
        (
            SketchAlgorithm::CountSketchWithHeap,
            SketchParams::CountSketchWithHeap { width, depth, .. },
            SketchAlgorithm::CountSketchWithHeap,
            SketchConfig::CountSketch { rows, cols },
        ) => *depth as i32 == *rows && *width as i32 == *cols,
        _ => false,
    }
}

/// Exact-aggregate analog of `summary_family_matches_sketch`, for
/// `AggKind::ExactAgg` sids. `ExactParams` variants carry no tuning
/// parameters, so this is a pure `ExactKind` identity check against the sid's
/// `AggregationType`, mirroring the canonical `AggregationType ->
/// ExactKind` mapping `asap_types::accumulator_spec` uses on the write
/// side. Count and Rate remain distinct families even though their runtime
/// accumulators share implementations with Sum and Increase.
///
/// Installed QueryPlans carry an explicit `ExactReadout`, and
/// `read_bound_materialization` serves those forms safely.
fn summary_family_matches_exact(family: &SummaryFamilyType, agg_type: AggregationType) -> bool {
    matches!(
        family,
        SummaryFamilyType::ExactAggregate(
            ExactKind::Sum | ExactKind::Count | ExactKind::Increase | ExactKind::Rate,
            _
        )
    ) && agg_type.planner_exact_family().as_ref() == Some(family)
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

/// `SketchConfig` (data_plane's per-sid stored params) -> `DeltaSketchKind`
/// (`delta_apply`'s decode/merge parameter carrier).
fn to_delta_kind(kind: SketchAlgorithm, config: &SketchConfig) -> Option<DeltaSketchKind> {
    // Default heap_size when bootstrapping an empty Heap state for a
    // delta-from-empty leading window -- `SketchConfig` carries no
    // heap_size (see `summary_params_match`'s doc), so this only matters
    // transiently: `CountMinSketchWithHeap::merge` takes `min(self,
    // other)`, so it converges to the real decoded value as soon as any
    // actual frame merges in. Matches this codebase's existing
    // heap_size-absent default (`accuracy.rs`).
    const DEFAULT_HEAP_SIZE: usize = 100;
    match (kind, config) {
        (
            SketchAlgorithm::UnivMon,
            SketchConfig::UnivMon {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            },
        ) => Some(DeltaSketchKind::UnivMon {
            heap_size: *heap_size,
            sketch_rows: *sketch_rows,
            sketch_cols: *sketch_cols,
            layers: *layers,
        }),
        (SketchAlgorithm::DDSketch, SketchConfig::DDSketch { relative_accuracy }) => {
            Some(DeltaSketchKind::DDSketch {
                alpha: *relative_accuracy,
            })
        }
        (SketchAlgorithm::Kll, SketchConfig::Kll { k }) => Some(DeltaSketchKind::Kll { k: *k }),
        (SketchAlgorithm::Hll, SketchConfig::Hll { precision }) => Some(DeltaSketchKind::Hll {
            precision: *precision,
        }),
        (SketchAlgorithm::Cms, SketchConfig::CountMin { rows, cols }) => {
            Some(DeltaSketchKind::Cms {
                rows: *rows as usize,
                cols: *cols as usize,
            })
        }
        (SketchAlgorithm::CountSketch, SketchConfig::CountSketch { rows, cols }) => {
            Some(DeltaSketchKind::CountSketch {
                rows: *rows as usize,
                cols: *cols as usize,
            })
        }
        (SketchAlgorithm::CmsWithHeap, SketchConfig::CountMin { rows, cols }) => {
            Some(DeltaSketchKind::CmsWithHeap {
                rows: *rows as usize,
                cols: *cols as usize,
                heap_size: DEFAULT_HEAP_SIZE,
            })
        }
        (SketchAlgorithm::CountSketchWithHeap, SketchConfig::CountSketch { rows, cols }) => {
            Some(DeltaSketchKind::CountSketchWithHeap {
                rows: *rows as usize,
                cols: *cols as usize,
                heap_size: DEFAULT_HEAP_SIZE,
            })
        }
        _ => None,
    }
}

/// Walk a canonical `QueryExpr` down to its first `Scan {
/// source: Source::TimeSeries { metric }, .. }` to recover the target
/// metric name.
/// (serving time must know which metric to check the `SketchStore`
/// against BEFORE binding — see that module's docs).
pub(crate) fn find_metric_in_query_expr(qe: &QueryExpr) -> Option<String> {
    match qe {
        QueryExpr::Scan {
            source: Source::TimeSeries { metric },
            ..
        } => Some(metric.clone()),
        QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Aggregate { child, .. }
        | QueryExpr::Dedup { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::PromqlSubquery { child, .. }
        | QueryExpr::TimeRange { child, .. }
        | QueryExpr::TimeShift { child, .. }
        | QueryExpr::SQLWindowFunc { child, .. } => find_metric_in_query_expr(child),
        QueryExpr::Concat { children, .. } => children.iter().find_map(find_metric_in_query_expr),
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
        AccuracyBound, Capability, SketchSampleState, SketchStore, SummarySeriesMetadata,
    };
    use planner_types::post_asap::{SummaryField, SummarySchema};
    use planner_types::pre_asap::{Column, ColumnRef, DataType, Schema};
    use std::rc::Rc;

    #[test]
    fn keyed_count_state_follows_planner_family_and_query_readout() {
        use asap_physical_operators::summary_kernels::KeyedSumCountAccumulator;
        use asap_types::query_plan::ExactReadout;

        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);
        let mut payload = KeyedSumCountAccumulator::for_family(ExactKind::Count);
        payload.update(key.clone(), 10.0);
        payload.update(key.clone(), 20.0);
        let state = GroupState::ExactAgg {
            entries: vec![Rc::new(BTreeMap::from([(
                60_000,
                Arc::new(payload) as Arc<dyn AggregateCore>,
            )]))],
            agg_type: AggregationType::Count,
        };
        assert_eq!(
            state.exact_value_for(ExactReadout::Count, &Some(key.clone()), 0, 60_000),
            Ok(Some(2.0))
        );
        assert!(state
            .exact_value_for(ExactReadout::Sum, &Some(key), 0, 60_000)
            .is_err());
    }

    #[test]
    fn state_merge_rejects_different_planner_families() {
        let index = SketchStore::new();
        let context = QueryExecutionContext {
            index: &index,
            t0_ms: 0,
            t1_ms: 60_000,
            is_cumulative: true,
            allowed_materializations: None,
        };
        let states = vec![
            GroupState::ExactAgg {
                entries: vec![],
                agg_type: AggregationType::Rate,
            },
            GroupState::ExactAgg {
                entries: vec![],
                agg_type: AggregationType::Increase,
            },
        ];
        assert!(matches!(
            context.merge_states(states),
            Err(SummaryExecutorError::UnsupportedFamily)
        ));
    }

    #[test]
    fn pane_only_reads_require_the_planned_evaluation_phase() {
        let binding = asap_types::query_plan::MaterializationBinding {
            full_window_slide_ms: None,
            item_labels: Vec::new(),
            materialization: asap_types::PolicyFingerprint(7).into(),
            stored_output_reference: asap_types::sds::StoredOutputReference::for_output(
                asap_types::PolicyFingerprint(7).into(),
            ),
            output_grouping: asap_types::query_plan::PhysicalGrouping::PerEntity,
            window_ms: 60_000,
            pane_origin_ms: Some(7_000),
            readout_lookback_ms: Some(60_000),
        };
        validate_binding_phase(&binding, 67_000).unwrap();
        assert!(validate_binding_phase(&binding, 68_000).is_err());

        let legacy = asap_types::query_plan::MaterializationBinding {
            full_window_slide_ms: None,
            item_labels: Vec::new(),
            pane_origin_ms: None,
            ..binding
        };
        assert!(validate_binding_phase(&legacy, 67_000).is_err());
    }

    fn kll_meta(sid: u64, metric: &str, group_by: &[&str]) -> SummarySeriesMetadata {
        let cfg = SketchConfig::Kll { k: 200 };
        SummarySeriesMetadata {
            storage_handle: sid,
            metric_name: metric.to_string(),
            group_by_keys: group_by
                .iter()
                .map(|s| s.to_string())
                .collect::<BTreeSet<_>>(),
            capability: Some(Capability::QuantileApprox(Some(SketchAlgorithm::Kll))),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                algorithm: SketchAlgorithm::Kll,
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

    // Neighboring overlapping complete windows are not additional answer panes.
    #[test]
    fn full_window_sketch_read_excludes_neighboring_windows() {
        use asap_types::query_plan::{MaterializationBinding, PhysicalGrouping};
        let index = SketchStore::new();
        let fp = asap_types::PolicyFingerprint(703);
        let mut metadata = kll_meta(1, "m", &[]);
        metadata.policy_fp = fp;
        index.register(metadata);
        for (start, values) in [
            (0, vec![1_000.0, 2_000.0, 3_000.0]),
            (20_000, vec![10.0, 20.0, 30.0]),
            (40_000, vec![1_000.0, 2_000.0, 3_000.0]),
        ] {
            index.append_sample(
                1,
                BTreeMap::new(),
                (start, start + 60_000),
                SketchSampleState {
                    bytes: encode_kll_items_proto(200, &values),
                    encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
                },
            );
        }
        let context = QueryExecutionContext {
            index: &index,
            t0_ms: 20_000,
            t1_ms: 80_000,
            is_cumulative: true,
            allowed_materializations: Some(BTreeSet::from([fp])),
        };
        let binding = MaterializationBinding {
            full_window_slide_ms: Some(20_000),
            materialization: fp.into(),
            stored_output_reference: super::super::test_plan::bound_reference(&index, fp.into()),
            output_grouping: PhysicalGrouping::PerEntity,
            item_labels: vec![],
            window_ms: 60_000,
            pane_origin_ms: Some(0),
            readout_lookback_ms: Some(60_000),
        };
        let unauthorized = QueryExecutionContext {
            allowed_materializations: Some(BTreeSet::from([asap_types::PolicyFingerprint(999)])),
            ..context
        };
        assert!(unauthorized.read_bound_materialization(&binding).is_err());
        let context = QueryExecutionContext {
            allowed_materializations: Some(BTreeSet::from([fp])),
            ..unauthorized
        };
        let states = context.read_bound_materialization(&binding).unwrap();
        let SummaryValue::Points(points, coverage) = context
            .readout_bound(&states[0].1, &SketchQuery::Quantile { q: 0.5 })
            .unwrap()
        else {
            panic!("expected points");
        };
        assert_eq!(points, vec![(80_000, 20.0)]);
        assert_eq!(coverage, Some((80_000, 80_000)));
    }

    /// One installed frequency summary merges panes before all four readouts.
    #[test]
    fn bound_univmon_merges_panes_for_four_readouts() {
        use crate::storage_engines::sketch_db::index::SketchEncoding;
        use crate::storage_engines::types::SerializableToSink;
        use asap_physical_operators::summary_kernels::univmon::UnivMonAccumulator;
        use asap_types::query_plan::{MaterializationBinding, PhysicalGrouping};
        let index = SketchStore::new();
        let fp = asap_types::PolicyFingerprint(701);
        let mut meta = kll_meta(1, "m", &["job"]);
        meta.policy_fp = fp;
        meta.agg_kind = AggKind::Sketch {
            algorithm: SketchAlgorithm::UnivMon,
            config: SketchConfig::UnivMon {
                heap_size: 32,
                sketch_rows: 5,
                sketch_cols: 1024,
                layers: 4,
            },
            spatial_filter_canonical: String::new(),
        };
        meta.accuracy = None;
        meta.capability = Some(Capability::CardinalityApprox);
        index.register(meta);
        for (start, values) in [(0, [1.0, 2.0]), (1000, [2.0, 3.0])] {
            let mut state = UnivMonAccumulator::new(32, 5, 1024, 4).unwrap();
            for value in values {
                state.insert_sample(value).unwrap();
            }
            index.append_sample(
                1,
                BTreeMap::from([("job".into(), "a".into())]),
                (start, start + 1000),
                SketchSampleState {
                    bytes: state.serialize_to_bytes(),
                    encoding: SketchEncoding::MsgpackFull,
                },
            );
        }
        let context = QueryExecutionContext {
            index: &index,
            t0_ms: 0,
            t1_ms: 2000,
            is_cumulative: true,
            allowed_materializations: Some(BTreeSet::from([fp])),
        };
        let binding = MaterializationBinding {
            full_window_slide_ms: None,
            materialization: fp.into(),
            stored_output_reference: super::super::test_plan::bound_reference(&index, fp.into()),
            output_grouping: PhysicalGrouping::PerEntity,
            item_labels: vec![],
            window_ms: 1000,
            pane_origin_ms: Some(0),
            readout_lookback_ms: Some(2000),
        };
        let states = context.read_bound_materialization(&binding).unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].0.get("job").unwrap(), "a");
        for (query, expected) in [
            (
                SketchQuery::PointCount {
                    key: ColumnRef::SampleValue,
                    value: None,
                },
                4.0,
            ),
            (SketchQuery::Cardinality, 3.0),
            (SketchQuery::FrequencyL2, 6.0f64.sqrt()),
            (SketchQuery::FrequencyEntropy, 1.5),
        ] {
            let SummaryValue::Points(points, _) =
                context.readout_bound(&states[0].1, &query).unwrap()
            else {
                panic!("expected scalar points")
            };
            assert_eq!(points.len(), 1);
            assert!(
                (points[0].1 - expected).abs() < 0.05,
                "{query:?}: {:?}",
                points
            );
        }
        let mut unknown = binding;
        unknown.materialization = asap_types::PolicyFingerprint(702).into();
        assert!(context.read_bound_materialization(&unknown).is_err());
    }

    #[test]
    fn typed_dds_quantile_interpolates_without_changing_portable_rank_semantics() {
        let mut sketch = asap_sketchlib::DdSketch::new(0.01);
        assert!(sketch_query_value(
            &SummaryState::Dd(sketch.clone()),
            &SketchQuery::Quantile { q: 0.9 }
        )
        .is_err());
        sketch.update(20.0);
        for q in [0.0, 0.5, 0.9, 1.0] {
            let value = sketch_query_value(
                &SummaryState::Dd(sketch.clone()),
                &SketchQuery::Quantile { q },
            )
            .unwrap();
            assert!((value - 20.0).abs() <= 0.2);
        }
        sketch.update(40.0);
        assert!(sketch.quantile(0.9).unwrap() < 21.0);
        for (q, expected) in [(0.0, 20.0), (0.5, 30.0), (0.9, 38.0), (1.0, 40.0)] {
            let value = sketch_query_value(
                &SummaryState::Dd(sketch.clone()),
                &SketchQuery::Quantile { q },
            )
            .unwrap();
            assert!((value - expected).abs() <= expected * 0.01);
        }
        assert!(sketch_query_value(
            &SummaryState::Dd(sketch),
            &SketchQuery::Quantile { q: f64::NAN }
        )
        .is_err());
    }
}
