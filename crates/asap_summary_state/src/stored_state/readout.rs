//! Planner-declared readouts over reconstructed summary states.
use super::delta_apply::SummaryState;
use asap_physical_operators::AggregateCore;
use planner_types::{post_asap::SketchQuery, pre_asap::ColumnRef};
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Unsupported(&'static str),
}
pub fn sketch_query_value(rs: &SummaryState, query: &SketchQuery) -> Result<f64, Error> {
    if let SummaryState::UnivMon(state) = rs {
        return state
            .estimate(query)
            .map_err(|_| Error::Unsupported("unsupported UnivMon readout"));
    }
    if matches!(rs, SummaryState::WeightedFrequency(_)) {
        return Err(Error::Unsupported(
            "Planner weighted-frequency heaps support TopK readout only",
        ));
    }
    match query {
        SketchQuery::FrequencyL2 | SketchQuery::FrequencyEntropy => Err(Error::Unsupported(
            "frequency moment readout requires UnivMon",
        )),
        SketchQuery::Quantile { q } => match rs {
            // Typed PromQL/continuous-percentile readout uses interpolation;
            // portable DDS `quantile` deliberately retains lower-rank parity.
            SummaryState::Dd(sketch) => {
                sketch
                    .inner
                    .quantile_interpolated(*q)
                    .ok_or(Error::Unsupported(
                        "DDS interpolated quantile is unavailable",
                    ))
            }
            SummaryState::Kll(sketch) => sketch
                .estimate(query)
                .map_err(|_| Error::Unsupported("KLL quantile must be in [0, 1]")),
            _ => Ok(rs.quantile(*q)),
        },
        SketchQuery::Cardinality => Ok(rs.cardinality()),
        // `key: ColumnRef::SampleValue, value: None` means "no specific
        // item" -- the bare bucket total. `key: Named(_), value: Some(v)`
        // is a per-item point lookup (e.g. `count(cms_metric{item="x"})`)
        // -- `value` is where the filter's actual value lives (see
        // `planner_types::post_asap::SketchQuery::PointCount`'s doc for why `readout`
        // can't resolve it itself). Any other combination (e.g. a `Named`
        // key with no value, or `SampleValue` with a value) is a shape
        // this executor doesn't expect to see and reports rather than
        // silently misreading.
        SketchQuery::PointCount {
            key: ColumnRef::SampleValue,
            value: None,
        } => rs.total().ok_or(Error::Unsupported(
            "sketch does not preserve total update mass",
        )),
        SketchQuery::PointCount {
            key: ColumnRef::Named(_) | ColumnRef::Qualified { .. },
            value: Some(v),
        } => rs.estimate(v).ok_or(Error::Unsupported(
            "PointCount by key requires a Frequency-family sketch (Cms/CountSketch/..WithHeap)",
        )),
        SketchQuery::PointCount { .. } => Err(Error::Unsupported(
            "unrecognized PointCount shape (key/value combination not expected)",
        )),
        // Both readout callers branch on `TopK` before ever calling this
        // function (see `readout_cumulative`/`readout_per_window`), so
        // this arm is unreachable in practice; kept for match
        // exhaustiveness (`SketchQuery` has no `#[non_exhaustive]`) and to
        // fail loudly rather than panic if that invariant is ever broken.
        SketchQuery::TopK { .. } => Err(Error::Unsupported(
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
pub fn topk_ranked(
    rs: &SummaryState,
    k: usize,
    group: &std::collections::BTreeMap<String, String>,
) -> Result<Vec<(String, f64)>, Error> {
    let mut items = rs.topk_items(group).ok_or(Error::Unsupported(
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

type Parameters = std::collections::HashMap<String, String>;

/// Merge already selected exact panes and read one population; an absent
/// population (empty MIN/MAX, counter with too few samples) is an error.
pub fn exact_readout(
    states: impl IntoIterator<Item = std::sync::Arc<dyn AggregateCore>>,
    statistic: crate::Statistic,
    key: &Option<crate::KeyByLabelValues>,
    parameters: &Parameters,
) -> Result<f64, String> {
    exact_readout_optional(states, statistic, key, parameters)?
        .ok_or_else(|| "empty exact population".to_string())
}

/// PromQL counter readouts omit a series with fewer than two samples. Other
/// state/type/range failures remain errors rather than empty results.
pub fn insufficient_counter_samples(
    state: &dyn AggregateCore,
    statistic: crate::Statistic,
) -> bool {
    asap_physical_operators::readout::insufficient_counter_samples(state, statistic)
}

/// Merge already selected exact panes with Planner's exact readout. `None`
/// means a counter population with too few samples is absent; an empty
/// MIN/MAX population is an error so the query can fall back.
pub fn exact_readout_optional(
    states: impl IntoIterator<Item = std::sync::Arc<dyn AggregateCore>>,
    statistic: crate::Statistic,
    key: &Option<crate::KeyByLabelValues>,
    parameters: &Parameters,
) -> Result<Option<f64>, String> {
    let range = super::codec::range_ms(parameters).map_err(|error| error.to_string())?;
    let value =
        asap_physical_operators::readout::exact_readout(states, statistic, range, key.as_ref())?;
    if value.is_none() && matches!(statistic, crate::Statistic::Min | crate::Statistic::Max) {
        return Err("empty exact population".into());
    }
    Ok(value)
}

#[cfg(test)]
mod counter_tests {
    use super::*;
    use crate::{KeyByLabelValues, Statistic};
    use asap_physical_operators::summary_kernels::exact::ExactAccumulator;
    use planner_types::post_asap::{ExactKind, ExactParams, SummaryFamilyType};
    use std::sync::Arc;

    fn counter(kind: ExactKind, params: ExactParams, keyed: bool) -> ExactAccumulator {
        ExactAccumulator::new(SummaryFamilyType::ExactAggregate(kind, params), keyed).unwrap()
    }
    fn range(start: &str, end: &str) -> Parameters {
        Parameters::from([
            ("range_start_ms".into(), start.into()),
            ("range_end_ms".into(), end.into()),
        ])
    }

    // A counter population with a single sample is absent, keyed or not.
    #[test]
    fn planner_counter_population_omits_insufficient_samples() {
        for (kind, params, statistic) in [
            (ExactKind::Rate, ExactParams::Rate, Statistic::Rate),
            (
                ExactKind::Increase,
                ExactParams::Increase,
                Statistic::Increase,
            ),
        ] {
            for keyed in [false, true] {
                let mut state = counter(kind.clone(), params.clone(), keyed);
                let key = keyed.then(|| KeyByLabelValues::new_with_labels(vec!["checkout".into()]));
                state.update(key.as_ref(), 10., 10_000);
                assert_eq!(
                    exact_readout_optional(
                        [Arc::new(state) as Arc<dyn AggregateCore>],
                        statistic,
                        &key,
                        &Default::default()
                    )
                    .unwrap(),
                    None
                );
            }
        }
    }

    // Repeated or single samples are absent; unknown keys, inverted ranges
    // and empty input remain errors.
    #[test]
    fn sparse_counter_is_absent_but_invalid_ranges_still_fail() {
        let label = KeyByLabelValues::new_with_labels(vec!["checkout".into()]);
        let mut keyed = counter(ExactKind::Rate, ExactParams::Rate, true);
        keyed.update(Some(&label), 10., 10_000);
        keyed.update(Some(&label), 10., 10_000);
        let read = |state: &ExactAccumulator, key: Option<KeyByLabelValues>, p: &Parameters| {
            exact_readout_optional(
                [Arc::new(state.clone()) as Arc<dyn AggregateCore>],
                Statistic::Rate,
                &key,
                p,
            )
        };
        let window = range("0", "60000");
        assert_eq!(read(&keyed, Some(label.clone()), &window).unwrap(), None);
        let mut repeated = counter(ExactKind::Rate, ExactParams::Rate, false);
        repeated.update(None, 10., 10_000);
        repeated.update(None, 10., 10_000);
        assert_eq!(read(&repeated, None, &window).unwrap(), None);
        let missing = KeyByLabelValues::new_with_labels(vec!["missing".into()]);
        assert!(read(&keyed, Some(missing), &window).is_err());
        keyed.update(Some(&label), 20., 20_000);
        assert!(read(&keyed, Some(label.clone()), &window)
            .unwrap()
            .is_some());
        assert!(read(&keyed, Some(label), &range("60000", "0")).is_err());
        assert!(exact_readout_optional([], Statistic::Rate, &None, &window).is_err());
    }

    // An empty MIN population is an error, not an absent series.
    #[test]
    fn empty_extremum_population_is_an_error() {
        let empty = counter(ExactKind::Min, ExactParams::Min, false);
        let states = || [Arc::new(empty.clone()) as Arc<dyn AggregateCore>];
        let none = Parameters::new();
        assert!(exact_readout_optional(states(), Statistic::Min, &None, &none).is_err());
        assert!(exact_readout(states(), Statistic::Min, &None, &none).is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Signed CountSketch counters must not be misreported as population mass.
    #[test]
    fn signed_sketch_bare_count_is_unsupported() {
        let state = SummaryState::CountSketch(
            asap_physical_operators::summary_kernels::CountSketchAccumulator::new(3, 16),
        );
        assert!(sketch_query_value(
            &state,
            &SketchQuery::PointCount {
                key: ColumnRef::SampleValue,
                value: None
            }
        )
        .is_err());
    }
}
