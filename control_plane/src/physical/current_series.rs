//! Lower Planner-selected current-series operators; never discover query rewrites here.
use super::compiler::{CompileError, PlanningQuery, PlanningRequest};
use asap_types::query_plan::{
    current_series::{SeriesPopulation, SeriesReadout},
    logical::{Grouping, LabelMatch, LabelMatcher, LogicalOperator},
};
use planner_types::post_asap::{current_series::*, SummaryExpr, SummaryNode, ValueOperation};

fn selected(node: &SummaryNode) -> Option<(&CurrentSeriesPopulation, &CurrentSeriesReadout)> {
    let SummaryExpr::ValueOperation {
        child,
        operation: ValueOperation::ReadCurrentSeries { readout },
        ..
    } = &node.expr
    else {
        return None;
    };
    let SummaryExpr::ValueOperation {
        operation: ValueOperation::MaintainCurrentSeries { population },
        ..
    } = &child.expr
    else {
        return None;
    };
    Some((population, readout))
}

pub(super) fn supported(request: &PlanningRequest) -> bool {
    request
        .queries
        .iter()
        .any(|q| selected(&q.post_asap).is_some())
}

pub(super) fn operator(
    request: &PlanningRequest,
    query: &PlanningQuery,
) -> Result<Option<LogicalOperator>, CompileError> {
    let Some((spec, readout)) = selected(&query.post_asap) else {
        return Ok(None);
    };
    let populations: std::collections::BTreeSet<_> = request
        .queries
        .iter()
        .filter_map(|q| {
            selected(&q.post_asap)
                .map(|(p, _)| serde_json::to_string(p).expect("typed population serializes"))
        })
        .collect();
    let max_bytes = request
        .retained_summary_memory_budget_bytes
        .unwrap_or(64 * 1024 * 1024)
        .min(1_073_741_824)
        / populations.len().max(1) as u64;
    let population = SeriesPopulation {
        metric: spec.metric.clone(),
        matchers: spec
            .matchers
            .iter()
            .map(|m| LabelMatcher {
                name: m.label.clone(),
                value: m.value.clone(),
                operation: match m.operation {
                    CurrentSeriesMatch::Equal => LabelMatch::Equal,
                    CurrentSeriesMatch::NotEqual => LabelMatch::NotEqual,
                    CurrentSeriesMatch::Regex => LabelMatch::Regex,
                    CurrentSeriesMatch::NotRegex => LabelMatch::NotRegex,
                },
            })
            .collect(),
        grouping: Grouping {
            labels: spec.grouping.clone(),
            without: spec.without,
        },
        lookback_ms: spec.lookback_ms,
        max_k: spec.max_k as u64,
        quantiles: spec.quantiles,
        max_bytes,
        max_series: 100_000.min((max_bytes / 1024) as usize),
        max_input_lag_ms: request
            .source_sample_interval_ms
            .unwrap_or(60_000)
            .saturating_add(request.query_staleness_margin_ms)
            .clamp(1, 300_000),
    };
    population.validate()?;
    let readout = match readout {
        CurrentSeriesReadout::Quantile { q } => SeriesReadout::Quantile { q: *q },
        CurrentSeriesReadout::TopK { k } => SeriesReadout::TopK { k: *k as u64 },
    };
    Ok(Some(LogicalOperator::CurrentSeries {
        population,
        readout,
    }))
}
