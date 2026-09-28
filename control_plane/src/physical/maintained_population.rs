//! Lower typed population operators according to executor membership capabilities.
use super::compiler::{CompileError, PhysicalCompilationRequest, QueryCompilationInput};
use asap_types::query_plan::{
    current_series::{SeriesPopulation, SeriesReadout},
    residual::{Grouping, LabelMatch, LabelMatcher, ResidualQueryOperator},
};
use planner_types::post_asap::{
    maintained_population::*, SummaryExpr, SummaryNode, ValueOperation,
};

fn selected(node: &SummaryNode) -> Option<(&MaintainedPopulation, &PopulationReadout)> {
    let SummaryExpr::ValueOperation {
        child,
        operation: ValueOperation::ReadPopulation { readout },
        ..
    } = &node.expr
    else {
        return None;
    };
    let SummaryExpr::ValueOperation {
        operation: ValueOperation::MaintainPopulation { population },
        ..
    } = &child.expr
    else {
        return None;
    };
    Some((population, readout))
}

pub(super) fn supported_node(node: &SummaryNode) -> bool {
    selected(node).is_some_and(|(population, _)| {
        matches!(population.input, PopulationInput::CurrentSeries(_))
    })
}

pub(super) fn supported(request: &PhysicalCompilationRequest) -> bool {
    request
        .queries
        .iter()
        .any(|q| supported_node(&q.selected_plan_root))
}

pub(super) fn operator(
    request: &PhysicalCompilationRequest,
    query: &QueryCompilationInput,
) -> Result<Option<ResidualQueryOperator>, CompileError> {
    let Some((spec, readout)) = selected(&query.selected_plan_root) else {
        return Ok(None);
    };
    let PopulationInput::CurrentSeries(input) = &spec.input else {
        return Err(CompileError::Query { query_id: query.query_id.clone(), reason: "maintained table-row populations require a row-update executor; remote-write current-series state is incompatible".into() });
    };
    let populations: std::collections::BTreeSet<_> = request
        .queries
        .iter()
        .filter_map(|q| {
            selected(&q.selected_plan_root)
                .map(|(p, _)| serde_json::to_string(p).expect("typed population serializes"))
        })
        .collect();
    let max_bytes = request
        .retained_summary_memory_budget_bytes
        .unwrap_or(64 * 1024 * 1024)
        .min(1_073_741_824)
        / populations.len().max(1) as u64;
    let population = SeriesPopulation {
        metric: input.metric.clone(),
        matchers: input
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
            labels: input.grouping.clone(),
            without: input.without,
        },
        lookback_ms: input.lookback_ms,
        max_k: spec.max_k as u64,
        quantiles: spec.quantiles,
        max_bytes,
        max_series: 100_000.min((max_bytes / 1024) as usize),
        max_input_lag_ms: request
            .scrape_interval_ms
            .unwrap_or(60_000)
            .saturating_add(request.query_retention_margin_ms)
            .max(1)
            .min(input.lookback_ms),
    };
    population.validate()?;
    let readout = match readout {
        PopulationReadout::Quantile { q } => SeriesReadout::Quantile { q: *q },
        PopulationReadout::TopK { k } => SeriesReadout::TopK { k: *k as u64 },
        PopulationReadout::Sum => SeriesReadout::Sum,
        PopulationReadout::Count => SeriesReadout::Count,
        PopulationReadout::Average => SeriesReadout::Average,
    };
    Ok(Some(ResidualQueryOperator::CurrentSeries {
        population,
        readout,
    }))
}
