//! Bind exact current-value aggregations without treating historical samples as a population.
use super::compiler::{CompileError, PlanningQuery, PlanningRequest};
use asap_types::query_plan::{
    current_series::{SeriesPopulation, SeriesReadout},
    logical::{Grouping, LabelMatch, LabelMatcher, LogicalOperator},
};
use promql_parser::{
    label::MatchOp,
    parser::{self, Expr, LabelModifier},
};

fn parse(query: &str) -> Option<(SeriesPopulation, SeriesReadout)> {
    let Expr::Aggregate(a) = parser::parse(query).ok()? else {
        return None;
    };
    let Expr::VectorSelector(selector) = a.expr.as_ref() else {
        return None;
    };
    if selector.offset.is_some()
        || selector.at.is_some()
        || !selector.matchers.or_matchers.is_empty()
    {
        return None;
    }
    let Expr::NumberLiteral(parameter) = a.param.as_deref()? else {
        return None;
    };
    if !parameter.val.is_finite() {
        return None;
    }
    let readout = match a.op.to_string().as_str() {
        "quantile" => SeriesReadout::Quantile { q: parameter.val },
        "topk" => SeriesReadout::TopK {
            k: (parameter.val as i64).max(0) as u64,
        },
        _ => return None,
    };
    let mut grouping = match &a.modifier {
        None => Grouping {
            labels: vec![],
            without: false,
        },
        Some(LabelModifier::Include(labels)) => Grouping {
            labels: labels.labels.clone(),
            without: false,
        },
        Some(LabelModifier::Exclude(labels)) => Grouping {
            labels: labels.labels.clone(),
            without: true,
        },
    };
    grouping.labels.sort();
    grouping.labels.dedup();
    let mut matchers: Vec<_> = selector
        .matchers
        .matchers
        .iter()
        .map(|m| LabelMatcher {
            name: m.name.clone(),
            value: m.value.clone(),
            operation: match m.op {
                MatchOp::Equal => LabelMatch::Equal,
                MatchOp::NotEqual => LabelMatch::NotEqual,
                MatchOp::Re(_) => LabelMatch::Regex,
                MatchOp::NotRe(_) => LabelMatch::NotRegex,
            },
        })
        .collect();
    matchers.sort_by_key(|m| {
        (
            m.name.clone(),
            m.value.clone(),
            format!("{:?}", m.operation),
        )
    });
    Some((
        SeriesPopulation {
            metric: selector.name.clone()?,
            matchers,
            grouping,
            lookback_ms: 300_000,
            max_input_lag_ms: 60_000,
            max_series: 100_000,
            max_bytes: 64 * 1024 * 1024,
            max_k: 0,
            quantiles: false,
        },
        readout,
    ))
}

pub(super) fn supported(request: &PlanningRequest) -> bool {
    request
        .queries
        .iter()
        .any(|q| parse(&q.query_string).is_some())
}

pub(super) fn operator(
    request: &PlanningRequest,
    query: &PlanningQuery,
) -> Result<Option<LogicalOperator>, CompileError> {
    let Some((mut population, readout)) = parse(&query.query_string) else {
        return Ok(None);
    };
    // Compare source/group semantics before adding workload-wide resource bounds.
    for other in &request.queries {
        if let Some((other_population, other_readout)) = parse(&other.query_string) {
            let mut identity = population.clone();
            identity.max_k = 0;
            identity.quantiles = false;
            if identity == other_population {
                match other_readout {
                    SeriesReadout::TopK { k } => population.max_k = population.max_k.max(k),
                    SeriesReadout::Quantile { .. } => population.quantiles = true,
                }
            }
        }
    }
    let populations: std::collections::BTreeSet<_> = request
        .queries
        .iter()
        .filter_map(|q| parse(&q.query_string).map(|(p, _)| p.key()))
        .collect();
    population.max_bytes = request
        .retained_summary_memory_budget_bytes
        .unwrap_or(population.max_bytes)
        .min(1_073_741_824)
        / populations.len().max(1) as u64;
    population.max_series = population
        .max_series
        .min((population.max_bytes / 1024) as usize);
    population.max_input_lag_ms = request
        .source_sample_interval_ms
        .unwrap_or(60_000)
        .saturating_add(request.query_staleness_margin_ms)
        .clamp(1, 300_000);
    population.validate()?;
    Ok(Some(LogicalOperator::CurrentSeries {
        population,
        readout,
    }))
}
