//! Bind a typed fixed-snapshot point-frequency query using offline evidence.
use std::rc::Rc;

use anyhow::{bail, Result};
use asap_aware_mapping::empirical_comparison::{
    OfflineComparisonEvidence, OfflineComparisonRequest,
};
use control_plane::{
    physical::post_asap::{bind_query_expr_with_cost_model, cost_model::ControlPlaneCostModel},
    planner_selection::frequency,
    types_v2::AccuracyTarget,
};
use planner_types::pre_asap::{AggIntent, Column, DataType, QueryExpr, Reduction, Schema, Source};
use serde_json::json;

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 4 {
        bail!("usage: offline_frequency_plan COMPARISON.json REQUEST.json INTEGER_KEY EPSILON");
    }
    let evidence: OfflineComparisonEvidence =
        serde_json::from_str(&std::fs::read_to_string(&args[0])?)?;
    let request: OfflineComparisonRequest =
        serde_json::from_str(&std::fs::read_to_string(&args[1])?)?;
    let item: i64 = args[2].parse()?;
    let epsilon: f64 = args[3].parse()?;
    if !epsilon.is_finite() || epsilon <= 0.0 || epsilon >= 1.0 {
        bail!("epsilon must be finite and strictly between zero and one");
    }
    let accuracy = AccuracyTarget::Epsilon(epsilon);
    let model = ControlPlaneCostModel::new(accuracy.clone())
        .with_offline_frequency_comparison(evidence, request.clone());
    let intent = frequency(accuracy, Some(("key".into(), item.to_string())));
    let AggIntent::Extension { payload, .. } = &intent else {
        unreachable!()
    };
    let recommendation = model.offline_frequency_recommendation(payload);
    let query = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![intent],
        output_names: vec![],
        having: None,
        child: Rc::new(QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: "offline_integer_snapshot".into(),
            },
            predicates: vec![],
            schema: Schema::with_time_index(
                vec![
                    Column::new("ts", DataType::Timestamp, false),
                    Column::new("key", DataType::Int64, false),
                    Column::new("value", DataType::Float64, false),
                ],
                0,
                vec![],
            ),
        }),
    };
    let bound = bind_query_expr_with_cost_model(&query, &model)?;
    let root_raw_fallback = matches!(&bound,
        control_plane::physical::post_asap::PhysicalExpr::Committed(
            control_plane::physical::post_asap::PostAsapPlan::Summary(node)
        ) if matches!(node.expr, planner_types::post_asap::SummaryExpr::KeepPreAsap(_)));
    let (recommendation, unavailable_reason) = match recommendation {
        Ok(value) => (Some(value), None),
        Err(reason) => (None, Some(reason)),
    };
    serde_json::to_writer_pretty(
        std::io::stdout(),
        &json!({
            "scope":"typed offline point-frequency recommendation and control-plane binding",
            "request":request, "item":item, "formal_epsilon":epsilon,
            "recommendation":recommendation,"unavailable_reason":unavailable_reason,
        "bound_plan":format!("{bound:#?}"), "root_raw_fallback":root_raw_fallback,
            "limitations":["The caller asserts the fixed-snapshot integer-key benchmark context",
                "Observed mean error applies to the recorded offline probe population, not an individual key guarantee",
                "No materializations deployed or data-plane execution performed"]
        }),
    )?;
    Ok(())
}
