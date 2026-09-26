use crate::input::{at_ms, Dataset, Suite};
use anyhow::{ensure, Context, Result};
use asap_types::query_plan::{residual::ResidualQueryOperator, QueryPlanNode};
use control_plane::physical::{
    compiler::{BackendLocalPlanningInput, CompiledPhysicalPlan},
    workload_cost::CandidateEvaluationStatus,
};
use serde_json::{json, Value};

pub fn snapshot(
    suite: &Suite,
    dataset: &Dataset,
    now: u64,
    base_time_ms: i64,
    benefit: bool,
) -> Result<BackendLocalPlanningInput> {
    let mut value: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-planning-snapshot.json"
    ))?;
    let queries: Vec<_> = suite.queries.iter().map(|q| {
        let ms=q.range.as_ref().map(|r| r.step_seconds*1000.).unwrap_or(60_000.);
        ensure!(ms.fract()==0. && ms>0. && ms<=u32::MAX as f64,"query recurrence needs positive integral milliseconds");
        let mut interval = ms as u64;
        let times = q.instant_offsets_seconds.iter().copied()
            .chain(q.range.iter().map(|r| r.start_offset_seconds))
            .map(|offset| at_ms(base_time_ms, offset)).collect::<Result<Vec<_>>>()?;
        let first = *times.first().context("query has no evaluation times")?;
        for time in times {
            interval = gcd(interval, time.abs_diff(first));
        }
        let phase = first.rem_euclid(interval as i64) as u64;
        Ok(json!({"query":q.expr,"demand":{"fixed_interval_at":{"interval":interval,"evaluation_phase":phase}},
            "requirements":{"accuracy":{"explicit":{"EpsilonDelta":{"epsilon":0.01,"delta":0.01}}},"response_latency":"unspecified"},
            "predictability":{"predictable":{"known_at":null}},"time_selection":{"scope":"real_time","lookback":null,"as_of":null}}))
    }).collect::<Result<_>>()?;
    value["query_workload"]["repeating_queries"] = json!(queries);
    let newest = dataset
        .series
        .iter()
        .flat_map(|s| &s.samples)
        .map(|s| at_ms(base_time_ms, s.offset_seconds))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
        .context("empty replay")?;
    let earliest = suite
        .queries
        .iter()
        .flat_map(|q| {
            q.instant_offsets_seconds
                .iter()
                .copied()
                .chain(q.range.iter().map(|r| r.start_offset_seconds))
        })
        .map(|t| at_ms(base_time_ms, t))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .min()
        .context("no evaluation times")?;
    value["implementation"]["query_staleness_margin_ms"] =
        json!(newest.saturating_sub(earliest).max(0) as u64);

    value["implementation"]["evidence_observed_at_unix_ms"] = json!(now);
    value["implementation"]["evidence_valid_for_ms"] = json!(600_000);
    value["implementation"]["window_cost_model"]["cost"]["observed_at_unix_ms"] = json!(now);
    value["implementation"]["window_cost_model"]["cost"]["valid_for_ms"] = json!(600_000);
    value["environment"]["observed_at_unix_ms"] = json!(now);
    value["environment"]["activation_unix_ms"] = json!(now);
    value["environment"]["max_evidence_age_ms"] = json!(600_000);
    value["environment"]["capability_snapshot_id"] = json!("promql-compliance");
    // Differential fixtures retain their declared compatibility defaults;
    // benefit uses the exact replay population and refuses ambiguous cadence.
    let (rate, cadence, count) = if benefit {
        dataset.uniform_demand()?
    } else {
        (
            100.,
            1000,
            dataset.series.iter().map(|s| s.samples.len()).sum(),
        )
    };
    value["data_workload"]["ingestion_rate"]["value"] = json!(rate);
    for (name, n) in [
        ("input_cardinality", dataset.series.len() as u64),
        ("ingestion_volume", count as u64),
        ("data_ingestion_interval", cadence),
    ] {
        value["data_workload"][name] =
            json!({"value":n,"source":"declared","observed_at_ms":null,"valid_for_ms":null});
    }
    value["implementation"]["scrape_interval_ms"] = json!(cadence);
    // This runner controls the complete finite replay population. Its bounds
    // are a closed-world fixture contract, never an inference about live data.
    let samples: Vec<f64> = dataset
        .series
        .iter()
        .flat_map(|series| series.samples.iter().map(|s| s.value))
        .collect();
    let lower = samples
        .iter()
        .copied()
        .reduce(f64::min)
        .context("empty replay")?;
    let upper = samples
        .iter()
        .copied()
        .reduce(f64::max)
        .context("empty replay")?;
    value["implementation"]["data_snapshot_id"] =
        json!(format!("finite-replay-{}-{now}", dataset.name));
    for query in &suite.queries {
        let root = control_plane::query_parser::parse_query_expr_canonical(
            &query.expr,
            planner_types::types::AccuracyTarget::EpsilonDelta {
                epsilon: 0.01,
                delta: 0.01,
            },
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let planner_types::pre_asap::QueryExpr::BinaryOp { lhs, rhs, .. } = root else {
            continue;
        };
        let domains: Vec<_> = [lhs, rhs].into_iter().filter(|operand| matches!(operand.as_ref(),
            planner_types::pre_asap::QueryExpr::Aggregate { measures, .. }
                if matches!(measures.as_slice(), [planner_types::pre_asap::AggIntent::Quantile { .. } | planner_types::pre_asap::AggIntent::Avg { .. }])
        )).map(|operand| json!({"operand":operand,"lower":lower,"upper":upper,
            "max_samples":samples.len(),"contract":"complete finite fixture replay; no other writers"})).collect();
        if domains.is_empty() {
            continue;
        }
        value["implementation"]["accuracy_evidence"][&query.expr] = json!({
            "query_string": query.expr,
            "data_snapshot_id": value["implementation"]["data_snapshot_id"],
            "data_workload": value["data_workload"],
            "source":"complete-finite-replay", "observed_at_unix_ms":now,"valid_for_ms":600_000,
            "quantile_operand_domains": domains
        });
    }
    Ok(serde_json::from_value(value)?)
}

pub fn validate_cost(plan: &CompiledPhysicalPlan) -> Result<()> {
    let report = plan
        .cost_comparison
        .as_ref()
        .context("missing cost comparison")?;
    ensure!(
        report.model_version == "backend-workload-resources-v1",
        "automatic cost path was bypassed"
    );
    ensure!(
        report.selected_plan_id == plan.envelope.plan_id
            && report.selected_manifest.plan_id == report.selected_plan_id,
        "selected identity mismatch"
    );
    ensure!(
        !report.component_costs.is_empty()
            && report
                .component_costs
                .keys()
                .eq(report.selected_manifest.components.keys()),
        "incomplete selected cost coverage"
    );
    let mut minimum = f64::INFINITY;
    let mut winner = None;
    for candidate in &report.candidate_evaluations {
        let selected = candidate.status == CandidateEvaluationStatus::Selected;
        let Some(total) = candidate.total_cost else {
            ensure!(!selected, "selected uncosted candidate");
            continue;
        };
        let automatic = candidate
            .automatic_cost
            .as_ref()
            .context("priced candidate has no resource breakdown")?;
        ensure!(
            automatic.model_version == report.model_version && !automatic.components.is_empty(),
            "invalid model/empty components"
        );
        ensure!(
            automatic.weights["cpu_seconds"] == 1.
                && automatic.weights["memory_byte_seconds"] == 1e-9
                && automatic.weights["network_bytes"] == 1e-8,
            "unexpected resource weights"
        );
        let mut sum = 0.;
        for (id, r) in &automatic.components {
            ensure!(
                [r.cpu_seconds, r.memory_byte_seconds, r.network_bytes]
                    .iter()
                    .all(|n| n.is_finite() && *n >= 0.),
                "invalid resource quantity {id}"
            );
            ensure!(
                (r.source == "analytical" && r.erp_record_ids.is_empty())
                    || (r.source == "erp+analytical" && !r.erp_record_ids.is_empty()),
                "invalid ERP provenance {id}"
            );
            let cost = r.cpu_seconds + r.memory_byte_seconds * 1e-9 + r.network_bytes * 1e-8;
            ensure!(cost.is_finite(), "resource cost overflow");
            sum += cost;
            if selected {
                ensure!(
                    report
                        .component_costs
                        .get(id)
                        .is_some_and(|n| equal(*n, cost)),
                    "selected component mismatch {id}"
                );
            }
        }
        ensure!(
            sum.is_finite() && total.is_finite() && equal(sum, total),
            "inconsistent candidate total"
        );
        minimum = minimum.min(total);
        if selected {
            ensure!(
                winner.is_none()
                    && candidate.plan_id == Some(report.selected_plan_id)
                    && automatic.components.len() == report.component_costs.len(),
                "selected candidate identity/coverage mismatch"
            );
            winner = Some(total);
        }
    }
    ensure!(
        winner.is_some_and(|n| equal(n, minimum)),
        "winner is not the least fully costed candidate"
    );
    Ok(())
}
fn equal(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-10 * a.abs().max(b.abs()).max(1.)
}

pub fn validate_local(plan: &CompiledPhysicalPlan) -> Result<()> {
    ensure!(!plan.query_plan.entries.is_empty(), "no installed queries");
    for entry in plan.query_plan.entries.values() {
        ensure!(!entry.nodes.is_empty(), "query has no nodes");
        for node in entry.nodes.values() {
            ensure!(
                !matches!(
                    node,
                    QueryPlanNode::ExactFallback { .. }
                        | QueryPlanNode::ExternalExact { .. }
                        | QueryPlanNode::Logical {
                            operator: ResidualQueryOperator::ExactSubquery { .. }
                                | ResidualQueryOperator::CandidateExactSubquery { .. },
                            ..
                        }
                ),
                "{} requires external exact execution",
                entry.query_id
            );
        }
    }
    Ok(())
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
