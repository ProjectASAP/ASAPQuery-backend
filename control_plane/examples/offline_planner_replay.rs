//! Offline control-plane binding, without starting collectors or query servers.
use std::{collections::HashSet, rc::Rc, time::Instant};

use anyhow::{bail, Context, Result};
use asap_aware_mapping::empirical_cost::{
    EmpiricalEvidenceProvider, EvidenceArtifact, EvidenceContext,
};
use control_plane::{
    physical::post_asap::{
        bind_query_expr_with_cost_model, cost_model::ControlPlaneCostModel, PhysicalExpr,
        PostAsapPlan,
    },
    query_parser::parse_query_expr_canonical,
    types_v2::AccuracyTarget,
};
use planner_types::post_asap::{SummaryExpr, SummaryFamilyType, SummaryNode};
use serde_json::{json, Value};

fn inspect(
    node: &Rc<SummaryNode>,
    model: &ControlPlaneCostModel,
    seen: &mut HashSet<usize>,
    states: &mut Vec<Value>,
    raw: &mut usize,
) {
    if !seen.insert(Rc::as_ptr(node) as usize) {
        return;
    }
    match &node.expr {
        SummaryExpr::KeepPreAsap(_) => *raw += 1,
        SummaryExpr::SummaryAgg { family, child, .. } => {
            if let SummaryFamilyType::Sketch(kind, _) = family {
                let lookup = model
                    .offline_evidence()
                    .map(|provider| provider.lookup(kind.algorithm(), kind.params()));
                let (measurement, reason) = match lookup {
                    Some(Ok(row)) => (
                        Some(json!({
                            "record_id": row.id,
                            "provenance": row.provenance,
                            "update_cpu_ns": row.metrics.update_cpu_ns,
                            "retained_bytes": row.metrics.retained_bytes,
                            "offline_error_observation": row.error,
                            "error_applies_to_current_query": false,
                        })),
                        None,
                    ),
                    Some(Err(error)) => (None, Some(error.to_string())),
                    None => (None, Some("offline evidence not supplied".into())),
                };
                states.push(json!({"algorithm":kind.algorithm(), "params":kind.params(), "measurement":measurement, "unavailable_reason":reason}));
            } else {
                states.push(json!({"exact_family":format!("{family:?}")}));
            }
            inspect(child, model, seen, states, raw);
        }
        SummaryExpr::SummaryEstimate { summary_input, .. }
        | SummaryExpr::SummaryDelete { summary_input, .. } => {
            inspect(summary_input, model, seen, states, raw)
        }
        SummaryExpr::SummaryMerge { children } => {
            for child in children {
                inspect(child, model, seen, states, raw);
            }
        }
        SummaryExpr::SummaryJoin {
            outer: lhs,
            inner: rhs,
            ..
        }
        | SummaryExpr::SummarySubtract {
            left: lhs,
            right: rhs,
        }
        | SummaryExpr::BinaryOp { lhs, rhs, .. } => {
            inspect(lhs, model, seen, states, raw);
            inspect(rhs, model, seen, states, raw);
        }
    }
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 1 && args.len() != 3 {
        bail!("usage: offline_planner_replay QUERIES.txt [EVIDENCE.json CONTEXT.json]");
    }
    let corpus = std::fs::read_to_string(&args[0])?;
    let evidence = if args.len() == 3 {
        let artifact: EvidenceArtifact = serde_json::from_str(&std::fs::read_to_string(&args[1])?)?;
        let context: EvidenceContext = serde_json::from_str(&std::fs::read_to_string(&args[2])?)?;
        Some((artifact, context))
    } else {
        None
    };
    let mut rows = Vec::new();
    let mut modes = vec!["exact", "default"];
    if evidence.is_some() {
        modes.push("empirical");
    }
    for mode in modes {
        let accuracy = if mode == "exact" {
            AccuracyTarget::Exact
        } else {
            AccuracyTarget::Epsilon(0.01)
        };
        let mut model = ControlPlaneCostModel::new(accuracy.clone());
        if mode == "empirical" {
            let (artifact, context) = evidence.as_ref().context("missing evidence")?;
            model = model.with_offline_evidence(EmpiricalEvidenceProvider::new(
                artifact.clone(),
                context.clone(),
            )?);
        }
        for query in corpus
            .lines()
            .map(str::trim)
            .filter(|q| !q.is_empty() && !q.starts_with('#'))
        {
            let start = Instant::now();
            let result = parse_query_expr_canonical(query, accuracy.clone()).and_then(|expr| {
                bind_query_expr_with_cost_model(&expr, &model).map_err(Into::into)
            });
            let elapsed_ns = start.elapsed().as_nanos();
            let result = match result {
                Ok(PhysicalExpr::Committed(PostAsapPlan::Summary(node))) => {
                    let mut states = Vec::new();
                    let mut raw = 0;
                    inspect(&node, &model, &mut HashSet::new(), &mut states, &mut raw);
                    json!({"status":"bound", "states":states, "raw_subtrees":raw,
                        "root_raw_fallback":matches!(node.expr, SummaryExpr::KeepPreAsap(_)),
                        "summary_plan":format!("{node:#?}")})
                }
                Ok(plan) => json!({"status":"other_physical_plan", "plan":format!("{plan:?}")}),
                Err(error) => json!({"status":"rejected", "reason":error.to_string()}),
            };
            rows.push(
                json!({"query":query,"mode":mode,"planning_elapsed_ns":elapsed_ns,"result":result}),
            );
        }
    }
    serde_json::to_writer_pretty(
        std::io::stdout(),
        &json!({
            "schema_version":1,
            "evaluation":"offline control-plane parser and typed summary binder",
            "corpus_path":args[0],
            "offline_context":evidence.as_ref().map(|(_, context)|context),
            "limitations":["No deployed execution or measured end-to-end speedup", "Binding does not establish executable placement or complete physical cost", "Offline point-frequency errors do not establish current query guarantees", "raw_subtrees includes necessary source scans beneath summaries"],
            "estimated_end_to_end_savings":null,
            "rows":rows
        }),
    )?;
    Ok(())
}
