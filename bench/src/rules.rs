//! The Kubernetes rule set: apiserver, etcd and kubelet rules from a
//! recorded rule trace, which ones the Planner and the data plane can run,
//! and the workload they form.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::Context;
use asap_executor::physical_planner::frontier_from_timing;
use control_plane::{plan_workload, DeploymentInputs};
use data_plane::precompute::Precompute;
use data_plane::query::{QueryEngine, Unanswered};
use data_plane::sds::{Sds, SummaryStore};

use crate::compare::SampleIndex;
use planner_types::workload::PlanningWorkload;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// One rule of the trace (`rule_trace.jsonl`).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Rule {
    pub name: String,
    pub query: String,
    pub interval_seconds: u64,
    #[serde(rename = "type")]
    pub kind: String,
    pub group: String,
}

/// A rule of the set and whether it runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurveyedRule {
    #[serde(flatten)]
    pub rule: Rule,
    /// Raw metrics it reads.
    pub metrics: Vec<String>,
    /// `None` when kept; otherwise why it is dropped.
    pub dropped: Option<String>,
}

/// The rule groups of the set: those of the apiserver, etcd and kubelet.
pub fn in_rule_set(group: &str) -> bool {
    ["apiserver", "etcd", "kubelet"]
        .iter()
        .any(|k| group.contains(k))
}

pub fn read_trace(path: &Path) -> anyhow::Result<Vec<Rule>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut rules: Vec<Rule> = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let rule: Rule = serde_json::from_str(line)?;
        if in_rule_set(&rule.group)
            && !rules
                .iter()
                .any(|r| r.name == rule.name && r.query == rule.query)
        {
            rules.push(rule);
        }
    }
    Ok(rules)
}

/// Identifiers outside quotes and label matchers: metric names, functions,
/// keywords and grouping labels.
fn identifiers(query: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = query.chars().peekable();
    let mut depth = 0;
    let mut current = String::new();
    while let Some(c) = chars.next() {
        if c == '"' {
            while let Some(d) = chars.next() {
                if d == '\\' {
                    chars.next();
                } else if d == '"' {
                    break;
                }
            }
            continue;
        }
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            _ => {}
        }
        if depth == 0 && (c.is_ascii_alphanumeric() || c == '_' || c == ':') {
            current.push(c);
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out.retain(|t| !t.chars().next().unwrap().is_ascii_digit());
    out
}

/// The generated metrics `query` reads, and the recording-rule outputs
/// (names with a colon) it reads.
pub fn metrics_read(query: &str, catalog: &[String]) -> (Vec<String>, Vec<String>) {
    let ids: BTreeSet<String> = identifiers(query).into_iter().collect();
    let metrics = ids
        .iter()
        .filter(|i| catalog.contains(i))
        .cloned()
        .collect();
    let recorded = ids.iter().filter(|i| i.contains(':')).cloned().collect();
    (metrics, recorded)
}

/// One repeating PromQL query entry of a Planner workload.
pub fn entry(query: &str, interval_ms: u64, accuracy: &Value) -> Value {
    json!({
        "query": query,
        "demand": {"fixed_interval": interval_ms},
        "requirements": {"accuracy": accuracy, "max_latency_ms": null},
        "predictability": {"predictable": {"known_at": null}},
    })
}

pub fn workload(entries: Vec<Value>, data_workload: Value) -> anyhow::Result<PlanningWorkload> {
    Ok(serde_json::from_value(json!({
        "query_workload": {"language": "promql", "query_batch": null, "repeating_queries": entries},
        "data_workload": data_workload,
    }))?)
}

/// Why the data plane cannot run `workload`'s plan, if it cannot: the plan
/// does not compile into the data plane, or an evaluation at `probe_ms`
/// over generated samples fails (for example an operator the executor
/// rejects only when it runs). A stored pane that is missing is not a
/// failure: nothing was ingested.
pub fn data_plane_check(
    workload: &PlanningWorkload,
    inputs: &DeploymentInputs,
    index: &SampleIndex,
    probe_ms: i64,
) -> Result<(), String> {
    let plan = plan_workload(workload, inputs).map_err(|e| format!("Planner: {e}"))?;
    let dag = &plan.dag.dag;
    let frontier = frontier_from_timing(dag).map_err(|e| format!("data plane: {e}"))?;
    let sds =
        Sds::from_dag(dag, &frontier).map_err(|e| format!("data plane (stored summaries): {e}"))?;
    Precompute::new(dag.clone(), &sds, 0).map_err(|e| format!("data plane (precompute): {e}"))?;
    let mut engine = QueryEngine::new(&plan, &sds);
    let store = SummaryStore::new();
    for query in &plan.queries {
        let (root, ranges) = match engine.raw_ranges(query, probe_ms, &store) {
            Err(Unanswered::Unsupported(reason)) => {
                return Err(format!("data plane (query engine): {reason}"))
            }
            Err(Unanswered::NotPlanned(q)) => return Err(format!("data plane: no root for {q}")),
            Err(_) => continue,
            Ok(answer) => answer,
        };
        let raw = ranges
            .iter()
            .map(|r| {
                (
                    r.node,
                    index.range(&r.metric, r.start_ms, r.end_ms).to_vec(),
                )
            })
            .collect();
        match engine.evaluate(root, probe_ms, &store, None, &raw) {
            Err(Unanswered::MissingPane(_)) | Ok(_) => {}
            Err(e) => {
                return Err(format!(
                    "data plane (evaluation over generated samples): {e}"
                ))
            }
        }
    }
    Ok(())
}

/// Surveys every rule alone: dropped when it reads a recording rule's
/// output, when the Planner cannot plan it, or when the data plane cannot
/// run its plan.
pub fn survey(
    rules: &[Rule],
    catalog: &[String],
    data_workload: impl Fn(&[String]) -> Value,
    accuracy: &Value,
    inputs: &DeploymentInputs,
    index: &SampleIndex,
    probe_ms: i64,
) -> Vec<SurveyedRule> {
    rules
        .iter()
        .map(|rule| {
            let (metrics, recorded) = metrics_read(&rule.query, catalog);
            let dropped = if !recorded.is_empty() {
                Some(format!(
                    "reads recording-rule output {}; the generator writes raw series only",
                    recorded.join(", ")
                ))
            } else if metrics.is_empty() {
                Some("reads no generated metric".into())
            } else {
                let entries = vec![entry(&rule.query, rule.interval_seconds * 1000, accuracy)];
                match workload(entries, data_workload(&metrics)) {
                    Err(e) => Some(format!("workload: {e}")),
                    Ok(w) => data_plane_check(&w, inputs, index, probe_ms).err(),
                }
            };
            SurveyedRule {
                rule: rule.clone(),
                metrics,
                dropped,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Metric names are read outside label matchers and quotes; colon names
    /// are recording-rule outputs.
    #[test]
    fn reads_metrics_and_recorded_outputs() {
        let catalog = vec!["up".to_string(), "apiserver_request_total".to_string()];
        let (metrics, recorded) = metrics_read(
            r#"sum by (cluster) (rate(apiserver_request_total{job="up"}[5m])) / sum(apiserver_request:burnrate1h)"#,
            &catalog,
        );
        assert_eq!(metrics, vec!["apiserver_request_total"]);
        assert_eq!(recorded, vec!["apiserver_request:burnrate1h"]);
    }
}
