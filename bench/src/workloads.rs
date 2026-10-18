//! The two benefit workloads as Planner `PlanningWorkload`s, with data facts
//! from the generator.

use std::path::Path;

use anyhow::Context;
use planner_types::workload::PlanningWorkload;
use serde::Serialize;
use serde_json::Value;

use crate::generator::{catalog_metrics, Generator, Scale};
use crate::rules::{self, SurveyedRule};

/// One query of a workload, as the harness evaluates it.
#[derive(Debug, Clone, Serialize)]
pub struct WorkloadQuery {
    pub name: String,
    pub query: String,
    pub interval_ms: i64,
    /// The query's `AccuracyTarget` (JSON).
    pub accuracy: Value,
}

pub struct Workload {
    pub queries: Vec<WorkloadQuery>,
    /// Raw metrics the queries read: what the generator writes.
    pub metrics: Vec<String>,
    pub planning: PlanningWorkload,
}

/// The planner-layering examples with recurring queries that run in
/// minutes. Example 3a (one-off reports over years) and 4a (monthly
/// reports over years) need years of data and are left out; Example 4's
/// B3 has Example 3b's query, so it is not repeated.
pub const EXAMPLES: &[&str] = &["example1", "example3b", "example4b"];

fn build(queries: Vec<WorkloadQuery>, generator: &Generator) -> anyhow::Result<Workload> {
    let catalog = catalog_metrics();
    let mut metrics: Vec<String> = queries
        .iter()
        .flat_map(|q| rules::metrics_read(&q.query, &catalog).0)
        .collect();
    metrics.sort();
    metrics.dedup();
    let entries = queries
        .iter()
        .map(|q| rules::entry(&q.query, q.interval_ms as u64, &q.accuracy))
        .collect();
    let planning = rules::workload(entries, generator.data_workload(&metrics))?;
    Ok(Workload {
        queries,
        metrics,
        planning,
    })
}

/// Example queries from `examples/`, each with its own interval and
/// accuracy target.
pub fn examples(dir: &Path, scale: Scale) -> anyhow::Result<Workload> {
    let mut queries = Vec::new();
    for example in EXAMPLES {
        let path = dir.join(format!("planner-layering-{example}.json"));
        let w: Value = serde_json::from_str(
            &std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?,
        )?;
        for (i, e) in w["query_workload"]["repeating_queries"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
        {
            queries.push(WorkloadQuery {
                name: format!("{example}/q{}", i + 1),
                query: e["query"].as_str().unwrap_or_default().to_string(),
                interval_ms: e["demand"]["fixed_interval"].as_i64().unwrap_or_default(),
                accuracy: e["requirements"]["accuracy"].clone(),
            });
        }
    }
    let generator = Generator::new(scale, |_| true);
    build(queries, &generator)
}

/// The kept rules of a survey, one query per distinct query text, each at
/// its rule group's interval with `accuracy`.
pub fn kube_rules(
    survey: &[SurveyedRule],
    scale: Scale,
    accuracy: &Value,
) -> anyhow::Result<Workload> {
    let mut queries: Vec<WorkloadQuery> = Vec::new();
    for r in survey.iter().filter(|r| r.dropped.is_none()) {
        if queries.iter().any(|q| q.query == r.rule.query) {
            continue;
        }
        let n = queries
            .iter()
            .filter(|q| q.name.starts_with(&r.rule.name))
            .count();
        queries.push(WorkloadQuery {
            name: if n == 0 {
                r.rule.name.clone()
            } else {
                format!("{}#{}", r.rule.name, n + 1)
            },
            query: r.rule.query.clone(),
            interval_ms: r.rule.interval_seconds as i64 * 1000,
            accuracy: accuracy.clone(),
        });
    }
    let generator = Generator::new(scale, |_| true);
    build(queries, &generator)
}
