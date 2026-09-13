//! Costed deployment of Planner-owned table population candidates.
use super::*;
use crate::physical::{
    publication::PhysicalPlanPublication,
    workload_cost::{CostDemand, WorkloadCostEvidence, WorkloadCostManifest},
};
use asap_types::query_plan::{table_rows::TableRowsMaintenance, QueryNodeId, QueryPlanNode};
use planner_types::post_asap::{
    SummaryExpr, SummaryFamilyType, SummaryField, SummaryNode, SummarySchema,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePopulationWorkload {
    pub workload: ClickHouseSqlAutomaticWorkload,
    pub maintenance: TableRowsMaintenance,
    pub horizon_seconds: f64,
    pub query_evaluations: BTreeMap<String, u64>,
    pub capability_snapshot_id: String,
    pub backend_compat: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePopulationDeployment {
    pub workload: TablePopulationWorkload,
    pub evidence: WorkloadCostEvidence,
    pub max_evidence_age_ms: u64,
}
#[derive(Debug, Serialize)]
pub struct TablePopulationCandidate {
    pub manifest: WorkloadCostManifest,
    #[serde(skip_serializing)]
    pub publication: PhysicalPlanPublication,
}
fn invalid(reason: impl Into<String>) -> ClickHousePlanningError {
    ClickHousePlanningError::Lower(reason.into())
}

pub async fn candidates(
    request: &TablePopulationWorkload,
) -> Result<Vec<TablePopulationCandidate>, ClickHousePlanningError> {
    request.maintenance.validate().map_err(invalid)?;
    if !request.horizon_seconds.is_finite()
        || request.horizon_seconds <= 0.0
        || request.capability_snapshot_id.trim().is_empty()
        || request.backend_compat.trim().is_empty()
        || request.workload.queries.is_empty()
        || request.query_evaluations.len() != request.workload.queries.len()
        || request.workload.queries.iter().any(|q| {
            request
                .query_evaluations
                .get(&q.sql)
                .is_none_or(|n| *n == 0)
        })
    {
        return Err(invalid(
            "table population requires complete workload demand and capability identity",
        ));
    }
    let catalog = SqlCatalog {
        tables: request.workload.tables.clone(),
    };
    let mut roots = Vec::new();
    for query in &request.workload.queries {
        roots.push(Rc::new(
            lower_sql_dialect(
                &query.sql,
                &catalog,
                SqlDialect::ClickhouseSQL,
                request.workload.accuracy.clone(),
            )
            .await
            .map_err(|e| invalid(e.to_string()))?,
        ));
    }
    for root in &roots {
        let mut current = root.as_ref();
        while let QueryExpr::Project { cols, child, .. } = current {
            if cols
                .iter()
                .any(|item| !matches!(item.expr, QueryExpr::Column(_)))
            {
                return Err(invalid("maintained SQL populations require column projections; scalar wrappers use native evaluation"));
            }
            current = child;
        }
    }
    let strategy =
        asap_aware_mapping::maintained_population::MaintainedPopulationStrategy::new(&roots);
    let selected = request
        .workload
        .queries
        .iter()
        .zip(&roots)
        .map(|(q, root)| {
            strategy
                .candidate(root)
                .map(|_| {
                    (
                        q.sql.clone(),
                        serde_json::to_value(root).expect("canonical population serializes"),
                    )
                })
                .ok_or_else(|| invalid(format!("SQL has no maintained-row candidate: {}", q.sql)))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;
    let (maintained, _) = compile_automatic_clickhouse_workload_selected(
        &request.workload,
        Some(&selected),
        Some(&request.maintenance),
    )
    .await?;
    let populations = maintained
        .query_plan
        .entries
        .values()
        .flat_map(|e| e.nodes.values())
        .filter_map(|n| {
            if let QueryPlanNode::ReadTablePopulation { population, .. } = n {
                Some(population.key())
            } else {
                None
            }
        })
        .collect::<std::collections::BTreeSet<_>>();
    if populations.len() > 64
        || populations
            .len()
            .saturating_mul(request.maintenance.max_bytes)
            > 1_073_741_824
    {
        return Err(invalid(
            "workload exceeds shared table population memory/task budget",
        ));
    }
    let mut native = maintained.clone();
    native.precompute_plan.executable_dags.clear();
    for entry in native.query_plan.entries.values_mut() {
        let canonical = roots
            .iter()
            .zip(&request.workload.queries)
            .find(|(_, q)| q.sql == entry.query_id)
            .map(|(root, _)| root)
            .ok_or_else(|| invalid("missing native root"))?;
        let schema = canonical
            .output_schema()
            .map_err(|e| invalid(e.to_string()))?;
        let node = Rc::new(SummaryNode {
            expr: SummaryExpr::KeepPreAsap(Rc::clone(canonical)),
            schema: SummarySchema {
                time_index: schema.time_index,
                fields: schema
                    .columns
                    .into_iter()
                    .map(|c| SummaryField {
                        name: c.name,
                        dtype: SummaryFamilyType::Plain(c.dtype),
                        nullable: c.nullable,
                    })
                    .collect(),
            },
            guarantee: None,
        });
        let semantic = planner_types::post_asap::compile_executable_dag_with_node_ids(&node)
            .map_err(|e| invalid(e.to_string()))?;
        entry.root = QueryNodeId(0);
        entry.nodes = BTreeMap::from([(
            entry.root,
            QueryPlanNode::ExactFallback {
                reason: "cost-selected native SQL alternative".into(),
            },
        )]);
        let installed = crate::physical::executable_binding::install_selected_dag(
            entry.query_id.clone(),
            &semantic.dag,
            entry.root,
            |_| None,
            |_| Some(entry.root),
        )
        .map_err(invalid)?;
        native
            .precompute_plan
            .executable_dags
            .insert(entry.query_id.clone(), installed);
    }
    native.validate().map_err(invalid)?;
    Ok(vec![
        TablePopulationCandidate {
            manifest: manifest(request, &maintained, false)?,
            publication: maintained,
        },
        TablePopulationCandidate {
            manifest: manifest(request, &native, true)?,
            publication: native,
        },
    ])
}

fn manifest(
    request: &TablePopulationWorkload,
    publication: &PhysicalPlanPublication,
    native: bool,
) -> Result<WorkloadCostManifest, ClickHousePlanningError> {
    let mut components = BTreeMap::new();
    let mut workload = BTreeMap::new();
    for entry in publication.query_plan.entries.values() {
        let evaluations = request.query_evaluations[&entry.query_id] as f64;
        workload.insert(entry.query_id.clone(), json!({"canonical": entry.canonical_query, "evaluations": evaluations, "accepted_snapshot_age_ms": request.maintenance.max_snapshot_age_ms}));
        components.insert(
            format!("query:{}", entry.query_id),
            CostDemand {
                implementation: json!({"nodes":entry.nodes, "native":native}),
                unit: "query_evaluation".into(),
                multiplicity: evaluations,
            },
        );
        // The exact backend remains provisioned for cold, stale, failed and rejected requests.
        components.insert(
            format!("fallback:{}", entry.query_id),
            CostDemand {
                implementation: json!({"sql":entry.query_id, "maintenance":request.maintenance}),
                unit: "horizon".into(),
                multiplicity: 1.0,
            },
        );
        for node in entry.nodes.values() {
            if let QueryPlanNode::ReadTablePopulation { population, .. } = node {
                for phase in [
                    "snapshot_source_scan_transfer",
                    "reconcile_sort",
                    "residency",
                    "retire",
                ] {
                    let refresh =
                        matches!(phase, "snapshot_source_scan_transfer" | "reconcile_sort");
                    components.insert(
                        format!("population:{}:{phase}", population.key()),
                        CostDemand {
                            implementation: json!({"population": population, "phase":phase}),
                            unit: if refresh {
                                "snapshot_refresh"
                            } else {
                                "horizon"
                            }
                            .into(),
                            multiplicity: if refresh {
                                1.0 + (request.horizon_seconds * 1000.0
                                    / request.maintenance.refresh_interval_ms as f64)
                                    .ceil()
                            } else {
                                1.0
                            },
                        },
                    );
                }
            }
        }
    }
    Ok(WorkloadCostManifest {
        plan_id: publication.query_plan.plan_id,
        plan_version: publication.query_plan.plan_version,
        planner_revision: crate::physical::compiler::PLANNER_REVISION.into(),
        capability_snapshot_id: request.capability_snapshot_id.clone(),
        backend_compat: request.backend_compat.clone(),
        horizon_seconds: request.horizon_seconds,
        workload,
        components,
    })
}

pub async fn compile(
    request: &TablePopulationDeployment,
) -> Result<PhysicalPlanPublication, ClickHousePlanningError> {
    if request.max_evidence_age_ms == 0 {
        return Err(invalid("evidence age limit must be positive"));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| invalid(e.to_string()))?
        .as_millis() as u64;
    request
        .evidence
        .validate_at(now, request.max_evidence_age_ms)
        .map_err(|e| invalid(e.to_string()))?;
    let mut priced = Vec::new();
    for candidate in candidates(&request.workload).await? {
        match request.evidence.price(&candidate.manifest) {
            Ok((cost, _)) => priced.push((cost.0, candidate.publication)),
            Err(("rejected", _)) => {}
            Err((_, reason)) => return Err(invalid(reason)),
        }
    }
    priced
        .into_iter()
        .min_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, plan)| plan)
        .ok_or_else(|| invalid("no executable table population alternative"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::pre_asap::{Column, DataType, Schema};
    pub fn request() -> TablePopulationWorkload {
        let queries = [
            "SELECT quantile(0.5)(value) FROM samples",
            "SELECT quantile(0.9)(value) FROM samples",
            "SELECT sum(value) FROM samples",
            "SELECT * FROM samples ORDER BY value DESC LIMIT 2",
            "SELECT * FROM samples ORDER BY value DESC LIMIT 4",
        ]
        .into_iter()
        .map(|sql| ClickHouseSqlWorkloadEntry {
            sql: sql.into(),
            start_ms: 1,
            end_ms: 1000,
            cumulative: false,
        })
        .collect::<Vec<_>>();
        TablePopulationWorkload {
            query_evaluations: queries.iter().map(|q| (q.sql.clone(), 100)).collect(),
            workload: ClickHouseSqlAutomaticWorkload {
                envelope: asap_types::precompute_plan::PlanEnvelope {
                    plan_id: 9901,
                    plan_version: 1,
                    generated_at_unix_ms: 1,
                    activation_unix_ms: 1,
                    expiry_unix_ms: None,
                    backend_compat: asap_types::precompute_plan::BACKEND_COMPAT.into(),
                    planner_revision: crate::physical::compiler::PLANNER_REVISION.into(),
                    capability_snapshot_id: "test".into(),
                },
                tables: HashMap::from([(
                    "samples".into(),
                    Schema {
                        columns: vec![Column::new("value", DataType::Float64, false)],
                        closed: true,
                        ..Default::default()
                    },
                )]),
                accuracy: AccuracyTarget::Exact,
                queries,
            },
            maintenance: TableRowsMaintenance {
                database: "default".into(),
                refresh_interval_ms: 100,
                max_snapshot_age_ms: 1000,
                max_rows: 1000,
                max_bytes: 1_000_000,
            },
            horizon_seconds: 60.0,
            capability_snapshot_id: "test".into(),
            backend_compat: asap_types::precompute_plan::BACKEND_COMPAT.into(),
        }
    }
    /// SQL consumes the typed Planner population and emits one shared, deployable state identity.
    #[tokio::test]
    async fn sql_population_candidates_are_shared_and_fully_priced() {
        let candidates = candidates(&request()).await.unwrap();
        assert_eq!(candidates.len(), 2);
        let states = candidates[0]
            .publication
            .query_plan
            .entries
            .values()
            .flat_map(|q| q.nodes.values())
            .filter_map(|n| {
                if let QueryPlanNode::ReadTablePopulation { population, .. } = n {
                    Some(population.key())
                } else {
                    None
                }
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(states.len(), 1);
        assert!(candidates[0]
            .manifest
            .components
            .keys()
            .any(|k| k.ends_with(":snapshot_source_scan_transfer")));
        assert!(candidates[1]
            .publication
            .query_plan
            .entries
            .values()
            .all(|q| matches!(q.nodes[&q.root], QueryPlanNode::ExactFallback { .. })));
    }
    // Deployment needs complete current evidence and chooses the native alternative when it is cheaper.
    #[tokio::test]
    async fn sql_population_selection_rejects_missing_quotes_and_can_choose_native() {
        use crate::physical::workload_cost::WorkloadQuote;
        let workload = request();
        let options = candidates(&workload).await.unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let mut deployment = TablePopulationDeployment {
            workload,
            max_evidence_age_ms: 60_000,
            evidence: WorkloadCostEvidence {
                backend_revision: crate::physical::compiler::BACKEND_REVISION.into(),
                planner_revision: crate::physical::compiler::PLANNER_REVISION.into(),
                data_snapshot_id: "rows-v1".into(),
                model_version: "measured-test".into(),
                observed_at_unix_ms: now,
                valid_for_ms: 60_000,
                quotes: options
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| WorkloadQuote {
                        unit_costs: c
                            .manifest
                            .components
                            .keys()
                            .map(|k| (k.clone(), if i == 0 { 100.0 } else { 1.0 }))
                            .collect(),
                        manifest: c.manifest,
                        executable: true,
                    })
                    .collect(),
            },
        };
        let selected = compile(&deployment).await.unwrap();
        assert!(selected
            .query_plan
            .entries
            .values()
            .all(|q| matches!(q.nodes[&q.root], QueryPlanNode::ExactFallback { .. })));
        let key = deployment.evidence.quotes[0]
            .unit_costs
            .keys()
            .next()
            .unwrap()
            .clone();
        deployment.evidence.quotes[0].unit_costs.remove(&key);
        assert!(compile(&deployment).await.is_err());
        deployment.evidence.quotes.clear();
        assert!(compile(&deployment).await.is_err());
    }
}
