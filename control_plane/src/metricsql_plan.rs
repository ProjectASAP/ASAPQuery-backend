use crate::query_plan::{ExecutableQueryPlan, QueryPlanError};
use asap_types::PolicyFingerprint;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MetricsQlPlanEntry {
    pub query_id: String,
    pub canonical_metricsql: String,
    pub executable: ExecutableQueryPlan,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_plan::{FallbackPolicy, InstantExecution, QueryNodeId, QueryPlanNode};

    #[test]
    fn sidecar_serde_never_invents_a_promql_identity() {
        let identity = "default_rollup(cpu[5m])".to_string();
        let catalog = MetricsQlPlanCatalog {
            plan_id: 7,
            plan_version: 3,
            entries: BTreeMap::from([(
                identity.clone(),
                MetricsQlPlanEntry {
                    query_id: "vm-q".into(),
                    canonical_metricsql: identity,
                    executable: ExecutableQueryPlan {
                        root: QueryNodeId(0),
                        nodes: BTreeMap::from([(
                            QueryNodeId(0),
                            QueryPlanNode::ExactFallback {
                                reason: "fixture".into(),
                            },
                        )]),
                        instant: InstantExecution {
                            lookback_ms: 300_000,
                            full_history: false,
                            cumulative_readout: false,
                        },
                        fallback: FallbackPolicy::ExactBackend,
                    },
                },
            )]),
        };
        let json = serde_json::to_string(&catalog).unwrap();
        assert!(json.contains("canonical_metricsql"));
        assert!(!json.contains("canonical_promql"));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MetricsQlPlanCatalog {
    pub plan_id: u64,
    pub plan_version: u64,
    pub entries: BTreeMap<String, MetricsQlPlanEntry>,
}

impl MetricsQlPlanCatalog {
    pub fn empty() -> Self {
        Self {
            plan_id: 0,
            plan_version: 0,
            entries: BTreeMap::new(),
        }
    }

    pub fn lookup(&self, identity: &str) -> Result<&MetricsQlPlanEntry, QueryPlanError> {
        self.entries
            .get(identity)
            .ok_or_else(|| QueryPlanError::QueryNotPlanned(identity.into()))
    }

    pub fn validate(&self, available: &BTreeSet<PolicyFingerprint>) -> Result<(), QueryPlanError> {
        for (identity, entry) in &self.entries {
            if identity != &entry.canonical_metricsql {
                return Err(QueryPlanError::Invalid(
                    "MetricsQL catalog key disagrees with its AST identity".into(),
                ));
            }
            entry
                .executable
                .execution_view(entry.query_id.clone(), String::new())
                .validate(available)?;
        }
        Ok(())
    }
}
