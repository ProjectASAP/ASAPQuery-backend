//! Shared installed QueryPlan contract and activation validation.
//!
//! ASAPPlanner owns semantic post-ASAP IR. Physical compilation binds every
//! maintained-summary leaf to one materialization and lowers edges to stable
//! node IDs. Serving executes this graph without reconstructing Planner IR or
//! searching for compatible materializations.

pub mod current_series;
pub mod residual;

#[deprecated(note = "Use query_plan::residual")]
pub use residual as logical;

use std::collections::{BTreeMap, BTreeSet};

use planner_types::post_asap::SketchQuery;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use crate::QueryLanguage;
use crate::{sds::SummaryDefinitionId, PolicyFingerprint};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryPlan {
    pub plan_id: u64,
    pub plan_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clickhouse_context: Option<ClickHousePlanningContext>,
    pub entries: BTreeMap<String, QueryPlanEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClickHousePlanningContext {
    pub tables: std::collections::HashMap<String, planner_types::pre_asap::Schema>,
    pub accuracy: planner_types::types::AccuracyTarget,
    /// A time template can have several concrete plans with different pane
    /// origins or materializations. Keep their physical bindings independent.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub window_templates: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FixedEvaluationRange {
    pub start_ms: u64,
    pub end_ms: u64,
    pub cumulative: bool,
}

impl QueryPlan {
    pub fn empty() -> Self {
        Self {
            plan_id: 0,
            plan_version: 0,
            clickhouse_context: None,
            entries: BTreeMap::new(),
        }
    }

    pub fn lookup(&self, promql: &str) -> Result<&QueryPlanEntry, QueryPlanError> {
        let identity = canonical_promql(promql)?;
        self.lookup_canonical(QueryLanguage::PromQl, &identity)
    }

    pub fn lookup_canonical(
        &self,
        language: QueryLanguage,
        identity: &str,
    ) -> Result<&QueryPlanEntry, QueryPlanError> {
        let key = Self::catalog_key(language, identity);
        self.entries
            .get(&key)
            .filter(|entry| entry.language == language)
            .ok_or_else(|| QueryPlanError::QueryNotPlanned(identity.into()))
    }

    pub fn catalog_key(language: QueryLanguage, identity: &str) -> String {
        match language {
            QueryLanguage::PromQl => identity.to_owned(),
            QueryLanguage::MetricsQl => format!("metricsql:{identity}"),
            QueryLanguage::ClickHouseSql => format!("clickhouse:{identity}"),
        }
    }

    pub fn lookup_clickhouse(
        &self,
        canonical_sql: &str,
    ) -> Result<&QueryPlanEntry, QueryPlanError> {
        self.lookup_canonical(QueryLanguage::ClickHouseSql, canonical_sql)
    }

    /// Validate semantic bindings against the authoritative snapshot before use.
    pub fn validate_against_catalog(
        &self,
        catalog: &crate::summary_catalog::SummaryCatalog,
    ) -> Result<(), QueryPlanError> {
        catalog
            .validate()
            .map_err(|error| QueryPlanError::Invalid(error.to_string()))?;
        if self.plan_id != catalog.plan_id || self.plan_version != catalog.plan_version {
            return Err(QueryPlanError::Invalid(
                "QueryPlan and SummaryCatalog have different plan identity/version".into(),
            ));
        }
        let available = catalog
            .materializations
            .keys()
            .copied()
            .map(Into::into)
            .collect();
        self.validate(&available)?;
        for entry in self.entries.values() {
            for binding in entry.materialization_bindings() {
                let identity = catalog
                    .materializations
                    .get(&binding.materialization)
                    .ok_or_else(|| {
                        QueryPlanError::Invalid(
                            "query binding references absent catalog materialization".into(),
                        )
                    })?;
                let _data = &catalog.data_descriptors[&identity.data_descriptor_id];
                if binding.window_ms == 0 {
                    return Err(QueryPlanError::Invalid(
                        "zero physical pane duration".into(),
                    ));
                }
                if binding.full_window_slide_ms.is_some()
                    != matches!(
                        identity.window_layout,
                        crate::WindowMaterializationLayout::FullWindow
                    )
                    || binding.full_window_slide_ms == Some(0)
                {
                    return Err(QueryPlanError::Invalid(
                        "query storage layout differs from catalog definition".into(),
                    ));
                }
                if binding.pane_origin_ms != identity.pane_origin_ms {
                    return Err(QueryPlanError::Invalid(
                        "query pane origin differs from catalog definition".into(),
                    ));
                }
            }
            for node in entry.nodes.values() {
                let QueryPlanNode::ExactReadout { input, readout } = node else {
                    continue;
                };
                if !matches!(readout, ExactReadout::Increase | ExactReadout::Rate) {
                    continue;
                }
                let Some(QueryPlanNode::ReadMaterialization { binding }) = entry.nodes.get(input)
                else {
                    return Err(QueryPlanError::Invalid(
                        "counter readout must directly consume one catalog materialization".into(),
                    ));
                };
                let identity = &catalog.materializations[&binding.materialization];
                let descriptor = &catalog.summary_descriptors[&identity.summary_descriptor_id];
                if !matches!(
                    descriptor.fidelity,
                    crate::sds::FidelityGuarantee::ExactCounter {
                        full_pane_coverage_required: true,
                        ..
                    }
                ) {
                    return Err(QueryPlanError::Invalid(
                        "rate/increase binding does not reference an exact counter SDS".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn validate(&self, available: &BTreeSet<PolicyFingerprint>) -> Result<(), QueryPlanError> {
        if self.plan_id != 0 && self.plan_version == 0 {
            return Err(QueryPlanError::Invalid(
                "non-bootstrap QueryPlan has zero plan_version".into(),
            ));
        }
        if let Some(context) = &self.clickhouse_context {
            for (template, identities) in &context.window_templates {
                if !template.starts_with("moving-window-v1:") || identities.is_empty() {
                    return Err(QueryPlanError::Invalid(
                        "invalid SQL window template index".into(),
                    ));
                }
                let mut seen = BTreeSet::new();
                for identity in identities {
                    let entry = self.lookup_clickhouse(identity)?;
                    if !seen.insert(identity)
                        || entry
                            .nodes
                            .values()
                            .any(|node| matches!(node, QueryPlanNode::ExternalExact { .. }))
                    {
                        return Err(QueryPlanError::Invalid(
                            "SQL window template has duplicate or external bindings".into(),
                        ));
                    }
                }
            }
        }
        for (identity, entry) in &self.entries {
            let expected = Self::catalog_key(entry.language, &entry.canonical_query);
            if identity != &expected {
                return Err(QueryPlanError::Invalid(format!(
                    "query map key `{identity}` differs from entry identity `{}`",
                    entry.canonical_query
                )));
            }
            match entry.language {
                QueryLanguage::PromQl | QueryLanguage::MetricsQl
                    if entry.fixed_evaluation.is_some() =>
                {
                    return Err(QueryPlanError::Invalid(
                        "PromQL query entry carries a ClickHouse fixed evaluation range".into(),
                    ));
                }
                QueryLanguage::ClickHouseSql => {
                    if self.clickhouse_context.is_none() {
                        return Err(QueryPlanError::Invalid(
                            "ClickHouse query entry has no planning context".into(),
                        ));
                    }
                    let Some(range) = entry.fixed_evaluation else {
                        return Err(QueryPlanError::Invalid(
                            "ClickHouse query entry has no fixed evaluation range".into(),
                        ));
                    };
                    if range.end_ms <= range.start_ms {
                        return Err(QueryPlanError::Invalid(
                            "ClickHouse query entry has an empty evaluation range".into(),
                        ));
                    }
                }
                QueryLanguage::PromQl | QueryLanguage::MetricsQl => {}
            }
            entry.validate(available)?;
        }
        Ok(())
    }
}

pub use crate::executable_plan::QueryNodeId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryPlanEntry {
    #[serde(default)]
    pub language: QueryLanguage,
    pub query_id: String,
    #[serde(alias = "canonical_promql")]
    pub canonical_query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_evaluation: Option<FixedEvaluationRange>,
    pub root: QueryNodeId,
    pub nodes: BTreeMap<QueryNodeId, QueryPlanNode>,
    pub instant: InstantExecution,
    pub fallback: FallbackPolicy,
}

fn topological_order(
    root: QueryNodeId,
    nodes: &BTreeMap<QueryNodeId, QueryPlanNode>,
) -> Result<Vec<QueryNodeId>, QueryPlanError> {
    fn visit(
        id: QueryNodeId,
        nodes: &BTreeMap<QueryNodeId, QueryPlanNode>,
        visiting: &mut BTreeSet<QueryNodeId>,
        visited: &mut BTreeSet<QueryNodeId>,
        out: &mut Vec<QueryNodeId>,
    ) -> Result<(), QueryPlanError> {
        if visited.contains(&id) {
            return Ok(());
        }
        if !visiting.insert(id) {
            return Err(QueryPlanError::Invalid(format!(
                "cycle detected at query node {}",
                id.0
            )));
        }
        let node = nodes
            .get(&id)
            .ok_or_else(|| QueryPlanError::Invalid(format!("missing query node {}", id.0)))?;
        for input in node.inputs() {
            visit(*input, nodes, visiting, visited, out)?;
        }
        visiting.remove(&id);
        visited.insert(id);
        out.push(id);
        Ok(())
    }
    let mut out = Vec::with_capacity(nodes.len());
    visit(
        root,
        nodes,
        &mut BTreeSet::new(),
        &mut BTreeSet::new(),
        &mut out,
    )?;
    Ok(out)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InstantExecution {
    pub lookback_ms: u64,
    pub full_history: bool,
    pub cumulative_readout: bool,
}

impl QueryPlanEntry {
    /// Materializations this executable DAG reads, in stable node order.
    /// Serving uses this set for readiness accounting; it never performs a
    /// catalog candidate search to reconstruct dependencies.
    pub fn materialization_bindings(&self) -> Vec<&MaterializationBinding> {
        self.nodes
            .values()
            .filter_map(|node| match node {
                QueryPlanNode::ReadMaterialization { binding } => Some(binding),
                _ => None,
            })
            .collect()
    }

    pub fn topological_order(&self) -> Result<Vec<QueryNodeId>, QueryPlanError> {
        topological_order(self.root, &self.nodes)
    }

    pub fn topological_order_from(
        &self,
        root: QueryNodeId,
    ) -> Result<Vec<QueryNodeId>, QueryPlanError> {
        topological_order(root, &self.nodes)
    }

    /// Validate references, bindings, reachability, and cycles before activation.
    pub fn validate(&self, available: &BTreeSet<PolicyFingerprint>) -> Result<(), QueryPlanError> {
        if !self.nodes.contains_key(&self.root) {
            return Err(QueryPlanError::Invalid(format!(
                "query `{}` has missing root {}",
                self.query_id, self.root.0
            )));
        }
        for (id, node) in &self.nodes {
            if let QueryPlanNode::Logical { operator, inputs } = node {
                operator.validate(inputs.len())?;
            }
            if matches!(node, QueryPlanNode::Scalar { value } if !value.is_finite()) {
                return Err(QueryPlanError::Invalid("non-finite scalar constant".into()));
            }
            if let QueryPlanNode::ExternalExact { request, inputs } = node {
                if request.expression.trim().is_empty() {
                    return Err(QueryPlanError::Invalid(
                        "external exact expression must not be empty".into(),
                    ));
                }
                if request.input_contracts.len() != inputs.len() {
                    return Err(QueryPlanError::Invalid(
                        "external exact input contracts must match DAG inputs".into(),
                    ));
                }
                if request.input_contracts.iter().any(|contract| {
                    matches!(contract, ExternalExactInput::CandidateMembership { item_label } if item_label.is_empty())
                }) {
                    return Err(QueryPlanError::Invalid(
                        "external exact candidate item label must not be empty".into(),
                    ));
                }
            }
            if let QueryPlanNode::CandidateTopK {
                k, completeness, ..
            } = node
            {
                if *k == 0 {
                    return Err(QueryPlanError::Invalid(
                        "CandidateTopK requires k > 0".into(),
                    ));
                }
                if matches!(
                    completeness,
                    CandidateCompleteness::Certified { guarantee }
                        if guarantee.metric
                            != planner_types::post_asap::ErrorMetric::TopKMembership
                            || guarantee.bound.evaluate().is_none()
                            || guarantee.failure_probability.evaluate().is_none()
                ) {
                    return Err(QueryPlanError::Invalid(
                        "invalid CandidateTopK completeness certificate".into(),
                    ));
                }
            }
            for input in node.inputs() {
                if !self.nodes.contains_key(input) {
                    return Err(QueryPlanError::Invalid(format!(
                        "query `{}` node {} references missing input {}",
                        self.query_id, id.0, input.0
                    )));
                }
            }
            if let QueryPlanNode::ReadMaterialization { binding } = node {
                if binding.readout_lookback_ms == Some(0) {
                    return Err(QueryPlanError::Invalid(
                        "zero semantic readout lookback".into(),
                    ));
                }
                if !available.contains(&binding.materialization.fingerprint()) {
                    return Err(QueryPlanError::Invalid(format!(
                        "query `{}` node {} references absent materialization {}",
                        self.query_id,
                        id.0,
                        binding.materialization.as_u64()
                    )));
                }
            }
        }
        let order = self.topological_order()?;
        if order.len() != self.nodes.len() {
            return Err(QueryPlanError::Invalid(format!(
                "query `{}` contains unreachable nodes",
                self.query_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FallbackPolicy {
    ExactBackend,
    Reject,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MaterializationBinding {
    /// Complete-window storage advances independently of its stored extent.
    /// None denotes disjoint pane storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_window_slide_ms: Option<u64>,
    pub materialization: SummaryDefinitionId,
    /// Query operator grouping applied while folding those SIDs.
    pub output_grouping: PhysicalGrouping,
    /// Labels whose values form an item identity inside a keyed sketch.
    #[serde(default, alias = "itemLabels", skip_serializing_if = "Vec::is_empty")]
    pub item_labels: Vec<String>,
    pub window_ms: u64,
    /// Unix millisecond timestamp on the materialized pane-boundary grid.
    /// Legacy plans deserialize this as unknown and fall back at read time.
    #[serde(
        default,
        alias = "paneOriginMs",
        skip_serializing_if = "Option::is_none"
    )]
    pub pane_origin_ms: Option<i64>,
    /// Semantic query lookback, independent of the physical pane duration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readout_lookback_ms: Option<u64>,
}

impl MaterializationBinding {
    /// Both range boundaries must identify complete stored state. Full windows
    /// use a start grid; their end grid is displaced by the window width.
    pub fn covers_range(&self, start_ms: u64, end_ms: u64) -> bool {
        let Some(origin) = self.pane_origin_ms else {
            return false;
        };
        if self.window_ms == 0 || end_ms <= start_ms {
            return false;
        }
        let start = i128::from(start_ms) - i128::from(origin);
        match self.full_window_slide_ms {
            Some(slide) => {
                slide != 0
                    && end_ms - start_ms == self.window_ms
                    && start.rem_euclid(i128::from(slide)) == 0
            }
            None => {
                start.rem_euclid(i128::from(self.window_ms)) == 0
                    && (i128::from(end_ms) - i128::from(origin))
                        .rem_euclid(i128::from(self.window_ms))
                        == 0
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", content = "keys", rename_all = "snake_case")]
pub enum PhysicalGrouping {
    PerEntity,
    Reduce(Vec<String>),
}

/// Result shape promised by an external exact engine. The backend uses this
/// contract to type-check downstream DAG nodes without depending on an
/// engine-specific response envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalExactOutput {
    Scalar,
    InstantVector,
    RangeVector,
    Relation { schema: serde_json::Value },
}

/// How an ordinary DAG input constrains an external exact evaluation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalExactInput {
    CandidateMembership { item_label: String },
}

/// Language-neutral request contract for an exact subtree. Evaluation time is
/// inherited from the containing QueryPlanEntry, avoiding a second time-range
/// envelope that could drift from the installed query plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExternalExactRequest {
    pub language: QueryLanguage,
    pub expression: String,
    pub output: ExternalExactOutput,
    /// Engine parameters forwarded without embedding transport details in the DAG.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, String>,
    /// Optional parameter names populated from the query entry's evaluation range.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_parameter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_parameter: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_contracts: Vec<ExternalExactInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryPlanNode {
    RelationalJoin {
        inputs: [QueryNodeId; 2],
        join_kind: planner_types::pre_asap::JoinKind,
        pred: serde_json::Value,
        left_schema: planner_types::post_asap::SummarySchema,
        right_schema: planner_types::post_asap::SummarySchema,
        output_schema: planner_types::post_asap::SummarySchema,
    },
    Relational {
        input: QueryNodeId,
        /// Serialized planner-owned operation. Keeping the wire form here makes
        /// the published catalog Send + Sync even though the planner AST uses Rc.
        operation: serde_json::Value,
        input_schema: planner_types::post_asap::SummarySchema,
        output_schema: planner_types::post_asap::SummarySchema,
    },
    Logical {
        operator: residual::ResidualQueryOperator,
        inputs: Vec<QueryNodeId>,
    },
    Scalar {
        value: f64,
    },
    Binary {
        inputs: [QueryNodeId; 2],
        operator: planner_types::pre_asap::ArithmeticOpKind,
    },
    ReduceSum {
        input: QueryNodeId,
        grouping: PhysicalGrouping,
    },
    ReadMaterialization {
        binding: MaterializationBinding,
    },
    SummaryEstimate {
        input: QueryNodeId,
        query: QueryReadout,
    },
    ExactReadout {
        input: QueryNodeId,
        readout: ExactReadout,
    },
    SummaryMerge {
        inputs: Vec<QueryNodeId>,
    },
    /// Use an approximate heap only as a membership sidecar, then rerank the
    /// matching exact counter readouts. `inputs[0]` is candidate membership;
    /// `inputs[1]` is the authoritative exact value vector.
    CandidateTopK {
        inputs: [QueryNodeId; 2],
        k: u64,
        grouping: residual::Grouping,
        completeness: CandidateCompleteness,
    },
    /// An exact subtree evaluated outside ASAP. Its results enter the query DAG
    /// like any other node output and may depend on summary-produced inputs.
    ExternalExact {
        request: ExternalExactRequest,
        inputs: Vec<QueryNodeId>,
    },
    ExactFallback {
        reason: String,
    },
}

impl QueryPlanNode {
    /// Operator label for logs: the serialized `op` tag, plus the residual
    /// `kind` for logical nodes, including the operation where applicable
    /// (e.g. `logical/aggregate/sum`).
    pub fn op_label(&self) -> &'static str {
        use residual::ResidualQueryOperator as R;
        match self {
            Self::RelationalJoin { .. } => "relational_join",
            Self::Relational { .. } => "relational",
            Self::Logical { operator, .. } => match operator {
                R::CurrentSeries { .. } => "logical/current_series",
                R::ExactSubquery { .. } => "logical/exact_subquery",
                R::CandidateExactSubquery { .. } => "logical/candidate_exact_subquery",
                R::Scan { .. } => "logical/scan",
                R::UnaryNegate => "logical/unary_negate",
                R::VectorToScalar => "logical/vector_to_scalar",
                R::Aggregate { operation, .. } => match operation {
                    residual::Aggregation::Sum => "logical/aggregate/sum",
                    residual::Aggregation::Max => "logical/aggregate/max",
                    residual::Aggregation::Min => "logical/aggregate/min",
                    residual::Aggregation::Avg => "logical/aggregate/avg",
                    residual::Aggregation::Count => "logical/aggregate/count",
                },
                R::TopKSelection { .. } => "logical/top_k_selection",
                R::Binary { .. } => "logical/binary",
                R::Temporal { .. } => "logical/temporal",
                R::Sort { .. } => "logical/sort",
                R::HistogramQuantile => "logical/histogram_quantile",
                R::Subquery { .. } => "logical/subquery",
            },
            Self::Scalar { .. } => "scalar",
            Self::Binary { .. } => "binary",
            Self::ReduceSum { .. } => "reduce_sum",
            Self::ReadMaterialization { .. } => "read_materialization",
            Self::SummaryEstimate { .. } => "summary_estimate",
            Self::ExactReadout { readout, .. } => match readout {
                ExactReadout::Sum => "exact_readout/sum",
                ExactReadout::Count => "exact_readout/count",
                ExactReadout::Increase => "exact_readout/increase",
                ExactReadout::Rate => "exact_readout/rate",
                ExactReadout::Min => "exact_readout/min",
                ExactReadout::Max => "exact_readout/max",
            },
            Self::SummaryMerge { .. } => "summary_merge",
            Self::CandidateTopK { .. } => "candidate_top_k",
            Self::ExternalExact { .. } => "external_exact",
            Self::ExactFallback { .. } => "exact_fallback",
        }
    }

    /// Bounded, query-text-free operator arguments for execution logs.
    /// The query ID links these details to the full installed plan when needed.
    pub fn log_syntax(&self) -> String {
        use residual::ResidualQueryOperator as R;
        match self {
            Self::RelationalJoin { join_kind, .. } => format!("join_kind={join_kind:?}"),
            Self::Relational { .. } => String::new(),
            Self::Logical { operator, .. } => match operator {
                R::CurrentSeries { readout, .. } => format!("readout={readout:?}"),
                R::ExactSubquery { .. } => String::new(),
                R::CandidateExactSubquery { .. } => String::new(),
                R::Scan {
                    metric,
                    matchers,
                    range_ms,
                    offset_ms,
                } => format!(
                    "metric={} matcher_count={} range_ms={range_ms:?} offset_ms={offset_ms}",
                    metric
                        .as_deref()
                        .map(|name| name.chars().take(64).collect::<String>())
                        .unwrap_or_default(),
                    matchers.len(),
                ),
                R::UnaryNegate | R::VectorToScalar | R::HistogramQuantile => String::new(),
                R::Aggregate {
                    operation,
                    grouping,
                } => format!(
                    "operation={operation:?} grouping={}",
                    log_grouping(&grouping.labels, grouping.without)
                ),
                R::TopKSelection { k, grouping } => format!(
                    "k={k} grouping={}",
                    log_grouping(&grouping.labels, grouping.without)
                ),
                R::Binary {
                    operation,
                    return_bool,
                } => format!("operation={operation:?} return_bool={return_bool}"),
                R::Temporal { operation } => format!("operation={operation:?}"),
                R::Sort { descending } => format!("descending={descending}"),
                R::Subquery {
                    range_ms,
                    step_ms,
                    offset_ms,
                } => format!("range_ms={range_ms} step_ms={step_ms} offset_ms={offset_ms}"),
            },
            Self::Scalar { value } => format!("value={value}"),
            Self::Binary { operator, .. } => format!("operation={operator:?}"),
            Self::ReduceSum { grouping, .. } => match grouping {
                PhysicalGrouping::PerEntity => "grouping=per_entity".into(),
                PhysicalGrouping::Reduce(labels) => {
                    format!("grouping=reduce({})", log_labels(labels))
                }
            },
            Self::ReadMaterialization { binding } => format!(
                "window_ms={} lookback_ms={:?}",
                binding.window_ms, binding.readout_lookback_ms
            ),
            Self::SummaryEstimate { query, .. } => match query {
                QueryReadout::FrequencyL2 => "readout=frequency_l2".into(),
                QueryReadout::FrequencyEntropy => "readout=frequency_entropy".into(),
                QueryReadout::Quantile { q } => format!("readout=quantile q={q}"),
                QueryReadout::PointCount { .. } => "readout=point_count".into(),
                QueryReadout::Cardinality => "readout=cardinality".into(),
                QueryReadout::TopK { k } => format!("readout=top_k k={k}"),
            },
            Self::ExactReadout { readout, .. } => format!("readout={readout:?}"),
            Self::SummaryMerge { .. } => String::new(),
            Self::CandidateTopK { k, grouping, .. } => format!(
                "k={k} grouping={}",
                log_grouping(&grouping.labels, grouping.without)
            ),
            Self::ExternalExact { .. } | Self::ExactFallback { .. } => String::new(),
        }
    }

    pub fn inputs(&self) -> &[QueryNodeId] {
        match self {
            Self::Scalar { .. } | Self::ReadMaterialization { .. } | Self::ExactFallback { .. } => {
                &[]
            }
            Self::Binary { inputs, .. } | Self::RelationalJoin { inputs, .. } => inputs,
            Self::ReduceSum { input, .. }
            | Self::Relational { input, .. }
            | Self::SummaryEstimate { input, .. }
            | Self::ExactReadout { input, .. } => std::slice::from_ref(input),
            Self::SummaryMerge { inputs }
            | Self::Logical { inputs, .. }
            | Self::ExternalExact { inputs, .. } => inputs,
            Self::CandidateTopK { inputs, .. } => inputs,
        }
    }
}

fn log_labels(labels: &[String]) -> String {
    let mut names = labels
        .iter()
        .take(8)
        .map(|label| label.chars().take(64).collect::<String>())
        .collect::<Vec<_>>();
    if labels.len() > 8 {
        names.push("...".into());
    }
    names.join(",")
}

fn log_grouping(labels: &[String], without: bool) -> String {
    format!(
        "{}({})",
        if without { "without" } else { "by" },
        log_labels(labels)
    )
}

pub use planner_types::post_asap::CandidateCompleteness;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExactReadout {
    Sum,
    Count,
    Increase,
    Rate,
    Min,
    Max,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryReadout {
    FrequencyL2,
    FrequencyEntropy,
    Quantile {
        q: f64,
    },
    PointCount {
        key: planner_types::pre_asap::ColumnRef,
        value: Option<String>,
    },
    Cardinality,
    TopK {
        k: usize,
    },
}

impl From<SketchQuery> for QueryReadout {
    fn from(query: SketchQuery) -> Self {
        match query {
            SketchQuery::FrequencyL2 => Self::FrequencyL2,
            SketchQuery::FrequencyEntropy => Self::FrequencyEntropy,
            SketchQuery::Quantile { q } => Self::Quantile { q },
            SketchQuery::PointCount { key, value } => Self::PointCount { key, value },
            SketchQuery::Cardinality => Self::Cardinality,
            SketchQuery::TopK { k } => Self::TopK { k },
        }
    }
}

impl From<QueryReadout> for SketchQuery {
    fn from(query: QueryReadout) -> Self {
        match query {
            QueryReadout::FrequencyL2 => Self::FrequencyL2,
            QueryReadout::FrequencyEntropy => Self::FrequencyEntropy,
            QueryReadout::Quantile { q } => Self::Quantile { q },
            QueryReadout::PointCount { key, value } => Self::PointCount { key, value },
            QueryReadout::Cardinality => Self::Cardinality,
            QueryReadout::TopK { k } => Self::TopK { k },
        }
    }
}

#[derive(Debug, Error)]
pub enum QueryPlanError {
    #[error("invalid PromQL query identity: {0}")]
    InvalidPromql(String),
    #[error("query is absent from the active QueryPlan: {0}")]
    QueryNotPlanned(String),
    #[error("post-ASAP DAG cannot be represented by the query executor: {0}")]
    UnsupportedNode(String),
    #[error("invalid QueryPlan: {0}")]
    Invalid(String),
}

pub fn canonical_promql(query: &str) -> Result<String, QueryPlanError> {
    promql_parser::parser::parse(query.trim())
        .map(|expr| expr.to_string())
        .map_err(|error| QueryPlanError::InvalidPromql(error.to_string()))
}

#[cfg(test)]
mod contract_tests {
    use super::{residual, QueryPlanNode};

    // Installed plans cross producer/query threads without Planner Rc state.
    #[test]
    fn installed_query_contract_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<super::QueryPlan>();
        assert_send_sync::<super::QueryPlanEntry>();
    }

    #[test]
    fn aggregate_log_labels_identify_the_operation() {
        for (operation, expected) in [
            (residual::Aggregation::Sum, "logical/aggregate/sum"),
            (residual::Aggregation::Count, "logical/aggregate/count"),
            (residual::Aggregation::Avg, "logical/aggregate/avg"),
        ] {
            let node = QueryPlanNode::Logical {
                operator: residual::ResidualQueryOperator::Aggregate {
                    operation,
                    grouping: residual::Grouping {
                        labels: vec!["service".into()],
                        without: false,
                    },
                },
                inputs: vec![],
            };
            assert_eq!(node.op_label(), expected);
            assert!(node.log_syntax().contains("grouping=by(service)"));
        }
        for (readout, expected) in [
            (super::ExactReadout::Sum, "exact_readout/sum"),
            (super::ExactReadout::Count, "exact_readout/count"),
        ] {
            assert_eq!(
                QueryPlanNode::ExactReadout {
                    input: super::QueryNodeId(1),
                    readout,
                }
                .op_label(),
                expected
            );
        }
    }

    #[test]
    fn execution_log_syntax_identifies_operator_without_query_text() {
        let binary = QueryPlanNode::Logical {
            operator: residual::ResidualQueryOperator::Binary {
                operation: residual::BinaryOperation::CheckedDiv,
                return_bool: false,
            },
            inputs: vec![super::QueryNodeId(1), super::QueryNodeId(2)],
        };
        assert_eq!(binary.op_label(), "logical/binary");
        assert_eq!(
            binary.log_syntax(),
            "operation=CheckedDiv return_bool=false"
        );

        let exact = QueryPlanNode::Logical {
            operator: residual::ResidualQueryOperator::ExactSubquery {
                query: "secret_metric{credential=\"secret\"}".into(),
            },
            inputs: vec![],
        };
        assert_eq!(exact.op_label(), "logical/exact_subquery");
        assert!(exact.log_syntax().is_empty());
    }
}
