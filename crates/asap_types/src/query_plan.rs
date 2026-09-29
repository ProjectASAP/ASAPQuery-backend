//! Shared installed QueryPlan contract and activation validation.
//!
//! ASAPPlanner owns semantic post-ASAP IR. Physical compilation binds every
//! maintained-summary leaf to one materialization and lowers edges to stable
//! node IDs. Serving executes this graph without reconstructing Planner IR or
//! searching for compatible materializations.

pub mod current_series;
mod native;
pub mod residual;

use std::collections::{BTreeMap, BTreeSet};

use planner_types::post_asap::SketchQuery;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use crate::QueryLanguage;
use crate::{sds::StoredOutputId, PolicyFingerprint};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryPlan {
    pub plan_id: u64,
    pub plan_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clickhouse_context: Option<ClickHousePlanningContext>,
    /// Selected semantic roots retained for provenance; serving executes
    /// `entries` and never reconstructs a plan from these documents.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub selected_dags: BTreeMap<String, crate::executable_plan::OwnedPostAsapDag>,
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
            selected_dags: BTreeMap::new(),
            entries: BTreeMap::new(),
        }
    }

    pub fn lookup(&self, promql: &str) -> Result<&QueryPlanEntry, QueryPlanError> {
        // Installed entries already carry validated canonical identities. The
        // common exact spelling requires no serving-time parser invocation.
        if let Some(entry) = self.entries.get(promql).filter(|entry| {
            entry.language == QueryLanguage::PromQl && entry.canonical_query == promql
        }) {
            return Ok(entry);
        }
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

    pub fn bind_catalog(
        &mut self,
        catalog: &crate::summary_catalog::SummaryCatalog,
    ) -> Result<(), QueryPlanError> {
        catalog
            .validate()
            .map_err(|e| QueryPlanError::Invalid(e.to_string()))?;
        for entry in self.entries.values_mut() {
            for node in entry.nodes.values_mut() {
                if let QueryPlanNode::ReadMaterialization { binding } = node {
                    binding.stored_output_reference = catalog
                        .output_reference(binding.materialization)
                        .map_err(|e| QueryPlanError::Invalid(e.to_string()))?;
                }
            }
        }
        self.validate_against_catalog(catalog)
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
        let available = catalog.outputs.keys().copied().map(Into::into).collect();
        self.validate(&available)?;
        for entry in self.entries.values() {
            for binding in entry.materialization_bindings() {
                let identity = catalog
                    .outputs
                    .get(&binding.materialization)
                    .ok_or_else(|| {
                        QueryPlanError::Invalid(
                            "query binding references absent catalog materialization".into(),
                        )
                    })?;
                if binding.stored_output_reference.definition_id != identity.definition_id {
                    return Err(QueryPlanError::Invalid(
                        "read definition differs from installed output".into(),
                    ));
                }
                let _data = &catalog.data_descriptors[&identity.data_descriptor_id];
                if binding.window_ms == 0 {
                    return Err(QueryPlanError::Invalid(
                        "zero physical pane duration".into(),
                    ));
                }
                if binding.full_window_slide_ms == Some(0) {
                    return Err(QueryPlanError::Invalid(
                        "query full-window cadence must be nonzero".into(),
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
                let identity = &catalog.outputs[&binding.materialization];
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
        for (query_id, selected) in &self.selected_dags {
            if query_id != &selected.query_id {
                return Err(QueryPlanError::Invalid(format!(
                    "selected DAG map key `{query_id}` differs from document query ID `{}`",
                    selected.query_id
                )));
            }
            if selected.schema_version != crate::executable_plan::OWNED_POST_ASAP_DAG_SCHEMA_VERSION
            {
                return Err(QueryPlanError::Invalid(format!(
                    "selected DAG `{query_id}` has unsupported schema version {}",
                    selected.schema_version
                )));
            }
            selected.decode().map_err(|error| {
                QueryPlanError::Invalid(format!("selected DAG `{query_id}` is invalid: {error}"))
            })?;
            let matching_entries = self
                .entries
                .values()
                .filter(|entry| entry.query_id == *query_id)
                .count();
            if matching_entries != 1 {
                return Err(QueryPlanError::Invalid(format!(
                    "selected DAG `{query_id}` must correspond to exactly one query entry; found {matching_entries}"
                )));
            }
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
    /// Planner-selected native computation, persisted before activation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_dag: Option<serde_json::Value>,
    #[serde(default)]
    pub language: QueryLanguage,
    pub query_id: String,
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

    /// The current external adapters provide evaluation time, not a snapshot
    /// token compatible with local SDS or current-series revisions.
    pub fn validate_snapshot_sources(&self) -> Result<(), QueryPlanError> {
        use query_time::QueryTimeOperator;
        let mut local = false;
        let mut external = false;
        for id in self.topological_order()? {
            match &self.nodes[&id] {
                QueryPlanNode::ReadMaterialization { .. }
                | QueryPlanNode::Logical {
                    operator: QueryTimeOperator::CurrentSeries { .. },
                    ..
                } => local = true,
                QueryPlanNode::ExternalExact { .. }
                | QueryPlanNode::Logical {
                    operator:
                        QueryTimeOperator::ExactSubquery { .. }
                        | QueryTimeOperator::CandidateExactSubquery { .. },
                    ..
                } => external = true,
                _ => {}
            }
        }
        if local && external {
            return Err(QueryPlanError::UnsupportedNode(
                "local state and external exact input have no common snapshot proof".into(),
            ));
        }
        Ok(())
    }

    /// Validate references, bindings, reachability, and cycles before activation.
    pub fn validate(&self, available: &BTreeSet<PolicyFingerprint>) -> Result<(), QueryPlanError> {
        self.validate_snapshot_sources()?;
        if !self.nodes.contains_key(&self.root) {
            return Err(QueryPlanError::Invalid(format!(
                "query `{}` has missing root {}",
                self.query_id, self.root.0
            )));
        }
        if self.physical_vector_binding().is_some() {
            self.recover_vector_physical_dag()?;
        } else if self.population_snapshot().is_some() {
            self.recover_population_physical_dag()?;
        } else if self.physical_dag.is_some() {
            return Err(QueryPlanError::Invalid(
                "physical program has no deployment input bindings".into(),
            ));
        }
        for (id, node) in &self.nodes {
            if let QueryPlanNode::PhysicalRelation { inputs, dag } = node {
                if self.language != QueryLanguage::ClickHouseSql {
                    return Err(QueryPlanError::Invalid(
                        "physical relation requires a SQL result binding".into(),
                    ));
                }
                let compiled =
                    asap_physical_operators::physical_planner::CompiledPhysicalDag::decode(dag)
                        .map_err(|e| QueryPlanError::Invalid(e.to_string()))?;
                for ((_, contract), input) in compiled.input_contracts().zip(inputs) {
                    if let Some(QueryPlanNode::ExternalExact { request, .. }) =
                        self.nodes.get(input)
                    {
                        let ExternalExactOutput::Relation { schema } = &request.output else {
                            return Err(QueryPlanError::Invalid(
                                "physical relation requires a relational external input".into(),
                            ));
                        };
                        let schema: planner_types::post_asap::SummarySchema =
                            serde_json::from_value(schema.clone())
                                .map_err(|e| QueryPlanError::Invalid(e.to_string()))?;
                        if &schema != contract.schema.as_ref() {
                            return Err(QueryPlanError::Invalid(
                                "physical relation input differs from its bound source schema"
                                    .into(),
                            ));
                        }
                    }
                }
                if compiled.roots().len() != 1 || compiled.input_contracts().count() != inputs.len()
                {
                    return Err(QueryPlanError::Invalid(
                        "physical relation boundary arity mismatch".into(),
                    ));
                }
            }
            if let QueryPlanNode::PhysicalFragment {
                inputs,
                dag,
                row_input,
                pruning,
            } = node
            {
                let compiled =
                    asap_physical_operators::physical_planner::CompiledPhysicalDag::decode(dag)
                        .map_err(|e| QueryPlanError::Invalid(e.to_string()))?;
                use asap_physical_operators::physical_planner::promql_values;
                let value_schema = |schema: &asap_physical_operators::values::Schema| {
                    schema == &promql_values::scalar_schema()
                        || schema == &promql_values::vector_schema()
                        || schema == &promql_values::matrix_schema()
                };
                let canonical_values = compiled
                    .input_contracts()
                    .all(|(_, input)| value_schema(&input.schema))
                    && compiled.roots().len() == 1
                    && value_schema(
                        &compiled
                            .output_contract(compiled.roots()[0])
                            .map_err(|e| QueryPlanError::Invalid(e.to_string()))?
                            .schema,
                    );
                if canonical_values {
                    if row_input.is_some() || pruning.is_some() {
                        return Err(QueryPlanError::Invalid("complete label-map computation cannot carry an external row-identity adapter".into()));
                    }
                } else {
                    for (_, contract) in compiled.input_contracts() {
                        let mut samples = 0;
                        for (index, field) in contract.schema.fields.iter().enumerate() {
                            use planner_types::{post_asap::SummaryFamilyType, pre_asap::DataType};
                            match &field.dtype {
                                SummaryFamilyType::Plain(DataType::Float64 | DataType::Int64) => {
                                    samples += 1
                                }
                                SummaryFamilyType::Plain(DataType::Utf8) => {}
                                SummaryFamilyType::Plain(DataType::Timestamp)
                                    if contract.schema.time_index == Some(index) => {}
                                _ => {
                                    return Err(QueryPlanError::Invalid(format!(
                                        "PromQL input binding cannot supply field {}",
                                        field.name
                                    )))
                                }
                            }
                        }
                        if samples > 1 {
                            return Err(QueryPlanError::Invalid(
                                "PromQL vector input has only one numeric sample per row".into(),
                            ));
                        }
                    }
                    if compiled.roots().len() != 1 {
                        return Err(QueryPlanError::Invalid(
                            "physical vector requires one root".into(),
                        ));
                    }
                    if let Some(row_input) = row_input {
                        let source = compiled.input_contracts().nth(*row_input).map(|(id, _)| id);
                        if source.is_none() || compiled.row_source(compiled.roots()[0]) != source {
                            return Err(QueryPlanError::Invalid(
                                "physical vector output must preserve its bound input rows".into(),
                            ));
                        }
                    } else {
                        if compiled.row_source(compiled.roots()[0]).is_some() {
                            return Err(QueryPlanError::Invalid(
                            "row-preserving physical output requires its input identity binding"
                                .into(),
                        ));
                        }
                        let output = compiled
                            .output_contract(compiled.roots()[0])
                            .map_err(|e| QueryPlanError::Invalid(e.to_string()))?;
                        let numeric = output
                            .schema
                            .fields
                            .iter()
                            .filter(|field| {
                                matches!(
                                    field.dtype,
                                    planner_types::post_asap::SummaryFamilyType::Plain(
                                        planner_types::pre_asap::DataType::Float64
                                            | planner_types::pre_asap::DataType::Int64
                                    )
                                )
                            })
                            .count();
                        if numeric != 1
                            || output.schema.fields.iter().any(|field| {
                                !matches!(
                                    field.dtype,
                                    planner_types::post_asap::SummaryFamilyType::Plain(
                                        planner_types::pre_asap::DataType::Float64
                                            | planner_types::pre_asap::DataType::Int64
                                            | planner_types::pre_asap::DataType::Utf8
                                            | planner_types::pre_asap::DataType::Timestamp
                                    )
                                )
                            })
                        {
                            return Err(QueryPlanError::Invalid(
                                "physical output cannot bind to a PromQL vector".into(),
                            ));
                        }
                    }
                }
                if let Some(pruning) = pruning {
                    if matches!(&pruning.completeness, CandidateCompleteness::Certified { guarantee }
                        if guarantee.metric != planner_types::post_asap::ErrorMetric::TopKMembership || guarantee.bound.evaluate().is_none() || guarantee.failure_probability.evaluate().is_none())
                    {
                        return Err(QueryPlanError::Invalid(
                            "invalid physical pruning certificate".into(),
                        ));
                    }
                    let contracts = compiled.input_contracts().collect::<Vec<_>>();
                    let left = contracts
                        .get(row_input.ok_or_else(|| {
                            QueryPlanError::Invalid("pruning requires preserved input rows".into())
                        })?)
                        .ok_or_else(|| QueryPlanError::Invalid("invalid row input".into()))?
                        .1;
                    let right = contracts
                        .get(pruning.candidate_input)
                        .ok_or_else(|| QueryPlanError::Invalid("invalid candidate input".into()))?
                        .1;
                    if matches!(
                        pruning.completeness,
                        CandidateCompleteness::Certified { .. }
                    ) && compiled.certified_pruning_keys(compiled.roots()[0])
                        != Some(pruning.keys.as_slice())
                    {
                        return Err(QueryPlanError::Invalid(
                            "certified pruning binding requires native coverage validation".into(),
                        ));
                    }
                    for &(l, r) in &pruning.keys {
                        if left
                            .schema
                            .fields
                            .get(l)
                            .zip(right.schema.fields.get(r))
                            .is_none_or(|(l, r)| l.dtype != r.dtype)
                        {
                            return Err(QueryPlanError::Invalid(
                                "invalid pruning key types".into(),
                            ));
                        }
                    }
                }
                if compiled.input_contracts().count() != inputs.len()
                    || compiled.roots().len() != 1
                    || row_input.is_some_and(|index| index >= inputs.len())
                {
                    return Err(QueryPlanError::Invalid(
                        "physical input/root binding mismatch".into(),
                    ));
                }
            }

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

            for input in node.inputs() {
                if !self.nodes.contains_key(input) {
                    return Err(QueryPlanError::Invalid(format!(
                        "query `{}` node {} references missing input {}",
                        self.query_id, id.0, input.0
                    )));
                }
            }
            if let QueryPlanNode::ReadMaterialization { binding } = node {
                if binding.stored_output_reference.stored_output_id != binding.materialization {
                    return Err(QueryPlanError::Invalid(
                        "read binding has invalid stored output or definition".into(),
                    ));
                }
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
    pub stored_output_reference: crate::sds::StoredOutputReference,
    /// Complete-window storage advances independently of its stored extent.
    /// None denotes disjoint pane storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_window_slide_ms: Option<u64>,
    pub materialization: StoredOutputId,
    /// Query operator grouping applied while folding those SIDs.
    pub output_grouping: PhysicalGrouping,
    /// Labels whose values form an item identity inside a keyed sketch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub item_labels: Vec<String>,
    pub window_ms: u64,
    /// Unix millisecond timestamp on the materialized pane-boundary grid.
    /// Legacy plans deserialize this as unknown and fall back at read time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
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

/// Required candidate rows must have authoritative values at the bound source.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PruningInputContract {
    pub candidate_input: usize,
    pub keys: Vec<(usize, usize)>,
    pub completeness: CandidateCompleteness,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryPlanNode {
    /// Typed deployment inputs for the Planner-provided vector computation.
    Physical {
        inputs: Vec<QueryNodeId>,
        source_nodes: Vec<u64>,
        max_bytes: u64,
    },
    /// Complete Planner-compiled relation computation; inputs follow its typed slots.
    PhysicalRelation {
        inputs: Vec<QueryNodeId>,
        dag: Vec<u8>,
    },
    /// Planner-compiled computation. Input order follows the physical input contracts.
    PhysicalFragment {
        inputs: Vec<QueryNodeId>,
        dag: Vec<u8>,
        row_input: Option<usize>,
        pruning: Option<PruningInputContract>,
    },

    Logical {
        operator: query_time::QueryTimeOperator,
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
    pub fn inputs(&self) -> &[QueryNodeId] {
        match self {
            Self::Scalar { .. } | Self::ReadMaterialization { .. } | Self::ExactFallback { .. } => {
                &[]
            }
            Self::Binary { inputs, .. } => inputs,
            Self::ReduceSum { input, .. }
            | Self::SummaryEstimate { input, .. }
            | Self::ExactReadout { input, .. } => std::slice::from_ref(input),
            Self::Physical { inputs, .. }
            | Self::PhysicalRelation { inputs, .. }
            | Self::PhysicalFragment { inputs, .. }
            | Self::SummaryMerge { inputs }
            | Self::Logical { inputs, .. }
            | Self::ExternalExact { inputs, .. } => inputs,
        }
    }
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

impl ExactReadout {
    /// Planner family required by this installed DAG readout node.
    pub fn planner_family(self) -> planner_types::post_asap::SummaryFamilyType {
        use planner_types::post_asap::{ExactKind, ExactParams, SummaryFamilyType};
        let (kind, params) = match self {
            Self::Sum => (ExactKind::Sum, ExactParams::Sum),
            Self::Count => (ExactKind::Count, ExactParams::Count),
            Self::Increase => (ExactKind::Increase, ExactParams::Increase),
            Self::Rate => (ExactKind::Rate, ExactParams::Rate),
            Self::Min => (ExactKind::Min, ExactParams::Min),
            Self::Max => (ExactKind::Max, ExactParams::Max),
        };
        SummaryFamilyType::ExactAggregate(kind, params)
    }
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
    // Installed plans cross producer/query threads without Planner Rc state.
    #[test]
    fn installed_query_contract_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<super::QueryPlan>();
        assert_send_sync::<super::QueryPlanEntry>();
    }
}

#[cfg(test)]
mod retired_plan_tests {
    // Row-preserving operators cannot opt out of the original vector identity.
    #[test]
    fn row_preserving_graph_requires_its_identity_binding() {
        use super::*;
        use asap_physical_operators::{
            operators::Operator,
            physical_planner::{CompiledPhysicalDag, InputContract},
        };
        use planner_types::{
            post_asap::{SummaryFamilyType, SummaryField, SummarySchema},
            pre_asap::DataType,
        };
        let schema = std::sync::Arc::new(SummarySchema {
            fields: vec![SummaryField {
                name: "value".into(),
                dtype: SummaryFamilyType::Plain(DataType::Float64),
                nullable: false,
            }],
            time_index: None,
        });
        let program = CompiledPhysicalDag::from_operators(
            [(0, InputContract::bounded(schema.clone()))].into(),
            [(1, (vec![0], Operator::limit(schema, 1, 0, vec![]).unwrap()))].into(),
            vec![1],
        )
        .unwrap();
        let mut entry = QueryPlanEntry {
            language: QueryLanguage::PromQl,
            query_id: "identity".into(),
            canonical_query: "m".into(),
            fixed_evaluation: None,
            physical_dag: None,
            root: QueryNodeId(1),
            nodes: [
                (
                    QueryNodeId(0),
                    QueryPlanNode::ExactFallback {
                        reason: "bound vector".into(),
                    },
                ),
                (
                    QueryNodeId(1),
                    QueryPlanNode::PhysicalFragment {
                        inputs: vec![QueryNodeId(0)],
                        dag: program.encode().unwrap(),
                        row_input: None,
                        pruning: None,
                    },
                ),
            ]
            .into(),
            instant: InstantExecution {
                lookback_ms: 0,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::Reject,
        };
        assert!(entry
            .validate(&BTreeSet::new())
            .unwrap_err()
            .to_string()
            .contains("identity binding"));
        if let QueryPlanNode::PhysicalFragment { row_input, .. } =
            entry.nodes.get_mut(&QueryNodeId(1)).unwrap()
        {
            *row_input = Some(0);
        }
        entry.validate(&BTreeSet::new()).unwrap();
    }

    #[test]
    fn uncompiled_relation_variants_are_not_accepted() {
        for kind in ["relational", "relational_join"] {
            let error =
                serde_json::from_value::<super::QueryPlanNode>(serde_json::json!({"op":kind}))
                    .unwrap_err();
            assert!(error.to_string().contains("unknown variant"), "{error}");
        }
    }
}
