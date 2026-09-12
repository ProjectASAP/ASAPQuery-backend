use super::compiler::{
    derived_window_cost, retained_state_count, CollectorMaterialization,
    MaterializationLifecycleEstimate, PlanningRequest, RuntimeRulePolicy,
};
use planner_types::post_asap::{PostAsapNodeId, SummaryWindowFramework};
use std::collections::{BTreeMap, BTreeSet};

/// Lower Planner's costed pane-reuse groups without changing logical readouts.
/// Only raw additive states are eligible; derived cohorts retain their full-window identity.
#[allow(clippy::too_many_arguments)]
pub(super) fn share_additive_panes(
    request: &PlanningRequest,
    materializations: &mut [asap_types::PrecomputeMaterialization],
    producers: &mut Vec<CollectorMaterialization>,
    plan_producers: &mut [CollectorMaterialization],
    bindings: &mut BTreeMap<(usize, PostAsapNodeId), asap_types::PolicyFingerprint>,
    policies: &mut BTreeMap<asap_types::PolicyFingerprint, RuntimeRulePolicy>,
    estimates: &mut BTreeMap<asap_types::PolicyFingerprint, MaterializationLifecycleEstimate>,
) {
    use asap_aware_mapping::pane_sharing::{select_shared_panes, PaneReuseCandidate};
    use asap_types::{AggregationType, WindowKind, WindowMaterializationLayout};
    let derived_sources = materializations
        .iter()
        .filter_map(|m| m.derived_input.as_ref())
        .flat_map(|d| d.inputs.iter().map(|id| id.fingerprint()))
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut physical = Vec::new();
    let mut offers = Vec::new();
    for m in materializations.iter() {
        let old = m.policy_fingerprint();
        if !seen.insert(old)
            || m.derived_input.is_some()
            || derived_sources.contains(&old)
            || !matches!(
                m.aggregation_type,
                AggregationType::Sum | AggregationType::MultipleSum
            )
        {
            continue;
        }
        let WindowMaterializationLayout::Pane { pane_secs } = m.window_layout else {
            continue;
        };
        if pane_secs == 0 || m.pane_origin_ms.is_none() {
            continue;
        }
        let consumers = bindings
            .iter()
            .filter(|(_, id)| **id == old)
            .map(|((query, _), _)| *query)
            .collect::<BTreeSet<_>>();
        let Some(&first) = consumers.first() else {
            continue;
        };
        let query = &request.queries[first];
        // Explicitly priced implementations are not repriced or replaced.
        if consumers.iter().any(|index| {
            let q = &request.queries[*index];
            q.lifecycle != query.lifecycle
                || q.accuracy != query.accuracy
                || !request.synthesized_window_queries.contains(&q.query_id)
        }) {
            continue;
        }
        let mut canonical = m.clone();
        canonical.window_size = pane_secs;
        canonical.slide_interval = pane_secs;
        canonical.window_type = WindowKind::Tumbling;
        let Some(policy) = policies.get(&old) else {
            continue;
        };
        let key = (
            canonical.policy_fingerprint(),
            serde_json::to_string(&query.lifecycle).unwrap(),
            serde_json::to_string(policy).unwrap(),
        );
        let mut maintenance = query.lifecycle.clone();
        maintenance.costs.read = 0.0;
        let Some(template) = query.window_implementations.first() else {
            continue;
        };
        let producer_cost = derived_window_cost(
            &template.cost,
            &maintenance,
            m.window_size,
            m.slide_interval,
            &m.window_layout,
            request.query_staleness_margin_ms,
        )
        .weighted_cost;
        let read_cost = query.lifecycle.costs.read * query.lifecycle.horizon_seconds
            / (f64::from(query.lifecycle.evaluation_interval_ms) / 1000.0)
            * (m.window_size / pane_secs) as f64
            * consumers.len() as f64;
        offers.push(PaneReuseCandidate {
            compatibility: key,
            lookback_ms: m.window_size.saturating_mul(1000),
            producer_cost,
            read_cost,
        });
        physical.push((old, canonical));
    }
    let groups = select_shared_panes(&offers);
    let mut target_counts = BTreeMap::new();
    for group in &groups {
        *target_counts
            .entry(physical[group.members[0]].1.policy_fingerprint())
            .or_insert(0) += 1;
    }
    let mut replacements = BTreeMap::new();
    for group in groups {
        let mut canonical = physical[group.members[0]].1.clone();
        canonical.num_aggregates_to_retain = Some(retained_state_count(
            group.lookback_ms,
            request.query_staleness_margin_ms,
            canonical.slide_interval * 1000,
            &canonical.window_layout,
        ));
        let new = canonical.policy_fingerprint();
        let members = group
            .members
            .iter()
            .map(|index| physical[*index].0)
            .collect::<BTreeSet<_>>();
        // A physical ID cannot hide a second policy/evidence cohort or an
        // independently installed producer that was not offered for sharing.
        if target_counts[&new] != 1
            || materializations.iter().any(|m| {
                let id = m.policy_fingerprint();
                id == new && !members.contains(&id)
            })
        {
            continue;
        }
        let mut combined = estimates[&physical[group.members[0]].0].clone();
        combined.materialization = new.into();
        combined.consumer_query_ids.clear();
        combined.expected_reads = 0.0;
        combined.expected_updates = 0.0;
        combined.lifecycle_cost = group.cost;
        combined.window_implementation_id = format!("shared-pane-{}", new.0);
        for index in group.members {
            let old = physical[index].0;
            if let Some(estimate) = estimates.remove(&old) {
                combined
                    .consumer_query_ids
                    .extend(estimate.consumer_query_ids);
                combined.expected_reads += estimate.expected_reads;
                combined.expected_updates =
                    combined.expected_updates.max(estimate.expected_updates);
            }
            replacements.insert(old, canonical.clone());
        }
        combined.consumer_query_ids.sort();
        combined.consumer_query_ids.dedup();
        estimates.insert(new, combined);
    }
    for m in materializations {
        if let Some(canonical) = replacements.get(&m.policy_fingerprint()) {
            *m = canonical.clone();
        }
    }
    for id in bindings.values_mut() {
        if let Some(canonical) = replacements.get(id) {
            *id = canonical.policy_fingerprint();
        }
    }
    for (old, canonical) in &replacements {
        if let Some(policy) = policies.remove(old) {
            policies.insert(canonical.policy_fingerprint(), policy);
        }
    }
    for producer in producers.iter_mut().chain(plan_producers.iter_mut()) {
        if let Some(canonical) = replacements.get(&producer.materialization.fingerprint()) {
            let new = canonical.policy_fingerprint();
            producer.materialization = new.into();
            producer.query_id = format!("state-{}", new.0);
            producer.window_secs = canonical.window_size;
            producer.slide_secs = canonical.slide_interval;
            producer.abstract_window_framework = SummaryWindowFramework::Tumbling;
            producer.window_implementation_id = estimates[&new].window_implementation_id.clone();
        }
    }
    let mut seen = BTreeSet::new();
    producers.retain(|producer| seen.insert(producer.materialization));
}
