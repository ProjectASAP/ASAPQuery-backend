use super::compiler::{
    derived_window_cost, gcd, retained_state_count, CollectorMaterialization,
    MaterializationLifecycleEstimate, PhysicalCompilationRequest, RuntimeRulePolicy,
};
use asap_types::WindowMaterializationLayout;
use planner_types::post_asap::{PostAsapNodeId, SummaryWindowFramework};
use std::collections::{BTreeMap, BTreeSet};

/// Select and install cheaper shared panes without changing logical readouts.
/// Only raw additive states are eligible; derived cohorts retain their full-window identity.
#[allow(clippy::too_many_arguments)]
pub(super) fn share_additive_panes(
    request: &PhysicalCompilationRequest,
    materializations: &mut [asap_types::PrecomputeMaterialization],
    producers: &mut Vec<CollectorMaterialization>,
    plan_producers: &mut [CollectorMaterialization],
    bindings: &mut BTreeMap<(usize, PostAsapNodeId), asap_types::PolicyFingerprint>,
    policies: &mut BTreeMap<asap_types::PolicyFingerprint, RuntimeRulePolicy>,
    estimates: &mut BTreeMap<asap_types::PolicyFingerprint, MaterializationLifecycleEstimate>,
) {
    use asap_types::{AggregationType, WindowKind};
    let derived_sources = materializations
        .iter()
        .filter_map(|m| m.derived_input.as_ref())
        .flat_map(|d| d.inputs.iter().map(|id| id.fingerprint()))
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut physical = Vec::new();
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut keys = Vec::new();
    let mut member_consumers = Vec::new();
    for m in materializations.iter() {
        let old = m.policy_fingerprint();
        if !seen.insert(old)
            || m.derived_input.is_some()
            || derived_sources.contains(&old)
            || !matches!(m.aggregation_type, AggregationType::Sum)
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
        let Some(estimate) = estimates.get(&old) else {
            continue;
        };
        // Provenance belongs to the selected layout, not to its request entry point.
        if consumers.iter().any(|index| {
            !request.queries[*index]
                .window_realization_candidates
                .iter()
                .any(|candidate| {
                    candidate.realization_id == estimate.window_realization_id && candidate.derived
                })
        }) {
            continue;
        }
        let Some(policy) = policies.get(&old) else {
            continue;
        };
        let mut lifecycle = query.summary_lifecycle_inputs.clone();
        lifecycle.evaluation_interval_ms = 0;
        if consumers.iter().any(|index| {
            let mut other = request.queries[*index].summary_lifecycle_inputs.clone();
            other.evaluation_interval_ms = 0;
            other != lifecycle || request.queries[*index].accuracy_target != query.accuracy_target
        }) {
            continue;
        }
        // Normalize only layout dimensions. Source, projection, grouping, state,
        // policy and unit-cost evidence must still agree before considering reuse.
        let mut canonical = m.clone();
        canonical.window_size = 1;
        canonical.slide_interval = 1;
        canonical.window_type = WindowKind::Tumbling;
        canonical.window_layout = WindowMaterializationLayout::Pane { pane_secs: 1 };
        canonical.pane_origin_ms = Some(0);
        let key = (
            canonical.policy_fingerprint(),
            serde_json::to_string(&lifecycle).unwrap(),
            serde_json::to_string(policy).unwrap(),
            serde_json::to_string(&query.accuracy_target).unwrap(),
        );
        let index = physical.len();
        if let Some(group) = groups.iter_mut().find(|group| keys[group[0]] == key) {
            group.push(index);
        } else {
            groups.push(vec![index]);
        }
        keys.push(key);
        member_consumers.push(consumers);
        physical.push((old, m.clone()));
    }
    let groups = select_shared_groups(request, &physical, &member_consumers, estimates, groups);
    for group in &groups {
        for &index in &group.members {
            let canonical = &mut physical[index].1;
            canonical.window_size = group.pane_secs;
            canonical.slide_interval = group.pane_secs;
            canonical.window_type = WindowKind::Tumbling;
            canonical.window_layout = WindowMaterializationLayout::Pane {
                pane_secs: group.pane_secs,
            };
            canonical.pane_origin_ms = Some(group.origin);
        }
    }
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
            request.query_retention_margin_ms,
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
        combined.window_realization_id = format!("shared-pane-{}", new.0);
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
            producer.window_layout = canonical.window_layout.clone();
            producer.pane_origin_ms = canonical.pane_origin_ms;
            producer.abstract_window_framework = SummaryWindowFramework::Tumbling;
            producer.window_realization_id = estimates[&new].window_realization_id.clone();
        }
    }
    let mut seen = BTreeSet::new();
    producers.retain(|producer| seen.insert(producer.materialization));
}

struct SharedGroup {
    members: Vec<usize>,
    lookback_ms: u64,
    cost: f64,
    savings: f64,
    pane_secs: u64,
    origin: i64,
}

fn select_shared_groups(
    request: &PhysicalCompilationRequest,
    physical: &[(
        asap_types::PolicyFingerprint,
        asap_types::PrecomputeMaterialization,
    )],
    member_consumers: &[BTreeSet<usize>],
    estimates: &BTreeMap<asap_types::PolicyFingerprint, MaterializationLifecycleEstimate>,
    groups: Vec<Vec<usize>>,
) -> Vec<SharedGroup> {
    let price = |members: Vec<usize>| {
        if members.len() < 2 {
            return None;
        }
        let pane_secs = members
            .iter()
            .map(|&i| physical[i].1.window_layout.base_pane_secs())
            .reduce(gcd)
            .unwrap();
        let origin = physical[members[0]]
            .1
            .pane_origin_ms
            .unwrap()
            .rem_euclid((pane_secs * 1_000) as i64);
        if members.iter().any(|&i| {
            physical[i]
                .1
                .pane_origin_ms
                .unwrap()
                .rem_euclid((pane_secs * 1_000) as i64)
                != origin
        }) {
            return None;
        }
        let mut independent = 0.0;
        let mut producer = 0.0_f64;
        let mut reads = 0.0;
        let mut lookback_ms = 0;
        for &index in &members {
            let (old, m) = &physical[index];
            let consumers = &member_consumers[index];
            let query = &request.queries[*consumers.first().unwrap()];
            let selected_id = &estimates[old].window_realization_id;
            let template = &query
                .window_realization_candidates
                .iter()
                .find(|candidate| &candidate.realization_id == selected_id)
                .unwrap()
                .cost;
            let mut maintenance = query.summary_lifecycle_inputs.clone();
            maintenance.costs.read = 0.0;
            independent += derived_window_cost(
                template,
                &maintenance,
                m.window_size,
                m.slide_interval,
                &m.window_layout,
                request.query_retention_margin_ms,
            )
            .weighted_cost;
            producer = producer.max(
                derived_window_cost(
                    template,
                    &maintenance,
                    m.window_size,
                    m.slide_interval,
                    &WindowMaterializationLayout::Pane { pane_secs },
                    request.query_retention_margin_ms,
                )
                .weighted_cost,
            );
            for &consumer in consumers {
                let lifecycle = &request.queries[consumer].summary_lifecycle_inputs;
                let unit_reads = lifecycle.costs.read * lifecycle.horizon_seconds
                    / (f64::from(lifecycle.evaluation_interval_ms) / 1_000.0);
                independent +=
                    unit_reads * (m.window_size / m.window_layout.base_pane_secs()) as f64;
                reads += unit_reads * (m.window_size / pane_secs) as f64;
            }
            lookback_ms = lookback_ms.max(m.window_size.saturating_mul(1_000));
        }
        let cost = producer + reads;
        if !independent.is_finite() || !cost.is_finite() || cost >= independent {
            return None;
        }
        Some(SharedGroup {
            members,
            lookback_ms,
            cost,
            savings: independent - cost,
            pane_secs,
            origin,
        })
    };
    let mut selected_groups = Vec::new();
    for mut remaining in groups {
        while remaining.len() >= 2 {
            if let Some(group) = price(remaining.clone()) {
                selected_groups.push(group);
                break;
            }
            // An incompatible phase or expensive fine-pane consumer must not
            // prevent the remaining consumers from sharing profitable state.
            let mut best: Option<SharedGroup> = None;
            for (position, &left) in remaining.iter().enumerate() {
                for &right in &remaining[position + 1..] {
                    if let Some(group) = price(vec![left, right]) {
                        if best
                            .as_ref()
                            .is_none_or(|previous| group.savings > previous.savings)
                        {
                            best = Some(group);
                        }
                    }
                }
            }
            let Some(group) = best else {
                break;
            };
            remaining.retain(|index| !group.members.contains(index));
            selected_groups.push(group);
        }
    }
    selected_groups
}
