//! Persists the `QueryWorkload` + `WorkloadCharacteristics` associated with
//! each planned `(metric, AggRole)` pair so that the re-planner can re-run
//! `plan()` without needing the original `QuerySpec` HTTP payload.
//!
//! **B2 full restructure**: the store is keyed by `(metric, AggRole)`
//! rather than `metric` alone. A single metric (e.g. `http_requests_total`)
//! that carries multiple PromQL shapes (sum/quantile/count) registers
//! one entry per role, each with its own plan and downstream
//! `AggregationConfig`. See [`crate::workload::AggRole`] for the
//! classification rules and the B2 PR description.
use std::collections::HashMap;
use std::sync::RwLock;

use crate::types::{QueryWorkload, WorkloadCharacteristics};
use crate::workload::AggRole;

/// Composite key `(metric_name, role)` for the store.
pub type WorkloadKey = (String, AggRole);

pub struct WorkloadStore {
    inner: RwLock<HashMap<WorkloadKey, (QueryWorkload, WorkloadCharacteristics)>>,
}

impl WorkloadStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Insert (or replace) the entry for a `(metric, role)` pair.
    ///
    /// **B2 contract**: prior collisions on metric alone would overwrite
    /// (silently dropping all but the last YAML entry). Now collisions
    /// only happen when metric AND role coincide — re-registering the
    /// same `(metric, role)` is the legitimate update path (controller
    /// HTTP `POST /api/v1/plan` re-issuing the same shape).
    pub fn set(
        &self,
        metric: impl Into<String>,
        role: AggRole,
        wl: QueryWorkload,
        wc: WorkloadCharacteristics,
    ) {
        self.inner
            .write()
            .unwrap()
            .insert((metric.into(), role), (wl, wc));
    }

    /// Returns a clone of `(workload, characteristics)` for the
    /// `(metric, role)` pair if known.
    pub fn get(
        &self,
        metric: &str,
        role: AggRole,
    ) -> Option<(QueryWorkload, WorkloadCharacteristics)> {
        self.inner
            .read()
            .unwrap()
            .get(&(metric.to_string(), role))
            .cloned()
    }

    /// Returns every `(workload, characteristics)` pair registered for
    /// `metric`, across all roles. Empty vec when nothing is registered
    /// for the metric. Order is unspecified — sort if determinism matters.
    pub fn get_all_for_metric(
        &self,
        metric: &str,
    ) -> Vec<(AggRole, QueryWorkload, WorkloadCharacteristics)> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .filter(|((m, _), _)| m == metric)
            .map(|((_, role), (wl, wc))| (*role, wl.clone(), wc.clone()))
            .collect()
    }

    /// Returns every `(metric, role)` key currently registered. Used by
    /// emit paths that walk the registry to build per-pair routing /
    /// aggregation tables.
    pub fn keys(&self) -> Vec<WorkloadKey> {
        self.inner.read().unwrap().keys().cloned().collect()
    }

    pub fn remove(&self, metric: &str, role: AggRole) {
        self.inner
            .write()
            .unwrap()
            .remove(&(metric.to_string(), role));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AggType;
    use std::collections::HashMap;
    use std::time::Duration;

    fn wl(name: &str) -> QueryWorkload {
        QueryWorkload {
            metric_name: name.into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: vec![AggType::Quantile],
            time_window: Duration::from_secs(300),
            repeat_every: None,
            accuracy_sla: 0.01,
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
    }

    #[test]
    fn set_and_get() {
        let s = WorkloadStore::new();
        s.set(
            "latency",
            AggRole::Quantile,
            wl("latency"),
            WorkloadCharacteristics::default(),
        );
        let (got, _) = s.get("latency", AggRole::Quantile).unwrap();
        assert_eq!(got.metric_name, "latency");
    }

    #[test]
    fn unknown_metric_returns_none() {
        let s = WorkloadStore::new();
        assert!(s.get("nope", AggRole::Quantile).is_none());
    }

    #[test]
    fn unknown_role_for_known_metric_returns_none() {
        let s = WorkloadStore::new();
        s.set("m", AggRole::Quantile, wl("m"), WorkloadCharacteristics::default());
        assert!(s.get("m", AggRole::Sum).is_none());
    }

    #[test]
    fn overwrite_same_role_replaces() {
        let s = WorkloadStore::new();
        s.set("m", AggRole::Quantile, wl("m"), WorkloadCharacteristics::default());
        let mut updated = wl("m");
        updated.accuracy_sla = 0.05;
        s.set("m", AggRole::Quantile, updated, WorkloadCharacteristics::default());
        let (got, _) = s.get("m", AggRole::Quantile).unwrap();
        assert_eq!(got.accuracy_sla, 0.05);
    }

    #[test]
    fn different_roles_for_same_metric_coexist() {
        // The core B2 contract: a single metric can carry multiple roles
        // and each entry persists independently of the others.
        let s = WorkloadStore::new();
        let mut wl_q = wl("http_requests_total");
        wl_q.accuracy_sla = 0.01;
        let mut wl_s = wl("http_requests_total");
        wl_s.accuracy_sla = 0.02;
        let mut wl_c = wl("http_requests_total");
        wl_c.accuracy_sla = 0.03;
        s.set(
            "http_requests_total",
            AggRole::Quantile,
            wl_q,
            WorkloadCharacteristics::default(),
        );
        s.set(
            "http_requests_total",
            AggRole::Sum,
            wl_s,
            WorkloadCharacteristics::default(),
        );
        s.set(
            "http_requests_total",
            AggRole::Count,
            wl_c,
            WorkloadCharacteristics::default(),
        );

        // All three persist (the pre-B2 store would have collapsed
        // them onto one key, only the last survives).
        assert_eq!(
            s.get("http_requests_total", AggRole::Quantile)
                .unwrap()
                .0
                .accuracy_sla,
            0.01
        );
        assert_eq!(
            s.get("http_requests_total", AggRole::Sum)
                .unwrap()
                .0
                .accuracy_sla,
            0.02
        );
        assert_eq!(
            s.get("http_requests_total", AggRole::Count)
                .unwrap()
                .0
                .accuracy_sla,
            0.03
        );
        // `get_all_for_metric` surfaces all three.
        let all = s.get_all_for_metric("http_requests_total");
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn remove_clears_only_target_role() {
        let s = WorkloadStore::new();
        s.set("m", AggRole::Quantile, wl("m"), WorkloadCharacteristics::default());
        s.set("m", AggRole::Sum, wl("m"), WorkloadCharacteristics::default());
        s.remove("m", AggRole::Quantile);
        assert!(s.get("m", AggRole::Quantile).is_none());
        assert!(s.get("m", AggRole::Sum).is_some());
    }

    #[test]
    fn three_http_requests_total_entries_persist_after_pre_pop_loop_mirror() {
        // B2 regression: mirror the (analyzer → planner →
        // workload_store.set) pre-pop loop from main.rs for the
        // three http_requests_total entries in mvp-workload.yaml.
        // Pre-B2 only the LAST entry survived; the role-keyed store
        // keeps Sum + Count distinct. (The two Sum-shaped entries
        // legitimately overwrite each other under the same key —
        // that's the operator-update path.)
        use crate::workload::{derive_agg_role, WorkloadEntry};
        let entries = vec![
            WorkloadEntry {
                metric_name: "http_requests_total".into(),
                query_string: Some("sum by (zone) (http_requests_total)".into()),
                accuracy_sla: 0.0,
                assign_to_role: "gateway".into(),
                sketch_family_override: None,
                target_path: None,
                grouping_labels: vec!["zone".into()],
                sample_p: 1.0,
                distinct_keys_per_window: None,
                item_label: None,
            },
            WorkloadEntry {
                metric_name: "http_requests_total".into(),
                query_string: Some("sum by (zone) (rate(http_requests_total[5m]))".into()),
                accuracy_sla: 0.01,
                assign_to_role: "agent".into(),
                sketch_family_override: None,
                target_path: None,
                grouping_labels: vec!["zone".into()],
                sample_p: 1.0,
                distinct_keys_per_window: None,
                item_label: None,
            },
            WorkloadEntry {
                metric_name: "http_requests_total".into(),
                query_string: Some(r#"count(http_requests_total{zone="z0"})"#.into()),
                accuracy_sla: 0.0,
                assign_to_role: "archive".into(),
                sketch_family_override: None,
                target_path: None,
                grouping_labels: vec!["zone".into()],
                sample_p: 1.0,
                distinct_keys_per_window: None,
                item_label: None,
            },
        ];

        let store = WorkloadStore::new();
        for entry in &entries {
            let role = derive_agg_role(entry);
            store.set(
                &entry.metric_name,
                role,
                wl(&entry.metric_name),
                WorkloadCharacteristics::default(),
            );
        }

        // Both distinct roles persist after the loop (vs pre-B2: only
        // the last `set` survives because the key was metric only).
        let all = store.get_all_for_metric("http_requests_total");
        let roles: std::collections::HashSet<_> = all.iter().map(|(r, _, _)| *r).collect();
        assert!(
            roles.contains(&crate::workload::AggRole::Sum),
            "Sum-role plan must survive after the pre-pop loop; got {roles:?}"
        );
        assert!(
            roles.contains(&crate::workload::AggRole::Count),
            "Count-role plan must survive after the pre-pop loop; got {roles:?}"
        );
    }

    #[test]
    fn keys_returns_all_pairs() {
        let s = WorkloadStore::new();
        s.set("a", AggRole::Quantile, wl("a"), WorkloadCharacteristics::default());
        s.set("b", AggRole::Sum, wl("b"), WorkloadCharacteristics::default());
        let mut keys = s.keys();
        keys.sort();
        assert_eq!(
            keys,
            vec![("a".to_string(), AggRole::Quantile), ("b".to_string(), AggRole::Sum)]
        );
    }
}
