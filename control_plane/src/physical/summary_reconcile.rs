//! Reconcile the control-plane desired SummaryCatalog with data-plane observed
//! instance metadata. Summary payloads remain owned by SummaryStore.

use std::collections::BTreeSet;

use asap_types::sds::{
    CatalogGeneration, InstanceLifecycle, MaterializationId, ObservedSummaryInventory,
    SummaryInstanceId, SummaryInstanceStatus,
};
use serde::{Deserialize, Serialize};

use super::summary_catalog::{SummaryCatalog, SummaryCatalogError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum SummaryReconcileAction {
    Create {
        materialization_id: MaterializationId,
    },
    Update {
        instance_id: SummaryInstanceId,
        materialization_id: MaterializationId,
    },
    Recover {
        instance_id: SummaryInstanceId,
        materialization_id: MaterializationId,
    },
    Retire {
        instance_id: SummaryInstanceId,
    },
    GarbageCollect {
        instance_id: SummaryInstanceId,
    },
    PromoteEphemeral {
        instance_id: SummaryInstanceId,
        materialization_id: MaterializationId,
    },
    ExpireEphemeral {
        instance_id: SummaryInstanceId,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum SummaryReconcileError {
    #[error("invalid desired catalog: {0}")]
    Catalog(#[from] SummaryCatalogError),
    #[error("invalid observed inventory: {0}")]
    Inventory(String),
}

pub fn reconcile_summary_inventory(
    desired: &SummaryCatalog,
    observed: &ObservedSummaryInventory,
    now_ms: i64,
    retiring_grace_ms: i64,
) -> Result<Vec<SummaryReconcileAction>, SummaryReconcileError> {
    desired.validate()?;
    observed
        .validate()
        .map_err(|error| SummaryReconcileError::Inventory(error.to_string()))?;
    let desired_generation = catalog_generation(desired)?;
    let mut represented = BTreeSet::new();
    let mut actions = Vec::new();

    for instance in observed.instances.values() {
        let desired_identity = desired.materializations.get(&instance.materialization_id);
        match &instance.lifecycle {
            InstanceLifecycle::Ephemeral { lease } if lease.expires_at_ms <= now_ms => {
                actions.push(SummaryReconcileAction::ExpireEphemeral {
                    instance_id: instance.instance_id.clone(),
                });
                continue;
            }
            InstanceLifecycle::Ephemeral { .. } => {
                if desired_identity.is_some_and(|identity| {
                    identity.summary_descriptor_id == instance.summary_descriptor_id
                        && identity.data_descriptor_id == instance.data_descriptor_id
                }) {
                    represented.insert(instance.materialization_id);
                    actions.push(SummaryReconcileAction::PromoteEphemeral {
                        instance_id: instance.instance_id.clone(),
                        materialization_id: instance.materialization_id,
                    });
                }
                continue;
            }
            InstanceLifecycle::Persistent => {}
        }

        let Some(identity) = desired_identity else {
            if instance.status == SummaryInstanceStatus::Retiring
                && now_ms.saturating_sub(instance.observed_at_ms) >= retiring_grace_ms.max(0)
            {
                actions.push(SummaryReconcileAction::GarbageCollect {
                    instance_id: instance.instance_id.clone(),
                });
            } else if instance.status != SummaryInstanceStatus::Retiring {
                actions.push(SummaryReconcileAction::Retire {
                    instance_id: instance.instance_id.clone(),
                });
            }
            continue;
        };
        represented.insert(instance.materialization_id);

        if matches!(
            instance.status,
            SummaryInstanceStatus::Failed | SummaryInstanceStatus::MissingPayload
        ) {
            actions.push(SummaryReconcileAction::Recover {
                instance_id: instance.instance_id.clone(),
                materialization_id: instance.materialization_id,
            });
        } else if identity.summary_descriptor_id != instance.summary_descriptor_id
            || identity.data_descriptor_id != instance.data_descriptor_id
            || instance.catalog_generation != desired_generation
            || desired.summary_descriptors[&identity.summary_descriptor_id].state_schema_version
                != instance.state_reference.state_schema_version
            || instance.status == SummaryInstanceStatus::Retiring
        {
            actions.push(SummaryReconcileAction::Update {
                instance_id: instance.instance_id.clone(),
                materialization_id: instance.materialization_id,
            });
        }
    }

    for id in desired.materializations.keys() {
        if !represented.contains(id) {
            actions.push(SummaryReconcileAction::Create {
                materialization_id: *id,
            });
        }
    }
    Ok(actions)
}

fn catalog_generation(catalog: &SummaryCatalog) -> Result<CatalogGeneration, SummaryCatalogError> {
    let reference = catalog.reference()?;
    Ok(CatalogGeneration {
        schema_version: reference.schema_version,
        plan_id: reference.plan_id,
        plan_version: reference.plan_version,
        snapshot_digest: reference.snapshot_sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::sds::{
        EphemeralLease, HalfOpenTimeRange, InstanceCompleteness, SummaryPlacement,
        SummaryStateReference,
    };
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
    use std::collections::BTreeMap;

    fn config(metric: &str) -> PrecomputeMaterialization {
        PrecomputeMaterialization::new(
            AggregationType::Sum,
            String::new(),
            Default::default(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            metric.into(),
            None,
            None,
            None,
        )
    }

    fn instance(
        catalog: &SummaryCatalog,
        id: &str,
        lifecycle: InstanceLifecycle,
        status: SummaryInstanceStatus,
        observed_at_ms: i64,
    ) -> asap_types::sds::SummaryInstance {
        let (materialization_id, identity) = catalog.materializations.iter().next().unwrap();
        asap_types::sds::SummaryInstance {
            instance_id: SummaryInstanceId::new(id).unwrap(),
            materialization_id: *materialization_id,
            summary_descriptor_id: identity.summary_descriptor_id.clone(),
            data_descriptor_id: identity.data_descriptor_id.clone(),
            time_range: HalfOpenTimeRange {
                start_ms: 0,
                end_ms: 60_000,
            },
            group_values: BTreeMap::new(),
            catalog_generation: catalog_generation(catalog).unwrap(),
            placement: SummaryPlacement {
                producer_id: "precompute-1".into(),
                storage_node_id: "store-1".into(),
            },
            state_reference: SummaryStateReference {
                store: "summary-store".into(),
                key: id.into(),
                state_schema_version: 1,
                generation: 1,
                sequence: 1,
                checksum: None,
            },
            status,
            completeness: InstanceCompleteness::Complete,
            lifecycle,
            observed_at_ms,
        }
    }

    fn inventory(instance: asap_types::sds::SummaryInstance, now: i64) -> ObservedSummaryInventory {
        ObservedSummaryInventory {
            schema_version: 1,
            reporter_id: "store-1".into(),
            inventory_version: 1,
            observed_at_ms: now,
            instances: BTreeMap::from([(instance.instance_id.clone(), instance)]),
        }
    }

    #[test]
    fn creates_missing_and_updates_stale_generation() {
        let catalog = SummaryCatalog::from_materializations(1, 2, &[config("a")]).unwrap();
        let empty = ObservedSummaryInventory {
            schema_version: 1,
            reporter_id: "store-1".into(),
            inventory_version: 1,
            observed_at_ms: 100,
            instances: BTreeMap::new(),
        };
        assert!(matches!(
            reconcile_summary_inventory(&catalog, &empty, 100, 10).unwrap()[0],
            SummaryReconcileAction::Create { .. }
        ));
        let mut value = instance(
            &catalog,
            "i",
            InstanceLifecycle::Persistent,
            SummaryInstanceStatus::Ready,
            100,
        );
        value.catalog_generation.plan_version -= 1;
        assert!(matches!(
            reconcile_summary_inventory(&catalog, &inventory(value, 100), 100, 10).unwrap()[0],
            SummaryReconcileAction::Update { .. }
        ));
    }

    #[test]
    fn recovers_missing_payload_and_retires_then_collects_removed_state() {
        let catalog = SummaryCatalog::from_materializations(1, 2, &[config("a")]).unwrap();
        let missing = instance(
            &catalog,
            "missing",
            InstanceLifecycle::Persistent,
            SummaryInstanceStatus::MissingPayload,
            90,
        );
        assert!(matches!(
            reconcile_summary_inventory(&catalog, &inventory(missing, 100), 100, 10).unwrap()[0],
            SummaryReconcileAction::Recover { .. }
        ));
        let empty = SummaryCatalog::from_materializations(1, 3, &[]).unwrap();
        let ready = instance(
            &catalog,
            "old",
            InstanceLifecycle::Persistent,
            SummaryInstanceStatus::Ready,
            90,
        );
        assert!(matches!(
            reconcile_summary_inventory(&empty, &inventory(ready, 100), 100, 10).unwrap()[0],
            SummaryReconcileAction::Retire { .. }
        ));
        let retiring = instance(
            &catalog,
            "old",
            InstanceLifecycle::Persistent,
            SummaryInstanceStatus::Retiring,
            80,
        );
        assert!(matches!(
            reconcile_summary_inventory(&empty, &inventory(retiring, 100), 100, 10).unwrap()[0],
            SummaryReconcileAction::GarbageCollect { .. }
        ));
    }

    #[test]
    fn promotes_matching_ephemeral_and_expires_lease_first() {
        let catalog = SummaryCatalog::from_materializations(1, 2, &[config("a")]).unwrap();
        let lease = |expires_at_ms| InstanceLifecycle::Ephemeral {
            lease: EphemeralLease {
                lease_id: "lease-1".into(),
                owner_id: "fast-path".into(),
                issued_at_ms: 10,
                expires_at_ms,
            },
        };
        let live = instance(
            &catalog,
            "live",
            lease(200),
            SummaryInstanceStatus::Ready,
            100,
        );
        assert!(matches!(
            reconcile_summary_inventory(&catalog, &inventory(live, 100), 100, 10).unwrap()[0],
            SummaryReconcileAction::PromoteEphemeral { .. }
        ));
        let expired = instance(
            &catalog,
            "expired",
            lease(100),
            SummaryInstanceStatus::Ready,
            100,
        );
        let actions =
            reconcile_summary_inventory(&catalog, &inventory(expired, 100), 100, 10).unwrap();
        assert!(matches!(
            actions[0],
            SummaryReconcileAction::ExpireEphemeral { .. }
        ));
        assert!(matches!(actions[1], SummaryReconcileAction::Create { .. }));
    }
}
