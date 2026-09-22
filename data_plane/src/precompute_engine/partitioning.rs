//! Data ownership for an entire installed precompute graph.
use std::collections::{BTreeMap, BTreeSet};

use asap_types::precompute_plan::PrecomputePlan;
use xxhash_rust::xxh64::xxh64;

/// A conservative locality proof. Raw producer paths are validated separately
/// at installation. Derived graphs use one owner until their complete reduction
/// paths can establish a finer partition without a cross-worker exchange.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum DagPartitioning {
    #[default]
    SingleWorker,
    Labels(Vec<String>),
    Population,
}

impl DagPartitioning {
    pub fn from_plan(plan: &PrecomputePlan) -> Self {
        if plan
            .materializations
            .iter()
            .any(|output| output.derived_input.is_some())
        {
            return Self::SingleWorker;
        }
        let mut common: Option<BTreeSet<String>> = None;
        for output in &plan.materializations {
            let labels = output
                .grouping_labels
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            common = Some(match common {
                None => labels,
                Some(previous) => previous.intersection(&labels).cloned().collect(),
            });
        }
        match common {
            Some(labels) if !labels.is_empty() => Self::Labels(labels.into_iter().collect()),
            _ if !plan.materializations.is_empty()
                && plan.materializations.iter().all(|output| {
                    output.partitioning == Some(asap_types::sds::PopulationPartitioning::PerEntity)
                }) =>
            {
                Self::Population
            }
            _ => Self::SingleWorker,
        }
    }

    pub fn owner(
        &self,
        population: &BTreeMap<String, String>,
        workers: usize,
    ) -> Result<usize, String> {
        if workers == 0 {
            return Err("precompute requires at least one worker".into());
        }
        match self {
            Self::SingleWorker => Ok(0),
            Self::Population => {
                let key = super::group_key::GroupKey::new(
                    population.iter().map(|(k, v)| (k.as_str(), v.as_str())),
                );
                Ok(xxh64(key.canonical_bytes(), 0) as usize % workers)
            }
            Self::Labels(names) => {
                let pairs = names
                    .iter()
                    .map(|name| {
                        population
                            .get(name)
                            .map(|value| (name.as_str(), value.as_str()))
                            .ok_or_else(|| {
                                format!("DAG partition label {name} is absent from population")
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let key = super::group_key::GroupKey::new(pairs);
                Ok(xxh64(key.canonical_bytes(), 0) as usize % workers)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Upstream series state and downstream grouped state have one owner.
    #[test]
    fn service_partition_preserves_series_and_reduction_locality() {
        let rule = DagPartitioning::Labels(vec!["service".into()]);
        let group = BTreeMap::from([("service".into(), "api".into())]);
        let expected = rule.owner(&group, 4).unwrap();
        for instance in ["a", "b", "c"] {
            let mut series = group.clone();
            series.insert("instance".into(), instance.into());
            assert_eq!(rule.owner(&series, 4).unwrap(), expected);
        }
        assert!(rule.owner(&BTreeMap::new(), 4).is_err());
        assert!(rule.owner(&group, 0).is_err());
        let owners = (0..64)
            .map(|i| {
                rule.owner(
                    &BTreeMap::from([("service".into(), format!("service-{i}"))]),
                    4,
                )
                .unwrap()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(owners.len(), 4);
    }
}
