use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use moka::sync::Cache;

use crate::storage_engines::types::KeyByLabelValues;

/// Collision-free, positional identity for a physical GROUP BY partition.
/// Label names are retained so different DAG projections cannot alias merely
/// because their values happen to be equal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    labels: Arc<[(Arc<str>, Arc<str>)]>,
    canonical: Arc<[u8]>,
}

impl GroupKey {
    pub fn project<'a>(
        names: impl IntoIterator<Item = &'a str>,
        labels: &HashMap<String, String>,
    ) -> Self {
        Self::new(
            names
                .into_iter()
                .map(|name| (name, labels.get(name).map(String::as_str).unwrap_or(""))),
        )
    }

    pub fn new<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        let labels: Arc<[(Arc<str>, Arc<str>)]> = pairs
            .into_iter()
            .map(|(name, value)| (Arc::from(name), Arc::from(value)))
            .collect::<Vec<_>>()
            .into();
        let mut canonical = Vec::new();
        canonical.extend_from_slice(b"ASAPGK\x01");
        canonical.extend_from_slice(&(labels.len() as u32).to_be_bytes());
        for (name, value) in labels.iter() {
            put_component(&mut canonical, name.as_bytes());
            put_component(&mut canonical, value.as_bytes());
        }
        Self {
            labels,
            canonical: canonical.into(),
        }
    }

    #[cfg(test)]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }

    pub fn values(&self) -> KeyByLabelValues {
        KeyByLabelValues::new_with_labels(
            self.labels
                .iter()
                .map(|(_, value)| value.to_string())
                .collect(),
        )
    }

    pub fn as_population_labels(&self) -> std::collections::BTreeMap<String, String> {
        self.labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }
}

impl std::fmt::Display for GroupKey {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("{")?;
        for (index, (name, value)) in self.labels.iter().enumerate() {
            if index != 0 {
                output.write_str(",")?;
            }
            write!(output, "{name}={value:?}")?;
        }
        output.write_str("}")
    }
}

fn put_component(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u32).to_be_bytes());
    target.extend_from_slice(value);
}

/// Bounded interner shared by ingress adapters. Repeated DAG consumers with
/// the same projection carry one immutable key allocation into workers.
pub fn intern(key: GroupKey) -> Arc<GroupKey> {
    intern_pairs(
        key.labels
            .iter()
            .map(|(name, value)| (name.as_ref(), value.as_ref())),
    )
}

/// Project and intern from borrowed labels. The hot path builds only the
/// compact lookup bytes on a cache hit; label/name strings are allocated once
/// when a new high-cardinality group first appears.
pub fn intern_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Arc<GroupKey> {
    let pairs = pairs.into_iter().collect::<Vec<_>>();
    let mut encoded = Vec::with_capacity(
        11 + pairs
            .iter()
            .map(|(name, value)| name.len() + value.len() + 8)
            .sum::<usize>(),
    );
    encoded.extend_from_slice(b"ASAPGK\x01");
    encoded.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
    for (name, value) in &pairs {
        put_component(&mut encoded, name.as_bytes());
        put_component(&mut encoded, value.as_bytes());
    }
    static INTERNER: OnceLock<Cache<Vec<u8>, Arc<GroupKey>>> = OnceLock::new();
    let interner = INTERNER.get_or_init(|| Cache::builder().max_capacity(131_072).build());
    interner.get_with(encoded, || Arc::new(GroupKey::new(pairs)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delimiters_names_order_and_missing_values_cannot_alias() {
        let a = GroupKey::new([("x", "a;b"), ("y", "c")]);
        let b = GroupKey::new([("x", "a"), ("y", "b;c")]);
        let reordered = GroupKey::new([("y", "c"), ("x", "a;b")]);
        let other_names = GroupKey::new([("p", "a;b"), ("q", "c")]);
        let missing = GroupKey::new([("x", ""), ("y", "c")]);
        for other in [&b, &reordered, &other_names, &missing] {
            assert_ne!(a.canonical_bytes(), other.canonical_bytes());
        }
        assert_eq!(a.values().labels, vec!["a;b", "c"]);
    }

    #[test]
    fn interner_reuses_equal_projection() {
        let left = intern(GroupKey::new([("region", "us-east"), ("job", "api")]));
        let right = intern(GroupKey::new([("region", "us-east"), ("job", "api")]));
        assert!(Arc::ptr_eq(&left, &right));
    }
}
