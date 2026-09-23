//! Composable row operators, independent of storage, query language and sketches.
use std::collections::{BTreeMap, BTreeSet};

/// A semijoin preserves value-row order and multiplicity; duplicate membership
/// keys never multiply rows. Missing membership keys are reported separately so
/// the deployment can enforce the pruning proof attached to its plan.
pub fn membership_filter<T, K: Ord + Clone>(
    members: impl IntoIterator<Item = K>,
    values: Vec<T>,
    identity: impl Fn(&T) -> K,
) -> (Vec<T>, BTreeSet<K>) {
    let members: BTreeSet<K> = members.into_iter().collect();
    let mut missing = members.clone();
    let rows = values
        .into_iter()
        .filter(|row| {
            let key = identity(row);
            missing.remove(&key);
            members.contains(&key)
        })
        .collect();
    (rows, missing)
}

/// Stable descending TopK per group. NaN sorts after numeric values; ties keep
/// input order. This operator does not know how its input was filtered or built.
pub fn grouped_topk<T, K: Ord>(
    values: Vec<T>,
    k: usize,
    group_key: impl Fn(&T) -> K,
    score: impl Fn(&T) -> f64,
) -> Vec<T> {
    let mut groups: BTreeMap<K, Vec<T>> = BTreeMap::new();
    for row in values {
        groups.entry(group_key(&row)).or_default().push(row);
    }
    groups
        .into_values()
        .flat_map(|mut rows| {
            rows.sort_by(|a, b| {
                let (a, b) = (score(a), score(b));
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => std::cmp::Ordering::Equal,
                    (true, false) => std::cmp::Ordering::Greater,
                    (false, true) => std::cmp::Ordering::Less,
                    (false, false) => b.total_cmp(&a),
                }
            });
            rows.truncate(k);
            rows
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semijoin_preserves_values_order_and_duplicates_without_ranking() {
        let (rows, missing) = membership_filter(
            ["b", "c", "b", "missing"],
            vec![("a", 100.), ("b", 2.), ("c", 9.), ("b", 3.)],
            |row| row.0,
        );
        assert_eq!(rows, vec![("b", 2.), ("c", 9.), ("b", 3.)]);
        assert_eq!(missing, BTreeSet::from(["missing"]));
        assert_eq!(
            grouped_topk(rows, 2, |_| (), |r| r.1),
            vec![("c", 9.), ("b", 3.)]
        );
    }

    #[test]
    fn grouped_ranking_preserves_ties_and_places_nan_last() {
        let rows = vec![("x", 0, f64::NAN), ("x", 1, 2.), ("y", 2, 8.), ("x", 3, 2.)];
        let ranked = grouped_topk(rows, 2, |r| r.0, |r| r.2);
        assert_eq!(ranked, vec![("x", 1, 2.), ("x", 3, 2.), ("y", 2, 8.)]);
        assert!(grouped_topk(vec![1], 0, |_| (), |r| *r as f64).is_empty());
    }

    #[test]
    fn empty_membership_removes_all_rows() {
        let (rows, missing) = membership_filter([], vec![1, 2], |r| *r);
        assert!(rows.is_empty());
        assert!(missing.is_empty());
    }
}
