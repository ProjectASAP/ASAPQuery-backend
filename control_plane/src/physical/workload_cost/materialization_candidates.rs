//! Bounded physical implementation search; semantic leaf legality is established
//! by the Planner-witness lowering before these stable keys are supplied.
use std::collections::BTreeSet;

#[derive(Debug)]
pub(super) struct MaterializationCandidateSets {
    pub candidate_key_sets: Vec<BTreeSet<String>>,
    pub exhaustive: bool,
    pub eligible_materialization_count: usize,
}

/// Enumerate every enabled_keys for up to four leaves. Larger forests retain all-materialized,
/// all-exact, then singleton/complement pairs in stable key order. The caller must
/// disclose bounded coverage; no unenumerated optimum is claimed. Reserve one
/// of the selector's 64 candidate slots for native execution.
pub(super) fn enumerate(keys: BTreeSet<String>) -> MaterializationCandidateSets {
    let eligible_materialization_count = keys.len();
    let ordered: Vec<_> = keys.iter().cloned().collect();
    let exhaustive = eligible_materialization_count <= 4;
    let mut candidate_key_sets = vec![keys.clone()];
    if exhaustive {
        for bits in 0..(1usize << eligible_materialization_count) {
            let enabled_keys = ordered
                .iter()
                .enumerate()
                .filter(|(i, _)| bits & (1 << i) != 0)
                .map(|(_, key)| key.clone())
                .collect();
            if !candidate_key_sets.contains(&enabled_keys) {
                candidate_key_sets.push(enabled_keys);
            }
        }
    } else {
        candidate_key_sets.push(BTreeSet::new());
        for key in ordered {
            for enabled_keys in [
                BTreeSet::from([key.clone()]),
                keys.difference(&BTreeSet::from([key])).cloned().collect(),
            ] {
                if candidate_key_sets.len() >= 63 {
                    break;
                }
                if !candidate_key_sets.contains(&enabled_keys) {
                    candidate_key_sets.push(enabled_keys);
                }
            }
        }
    }
    MaterializationCandidateSets {
        candidate_key_sets,
        exhaustive,
        eligible_materialization_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn small_forests_cover_every_mixed_path_once() {
        let result = enumerate(BTreeSet::from(["a".into(), "b".into()]));
        assert!(result.exhaustive);
        assert_eq!(result.eligible_materialization_count, 2);
        assert_eq!(result.candidate_key_sets.len(), 4);
        assert_eq!(
            result
                .candidate_key_sets
                .iter()
                .collect::<BTreeSet<_>>()
                .len(),
            4
        );
        assert!(result
            .candidate_key_sets
            .contains(&BTreeSet::from(["a".into()])));
        assert!(result
            .candidate_key_sets
            .contains(&BTreeSet::from(["b".into()])));
    }
    #[test]
    fn large_inventory_reserves_native_slot_and_discloses_truncation() {
        let keys = (0..100).map(|i| format!("{i:03}")).collect();
        let result = enumerate(keys);
        assert!(!result.exhaustive);
        assert_eq!(result.candidate_key_sets.len(), 63);
        assert_eq!(result.candidate_key_sets[0].len(), 100);
        assert!(result.candidate_key_sets[1].is_empty());
        assert_eq!(
            result
                .candidate_key_sets
                .iter()
                .collect::<BTreeSet<_>>()
                .len(),
            63
        );
    }
    #[test]
    fn no_materialization_has_one_exact_implementation() {
        assert_eq!(
            enumerate(BTreeSet::new()).candidate_key_sets,
            vec![BTreeSet::new()]
        );
    }
}
