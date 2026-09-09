//! Bounded physical implementation search; semantic leaf legality is established
//! by the Planner-witness lowering before these stable keys are supplied.
use std::collections::BTreeSet;

#[derive(Debug)]
pub(super) struct IndexMasks {
    pub masks: Vec<BTreeSet<String>>,
    pub exhaustive: bool,
    pub eligible_leaves: usize,
}

/// Enumerate every mask for up to four leaves. Larger forests retain all-index,
/// all-raw, then singleton/complement pairs in stable key order. The caller must
/// disclose bounded coverage; no unenumerated optimum is claimed. Reserve one
/// of the selector's 64 candidate slots for native execution.
pub(super) fn enumerate(keys: BTreeSet<String>) -> IndexMasks {
    let eligible_leaves = keys.len();
    let ordered: Vec<_> = keys.iter().cloned().collect();
    let exhaustive = eligible_leaves <= 4;
    let mut masks = vec![keys.clone()];
    if exhaustive {
        for bits in 0..(1usize << eligible_leaves) {
            let mask = ordered
                .iter()
                .enumerate()
                .filter(|(i, _)| bits & (1 << i) != 0)
                .map(|(_, key)| key.clone())
                .collect();
            if !masks.contains(&mask) {
                masks.push(mask);
            }
        }
    } else {
        masks.push(BTreeSet::new());
        for key in ordered {
            for mask in [
                BTreeSet::from([key.clone()]),
                keys.difference(&BTreeSet::from([key])).cloned().collect(),
            ] {
                if masks.len() >= 63 {
                    break;
                }
                if !masks.contains(&mask) {
                    masks.push(mask);
                }
            }
        }
    }
    IndexMasks {
        masks,
        exhaustive,
        eligible_leaves,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn small_forests_cover_every_mixed_path_once() {
        let result = enumerate(BTreeSet::from(["a".into(), "b".into()]));
        assert!(result.exhaustive);
        assert_eq!(result.eligible_leaves, 2);
        assert_eq!(result.masks.len(), 4);
        assert_eq!(result.masks.iter().collect::<BTreeSet<_>>().len(), 4);
        assert!(result.masks.contains(&BTreeSet::from(["a".into()])));
        assert!(result.masks.contains(&BTreeSet::from(["b".into()])));
    }
    #[test]
    fn large_inventory_reserves_native_slot_and_discloses_truncation() {
        let keys = (0..100).map(|i| format!("{i:03}")).collect();
        let result = enumerate(keys);
        assert!(!result.exhaustive);
        assert_eq!(result.masks.len(), 63);
        assert_eq!(result.masks[0].len(), 100);
        assert!(result.masks[1].is_empty());
        assert_eq!(result.masks.iter().collect::<BTreeSet<_>>().len(), 63);
    }
    #[test]
    fn no_index_has_one_local_implementation() {
        assert_eq!(enumerate(BTreeSet::new()).masks, vec![BTreeSet::new()]);
    }
}
