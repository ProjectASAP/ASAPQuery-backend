//! Series are interned once, at decode: each label set gets one `u32` id
//! and one shared `Series`, which everything downstream keys by.

use std::collections::BTreeMap;
use std::sync::Arc;

use data_plane::series_ids::{intern, intern_pairs};

fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// The same label set is one series whether it arrives as a map or as
/// Remote Write label pairs in any order; another set is another series.
#[test]
fn one_label_set_is_one_series() {
    let map = labels(&[("__name__", "up"), ("job", "api"), ("instance", "1")]);
    let a = intern(&map);
    let b = intern_pairs(&[("job", "api"), ("__name__", "up"), ("instance", "1")]);
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(a.id, b.id);
    assert_eq!(&*a.metric, "up");
    assert_eq!(**a, map);
    let other = intern_pairs(&[("__name__", "up"), ("job", "api"), ("instance", "2")]);
    assert_ne!(other.id, a.id);
    // A value that moves across a label boundary is another label set.
    let shifted = intern_pairs(&[("__name__", "up"), ("job", "ap"), ("instance", "i1")]);
    assert_ne!(shifted.id, a.id);
}

/// Interning from many threads at once gives each label set one id.
#[test]
fn interning_is_consistent_across_threads() {
    let threads: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(|| {
                (0..500)
                    .map(|i| intern_pairs(&[("__name__", "m"), ("i", &i.to_string())]).id)
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let ids: Vec<Vec<u32>> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert!(ids.windows(2).all(|w| w[0] == w[1]));
    let mut distinct = ids[0].clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), 500);
}
