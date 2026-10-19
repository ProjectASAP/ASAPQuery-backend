//! Series interning: each label set is mapped to one shared [`Series`], with
//! a `u32` id, once, when Remote Write (or remote read) delivers it.
//! Everything downstream (sharding, the per-series caches, the raw buffer)
//! keys by the id or the shared pointer, so no stage hashes or compares a
//! label map per sample.
//!
//! The interner is process-wide: ids are the same for a label set whichever
//! request, worker or query thread meets it. It never forgets a series.

use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::ops::Deref;
use std::sync::{Arc, OnceLock, RwLock};

use ahash::RandomState;

/// One label set, interned.
#[derive(Debug)]
pub struct Series {
    /// The series' id, dense from 0 in the order series are first seen.
    pub id: u32,
    /// A hash of the label set's bytes, which shards the series.
    pub hash: u64,
    /// The `__name__` label (empty without one).
    pub metric: Arc<str>,
    labels: BTreeMap<String, String>,
}

impl Deref for Series {
    type Target = BTreeMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.labels
    }
}

impl PartialEq for Series {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Series {}

impl PartialOrd for Series {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Series {
    /// By labels, as Prometheus orders series.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.id == other.id {
            return std::cmp::Ordering::Equal;
        }
        self.labels.cmp(&other.labels)
    }
}

impl std::hash::Hash for Series {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.id);
    }
}

#[derive(Default)]
struct Interner {
    /// Series ids by label-set hash; a hash collision lists several.
    by_hash: ahash::HashMap<u64, Vec<u32>>,
    series: Vec<Arc<Series>>,
}

fn interner() -> &'static RwLock<Interner> {
    static INTERNER: OnceLock<RwLock<Interner>> = OnceLock::new();
    INTERNER.get_or_init(|| RwLock::new(Interner::default()))
}

/// Fixed seeds, so a label set hashes (and shards) the same in every run.
fn hasher() -> ahash::AHasher {
    RandomState::with_seeds(0x5eed_0001, 0x5eed_0002, 0x5eed_0003, 0x5eed_0004).build_hasher()
}

/// The hash of `pairs`, sorted by name: each name and value with its
/// length, so a byte moving across a boundary changes the hash.
fn hash_sorted<'a>(pairs: impl Iterator<Item = (&'a str, &'a str)>) -> u64 {
    let mut hasher = hasher();
    for (name, value) in pairs {
        hasher.write_usize(name.len());
        hasher.write(name.as_bytes());
        hasher.write_usize(value.len());
        hasher.write(value.as_bytes());
    }
    hasher.finish()
}

/// The series of the label pairs `pairs`, in any order (Remote Write sends
/// them sorted by name); interned on first sight.
pub fn intern_pairs<N: AsRef<str>, V: AsRef<str>>(pairs: &[(N, V)]) -> Arc<Series> {
    let mut sorted: Vec<(&str, &str)> = pairs
        .iter()
        .map(|(n, v)| (n.as_ref(), v.as_ref()))
        .collect();
    if !sorted.windows(2).all(|w| w[0].0 <= w[1].0) {
        sorted.sort_unstable_by(|a, b| a.0.cmp(b.0));
    }
    let hash = hash_sorted(sorted.iter().copied());
    let matches = |series: &Series| {
        series.labels.len() == sorted.len()
            && series
                .labels
                .iter()
                .zip(&sorted)
                .all(|((n, v), (name, value))| n == name && v == value)
    };
    {
        let interner = interner().read().expect("interner lock");
        for &id in interner.by_hash.get(&hash).into_iter().flatten() {
            let series = &interner.series[id as usize];
            if matches(series) {
                return series.clone();
            }
        }
    }
    let mut interner = interner().write().expect("interner lock");
    for &id in interner.by_hash.get(&hash).into_iter().flatten() {
        let series = &interner.series[id as usize];
        if matches(series) {
            return series.clone();
        }
    }
    let labels: BTreeMap<String, String> = sorted
        .iter()
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect();
    let id = u32::try_from(interner.series.len()).expect("fewer than 2^32 series");
    let series = Arc::new(Series {
        id,
        hash,
        metric: labels.get("__name__").map_or("", String::as_str).into(),
        labels,
    });
    interner.series.push(series.clone());
    interner.by_hash.entry(hash).or_default().push(id);
    series
}

/// The series of `labels`; interned on first sight.
pub fn intern(labels: &BTreeMap<String, String>) -> Arc<Series> {
    let pairs: Vec<(&str, &str)> = labels
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    intern_pairs(&pairs)
}
