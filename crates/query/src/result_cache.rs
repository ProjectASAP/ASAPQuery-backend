//! The query result cache: a planned query root's final answer at one
//! evaluation time `T`, kept so a dashboard that refreshes a range panel
//! every few seconds computes only the steps it has not answered before.
//! An instant query at `T` and a range step at `T` share one entry.
//!
//! Only a final answer is kept: one whose inputs can no longer change
//! ([`crate::CompiledQueries`] decides). Entries are dropped once `T`
//! falls further behind the watermark than any planned evaluation reaches
//! back (the plan's longest retention plus the view retention margin), and
//! the least recently used while more than `max_bytes` are held.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_executor::planner::plan::physical_asap_dag::{PhysicalASAPDAG, PhysicalASAPNodeId};

use crate::ResultSample;

/// One evaluation's answer, shared by every request that reads it.
pub type Answer = Arc<Vec<ResultSample>>;

/// An entry's key: the query root and the evaluation time.
type Key = (PhysicalASAPNodeId, i64);

struct Entry {
    answer: Answer,
    bytes: usize,
    /// When it was last read or stored, on the cache's use clock.
    used: u64,
}

#[derive(Default)]
struct Entries {
    by_key: HashMap<Key, Entry>,
    /// Keys by last use, least recent first.
    by_use: BTreeMap<u64, Key>,
    /// Keys by evaluation time, oldest first.
    by_time: BTreeSet<(i64, PhysicalASAPNodeId)>,
    clock: u64,
    bytes: usize,
}

impl Entries {
    fn remove(&mut self, key: Key) -> Option<Entry> {
        let entry = self.by_key.remove(&key)?;
        self.by_use.remove(&entry.used);
        self.by_time.remove(&(key.1, key.0));
        self.bytes -= entry.bytes;
        Some(entry)
    }
}

pub struct ResultCache {
    entries: Mutex<Entries>,
    /// How far behind the watermark an evaluation time may be and still be
    /// asked for; `None`: unbounded (only `max_bytes` evicts).
    horizon_ms: Option<i64>,
    max_bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl ResultCache {
    pub fn new(horizon_ms: Option<i64>, max_bytes: usize) -> Self {
        Self {
            entries: Mutex::default(),
            horizon_ms,
            max_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// How far back from the watermark `dag`'s evaluations read: its
    /// longest retention (every view and raw-data `Scan` keeps its rows
    /// that far back from the newest evaluation time) plus `margin_ms`;
    /// `None` when a `Scan` reads its rows unbounded.
    pub fn horizon_ms(dag: &PhysicalASAPDAG, margin_ms: u64) -> Option<i64> {
        let unbounded_scan = dag.nodes.iter().any(|node| {
            node.retention_ms.is_none()
                && matches!(node.payload, Operator::NonASAP(NonASAPOp::Scan { .. }))
        });
        if unbounded_scan {
            return None;
        }
        let longest = dag.nodes.iter().filter_map(|n| n.retention_ms).max()?;
        i64::try_from(longest.saturating_add(margin_ms)).ok()
    }

    /// `root`'s answer at `time_ms`, if kept.
    pub fn get(&self, root: PhysicalASAPNodeId, time_ms: i64) -> Option<Answer> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let entries = &mut *entries;
        entries.clock += 1;
        let clock = entries.clock;
        let Some(entry) = entries.by_key.get_mut(&(root, time_ms)) else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        entries.by_use.remove(&entry.used);
        entry.used = clock;
        entries.by_use.insert(clock, (root, time_ms));
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(entry.answer.clone())
    }

    /// Keeps `answer`, `root`'s final answer at `time_ms`, then drops what
    /// is older than the horizon behind `watermark_ms` and the least
    /// recently used beyond `max_bytes`.
    pub fn insert(
        &self,
        root: PhysicalASAPNodeId,
        time_ms: i64,
        answer: Answer,
        watermark_ms: Option<i64>,
    ) {
        let bytes = answer_bytes(&answer);
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let entries = &mut *entries;
        entries.clock += 1;
        let used = entries.clock;
        entries.remove((root, time_ms));
        entries.by_key.insert(
            (root, time_ms),
            Entry {
                answer,
                bytes,
                used,
            },
        );
        entries.by_use.insert(used, (root, time_ms));
        entries.by_time.insert((time_ms, root));
        entries.bytes += bytes;
        let mut evicted = 0;
        if let (Some(horizon), Some(watermark)) = (self.horizon_ms, watermark_ms) {
            let oldest_kept_ms = watermark.saturating_sub(horizon);
            while let Some(&(time, node)) = entries.by_time.first() {
                if time >= oldest_kept_ms {
                    break;
                }
                entries.remove((node, time));
                evicted += 1;
            }
        }
        while entries.bytes > self.max_bytes {
            let Some((_, &key)) = entries.by_use.first_key_value() else {
                break;
            };
            entries.remove(key);
            evicted += 1;
        }
        self.evictions.fetch_add(evicted, Ordering::Relaxed);
    }

    /// Lookups that found an answer.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Lookups that found none.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Answers dropped by age or by bytes.
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Answers kept.
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .by_key
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes of the answers kept (estimated).
    pub fn bytes(&self) -> usize {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).bytes
    }
}

/// The heap bytes an answer holds, estimated: each sample, and each label
/// map only this answer holds (an input series' map is the interner's,
/// shared by every answer of that series) with its names, values and tree
/// nodes.
fn answer_bytes(answer: &[ResultSample]) -> usize {
    const LABEL_NODE_BYTES: usize = 2 * std::mem::size_of::<String>() + 16;
    std::mem::size_of_val(answer)
        + answer
            .iter()
            .filter(|sample| Arc::strong_count(&sample.labels) == 1)
            .flat_map(|sample| sample.labels.iter())
            .map(|(name, value)| name.capacity() + value.capacity() + LABEL_NODE_BYTES)
            .sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(series: usize) -> Answer {
        Arc::new(
            (0..series)
                .map(|i| ResultSample {
                    labels: Arc::new([("job".to_string(), format!("j{i}"))].into()),
                    value: i as f64,
                })
                .collect(),
        )
    }

    /// An answer is read back by its root and time only; one older than the
    /// horizon behind the watermark is dropped.
    #[test]
    fn evicts_by_age() {
        let cache = ResultCache::new(Some(60_000), usize::MAX);
        cache.insert(1, 10_000, answer(2), Some(10_000));
        assert_eq!(cache.get(1, 10_000).unwrap().len(), 2);
        assert!(cache.get(2, 10_000).is_none());
        assert!(cache.get(1, 20_000).is_none());
        cache.insert(1, 70_000, answer(1), Some(70_000));
        assert!(cache.get(1, 10_000).is_some(), "exactly at the horizon");
        cache.insert(1, 71_000, answer(1), Some(71_000));
        assert!(cache.get(1, 10_000).is_none());
        assert_eq!(cache.evictions(), 1);
        assert_eq!((cache.hits(), cache.misses()), (2, 3));
        assert_eq!(cache.len(), 2);
    }

    /// Beyond `max_bytes`, the least recently used answer goes first.
    #[test]
    fn evicts_least_recently_used_by_bytes() {
        let one = answer_bytes(&answer(10));
        let cache = ResultCache::new(None, 2 * one);
        cache.insert(1, 1, answer(10), None);
        cache.insert(1, 2, answer(10), None);
        cache.get(1, 1);
        cache.insert(1, 3, answer(10), None);
        assert!(cache.get(1, 1).is_some());
        assert!(cache.get(1, 2).is_none(), "least recently used");
        assert!(cache.get(1, 3).is_some());
        assert_eq!(cache.bytes(), 2 * one);
        assert_eq!(cache.evictions(), 1);
    }
}
