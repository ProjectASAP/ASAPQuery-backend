//! A decoder's series cache: series handles by the bytes of a key, such as
//! a series' encoded labels, kept between requests. Shared with the other
//! ingest decoders that walk protobuf in place (`otlp`).

/// Series handles by the bytes of their key. A sender writes its series in
/// the same order request after request, so each entry remembers the entry
/// looked up after it last time, and a lookup first compares the key with
/// that prediction: a hit costs one compare, no hash. Every hit is a
/// byte-for-byte match of the key.
pub struct SeriesCache<S: Copy> {
    entries: Vec<CacheEntry<S>>,
    /// Entries by the hash of their key.
    by_key: hashbrown::HashTable<u32>,
    /// The entry looked up last.
    last: Option<u32>,
}

struct CacheEntry<S> {
    key: Box<[u8]>,
    series: S,
    /// The entry looked up right after this one, last time.
    next: Option<u32>,
}

impl<S: Copy> SeriesCache<S> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            by_key: hashbrown::HashTable::new(),
            last: None,
        }
    }

    fn hash(key: &[u8]) -> u64 {
        static STATE: std::sync::OnceLock<ahash::RandomState> = std::sync::OnceLock::new();
        STATE.get_or_init(ahash::RandomState::new).hash_one(key)
    }

    /// The series of `key`, if cached.
    #[inline]
    pub fn get(&mut self, key: &[u8]) -> Option<S> {
        let predicted = self.last.and_then(|last| self.entries[last as usize].next);
        if let Some(index) = predicted {
            let entry = &self.entries[index as usize];
            if *entry.key == *key {
                self.last = Some(index);
                return Some(entry.series);
            }
        }
        let entries = &self.entries;
        let index = *self
            .by_key
            .find(Self::hash(key), |&i| *entries[i as usize].key == *key)?;
        self.follow(index);
        Some(self.entries[index as usize].series)
    }

    /// Caches `series` under `key`, which is not cached yet.
    pub fn insert(&mut self, key: &[u8], series: S) {
        let index = u32::try_from(self.entries.len()).expect("fewer than 2^32 series");
        self.entries.push(CacheEntry {
            key: key.into(),
            series,
            next: None,
        });
        let entries = &self.entries;
        self.by_key.insert_unique(Self::hash(key), index, |&i| {
            Self::hash(&entries[i as usize].key)
        });
        self.follow(index);
    }

    /// Records that `index` was looked up after the last entry.
    fn follow(&mut self, index: u32) {
        if let Some(last) = self.last {
            self.entries[last as usize].next = Some(index);
        }
        self.last = Some(index);
    }
}
