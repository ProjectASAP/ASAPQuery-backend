//! Centralized series_id resolver — Phase 4 of the controller-into-backend
//! refactor (2026-05).
//!
//! The asap-query-backend host is the SOLE minter of `series_id`s in the
//! pipeline. Agents and gateway are transparent forwarders for sids whose
//! input identity matches the output identity; for rollup-output identities
//! at the gateway, the gateway itself calls back to this resolver (same
//! flow as agents — it just happens to be a hop closer).
//!
//! The resolver is content-addressable and idempotent: same `(metric_name,
//! attribute_set)` input always produces the same `series_id` for the
//! lifetime of the cache. This is the invariant that makes
//! attribute-fallback recovery work — a recovered agent re-emits with full
//! attributes, the resolver returns the same sid that was assigned before
//! the agent's crash, and sketch state under that sid stays coherent.
//!
//! Cache key: a deterministic fingerprint of `(metric_name, sorted
//! [attr_key, attr_value] pairs)`. Both sender and resolver MUST compute
//! the fingerprint the same way; the fingerprint algorithm here mirrors
//! the patched OTel-Go exporter's `attributesFingerprint` in
//! `opentelemetry-go-patch/exporters/otlp/otlpmetric/otlpmetricgrpc/
//! internal/series/dictionary.go`.
//!
//! See design doc §5.4 ("Idempotency invariant on `ResolveSeriesIDs`")
//! at `docs/design-controller-into-backend.md`.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Canonical fingerprint key — `(metric_name, attrs_fingerprint)`.
///
/// `attrs_fingerprint` is a string produced by canonicalizing the
/// attribute set: keys sorted lexicographically, then `key=value;`-joined.
/// This matches the format the patched OTel-Go exporter writes into
/// `SeriesAssignment.attributes_fingerprint`, so cache hits across the
/// agent's exporter cache and this backend resolver align bit-exactly.
type CacheKey = (String, String);

/// Idempotent compute-or-mint resolver. Atomic per-key — concurrent
/// `resolve()` calls for the same `(metric, attrs)` from different agents
/// or different DataPoints in the same Export request always observe the
/// same sid, no spurious mints.
pub struct SeriesIdResolver {
    cache: DashMap<CacheKey, u64>,
    next_sid: AtomicU64,
}

impl SeriesIdResolver {
    pub fn new() -> Self {
        Self {
            cache: DashMap::new(),
            // sid=0 is reserved for "unresolved/uncached"; start minting at 1.
            next_sid: AtomicU64::new(1),
        }
    }

    /// Resolve `(metric_name, attrs)` to a series_id. Returns the existing
    /// sid if this `(metric, attrs)` tuple was already registered;
    /// otherwise mints a fresh sid, caches it, and returns the new value.
    ///
    /// Idempotent: repeated calls with the same input ALWAYS return the
    /// same sid for the lifetime of the cache. After a backend restart
    /// without persistence, the cache is empty — recovered agents emit
    /// with attributes, and this method mints fresh sids (potentially
    /// different from the pre-restart values). Old sids the agents had
    /// cached are signalled as stale via the response's
    /// `unknown_series_ids` field; agents evict and re-resolve.
    pub fn resolve(&self, metric_name: &str, attrs_fingerprint: &str) -> u64 {
        let key = (metric_name.to_string(), attrs_fingerprint.to_string());
        // DashMap::entry().or_insert_with() is atomic — concurrent
        // callers for the same key serialize on the bucket lock.
        let entry = self
            .cache
            .entry(key)
            .or_insert_with(|| self.next_sid.fetch_add(1, Ordering::Relaxed));
        *entry
    }

    /// Look up an existing sid without minting. Returns `None` if the
    /// `(metric, attrs)` tuple is not in the cache. Used by the OTLP
    /// receive path to check whether an incoming sid (without attrs) is
    /// recognized — sids the backend doesn't recognize go into the
    /// response's `unknown_series_ids` so the sender re-sends with attrs.
    pub fn lookup(&self, metric_name: &str, attrs_fingerprint: &str) -> Option<u64> {
        let key = (metric_name.to_string(), attrs_fingerprint.to_string());
        self.cache.get(&key).map(|v| *v)
    }

    /// Reverse lookup: given a sid, is it known? Used at receive time
    /// when an Export carries a sid != 0 with empty attributes — backend
    /// must verify it knows the sid; otherwise stamp `unknown_series_ids`
    /// in the response.
    pub fn is_known(&self, sid: u64) -> bool {
        self.cache.iter().any(|kv| *kv.value() == sid)
    }

    /// Number of registered identities. Used for telemetry / debugging.
    pub fn len(&self) -> usize {
        self.cache.len()
    }
}

impl Default for SeriesIdResolver {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute the canonical attributes fingerprint matching the patched
/// OTel-Go exporter's `attributesFingerprint`. Both sides MUST produce
/// the same string for the same attribute set — sender uses it to look
/// up its local cache; receiver uses it as the resolver's cache key.
///
/// Format: `key1=value1;key2=value2;...` where keys are sorted
/// lexicographically. Mirrors
/// `opentelemetry-go-patch/exporters/otlp/otlpmetric/otlpmetricgrpc/
/// internal/series/dictionary.go::attributesFingerprint`.
pub fn canonical_attrs_fingerprint(attrs: &[(&str, &str)]) -> String {
    let mut sorted: Vec<(&str, &str)> = attrs.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    let mut buf = String::new();
    for (k, v) in sorted {
        buf.push_str(k);
        buf.push('=');
        buf.push_str(v);
        buf.push(';');
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotent_same_input_same_sid() {
        let r = SeriesIdResolver::new();
        let sid1 = r.resolve("http_requests_total", "zone=z0;");
        let sid2 = r.resolve("http_requests_total", "zone=z0;");
        assert_eq!(sid1, sid2, "same input must produce same sid");
    }

    #[test]
    fn distinct_inputs_distinct_sids() {
        let r = SeriesIdResolver::new();
        let s_z0 = r.resolve("metric_a", "zone=z0;");
        let s_z1 = r.resolve("metric_a", "zone=z1;");
        assert_ne!(s_z0, s_z1);
    }

    #[test]
    fn distinct_metrics_same_attrs_distinct_sids() {
        let r = SeriesIdResolver::new();
        let s_a = r.resolve("metric_a", "zone=z0;");
        let s_b = r.resolve("metric_b", "zone=z0;");
        assert_ne!(s_a, s_b);
    }

    #[test]
    fn fingerprint_sorts_keys() {
        let f1 = canonical_attrs_fingerprint(&[("zone", "z0"), ("rack", "r00")]);
        let f2 = canonical_attrs_fingerprint(&[("rack", "r00"), ("zone", "z0")]);
        assert_eq!(f1, f2, "fingerprint must be order-independent");
        assert_eq!(f1, "rack=r00;zone=z0;");
    }

    #[test]
    fn lookup_returns_existing_without_mint() {
        let r = SeriesIdResolver::new();
        let sid = r.resolve("m", "k=v;");
        assert_eq!(r.lookup("m", "k=v;"), Some(sid));
        assert_eq!(r.lookup("m", "k=v2;"), None);
    }
}
