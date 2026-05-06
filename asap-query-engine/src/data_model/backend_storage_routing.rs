//! Per-metric storage-backend routing table consulted by the HTTP query
//! handler at request time.
//!
//! ## Why this exists
//!
//! Phase-5 (PR #87) wired the `EngineRouter` into the HTTP query handler,
//! but the per-metric `StorageBackend` axis was sourced from
//! `StreamingConfig::storage_backend()` — a single field that applies to
//! the entire streaming config. In production deploys (the
//! `precompute_engine` binary loading `backend-streaming.yaml`) the
//! field decodes via `Self::new(...)` which always defaults to
//! `SketchWarmTier`, so the handler always took the
//! `SimpleEngine`-direct-dispatch branch and the `EngineRouter` was
//! effectively bypassed for every query — the `data_source:
//! gorilla_archive` info-line never landed on cold-archive responses
//! even when the chunks were on disk in MinIO.
//!
//! The fix lives **outside** the streaming pipeline: the streaming
//! engine on the OTLP-ingest path never sees Gorilla data (the
//! `gorillas3processor` writes chunks directly to S3), so there is
//! nothing for `StreamingConfig::from_yaml_data` to learn. What we
//! actually need is a tiny standalone routing table — one entry per
//! metric whose storage backend differs from the default — that the
//! HTTP handler consults to pick the right engine for each query.
//!
//! ## Schema
//!
//! ```yaml
//! # deploy/configs/backend-storage-routing.yaml
//! default: sketch_warm_tier           # StorageBackend; optional
//! metrics:
//!   http_requests_total: gorilla_s3_archive
//!   audit_events:        gorilla_s3_archive
//!   foo_count:           cold_jsonl_fallback
//! ```
//!
//! Valid `StorageBackend` values mirror the snake-cased serde tags on
//! `asap_types::StorageBackend`: `sketch_warm_tier`,
//! `gorilla_s3_archive`, `cold_jsonl_fallback`, `double_write`.
//!
//! Loaded once at backend startup (CLI flag `--backend-storage-routing`
//! on `precompute_engine`) and stored in `AppState`. Lookup is
//! O(metric-name-hash); a query that doesn't match any entry falls back
//! to `default` (which itself falls back to `SketchWarmTier`).
//!
//! ## Out of scope
//!
//! * Hot reload — the controller's plan-push is the long-term answer
//!   for per-metric routing; this YAML layer is the bridge that
//!   unblocks issue #46 criterion ⑤ until the plan-push lands. Adding
//!   hot reload is a one-line `ArcSwap` swap; deferred for now to keep
//!   the diff small and reviewable.
//! * Per-`(metric, statistic, accuracy)` granularity — `StorageBackend`
//!   already encodes the `DoubleWrite` axis the cost-aware dispatcher
//!   uses to pick warm-vs-archive per query.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use asap_types::StorageBackend;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// On-disk YAML schema. Public only so the loader / tests can build it
/// from literals; runtime callers should go through
/// [`BackendStorageRouting`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BackendStorageRoutingYaml {
    /// Fallback storage backend for any metric not explicitly listed in
    /// `metrics`. Optional; defaults to `SketchWarmTier`.
    #[serde(default)]
    default: StorageBackend,
    /// Per-metric overrides keyed by the bare metric name (no labels).
    #[serde(default)]
    metrics: HashMap<String, StorageBackend>,
}

/// In-memory routing table consulted by the HTTP handler at request
/// time. Build via [`Self::from_yaml_file`] / [`Self::from_yaml_str`]
/// or [`Self::empty`] (everything routes to `SketchWarmTier`).
#[derive(Debug, Clone)]
pub struct BackendStorageRouting {
    default: StorageBackend,
    metrics: HashMap<String, StorageBackend>,
}

impl BackendStorageRouting {
    /// Build an empty router — every metric resolves to
    /// `SketchWarmTier`. Equivalent to "no routing config at all" and
    /// preserves pre-Phase-5 dispatch (`SimpleEngine` direct path).
    pub fn empty() -> Self {
        Self {
            default: StorageBackend::default(),
            metrics: HashMap::new(),
        }
    }

    /// Construct directly. Used by the YAML loader and tests; production
    /// callers go through [`Self::from_yaml_file`].
    pub fn new(default: StorageBackend, metrics: HashMap<String, StorageBackend>) -> Self {
        Self { default, metrics }
    }

    /// Parse YAML text. See module docs for the schema.
    pub fn from_yaml_str(text: &str) -> Result<Self> {
        let parsed: BackendStorageRoutingYaml =
            serde_yaml::from_str(text).context("failed to parse backend-storage-routing YAML")?;
        Ok(Self {
            default: parsed.default,
            metrics: parsed.metrics,
        })
    }

    /// Read + parse a YAML file. Returns the populated router on
    /// success. The caller is expected to log the entry count at
    /// startup so operators can spot-check the deployment.
    pub fn from_yaml_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read backend-storage-routing YAML: {:?}", path))?;
        let routing = Self::from_yaml_str(&text)?;
        info!(
            path = %path.display(),
            default = ?routing.default,
            entries = routing.metrics.len(),
            "Loaded backend-storage-routing YAML",
        );
        Ok(routing)
    }

    /// Look up the storage backend for `metric_name`. Falls back to the
    /// table's `default` (which itself defaults to `SketchWarmTier`)
    /// when the metric is not listed.
    pub fn lookup(&self, metric_name: &str) -> StorageBackend {
        match self.metrics.get(metric_name).copied() {
            Some(backend) => {
                debug!(
                    metric = metric_name,
                    backend = ?backend,
                    "backend-storage-routing: per-metric override",
                );
                backend
            }
            None => {
                debug!(
                    metric = metric_name,
                    default = ?self.default,
                    "backend-storage-routing: no override, using default",
                );
                self.default
            }
        }
    }

    /// Read-only view of the configured default. Tests use this; the
    /// HTTP handler goes through `lookup`.
    pub fn default_backend(&self) -> StorageBackend {
        self.default
    }

    /// Number of explicit per-metric overrides. Tests / operator
    /// tooling.
    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    /// `true` iff no per-metric overrides are configured. Equivalent to
    /// `Self::empty()` but preserves a custom `default`.
    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }
}

impl Default for BackendStorageRouting {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_router_routes_everything_to_warm_tier() {
        let r = BackendStorageRouting::empty();
        assert_eq!(r.lookup("anything"), StorageBackend::SketchWarmTier);
        assert_eq!(r.lookup("http_requests_total"), StorageBackend::SketchWarmTier);
    }

    #[test]
    fn yaml_with_per_metric_override_routes_correctly() {
        let yaml = r#"
default: sketch_warm_tier
metrics:
  http_requests_total: gorilla_s3_archive
  audit_events: cold_jsonl_fallback
"#;
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        assert_eq!(
            r.lookup("http_requests_total"),
            StorageBackend::GorillaS3Archive
        );
        assert_eq!(
            r.lookup("audit_events"),
            StorageBackend::ColdJsonlFallback
        );
        assert_eq!(r.lookup("unlisted"), StorageBackend::SketchWarmTier);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn yaml_default_only_routes_all_metrics_to_default() {
        let yaml = "default: gorilla_s3_archive\n";
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        assert_eq!(r.lookup("anything"), StorageBackend::GorillaS3Archive);
        assert!(r.is_empty());
        assert_eq!(r.default_backend(), StorageBackend::GorillaS3Archive);
    }

    #[test]
    fn yaml_omitted_default_falls_back_to_sketch_warm() {
        let yaml = "metrics:\n  foo: gorilla_s3_archive\n";
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        assert_eq!(r.lookup("foo"), StorageBackend::GorillaS3Archive);
        assert_eq!(r.lookup("bar"), StorageBackend::SketchWarmTier);
    }

    #[test]
    fn empty_yaml_is_valid_and_empty() {
        let r = BackendStorageRouting::from_yaml_str("").expect("empty parse");
        assert_eq!(r.lookup("foo"), StorageBackend::SketchWarmTier);
        assert!(r.is_empty());
    }

    #[test]
    fn invalid_yaml_returns_error() {
        let yaml = "metrics: not-a-map\n";
        assert!(BackendStorageRouting::from_yaml_str(yaml).is_err());
    }
}
