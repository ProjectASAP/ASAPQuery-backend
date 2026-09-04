//! Per-sketch performance / capability profile — the physical cost-model
//! surface.
//!
//! Moved out of `physical::runtime_capability` (Stage 4 of the
//! `promql_utilities` retirement / `physical::post_asap` re-layering): this is a
//! cost-model concern (insert/query throughput, memory, CPU, transmission
//! size, which intents each sketch family serves), read by the optimizer
//! for cost-based plan rewriting and by the physical planner to check
//! whether a sketch fits within a stage's budget — it was never L4 IR, just
//! filed alongside it because both modules touched `SketchAlgorithm`.
//!
//! Distinct from [`crate::physical::post_asap::schema::SketchStateMetadata`] —
//! that struct carries the **L4 type-system flags** (`mergeable` /
//! `subtractable` / `deletable`) that gate `SketchMerge` / `SketchSubtract`
//! / `SketchDelete` at plan-time. `SketchCapability` here is the
//! **perf / feasibility / intent-routing** profile consumed by the cost
//! model and the optimizer's binding rules — read at every plan-rewrite
//! call site, whereas `SketchStateMetadata` is sealed onto each
//! `PhysicalExpr` edge once the binding rule fires.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use planner_types::post_asap::SketchAlgorithm;

/// Performance and capability profile for a single sketch family.
///
/// Used by the optimizer to compare candidates and by the physical
/// planner to check whether a sketch fits within a stage's budget.
/// Populated from compiled-in defaults via [`default_capability_table`]
/// or overridden at runtime via [`load_capability_overrides`].
#[derive(Debug, Clone)]
pub struct SketchCapability {
    /// Insertion throughput (samples/sec at 1 core).
    pub insert_throughput: f64,
    /// Query throughput (queries/sec at 1 core).
    pub query_throughput: f64,
    /// Memory footprint per series (bytes).
    pub memory_bytes_per_series: u64,
    /// CPU cost per insert (µs/sample).
    pub cpu_micros_per_insert: f64,
    /// Transmission size per flush (bytes).
    pub transmission_bytes: u64,
    /// Whether the sketch supports merge (`sketch(A∪B) = merge(sketch(A), sketch(B))`).
    pub mergeable: bool,
    /// Whether the sketch supports delta encoding.
    pub supports_delta: bool,
    /// Whether the sketch supports sliding windows natively.
    pub supports_sliding_window: bool,
}

// ── YAML override loader ─────────────────────────────────────────────────────

/// YAML-serialisable capability profile (matches `sketch_capabilities.yml`).
#[derive(Debug, Clone, Deserialize, Serialize)]
struct SketchCapabilityYaml {
    insert_throughput: f64,
    query_throughput: f64,
    memory_bytes_per_series: u64,
    cpu_micros_per_insert: f64,
    transmission_bytes: u64,
    mergeable: bool,
    supports_delta: bool,
    supports_sliding_window: bool,
}

impl SketchCapabilityYaml {
    fn to_capability(&self) -> SketchCapability {
        SketchCapability {
            insert_throughput: self.insert_throughput,
            query_throughput: self.query_throughput,
            memory_bytes_per_series: self.memory_bytes_per_series,
            cpu_micros_per_insert: self.cpu_micros_per_insert,
            transmission_bytes: self.transmission_bytes,
            mergeable: self.mergeable,
            supports_delta: self.supports_delta,
            supports_sliding_window: self.supports_sliding_window,
        }
    }
}

/// YAML file structure for all sketch capabilities. Mirrors
/// `control_plane/sketch_capabilities.yml` 1:1.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct SketchCapabilitiesFile {
    ddsketch: SketchCapabilityYaml,
    kll: SketchCapabilityYaml,
    hll: SketchCapabilityYaml,
    count_sketch: SketchCapabilityYaml,
    count_min_sketch: SketchCapabilityYaml,
}

/// Compiled-in capability defaults — one entry per [`SketchAlgorithm`].
/// Replaces the per-variant `sketch_capability(SketchType)` function
/// that previously lived in `algebra/optimizer.rs`. Numerical values
/// are mirrored from the YAML so the in-process defaults match the
/// reference deployment file.
pub fn default_capability_table() -> HashMap<SketchAlgorithm, SketchCapability> {
    let mut map = HashMap::new();
    map.insert(
        SketchAlgorithm::DDSketch,
        SketchCapability {
            insert_throughput: 10_000_000.0,
            query_throughput: 50_000_000.0,
            memory_bytes_per_series: 4_096,
            cpu_micros_per_insert: 0.1,
            transmission_bytes: 4_096,
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
    );
    map.insert(
        SketchAlgorithm::Kll,
        SketchCapability {
            insert_throughput: 5_000_000.0,
            query_throughput: 20_000_000.0,
            memory_bytes_per_series: 8_192,
            cpu_micros_per_insert: 0.2,
            transmission_bytes: 8_192,
            mergeable: true,
            supports_delta: false,
            supports_sliding_window: false,
        },
    );
    map.insert(
        SketchAlgorithm::Hll,
        SketchCapability {
            insert_throughput: 20_000_000.0,
            query_throughput: 100_000_000.0,
            memory_bytes_per_series: 16_384,
            cpu_micros_per_insert: 0.05,
            transmission_bytes: 16_384,
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
    );
    map.insert(
        SketchAlgorithm::CountSketch,
        SketchCapability {
            insert_throughput: 8_000_000.0,
            query_throughput: 10_000_000.0,
            memory_bytes_per_series: 80_000,
            cpu_micros_per_insert: 0.5,
            transmission_bytes: 80_000,
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
    );
    map.insert(
        SketchAlgorithm::Cms,
        SketchCapability {
            insert_throughput: 8_000_000.0,
            query_throughput: 10_000_000.0,
            memory_bytes_per_series: 80_000,
            cpu_micros_per_insert: 0.5,
            transmission_bytes: 80_000,
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
    );
    map
}

/// Load sketch capability overrides from a YAML file. Falls back to
/// [`default_capability_table`] if the file is missing or malformed.
/// Physical replacement for the retired logical capability loader.
///
/// Env var: `CONTROLLER_SKETCH_CAPABILITIES=path/to/this/file.yml`.
pub fn load_capability_overrides(path: &str) -> HashMap<SketchAlgorithm, SketchCapability> {
    if let Ok(contents) = std::fs::read_to_string(path) {
        if let Ok(file) = serde_yaml::from_str::<SketchCapabilitiesFile>(&contents) {
            let mut map = HashMap::new();
            map.insert(SketchAlgorithm::DDSketch, file.ddsketch.to_capability());
            map.insert(SketchAlgorithm::Kll, file.kll.to_capability());
            map.insert(SketchAlgorithm::Hll, file.hll.to_capability());
            map.insert(
                SketchAlgorithm::CountSketch,
                file.count_sketch.to_capability(),
            );
            map.insert(SketchAlgorithm::Cms, file.count_min_sketch.to_capability());
            return map;
        }
    }
    default_capability_table()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_table_carries_all_five_sketch_kinds() {
        let t = default_capability_table();
        assert!(t.contains_key(&SketchAlgorithm::DDSketch));
        assert!(t.contains_key(&SketchAlgorithm::Kll));
        assert!(t.contains_key(&SketchAlgorithm::Hll));
        assert!(t.contains_key(&SketchAlgorithm::Cms));
        assert!(t.contains_key(&SketchAlgorithm::CountSketch));
    }

    #[test]
    fn default_table_ddsketch_has_runtime_cost_profile() {
        let t = default_capability_table();
        let cap = t.get(&SketchAlgorithm::DDSketch).unwrap();
        assert!(cap.mergeable);
    }

    #[test]
    fn default_table_hll_has_runtime_cost_profile() {
        let t = default_capability_table();
        let cap = t.get(&SketchAlgorithm::Hll).unwrap();
        assert!(cap.query_throughput > 0.0);
    }

    #[test]
    fn load_overrides_missing_path_returns_defaults() {
        let loaded = load_capability_overrides("/nonexistent/path/sketch_capabilities.yml");
        let defaults = default_capability_table();
        // Same set of keys, same defaults — we don't assert byte
        // equality on the SketchCapability values because they don't
        // impl PartialEq.
        assert_eq!(loaded.len(), defaults.len());
        for k in defaults.keys() {
            assert!(loaded.contains_key(k));
        }
    }
}
