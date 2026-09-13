//! Per-family OTLP wire footprints used to rank candidate sketch families.
//!
//! Sizes assume delta encoding where supported. KLL uses full state because its
//! randomized compaction is not additively mergeable. Defaults are conservative
//! estimates for observability workloads, rather than measured telemetry.

use planner_types::post_asap::SketchAlgorithm;

// ── Wire-cost table ──────────────────────────────────────────────────────────

/// Per-window wire footprint of one sketch family.
///
/// `state_bytes` is the per-flush payload after delta encoding (where
/// supported); `envelope_bytes` is the OTLP `SketchEnvelope` overhead
/// (gRPC framing + resource attributes + scope info).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SketchWireCost {
    /// Per-flush sketch state bytes (delta-encoded where supported).
    pub state_bytes: u64,
    /// OTLP envelope bytes per flush — gRPC framing + resource attrs + scope.
    pub envelope_bytes: u64,
}

impl SketchWireCost {
    /// Total wire bytes per flush (state + envelope).
    pub const fn per_flush(&self) -> u64 {
        self.state_bytes + self.envelope_bytes
    }
}

/// Phase ε.1 wire-cost table — one entry per sketch family.
///
/// All values are delta-encoded where a delta variant exists. KLL has no
/// delta variant per `Implementation.tex`; uses full state. Numbers are
/// conservative for typical observability workloads; a future iteration
/// can refine via observed agent telemetry.
#[derive(Debug, Clone, Copy)]
pub struct WireCostTable {
    /// DDSketch with sparse bucket-map delta encoding.
    pub ddsketch_delta: SketchWireCost,
    /// KLL — no delta variant, ships full compactor hierarchy.
    pub kll_full: SketchWireCost,
    /// HLL register-array delta (~60% of 16 KB full).
    pub hll_delta: SketchWireCost,
    /// Count-Min row-by-row delta (~70% of 6 KB full).
    pub count_min_delta: SketchWireCost,
    /// Count-Sketch sparse-cell delta (~60% of 400 KB full).
    pub count_sketch_delta: SketchWireCost,
}

impl WireCostTable {
    /// The default Phase ε.1 cost table — see module docs for the source
    /// rationale.
    pub const fn default_phase_eps_1() -> Self {
        Self {
            ddsketch_delta: SketchWireCost {
                state_bytes: 600,
                envelope_bytes: 200,
            },
            kll_full: SketchWireCost {
                state_bytes: 3_000,
                envelope_bytes: 200,
            },
            hll_delta: SketchWireCost {
                state_bytes: 10_000,
                envelope_bytes: 200,
            },
            count_min_delta: SketchWireCost {
                state_bytes: 4_000,
                envelope_bytes: 200,
            },
            count_sketch_delta: SketchWireCost {
                state_bytes: 250_000,
                envelope_bytes: 200,
            },
        }
    }

    /// Look up per-flush wire cost. Heap-bearing sketches reuse their bare family
    /// cost; heap bytes are not priced separately. KMV and Theta reuse the HLL
    /// estimate because no independent measurement is available.
    pub const fn for_algorithm(&self, algorithm: &SketchAlgorithm) -> SketchWireCost {
        match algorithm {
            // No collector wire implementation is available for this family.
            SketchAlgorithm::UnivMon => SketchWireCost {
                state_bytes: u64::MAX,
                envelope_bytes: 0,
            },
            SketchAlgorithm::DDSketch => self.ddsketch_delta,
            SketchAlgorithm::Kll => self.kll_full,
            SketchAlgorithm::Hll => self.hll_delta,
            SketchAlgorithm::Kmv | SketchAlgorithm::Theta => self.hll_delta,
            SketchAlgorithm::Cms => self.count_min_delta,
            SketchAlgorithm::CmsWithHeap => self.count_min_delta,
            SketchAlgorithm::CountSketch => self.count_sketch_delta,
            SketchAlgorithm::CountSketchWithHeap => self.count_sketch_delta,
        }
    }
}

impl Default for WireCostTable {
    fn default() -> Self {
        Self::default_phase_eps_1()
    }
}
