//! Physical sketch wire-cost table — per-family OTLP wire footprint.
//!
//! Retirement note (2026-07): this file originally also carried a
//! Phase ε.1 three-mode bind-placement selector (`BindMode` +
//! `select_bind_mode` + `WireWorkload` + `break_even_samples` +
//! `est_wire_bytes_per_window_per_series`) that decided whether a
//! sketch runs at the edge, ships raw for backend-side sketching, or
//! falls back to a Prometheus archive, based on per-workload wire-cost
//! break-even math. It was fully designed and unit-tested but never
//! wired into `physical::post_asap::lower::bind_query_expr` (which always
//! produces `PhysicalExpr::Committed` — see that function's doc) or
//! anywhere else; `PhysicalExpr::RawAtEdgeSketchAtBackend` /
//! `RawAtEdgePrometheusArchive` remain structurally unreachable in
//! production as a result. Removed as dead code rather than kept as
//! speculative scaffolding. Recoverable from git history
//! (`chore/retire-tier2-scaffolding`, 2026-07) if bind-mode placement
//! becomes active work.
//!
//! What's left is the cost table alone — genuinely load-bearing today
//! via [`WireCostTable::for_kind`], consumed by
//! `physical::post_asap::cost_model::ControlPlaneCostModel` to rank
//! candidate sketch families by wire cost.
//!
//! ## Cost table source
//!
//! Sketch wire-state sizes are delta-encoded where the sketch family
//! supports it. KLL has no delta variant per `Implementation.tex`
//! (randomised compaction is not additively mergeable; the wire payload
//! is always full state). Numbers are conservative for typical
//! observability workloads — a future iteration will refine via observed
//! agent telemetry once the OnlineMetricsStore feeds back into the
//! planner.

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

    /// Lookup the per-flush cost for a sketch family.
    ///
    /// `SketchAlgorithm` (unlike the retired `physical::post_asap::SketchAlgorithm`)
    /// distinguishes heap-bearing from bare frequency sketches at the
    /// kind level rather than via a `with_heap` param flag. This table
    /// never modeled the heap's extra bytes separately (the old
    /// `for_kind` took a bare `SketchAlgorithm` with no visibility into
    /// `with_heap` at all) — `CmsWithHeap`/`CountSketchWithHeap` reuse
    /// their bare counterpart's cost to preserve that exact behavior.
    /// `Kmv`/`Theta` have no established cost number (nothing in this
    /// repo binds a cardinality intent to either today — the candidate
    /// list stays `Hll`-only, see `capability.rs`); they reuse `hll_delta`
    /// as a same-order-of-magnitude placeholder pending real numbers if
    /// this repo ever adopts them.
    // Exhaustive over `SketchAlgorithm` alone now (ASAPPlanner#218 split the
    // old flat `SummaryKind` into `SketchAlgorithm`/`ExactKind` -- the exact-
    // accumulator arm this match used to need, and its
    // "exact accumulators have no sketch wire-state cost" panic, are
    // unreachable by construction now instead of at runtime; see
    // control_plane/docs/design-asapplanner-pin-migration.md).
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
