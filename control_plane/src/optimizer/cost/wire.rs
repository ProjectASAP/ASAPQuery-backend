//! Phase ε.1 — three-mode wire cost model with native OTLP everywhere.
//!
//! The Phase β / γ planner had two transmission decisions: raw vs. sketch
//! (in `delta_cost_model.rs`) and exact vs. approximate (in `cost_model.rs`).
//! Phase ε.1 collapses those two axes into a single tri-mode selector that
//! also names where the work happens:
//!
//! | Mode                              | Edge action                | Wire                         | Backend role                 | Accuracy        |
//! |-----------------------------------|----------------------------|------------------------------|------------------------------|-----------------|
//! | [`BindMode::SketchAtEdge`]        | sketch processor at edge   | edge → backend OTLP          | ASAPQueryEngine over sketch     | bounded ε > 0   |
//! | [`BindMode::RawAtEdgeSketchAtBackend`] | no sketch processor   | edge → backend OTLP (raw)    | builds sketches at ingest    | bounded ε > 0   |
//! | [`BindMode::RawAtEdgePrometheusArchive`] | no sketch processor | edge → Prometheus OTLP HTTP | backend HTTP-forwards queries | exact (ε = 0)   |
//!
//! All three modes use OTLP on the wire (Mode 3 talks to Prometheus's
//! native OTLP receiver at `/api/v1/otlp/v1/metrics`, exposed by the
//! `--web.enable-otlp-receiver` flag in Prometheus 2.47+). This collapses
//! the agent's exporter vocabulary to a single family (`otlp` /
//! `otlphttp`) — no separate `prometheusremotewrite` exporter is needed.
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

#![allow(dead_code)]

use crate::sketch_algebra::params::SketchKind;
use crate::types::WorkloadCharacteristics;

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
    pub const fn for_kind(&self, kind: &SketchKind) -> SketchWireCost {
        match kind {
            SketchKind::DDSketch => self.ddsketch_delta,
            SketchKind::Kll => self.kll_full,
            SketchKind::Hll => self.hll_delta,
            SketchKind::Cms => self.count_min_delta,
            SketchKind::CountSketch => self.count_sketch_delta,
        }
    }
}

impl Default for WireCostTable {
    fn default() -> Self {
        Self::default_phase_eps_1()
    }
}

/// Break-even point in samples-per-window-per-series for a sketch
/// family — the workload size at which the sketch's per-flush wire cost
/// equals the raw OTLP cost of shipping every sample.
///
/// `break_even = ceil((state_bytes + envelope_bytes) / per_sample_bytes)`.
///
/// At 50 B per raw sample (Phase ε.1 default), this yields the canonical
/// table:
///
/// - DDSketch+delta: 16 samples/window
/// - KLL (full):     64 samples/window
/// - HLL+delta:      204 samples/window
/// - Count-Min+delta: 84 samples/window
/// - CountSketch+delta: 5,004 samples/window
pub fn break_even_samples(cost: SketchWireCost, per_sample_bytes: u64) -> u64 {
    if per_sample_bytes == 0 {
        return u64::MAX;
    }
    let total = cost.per_flush();
    // ceil division — at exactly the break-even sample count, sketch and
    // raw are equal; one more sample tips the balance to sketch.
    total.div_ceil(per_sample_bytes)
}

// ── Workload extensions for wire cost ────────────────────────────────────────

/// Per-workload inputs the wire cost model needs that aren't on
/// [`WorkloadCharacteristics`] today.
///
/// Why a parallel struct rather than fattening `WorkloadCharacteristics`:
/// the existing struct is consumed by the delta cost model (which has a
/// different feature set — fill rate, distinct keys per window, etc.).
/// Phase ε.1 adds three orthogonal fields and the wire cost model uses
/// exactly those plus `samples_per_sec_per_series` from the existing
/// struct. Folding both into one struct conflates two cost surfaces.
///
/// Convertible from [`WorkloadCharacteristics`] via [`Self::from_chars`].
#[derive(Debug, Clone, PartialEq)]
pub struct WireWorkload {
    /// Samples emitted per window per series — derived from emission rate
    /// × window. Drives the raw-vs-sketch break-even.
    pub samples_per_window_per_series: u64,
    /// Wire bytes of one raw OTLP metric data point after protobuf
    /// encoding. Default 50 (typical observability workload — counter or
    /// gauge with a small label set, after OTLP delta encoding kicks in
    /// at the SDK).
    pub per_sample_bytes: u64,
    /// Accuracy SLA (relative error). Already present on [`crate::types::QueryWorkload`];
    /// duplicated here so the wire cost model can be exercised without a
    /// full `QueryWorkload`.
    pub accuracy_sla: f64,
    /// Edge CPU budget in cores. `None` = unbounded.
    pub edge_cpu_budget: Option<f64>,
    /// Edge RAM budget in bytes. `None` = unbounded.
    pub edge_ram_budget: Option<u64>,
}

impl WireWorkload {
    /// Build a [`WireWorkload`] from existing
    /// [`WorkloadCharacteristics`] + a window duration. Useful for the
    /// planner's wire-cost decision when the caller already has a
    /// `WorkloadCharacteristics` for the delta model.
    pub fn from_chars(wc: &WorkloadCharacteristics, window_secs: u64, accuracy_sla: f64) -> Self {
        let samples_per_window =
            (wc.samples_per_sec_per_series * window_secs as f64).round() as u64;
        Self {
            samples_per_window_per_series: samples_per_window,
            per_sample_bytes: wc.bytes_per_raw_sample as u64,
            accuracy_sla,
            edge_cpu_budget: None,
            edge_ram_budget: wc.memory_budget_bytes,
        }
    }

    /// Default workload — 1 Hz × 60 s × 60 samples, 50 B per sample, 1%
    /// SLA, no edge budgets. Useful for tests and as a safe fallback.
    pub fn default_phase_eps_1() -> Self {
        Self {
            samples_per_window_per_series: 60,
            per_sample_bytes: 50,
            accuracy_sla: 0.01,
            edge_cpu_budget: None,
            edge_ram_budget: None,
        }
    }
}

impl Default for WireWorkload {
    fn default() -> Self {
        Self::default_phase_eps_1()
    }
}

// ── BindMode ─────────────────────────────────────────────────────────────────

/// Three-mode placement of the sketch + transmission work. Returned by
/// [`select_bind_mode`].
#[derive(Debug, Clone, PartialEq)]
pub enum BindMode {
    /// Mode 1: sketch processor at the edge ships sketch state via OTLP
    /// to the backend's ASAP tier. The default for high-sample-per-window
    /// workloads where the sketch's per-flush wire cost beats raw OTLP.
    SketchAtEdge { family: SketchKind },

    /// Mode 2: no sketch processor at edge; raw OTLP forwards to the
    /// gateway/backend, which builds sketches at ingest. Picked when the
    /// edge is resource-constrained (CPU / RAM) but the sketch still
    /// wins on backend-side bandwidth + accuracy.
    RawAtEdgeSketchAtBackend { family: SketchKind },

    /// Mode 3: no sketch processor at edge; raw OTLP ships directly to
    /// Prometheus's native OTLP receiver. Backend HTTP-forwards queries
    /// to Prometheus's `/api/v1/query` endpoint. Picked when no sketch
    /// family beats raw on bandwidth (low cardinality OR low
    /// samples_per_window). Accuracy is exact (ε = 0).
    RawAtEdgePrometheusArchive,
}

// ── Bind-mode selection ──────────────────────────────────────────────────────

/// Selection priority (Phase ε.1):
///
/// 1. [`BindMode::SketchAtEdge`] — sketch beats raw on bandwidth AND
///    `edge_cpu_budget` allows.
/// 2. [`BindMode::RawAtEdgeSketchAtBackend`] — sketch wins backend-side
///    but `edge_cpu_budget` tight.
/// 3. [`BindMode::RawAtEdgePrometheusArchive`] — no sketch family beats
///    raw on bandwidth (samples_per_window × per_sample_bytes < min
///    sketch state).
///
/// `families` is the set of sketch families the L4 binding rule
/// considers viable for the workload (e.g. `[Kll, DDSketch]` for a
/// quantile intent). The planner picks the family with the lowest
/// per-flush wire cost from among the viable ones.
///
/// `edge_cpu_tight` is a single Boolean for whether the edge can afford
/// to run a sketch processor — Phase ε.1 treats this as a binary signal
/// (either the operator declared a tight budget, or they didn't); a
/// future revision will consult observed processor CPU from the
/// OnlineMetricsStore.
pub fn select_bind_mode(
    families: &[SketchKind],
    workload: &WireWorkload,
    table: &WireCostTable,
) -> BindMode {
    let raw_bytes_per_window = workload
        .samples_per_window_per_series
        .saturating_mul(workload.per_sample_bytes);

    // Find the cheapest viable sketch family (the one whose per-flush
    // sketch state ≤ the raw bytes). If no family wins on bandwidth,
    // Mode 3 (Prometheus archive) is correct: no sketch amortises.
    let cheapest_winner = families
        .iter()
        .map(|f| (f.clone(), table.for_kind(f)))
        .filter(|(_, c)| c.per_flush() < raw_bytes_per_window)
        .min_by_key(|(_, c)| c.per_flush());

    match cheapest_winner {
        Some((family, _)) => {
            // Sketch beats raw — pick edge vs. backend by the CPU budget.
            if edge_cpu_budget_tight(workload) {
                BindMode::RawAtEdgeSketchAtBackend { family }
            } else {
                BindMode::SketchAtEdge { family }
            }
        }
        None => {
            // No sketch beats raw → Prometheus archive (Mode 3). Note
            // this also covers the degenerate `families.is_empty()` case
            // (e.g. a workload without a quantile / cardinality / topk
            // intent).
            BindMode::RawAtEdgePrometheusArchive
        }
    }
}

/// Phase ε.1 edge-CPU heuristic. Treats anything ≤ 0.25 cores as tight —
/// running a sketch processor at-rate (>1 kHz typical observability
/// workload) needs roughly 0.5 cores headroom for a single sketch family
/// based on the cross-host-parity benchmark numbers. A future revision
/// will replace this with an OnlineMetricsStore consultation.
fn edge_cpu_budget_tight(w: &WireWorkload) -> bool {
    matches!(w.edge_cpu_budget, Some(b) if b <= 0.25)
}

// ── Wire-bytes-per-window estimator ──────────────────────────────────────────

/// Estimated per-window per-series wire bytes for a given bind mode.
///
/// Used by the snapshot tests to confirm the planner's mode pick is
/// monotonic in workload size.
pub fn est_wire_bytes_per_window_per_series(
    mode: &BindMode,
    workload: &WireWorkload,
    table: &WireCostTable,
) -> u64 {
    match mode {
        BindMode::SketchAtEdge { family } | BindMode::RawAtEdgeSketchAtBackend { family } => {
            // Same edge → backend wire footprint either way (the sketch
            // state crosses the gateway in mode 1, the raw samples then
            // sketched do in mode 2; backend ingest cost is mode-2-higher
            // but that's a backend concern, not a wire concern).
            //
            // NOTE: mode 2 actually pays the raw cost to ship to the
            // gateway, then a sketch cost gateway → backend. We surface
            // the **edge-egress** cost here because that's what the
            // bandwidth bench measures.
            match mode {
                BindMode::SketchAtEdge { .. } => table.for_kind(family).per_flush(),
                BindMode::RawAtEdgeSketchAtBackend { .. } => workload
                    .samples_per_window_per_series
                    .saturating_mul(workload.per_sample_bytes),
                _ => unreachable!(),
            }
        }
        BindMode::RawAtEdgePrometheusArchive => workload
            .samples_per_window_per_series
            .saturating_mul(workload.per_sample_bytes),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// The per-sketch break-even values committed in Phase ε.1. The
    /// "Resulting break-evens at 50 B per raw sample" line in the design
    /// doc must match these.
    #[test]
    fn break_even_table_at_50_bytes_per_sample() {
        let t = WireCostTable::default();
        assert_eq!(
            break_even_samples(t.ddsketch_delta, 50),
            16,
            "DDSketch+delta"
        );
        assert_eq!(break_even_samples(t.kll_full, 50), 64, "KLL full");
        assert_eq!(break_even_samples(t.hll_delta, 50), 204, "HLL+delta");
        assert_eq!(
            break_even_samples(t.count_min_delta, 50),
            84,
            "Count-Min+delta"
        );
        assert_eq!(
            break_even_samples(t.count_sketch_delta, 50),
            5_004,
            "Count-Sketch+delta"
        );
    }

    /// Per-mode wire bytes at the 1 Hz × 60 s × 60 samples baseline.
    /// Mode 1 (sketch at edge) ships the sketch state; Modes 2/3 ship raw
    /// samples. The planner uses these to pick the cheapest mode.
    #[test]
    fn est_wire_bytes_baseline() {
        let table = WireCostTable::default();
        let w = WireWorkload::default_phase_eps_1();
        // 60 samples × 50 B = 3 000 B raw.
        let raw =
            est_wire_bytes_per_window_per_series(&BindMode::RawAtEdgePrometheusArchive, &w, &table);
        assert_eq!(raw, 3_000);
        // DDSketch state — 600 + 200 = 800 B. Sketch wins.
        let ddsketch = est_wire_bytes_per_window_per_series(
            &BindMode::SketchAtEdge {
                family: SketchKind::DDSketch,
            },
            &w,
            &table,
        );
        assert_eq!(ddsketch, 800);
        // HLL state — 10 000 + 200 = 10 200 B. HLL loses at 60 samples.
        let hll = est_wire_bytes_per_window_per_series(
            &BindMode::SketchAtEdge {
                family: SketchKind::Hll,
            },
            &w,
            &table,
        );
        assert_eq!(hll, 10_200);
    }

    /// Bind precedence: when the cheapest sketch wins on bandwidth and
    /// the edge CPU budget allows, Mode 1 is picked.
    #[test]
    fn bind_mode_picks_sketch_at_edge_when_sketch_wins_and_cpu_ok() {
        let table = WireCostTable::default();
        // 600 samples × 50 B = 30 000 B raw — DDSketch state (800 B) wins.
        let w = WireWorkload {
            samples_per_window_per_series: 600,
            per_sample_bytes: 50,
            accuracy_sla: 0.01,
            edge_cpu_budget: None,
            edge_ram_budget: None,
        };
        let mode = select_bind_mode(&[SketchKind::DDSketch, SketchKind::Kll], &w, &table);
        assert_eq!(
            mode,
            BindMode::SketchAtEdge {
                family: SketchKind::DDSketch
            }
        );
    }

    /// Bind precedence: when the cheapest sketch wins on bandwidth but
    /// the edge CPU budget is tight (≤ 0.25 cores), Mode 2 is picked
    /// — sketch builds at the backend instead of the edge.
    #[test]
    fn bind_mode_picks_raw_at_edge_sketch_at_backend_when_cpu_tight() {
        let table = WireCostTable::default();
        let w = WireWorkload {
            samples_per_window_per_series: 600,
            per_sample_bytes: 50,
            accuracy_sla: 0.01,
            edge_cpu_budget: Some(0.1),
            edge_ram_budget: None,
        };
        let mode = select_bind_mode(&[SketchKind::DDSketch, SketchKind::Kll], &w, &table);
        assert_eq!(
            mode,
            BindMode::RawAtEdgeSketchAtBackend {
                family: SketchKind::DDSketch
            }
        );
    }

    /// Bind precedence: when no sketch family beats raw on bandwidth,
    /// Mode 3 (Prometheus archive) is picked. Raw goes to Prometheus's
    /// native OTLP receiver; query-time HTTP forwarding hits
    /// `/api/v1/query`.
    #[test]
    fn bind_mode_picks_prometheus_archive_when_no_sketch_wins_on_bandwidth() {
        let table = WireCostTable::default();
        // 60 samples × 50 B = 3 000 B raw. DDSketch is 800 B (wins) — but
        // the only candidate is HLL (10 200 B, loses) and CountSketch
        // (250 200 B, loses). So no sketch beats raw → Mode 3.
        let w = WireWorkload {
            samples_per_window_per_series: 60,
            per_sample_bytes: 50,
            accuracy_sla: 0.01,
            edge_cpu_budget: None,
            edge_ram_budget: None,
        };
        let mode = select_bind_mode(&[SketchKind::Hll, SketchKind::CountSketch], &w, &table);
        assert_eq!(mode, BindMode::RawAtEdgePrometheusArchive);
    }

    /// Empty family list — no sketch is considered, so Mode 3 by default.
    #[test]
    fn bind_mode_empty_family_list_returns_prometheus_archive() {
        let table = WireCostTable::default();
        let w = WireWorkload::default_phase_eps_1();
        let mode = select_bind_mode(&[], &w, &table);
        assert_eq!(mode, BindMode::RawAtEdgePrometheusArchive);
    }

    /// Snapshot: Phase ε.1 break-even-table values committed for the
    /// report. If any of these change, the report numbers must follow.
    #[test]
    fn break_even_table_snapshot_phase_eps_1() {
        let t = WireCostTable::default();
        // (sketch family, expected break-even at 50 B per sample)
        let cases = [
            (t.ddsketch_delta, 16u64),
            (t.kll_full, 64),
            (t.hll_delta, 204),
            (t.count_min_delta, 84),
            (t.count_sketch_delta, 5_004),
        ];
        for (cost, expected) in cases {
            assert_eq!(break_even_samples(cost, 50), expected);
        }
    }

    /// Snapshot scenario from the report: 1 Hz × 60 s × 60 samples + HLL
    /// → control plane picks Mode 3 (Prometheus archive) because HLL
    /// state (10 200 B) > raw cost (3 000 B).
    #[test]
    fn snapshot_workload_1hz_60s_hll_picks_prometheus_archive() {
        let table = WireCostTable::default();
        let w = WireWorkload {
            samples_per_window_per_series: 60,
            per_sample_bytes: 50,
            accuracy_sla: 0.01,
            edge_cpu_budget: None,
            edge_ram_budget: None,
        };
        let mode = select_bind_mode(&[SketchKind::Hll], &w, &table);
        assert_eq!(mode, BindMode::RawAtEdgePrometheusArchive);
    }

    /// Snapshot scenario from the report: 10 Hz × 60 s × 600 samples +
    /// HLL → control plane picks Mode 1 (HLL at edge wins now). Raw cost
    /// (30 000 B) > HLL state (10 200 B), and edge CPU budget is open.
    #[test]
    fn snapshot_workload_10hz_60s_hll_picks_sketch_at_edge() {
        let table = WireCostTable::default();
        let w = WireWorkload {
            samples_per_window_per_series: 600,
            per_sample_bytes: 50,
            accuracy_sla: 0.01,
            edge_cpu_budget: None,
            edge_ram_budget: None,
        };
        let mode = select_bind_mode(&[SketchKind::Hll], &w, &table);
        assert_eq!(
            mode,
            BindMode::SketchAtEdge {
                family: SketchKind::Hll
            }
        );
    }

    /// Snapshot scenario from the report: 1 Hz × 60 s + edge_cpu_budget=0.1
    /// cores → Mode 2 (raw at edge → sketch at backend) to save edge CPU.
    /// At 1 Hz × 60 s × 60 samples × 50 B = 3 000 B raw, only DDSketch
    /// (800 B) and KLL (3 200 B) are viable; DDSketch is the cheapest
    /// sketch winner. Note: in this scenario the workload size is 60
    /// samples — DDSketch still wins (800 B < 3 000 B), so the cost
    /// model nominates it; the tight edge-CPU budget then routes to
    /// backend-side sketching.
    #[test]
    fn snapshot_workload_tight_edge_cpu_picks_raw_at_edge_sketch_at_backend() {
        let table = WireCostTable::default();
        let w = WireWorkload {
            samples_per_window_per_series: 60,
            per_sample_bytes: 50,
            accuracy_sla: 0.01,
            edge_cpu_budget: Some(0.1),
            edge_ram_budget: None,
        };
        let mode = select_bind_mode(&[SketchKind::DDSketch, SketchKind::Kll], &w, &table);
        assert_eq!(
            mode,
            BindMode::RawAtEdgeSketchAtBackend {
                family: SketchKind::DDSketch
            }
        );
    }

    /// Snapshot scenario at 1 Hz × 60 s × 5 K cardinality (control plane
    /// picks Mode 3 for every sketch except DDSketch / KLL because at 60
    /// samples/window only the two smallest sketch families amortize).
    /// The report lists this matrix.
    #[test]
    fn snapshot_5k_cardinality_1hz_60s_per_family_mode() {
        let table = WireCostTable::default();
        let w = WireWorkload {
            samples_per_window_per_series: 60,
            per_sample_bytes: 50,
            accuracy_sla: 0.01,
            edge_cpu_budget: None,
            edge_ram_budget: None,
        };
        // raw = 3 000 B/window/series.
        // DDSketch: 800 B → wins
        // KLL: 3 200 B → loses (KLL full state > raw at 60 samples)
        // HLL: 10 200 B → loses
        // CountMin: 4 200 B → loses
        // CountSketch: 250 200 B → loses
        let dd = select_bind_mode(&[SketchKind::DDSketch], &w, &table);
        assert_eq!(
            dd,
            BindMode::SketchAtEdge {
                family: SketchKind::DDSketch
            }
        );
        let kll = select_bind_mode(&[SketchKind::Kll], &w, &table);
        assert_eq!(kll, BindMode::RawAtEdgePrometheusArchive);
        let hll = select_bind_mode(&[SketchKind::Hll], &w, &table);
        assert_eq!(hll, BindMode::RawAtEdgePrometheusArchive);
        let cms = select_bind_mode(&[SketchKind::Cms], &w, &table);
        assert_eq!(cms, BindMode::RawAtEdgePrometheusArchive);
        let cs = select_bind_mode(&[SketchKind::CountSketch], &w, &table);
        assert_eq!(cs, BindMode::RawAtEdgePrometheusArchive);
    }
}
