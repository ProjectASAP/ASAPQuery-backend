// planner/delta_cost_model.rs
//
// Delta transmission cost model.
//
// Compares three transmission strategies for a given workload and sketch plan:
//
//   1. Raw pass-through   – send every raw OTLP sample unchanged.
//   2. Sketch (full)      – send a complete sketch payload each flush.
//   3. Sketch (delta)     – send only the cells that changed since the last
//                           flush (sparse delta encoding).
//
// The two key inputs that drive the delta decision are:
//
//   Fill rate  – fraction of sketch cells that change per flush period.
//                Lower fill rate → fewer cells in delta → better compression.
//                Fill rate is a function of distinct keys per flush period,
//                which itself depends on the flush rate.
//
//   Flush rate – how often the sketch is transmitted (Hz).
//                Window mode : 1 / window_duration_secs
//                Batch mode  : 1 / repeat_every_secs  (or 1 Hz as fallback)
//
//                Shorter flush periods mean fewer inserts per period →
//                lower fill rate → better delta compression ratio, but
//                also more flushes per second → higher CPU overhead per
//                second (though the same CPU per sample).

use std::collections::HashMap;
use std::time::Duration;

use crate::types::*;

// ── Delta benchmark table ─────────────────────────────────────────────────────
//
// Source: deltaaccbench (2026-03-15), sketch dims 5×2048, Zipf s=1.1,
//         10-second tumbling window, 2000 inserts/window.
//
// compression_at_fillX : full_bytes / delta_bytes at that estimated fill rate.
// cpu_micros_per_flush  : extra CPU for snapshot-diff + sparse-encode, per
//                         flush, per sketch instance (µs).
// snapshot_bytes        : memory for one previous-state snapshot (bytes).

#[derive(Debug, Clone, Copy)]
pub struct DeltaCosts {
    pub compression_at_fill_1pct: f64,
    pub compression_at_fill_5pct: f64,
    pub compression_at_fill_20pct: f64,
    /// Additional CPU per flush per sketch instance (µs).
    pub cpu_micros_per_flush: f64,
    /// Memory for one snapshot of this sketch type (bytes).
    pub snapshot_bytes_per_sketch: u64,
    pub supports_delta: bool,
}

pub fn delta_benchmark_table() -> HashMap<SketchType, DeltaCosts> {
    [
        (
            SketchType::CountMinSketch,
            DeltaCosts {
                // 5 rows × 2048 cols × 3 fields (count, sum, sum²) × 8 B = 245 760 B snapshot.
                compression_at_fill_1pct: 50.0,
                compression_at_fill_5pct: 20.0,
                compression_at_fill_20pct: 5.0,
                cpu_micros_per_flush: 120.0,
                snapshot_bytes_per_sketch: 245_760,
                supports_delta: true,
            },
        ),
        (
            SketchType::CountSketch,
            DeltaCosts {
                // 5 rows × 2048 cols × 1 field × 8 B = 81 920 B snapshot.
                compression_at_fill_1pct: 35.0,
                compression_at_fill_5pct: 15.0,
                compression_at_fill_20pct: 4.0,
                cpu_micros_per_flush: 80.0,
                snapshot_bytes_per_sketch: 81_920,
                supports_delta: true,
            },
        ),
        (
            SketchType::HLL,
            DeltaCosts {
                // precision=14 → 2^14 = 16 384 uint8 registers.
                compression_at_fill_1pct: 12.0,
                compression_at_fill_5pct: 6.0,
                compression_at_fill_20pct: 2.5,
                cpu_micros_per_flush: 30.0,
                snapshot_bytes_per_sketch: 16_384,
                supports_delta: true,
            },
        ),
        (
            SketchType::DDSketch,
            DeltaCosts {
                // Bucket map snapshot ~8 KB (sparse, relative-accuracy-dependent).
                compression_at_fill_1pct: 8.0,
                compression_at_fill_5pct: 3.0,
                compression_at_fill_20pct: 1.5,
                cpu_micros_per_flush: 20.0,
                snapshot_bytes_per_sketch: 8_192,
                supports_delta: true,
            },
        ),
        (
            SketchType::KLL,
            DeltaCosts {
                // KLL uses a compactor hierarchy; no delta implementation exists.
                compression_at_fill_1pct: 1.0,
                compression_at_fill_5pct: 1.0,
                compression_at_fill_20pct: 1.0,
                cpu_micros_per_flush: 0.0,
                snapshot_bytes_per_sketch: 0,
                supports_delta: false,
            },
        ),
    ]
    .into()
}

// ── Flush rate ────────────────────────────────────────────────────────────────

/// Effective flush period in seconds.
///
/// Window mode : window_duration (fixed tumbling window boundary).
/// Batch mode  : repeat_every from the query workload (how often the metric
///               is re-evaluated / a new batch arrives), or 1 s if unset.
///
/// A shorter flush period means:
///   • fewer inserts accumulate per period  → lower fill rate → better delta
///   • more flushes per second              → higher total CPU overhead
pub fn flush_period_secs(plan: &CollectionPlan, w: &QueryWorkload) -> f64 {
    if let Some(wd) = plan.agent_config.window_duration {
        return wd.as_secs_f64();
    }
    // Batch mode: use repeat_every as a proxy for the batch arrival interval.
    w.repeat_every
        .unwrap_or(Duration::from_secs(1))
        .as_secs_f64()
        .max(0.001) // guard against zero
}

// ── Distinct key estimation ───────────────────────────────────────────────────

/// Estimates the number of distinct keys observed in one flush period.
///
/// For CMS / CS this determines how many sketch cells are touched.
/// For HLL this determines how many registers receive new maximum values.
/// For DDSketch this determines what fraction of value buckets are active.
///
/// If the caller provided `distinct_keys_per_window` we use that directly.
/// Otherwise we apply a distribution-specific analytic approximation.
fn estimate_distinct_keys(inserts_per_flush: f64, wc: &WorkloadCharacteristics) -> f64 {
    if let Some(dk) = wc.distinct_keys_per_window {
        return dk as f64;
    }
    match wc.data_distribution {
        // Zipf (s ≈ 1.1): distinct count grows sub-linearly as N^(1/s) ≈ N^0.91.
        // Scaling factor 0.55 calibrated against deltaaccbench at s=1.1.
        DataDistribution::Zipf => 0.55 * inserts_per_flush.powf(0.91),
        // Uniform: every insert is a new distinct key in the worst case.
        DataDistribution::Uniform => inserts_per_flush,
        // Bursty: traffic concentrates in a small key subset; use conservative
        // sub-linear growth similar to Zipf but more concentrated.
        DataDistribution::Bursty => 0.30 * inserts_per_flush.powf(0.85),
    }
}

// ── Fill rate estimation ──────────────────────────────────────────────────────

/// Estimates the sketch fill rate: the fraction of cells / registers that
/// change in one flush period.
///
/// Fill rate drives the delta compression ratio (via [`interpolate_compression`]).
/// It depends on the flush period because:
///   • longer flush period  → more inserts accumulate → more cells touched
///   • shorter flush period → fewer inserts → fewer cells touched → better delta
///
/// Per sketch type:
///
/// CMS / CS: each distinct key touches `rows` cells (one per hash function row).
///           Fill rate ≈ min(1, distinct_keys / cols).
///
/// HLL:      each distinct key may update one of 2^precision registers.
///           Delta only sends registers that *increased* since last flush.
///           Fill rate uses the birthday-problem approximation:
///             1 − exp(−distinct / registers)
///           This is an upper bound at steady state (many registers already
///           hold near-maximum values and are seldom updated).
///
/// DDSketch: value range within the flush period determines which log-scale
///           buckets are touched.  Empirical baseline 10 % at a 10-second
///           window, scaled by the flush period.
///
/// KLL:      no delta implementation; always returns 0.
pub fn estimate_fill_rate(
    wc: &WorkloadCharacteristics,
    plan: &CollectionPlan,
    w: &QueryWorkload,
) -> f64 {
    let flush_secs = flush_period_secs(plan, w);
    let inserts_per_flush = wc.samples_per_sec_per_series * wc.series_count as f64 * flush_secs;
    let distinct = estimate_distinct_keys(inserts_per_flush, wc);
    match &plan.agent_config.sketch_params {
        SketchParams::CountMinSketch { cols, .. } => {
            let cols = *cols as f64;
            if cols > 0.0 {
                (distinct / cols).min(1.0)
            } else {
                0.05
            }
        }
        SketchParams::CountSketch { .. } => {
            // CountSketch uses epsilon-based sizing; approximate cols ≈ 1/ε².
            0.05 // safe fallback
        }
        SketchParams::HLL { precision } => {
            let registers = (1u64 << (*precision).max(1)) as f64;
            1.0_f64 - (-distinct / registers).exp()
        }
        SketchParams::DDSketch { .. } => {
            // Scale linearly around the 10-second benchmark baseline.
            let scale = (flush_secs / 10.0).clamp(0.2, 8.0);
            (0.10 * scale).min(0.80)
        }
        SketchParams::KLL { .. } => 0.0,
    }
}

// ── Compression ratio interpolation ──────────────────────────────────────────

/// Linearly interpolates the delta compression ratio from the three-point
/// benchmark table entries at 1 %, 5 %, and 20 % fill rate.
///
/// Above 20 % fill the ratio decays toward 1.0 (delta payload ≈ full
/// payload), eventually crossing 1.0 when the sparse encoding overhead
/// (cell indices) exceeds the savings.
pub fn interpolate_compression(costs: &DeltaCosts, fill_rate: f64) -> f64 {
    if fill_rate <= 0.01 {
        costs.compression_at_fill_1pct
    } else if fill_rate <= 0.05 {
        let t = (fill_rate - 0.01) / (0.05 - 0.01);
        lerp(
            costs.compression_at_fill_1pct,
            costs.compression_at_fill_5pct,
            t,
        )
    } else if fill_rate <= 0.20 {
        let t = (fill_rate - 0.05) / (0.20 - 0.05);
        lerp(
            costs.compression_at_fill_5pct,
            costs.compression_at_fill_20pct,
            t,
        )
    } else {
        // Linear extrapolation toward 1.0 at 100 % fill.
        let t = ((fill_rate - 0.20) / 0.80).min(1.0);
        lerp(costs.compression_at_fill_20pct, 1.0, t)
    }
}

fn lerp(a: f64, b: f64, t: f64) -> f64 {
    a + (b - a) * t
}

// ── Bandwidth helpers ─────────────────────────────────────────────────────────

/// Raw OTLP pass-through bandwidth (bytes/sec).
pub fn raw_bytes_per_sec(wc: &WorkloadCharacteristics) -> f64 {
    wc.series_count as f64 * wc.samples_per_sec_per_series * wc.bytes_per_raw_sample as f64
}

/// Full-sketch outbound bandwidth (bytes/sec).
///
/// Uses the sketch cost table entry `bytes_per_series_per_sec` scaled by
/// the number of sketch instances:
///   instances = series_count  (each input series produces one sketch output)
///
/// The dim_multiplier in the existing `PlanScore` captures the QUERY fanout
/// (how many group-by combinations exist); for bandwidth estimation we treat
/// `series_count` as the total sketch instances after aggregation.
pub fn sketch_full_bytes_per_sec(
    wc: &WorkloadCharacteristics,
    bytes_per_series_per_sec: f64,
) -> f64 {
    wc.series_count as f64 * bytes_per_series_per_sec
}

// ── Minimum delta compression ratio to enable delta ──────────────────────────

/// Delta must offer at least this compression over full-sketch to be worth
/// the snapshot memory and diff CPU overhead.
pub const MIN_DELTA_COMPRESSION_RATIO: f64 = 2.0;

/// Minimum total sample rate (series × Hz) below which sketches add more
/// overhead than they save; the planner falls back to raw pass-through.
pub const RAW_PASSTHROUGH_SAMPLE_RATE_THRESHOLD: f64 = 10.0;

// ── Main decision function ────────────────────────────────────────────────────

/// Decides the delta transmission mode for the given plan and workload.
///
/// Returns the resolved [`DeltaDecision`] and the full
/// [`TransmissionCostSummary`] for all three strategies.
///
/// Decision order:
///   1. If total sample rate < threshold → UseRaw (sketch overhead > saving).
///   2. If sketch type has no delta support → UseFullSketch.
///   3. Estimate fill rate from flush period and distribution.
///   4. Estimate compression ratio from fill rate.
///   5. If ratio < MIN_DELTA_COMPRESSION_RATIO → UseFullSketch.
///   6. If delta snapshot memory > budget → UseFullSketch.
///   7. Otherwise → UseDelta.
pub fn decide_delta(
    plan: &CollectionPlan,
    w: &QueryWorkload,
    wc: &WorkloadCharacteristics,
    bytes_per_series_per_sec: f64,
) -> (DeltaDecision, TransmissionCostSummary) {
    let table = delta_benchmark_table();
    let st = &plan.agent_config.sketch_type;

    let raw_bw = raw_bytes_per_sec(wc);
    let full_bw = sketch_full_bytes_per_sec(wc, bytes_per_series_per_sec);
    let flush_secs = flush_period_secs(plan, w);
    let flush_hz = if flush_secs > 0.0 {
        1.0 / flush_secs
    } else {
        1.0
    };

    // ── 1. Workload too small for sketching ──────────────────────────────────
    let total_sample_rate = wc.series_count as f64 * wc.samples_per_sec_per_series;
    if total_sample_rate < RAW_PASSTHROUGH_SAMPLE_RATE_THRESHOLD {
        let summary = TransmissionCostSummary {
            raw_bytes_per_sec: raw_bw,
            sketch_full_bytes_per_sec: full_bw,
            sketch_delta_bytes_per_sec: 0.0,
            delta_cpu_overhead_micros_per_sample: 0.0,
            delta_memory_overhead_bytes: 0.0,
            estimated_fill_rate: 0.0,
            flush_rate_hz: flush_hz,
        };
        return (
            DeltaDecision::UseRaw {
                reason: RawDataReason::WorkloadTooSmall,
                estimated_raw_bytes_per_sec: raw_bw,
            },
            summary,
        );
    }

    // ── 2. Sketch type does not support delta ────────────────────────────────
    let Some(&costs) = table.get(st) else {
        // Unknown sketch type – treat as no delta.
        let summary = TransmissionCostSummary {
            raw_bytes_per_sec: raw_bw,
            sketch_full_bytes_per_sec: full_bw,
            sketch_delta_bytes_per_sec: 0.0,
            delta_cpu_overhead_micros_per_sample: 0.0,
            delta_memory_overhead_bytes: 0.0,
            estimated_fill_rate: 0.0,
            flush_rate_hz: flush_hz,
        };
        return (
            DeltaDecision::UseFullSketch {
                reason: DeltaSkipReason::SketchTypeUnsupported,
                estimated_full_bytes_per_sec: full_bw,
            },
            summary,
        );
    };

    if !costs.supports_delta {
        let summary = TransmissionCostSummary {
            raw_bytes_per_sec: raw_bw,
            sketch_full_bytes_per_sec: full_bw,
            sketch_delta_bytes_per_sec: 0.0,
            delta_cpu_overhead_micros_per_sample: 0.0,
            delta_memory_overhead_bytes: 0.0,
            estimated_fill_rate: 0.0,
            flush_rate_hz: flush_hz,
        };
        return (
            DeltaDecision::UseFullSketch {
                reason: DeltaSkipReason::SketchTypeUnsupported,
                estimated_full_bytes_per_sec: full_bw,
            },
            summary,
        );
    }

    // ── 3. Fill rate ─────────────────────────────────────────────────────────
    let fill_rate = estimate_fill_rate(wc, plan, w);

    // ── 4. Delta compression ratio and bandwidth ─────────────────────────────
    let compression_ratio = interpolate_compression(&costs, fill_rate);
    let delta_bw = full_bw / compression_ratio;

    // ── 5. CPU overhead ──────────────────────────────────────────────────────
    // Total CPU added per second by delta diff + sparse encode:
    //   cpu_per_sec = cpu_per_flush × flushes_per_sec × series_count × dim_mult
    // Amortised per sample (what the operator cares about):
    //   cpu_per_sample_µs = cpu_per_flush / (samples_per_sec_per_series × flush_secs)
    let dim_mult = (plan.agent_config.aggregate_by.len() + 1) as f64;
    let cpu_per_sample_us = if wc.samples_per_sec_per_series > 0.0 && flush_secs > 0.0 {
        costs.cpu_micros_per_flush / (wc.samples_per_sec_per_series * flush_secs)
    } else {
        0.0
    };

    // ── 6. Memory overhead ───────────────────────────────────────────────────
    // One snapshot per sketch instance; the number of sketch instances
    // equals series_count × dim_mult (each group-by partition is separate).
    let snapshot_mem = wc.series_count as f64 * dim_mult * costs.snapshot_bytes_per_sketch as f64;

    let summary = TransmissionCostSummary {
        raw_bytes_per_sec: raw_bw,
        sketch_full_bytes_per_sec: full_bw,
        sketch_delta_bytes_per_sec: delta_bw,
        delta_cpu_overhead_micros_per_sample: cpu_per_sample_us,
        delta_memory_overhead_bytes: snapshot_mem,
        estimated_fill_rate: fill_rate,
        flush_rate_hz: flush_hz,
    };

    // ── 5b. Compression ratio below minimum ──────────────────────────────────
    if compression_ratio < MIN_DELTA_COMPRESSION_RATIO {
        return (
            DeltaDecision::UseFullSketch {
                reason: if fill_rate > 0.20 {
                    DeltaSkipReason::FillRateTooHigh
                } else {
                    DeltaSkipReason::CompressionRatioBelowThreshold
                },
                estimated_full_bytes_per_sec: full_bw,
            },
            summary,
        );
    }

    // ── 6b. Memory budget exceeded ───────────────────────────────────────────
    if let Some(budget) = wc.memory_budget_bytes {
        if snapshot_mem as u64 > budget {
            return (
                DeltaDecision::UseFullSketch {
                    reason: DeltaSkipReason::MemoryBudgetExceeded,
                    estimated_full_bytes_per_sec: full_bw,
                },
                summary,
            );
        }
    }

    // ── 7. Delta is beneficial ───────────────────────────────────────────────
    (
        DeltaDecision::UseDelta {
            threshold: 1.0, // lossless sparse threshold
            estimated_compression_ratio: compression_ratio,
            estimated_delta_bytes_per_sec: delta_bw,
            delta_cpu_overhead_micros_per_sample: cpu_per_sample_us,
            delta_memory_overhead_bytes: snapshot_mem,
        },
        summary,
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::rules::default_sketch_params;
    use chrono::Utc;
    use std::collections::HashMap;

    fn workload_for(agg: AggType) -> QueryWorkload {
        QueryWorkload {
            metric_name: "m".into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: vec![agg],
            time_window: Duration::from_secs(300),
            repeat_every: Some(Duration::from_secs(10)),
            accuracy_sla: 0.01,
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
    }

    fn make_plan(st: SketchType, window: Option<Duration>) -> CollectionPlan {
        let params = default_sketch_params(&st, 0.01);
        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Sketch,
                sketch_type: st.clone(),
                sketch_params: params,
                aggregate_by: vec![],
                label_matchers: vec![],
                window_duration: window,
                mode: if window.is_some() {
                    ProcessorMode::Window
                } else {
                    ProcessorMode::Batch
                },
                enable_self_monitoring: true,
                transmit_sketch: true,
                drop_original: true,
                delta_transmission: false,
                delta_threshold: 0.0,
                gos: None,
                enable_series_id: false,
                series_id_ttl_secs: 0,

                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            precompute: vec![],
            valid_until: Utc::now(),
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
        }
    }

    fn default_wc() -> WorkloadCharacteristics {
        // 10 series × 10 Hz keeps inserts-per-window well below cols=2048,
        // so fill rates stay sub-saturated and delta decisions are meaningful.
        WorkloadCharacteristics {
            series_count: 10,
            samples_per_sec_per_series: 10.0,
            bytes_per_raw_sample: 100,
            distinct_keys_per_window: None,
            data_distribution: DataDistribution::Zipf,
            memory_budget_bytes: None,
        }
    }

    // ── flush_period_secs ─────────────────────────────────────────────────────

    #[test]
    fn flush_period_uses_window_duration_in_window_mode() {
        let w = workload_for(AggType::Frequency);
        let plan = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(30)));
        assert_eq!(flush_period_secs(&plan, &w), 30.0);
    }

    #[test]
    fn flush_period_uses_repeat_every_in_batch_mode() {
        let mut w = workload_for(AggType::Frequency);
        w.repeat_every = Some(Duration::from_secs(15));
        let plan = make_plan(SketchType::CountMinSketch, None);
        assert_eq!(flush_period_secs(&plan, &w), 15.0);
    }

    #[test]
    fn flush_period_batch_fallback_is_one_second() {
        let mut w = workload_for(AggType::Frequency);
        w.repeat_every = None;
        let plan = make_plan(SketchType::CountMinSketch, None);
        assert_eq!(flush_period_secs(&plan, &w), 1.0);
    }

    // ── fill rate ─────────────────────────────────────────────────────────────

    #[test]
    fn fill_rate_cms_increases_with_longer_window() {
        // More inserts per flush → more cells touched → higher fill rate.
        let w_short = workload_for(AggType::Frequency);
        let w_long = workload_for(AggType::Frequency);
        let plan_short = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(10)));
        let plan_long = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(120)));
        let wc = default_wc();
        let fr_short = estimate_fill_rate(&wc, &plan_short, &w_short);
        let fr_long = estimate_fill_rate(&wc, &plan_long, &w_long);
        assert!(
            fr_long > fr_short,
            "longer window should give higher fill rate: short={fr_short:.4} long={fr_long:.4}"
        );
    }

    #[test]
    fn fill_rate_uniform_higher_than_zipf() {
        // Uniform distribution touches more unique cells than Zipf.
        let w = workload_for(AggType::Frequency);
        let plan = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(10)));
        let wc_zipf = WorkloadCharacteristics {
            data_distribution: DataDistribution::Zipf,
            ..default_wc()
        };
        let wc_unif = WorkloadCharacteristics {
            data_distribution: DataDistribution::Uniform,
            ..default_wc()
        };
        let fr_zipf = estimate_fill_rate(&wc_zipf, &plan, &w);
        let fr_unif = estimate_fill_rate(&wc_unif, &plan, &w);
        assert!(
            fr_unif > fr_zipf,
            "uniform should have higher fill rate than Zipf: zipf={fr_zipf:.4} unif={fr_unif:.4}"
        );
    }

    #[test]
    fn fill_rate_kll_is_zero() {
        let w = workload_for(AggType::Quantile);
        let plan = make_plan(SketchType::KLL, Some(Duration::from_secs(30)));
        let fr = estimate_fill_rate(&default_wc(), &plan, &w);
        assert_eq!(fr, 0.0, "KLL has no delta; fill rate must be 0");
    }

    #[test]
    fn fill_rate_hll_bounded() {
        let w = workload_for(AggType::Cardinality);
        let plan = make_plan(SketchType::HLL, Some(Duration::from_secs(60)));
        let fr = estimate_fill_rate(&default_wc(), &plan, &w);
        assert!(fr > 0.0 && fr <= 1.0, "HLL fill rate out of range: {fr}");
    }

    // ── compression interpolation ─────────────────────────────────────────────

    #[test]
    fn compression_at_1pct_returns_table_entry() {
        let costs = delta_benchmark_table()[&SketchType::CountMinSketch];
        let r = interpolate_compression(&costs, 0.005);
        assert_eq!(r, costs.compression_at_fill_1pct);
    }

    #[test]
    fn compression_monotone_decreasing_with_fill_rate() {
        let costs = delta_benchmark_table()[&SketchType::CountMinSketch];
        let r1 = interpolate_compression(&costs, 0.01);
        let r5 = interpolate_compression(&costs, 0.05);
        let r20 = interpolate_compression(&costs, 0.20);
        let r80 = interpolate_compression(&costs, 0.80);
        assert!(
            r1 >= r5,
            "compression should decrease as fill rises: {r1} vs {r5}"
        );
        assert!(r5 >= r20, "{r5} vs {r20}");
        assert!(r20 >= r80, "{r20} vs {r80}");
    }

    #[test]
    fn compression_at_100pct_is_near_one() {
        let costs = delta_benchmark_table()[&SketchType::CountMinSketch];
        let r = interpolate_compression(&costs, 1.0);
        assert!(
            r <= 1.05,
            "at 100 % fill compression ratio should be ~1: {r}"
        );
    }

    // ── decide_delta branches ─────────────────────────────────────────────────

    #[test]
    fn kll_yields_sketch_type_unsupported() {
        let w = workload_for(AggType::Quantile);
        let plan = make_plan(SketchType::KLL, Some(Duration::from_secs(30)));
        let (decision, _) = decide_delta(&plan, &w, &default_wc(), 80.0);
        assert!(
            matches!(
                decision,
                DeltaDecision::UseFullSketch {
                    reason: DeltaSkipReason::SketchTypeUnsupported,
                    ..
                }
            ),
            "KLL should be unsupported: {decision:?}"
        );
    }

    #[test]
    fn tiny_workload_yields_use_raw() {
        let w = workload_for(AggType::Frequency);
        let plan = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(10)));
        let wc = WorkloadCharacteristics {
            series_count: 1,
            samples_per_sec_per_series: 0.5, // total = 0.5 Hz < threshold
            ..default_wc()
        };
        let (decision, _) = decide_delta(&plan, &w, &wc, 200.0);
        assert!(
            matches!(
                decision,
                DeltaDecision::UseRaw {
                    reason: RawDataReason::WorkloadTooSmall,
                    ..
                }
            ),
            "tiny workload should fall back to raw: {decision:?}"
        );
    }

    #[test]
    fn cms_zipf_short_window_uses_delta() {
        // 1000 series, 100 Hz, 10s window, Zipf → low fill rate → good compression.
        let w = workload_for(AggType::Frequency);
        let plan = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(10)));
        let (decision, summary) = decide_delta(&plan, &w, &default_wc(), 200.0);
        assert!(
            matches!(decision, DeltaDecision::UseDelta { .. }),
            "CMS Zipf 10s should use delta (fill={:.3}): {decision:?}",
            summary.estimated_fill_rate
        );
    }

    #[test]
    fn cms_uniform_fills_fast_at_long_window() {
        // Uniform distribution + very long window → high fill rate → skip delta.
        let w = workload_for(AggType::Frequency);
        let plan = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(3600)));
        let wc = WorkloadCharacteristics {
            data_distribution: DataDistribution::Uniform,
            ..default_wc()
        };
        let (decision, summary) = decide_delta(&plan, &w, &wc, 200.0);
        assert!(
            matches!(
                decision,
                DeltaDecision::UseFullSketch {
                    reason: DeltaSkipReason::FillRateTooHigh
                        | DeltaSkipReason::CompressionRatioBelowThreshold,
                    ..
                }
            ),
            "CMS uniform 1h should skip delta (fill={:.3}): {decision:?}",
            summary.estimated_fill_rate
        );
    }

    #[test]
    fn memory_budget_blocks_delta() {
        let w = workload_for(AggType::Frequency);
        let plan = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(10)));
        // Budget of 1 byte — way below snapshot requirement.
        let wc = WorkloadCharacteristics {
            memory_budget_bytes: Some(1),
            ..default_wc()
        };
        let (decision, _) = decide_delta(&plan, &w, &wc, 200.0);
        assert!(
            matches!(
                decision,
                DeltaDecision::UseFullSketch {
                    reason: DeltaSkipReason::MemoryBudgetExceeded,
                    ..
                }
            ),
            "memory budget exceeded should skip delta: {decision:?}"
        );
    }

    #[test]
    fn hll_short_window_uses_delta() {
        let w = workload_for(AggType::Cardinality);
        let plan = make_plan(SketchType::HLL, Some(Duration::from_secs(10)));
        let (decision, _) = decide_delta(&plan, &w, &default_wc(), 40.0);
        assert!(
            matches!(decision, DeltaDecision::UseDelta { .. }),
            "HLL with short window + Zipf should use delta: {decision:?}"
        );
    }

    #[test]
    fn delta_bw_less_than_full_bw_when_delta_used() {
        let w = workload_for(AggType::Frequency);
        let plan = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(10)));
        let (_, summary) = decide_delta(&plan, &w, &default_wc(), 200.0);
        if summary.sketch_delta_bytes_per_sec > 0.0 {
            assert!(
                summary.sketch_delta_bytes_per_sec < summary.sketch_full_bytes_per_sec,
                "delta bw should be less than full: delta={} full={}",
                summary.sketch_delta_bytes_per_sec,
                summary.sketch_full_bytes_per_sec
            );
        }
    }

    #[test]
    fn cpu_overhead_lower_with_longer_flush_period() {
        // Amortised CPU per sample = cpu_per_flush / (samples_per_sec × flush_secs).
        // Longer flush period → more samples to amortise over → lower per-sample cost.
        let w = workload_for(AggType::Frequency);
        let plan_10s = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(10)));
        let plan_60s = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(60)));
        let (_, s10) = decide_delta(&plan_10s, &w, &default_wc(), 200.0);
        let (_, s60) = decide_delta(&plan_60s, &w, &default_wc(), 200.0);
        assert!(
            s60.delta_cpu_overhead_micros_per_sample < s10.delta_cpu_overhead_micros_per_sample,
            "longer flush period should lower per-sample CPU overhead: \
             10s={:.4}µs 60s={:.4}µs",
            s10.delta_cpu_overhead_micros_per_sample,
            s60.delta_cpu_overhead_micros_per_sample
        );
    }

    #[test]
    fn memory_overhead_scales_with_series_count() {
        let w = workload_for(AggType::Frequency);
        let plan = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(10)));
        let wc_small = WorkloadCharacteristics {
            series_count: 10,
            ..default_wc()
        };
        let wc_large = WorkloadCharacteristics {
            series_count: 10_000,
            ..default_wc()
        };
        let (_, s_small) = decide_delta(&plan, &w, &wc_small, 200.0);
        let (_, s_large) = decide_delta(&plan, &w, &wc_large, 200.0);
        assert!(
            s_large.delta_memory_overhead_bytes > s_small.delta_memory_overhead_bytes,
            "more series → more snapshot memory"
        );
        // Ratio should be proportional to series_count ratio (10 000 / 10 = 1000).
        let ratio = s_large.delta_memory_overhead_bytes / s_small.delta_memory_overhead_bytes;
        assert!(
            (ratio - 1000.0).abs() < 1.0,
            "memory should scale linearly with series_count: ratio={ratio}"
        );
    }

    #[test]
    fn summary_raw_bw_matches_series_rate_size() {
        let w = workload_for(AggType::Frequency);
        let plan = make_plan(SketchType::CountMinSketch, Some(Duration::from_secs(10)));
        let wc = WorkloadCharacteristics {
            series_count: 500,
            samples_per_sec_per_series: 10.0,
            bytes_per_raw_sample: 120,
            ..default_wc()
        };
        let (_, summary) = decide_delta(&plan, &w, &wc, 200.0);
        let expected = 500.0 * 10.0 * 120.0;
        assert!(
            (summary.raw_bytes_per_sec - expected).abs() < 0.01,
            "raw_bw={} expected={expected}",
            summary.raw_bytes_per_sec
        );
    }
}
