//! Autonomous allocation, slice 3: derive the per-metric **knobs** — sampling
//! probability `p` and the CDM delta-transmission threshold `δ` — from the
//! target accuracy `ε`.
//!
//! Slice 2 ([`crate::query_planning`]) answered *which sketches* a query set
//! needs. This slice answers *how to run them*: it attaches `p` and `δ` to each
//! planned metric, completing `(ε, queries) → {sketches, p, δ}`.
//!
//! Two derivations, with an important asymmetry:
//!   * **`p` is fully autonomous** from `(ε, rate)` via the unified ε-floor law
//!     `p = 1/(1+ε²·rate)` — same law the data plane applies per-edge
//!     ([`epsilon_sample_floor`]). `rate` is the per-window update count.
//!   * **`δ` is only *sized* by ε.** The CDM threshold `τ` (the value of
//!     interest the monitor alerts on) is a *user/query input*, not derivable
//!     from ε. Given `τ` and the site count `k`, the even-split per-site slack
//!     is `δ = ε·τ / k`. Metrics with no monitored threshold get no `δ`.
//!
//! [`epsilon_sample_floor`]: (mirrors `data_plane::monitor::sampling_alloc::epsilon_sample_floor`;
//! control_plane has no dependency on data_plane, so the canonical one-liner is
//! re-stated here — keep the two in sync.)

use std::collections::HashMap;

use serde::Serialize;

use crate::query_planning::{plan_sketches_for_queries, QuerySetPlan};
use crate::runtime_samples::RuntimeSamplesStore;
use crate::types::SketchType;

/// Admission sampling probability under the unified ε-floor law,
/// `p = 1/(1 + ε²·rate)`.
///
/// `rate` is the per-window update count for the metric (NOT a probability).
/// Degenerate inputs (`rate ≤ 0` or `ε ≤ 0`) mean "no sampling" → `1.0`.
/// Mirrors `data_plane::monitor::sampling_alloc::epsilon_sample_floor`.
pub fn derive_sample_p(epsilon: f64, rate: f64) -> f64 {
    if rate <= 0.0 || epsilon <= 0.0 {
        return 1.0;
    }
    1.0 / (1.0 + epsilon * epsilon * rate)
}

/// Per-site CDM delta-transmission slack, `δ = ε·τ / k`.
///
/// The global threshold `τ` is the monitored value of interest (a user/query
/// input). The even-split CDM construction gives each of `k` sites a local
/// slack whose deviations sum to at most the global `ε·τ` budget. `k` is
/// clamped to ≥ 1. (A rate-weighted split is a possible refinement; the even
/// split is the baseline CDM allocation.)
pub fn derive_delta_threshold(epsilon: f64, tau: f64, n_sites: u32) -> f64 {
    let k = n_sites.max(1) as f64;
    epsilon * tau / k
}

/// GOS budget split (design §7, Layer B) — **exact linear peel**.
///
/// The staleness term `ε_st` is a *deterministic worst-case* bound, so it adds
/// **linearly** to the random error (Theorem 1): the composition is
/// `√(ε_sk² + ε_sa²) + ε_st ≤ ε_q`, NOT a three-way quadrature. This deriver
/// therefore peels `ε_st` off linearly first and splits only the *random*
/// residual between the sketch and sampling:
///
/// 1. linear headroom above the sketch: `excess = ε_q − ε_sk` (0 if the sketch
///    already consumes the budget);
/// 2. give a fraction `t = w_comm/(w_edge+w_comm)` of it to staleness:
///    `ε_st = t·excess` (more comm-weight ⇒ looser thresholds ⇒ larger `ε_st`);
/// 3. the remaining *random* budget is `ε_rand = ε_q − ε_st`, split in
///    quadrature: `ε_sa = √(ε_rand² − ε_sk²)`.
///
/// This satisfies `√(ε_sk² + ε_sa²) + ε_st = ε_q` **exactly**. Limits:
///   * `w_edge = 0` ⇒ `t=1` ⇒ `ε_st = ε_q−ε_sk`, `ε_rand=ε_sk`, `ε_sa=0` ⇒
///     **no sampling** (`p=1`), cheap communication;
///   * `w_comm = 0` ⇒ `t=0` ⇒ `ε_st=0`, `ε_sa=√(ε_q²−ε_sk²)` ⇒ coarse sampling,
///     tight thresholds.
pub fn split_budget(eps_total: f64, eps_sketch: f64, w_edge: f64, w_comm: f64) -> (f64, f64) {
    if eps_total <= eps_sketch {
        return (0.0, 0.0); // the sketch already uses the whole ε budget
    }
    let excess = eps_total - eps_sketch; // linear headroom above the sketch
    let t = if w_edge + w_comm <= 0.0 {
        0.0
    } else {
        w_comm / (w_edge + w_comm)
    };
    let eps_st = t * excess;
    let eps_rand = eps_total - eps_st; // random budget after the linear peel
    let eps_sa = (eps_rand * eps_rand - eps_sketch * eps_sketch)
        .max(0.0)
        .sqrt();
    (eps_sa, eps_st)
}

/// The runtime knobs derived for one metric.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricKnobs {
    /// Admission sampling probability (ε-floor). `1.0` = no sampling.
    pub sample_p: f64,
    /// Per-site CDM delta-transmission slack, present only for metrics that
    /// carry a monitored threshold `τ`.
    pub delta_threshold: Option<f64>,
}

/// A fully-allocated metric: the sketches to stand up plus the runtime knobs.
#[derive(Debug, Clone, PartialEq)]
pub struct AllocatedMetric {
    pub metric_name: String,
    pub sketches: Vec<SketchType>,
    pub knobs: MetricKnobs,
}

/// Attach ε-derived knobs to every metric in a [`QuerySetPlan`].
///
/// * `rate_for(metric)` → the metric's estimated per-window update rate (drives
///   `p`). At call sites this comes from runtime telemetry or a workload
///   estimate; kept as a closure so the deriver stays pure and testable.
/// * `monitor_for(metric)` → `Some((τ, k))` if the metric carries a CDM
///   threshold (drives `δ`), else `None` (no delta-transmission).
pub fn allocate_knobs(
    epsilon: f64,
    plan: &QuerySetPlan,
    mut rate_for: impl FnMut(&str) -> f64,
    mut monitor_for: impl FnMut(&str) -> Option<(f64, u32)>,
) -> Vec<AllocatedMetric> {
    plan.per_metric
        .iter()
        .map(|m| {
            let sample_p = derive_sample_p(epsilon, rate_for(&m.metric_name));
            let delta_threshold =
                monitor_for(&m.metric_name).map(|(tau, k)| derive_delta_threshold(epsilon, tau, k));
            AllocatedMetric {
                metric_name: m.metric_name.clone(),
                sketches: m.sketches.clone(),
                knobs: MetricKnobs {
                    sample_p,
                    delta_threshold,
                },
            }
        })
        .collect()
}

/// Wire-serializable view of one planned metric (`POST /api/v1/plan/auto`).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlannedMetric {
    pub metric: String,
    pub sketches: Vec<SketchType>,
    pub sample_p: f64,
    pub delta_threshold: Option<f64>,
}

/// A query routed to the cold tier, with the human-readable reason.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ColdEntry {
    pub query: String,
    pub reason: Option<String>,
}

/// The full autonomous-allocation response: per-metric warm plan + cold-tier
/// fallthrough, for a given `(ε, queries)` (and optional rate/monitor inputs).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AutoPlanResponse {
    pub epsilon: f64,
    pub metrics: Vec<PlannedMetric>,
    pub cold_only: Vec<ColdEntry>,
    /// Number of metrics actually applied (monitor injected → repost emits it →
    /// coordinator derives live `p`) when the request set `apply: true`. `None`
    /// for a dry-run plan.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied: Option<usize>,
}

/// Best-effort per-metric update rate from runtime telemetry.
///
/// The runtime-samples store is keyed by `(source, sketch, impl)` and carries
/// no metric name, so a metric's rate can only be approximated by the
/// throughput of the sketch *families* it was allocated: this sums the freshest
/// `throughput_items_per_sec.mean` across every telemetry key whose sketch tag
/// matches one of `sketches`. Returns `None` when no matching telemetry exists
/// (caller falls back to a default). The throughput is per-second; it feeds the
/// plan-time ε-floor `p` as a display estimate — the authoritative live `p` is
/// the data-plane coordinator's (computed from the monitor ε).
pub fn metric_rate_from_telemetry(
    store: &RuntimeSamplesStore,
    sketches: &[SketchType],
) -> Option<f64> {
    let mut total = 0.0;
    let mut matched = false;
    for key in store.keys() {
        if !sketches.iter().any(|s| sketch_tag_matches(&key.sketch, s)) {
            continue;
        }
        if let Some(rec) = store.latest(&key) {
            if let Some(tp) = rec.payload["bench"]["throughput_items_per_sec"]["mean"].as_f64() {
                total += tp;
                matched = true;
            }
        }
    }
    matched.then_some(total)
}

/// Loose match of a telemetry `sketch` tag to a [`SketchType`] family.
fn sketch_tag_matches(tag: &str, want: &SketchType) -> bool {
    let t = tag.to_lowercase();
    match want {
        SketchType::DDSketch => t.contains("ddsketch"),
        SketchType::KLL => t.contains("kll"),
        SketchType::HLL => t.contains("hll"),
        SketchType::CountSketch => t.contains("countsketch"),
        SketchType::CountMinSketch => t.contains("cms") || t.contains("countmin"),
    }
}

/// End-to-end autonomous allocation: `(ε, queries) → AutoPlanResponse`.
///
/// Runs slice 2 ([`plan_sketches_for_queries`]) then slice 3
/// ([`allocate_knobs`]), flattening to a serializable response. Per metric the
/// ε-floor `p` rate is resolved by `rate_for(metric, &sketches)` (e.g.
/// [`metric_rate_from_telemetry`]); when it returns `None`, `default_rate` is
/// used. `monitors[metric] = (τ, sites)` adds the CDM `δ` for monitored metrics.
/// Kept in the lib so it is unit-testable without standing up the HTTP server.
pub fn build_auto_plan<S, R>(
    epsilon: f64,
    queries: &[S],
    default_rate: f64,
    mut rate_for: R,
    monitors: &HashMap<String, (f64, u32)>,
) -> AutoPlanResponse
where
    S: AsRef<str>,
    R: FnMut(&str, &[SketchType]) -> Option<f64>,
{
    let plan = plan_sketches_for_queries(queries.iter().map(|s| s.as_ref()));
    // Resolve a rate per metric up front (telemetry → default fallback) so the
    // ε-floor closure below is a simple lookup.
    let rate_map: HashMap<String, f64> = plan
        .per_metric
        .iter()
        .map(|m| {
            let r = rate_for(&m.metric_name, &m.sketches).unwrap_or(default_rate);
            (m.metric_name.clone(), r)
        })
        .collect();
    let allocated = allocate_knobs(
        epsilon,
        &plan,
        |m| rate_map.get(m).copied().unwrap_or(default_rate),
        |m| monitors.get(m).copied(),
    );

    let metrics = allocated
        .into_iter()
        .map(|a| PlannedMetric {
            metric: a.metric_name,
            sketches: a.sketches,
            sample_p: a.knobs.sample_p,
            delta_threshold: a.knobs.delta_threshold,
        })
        .collect();
    let cold_only = plan
        .cold_only
        .into_iter()
        .map(|c| ColdEntry {
            query: c.query,
            reason: c.reason.map(|r| format!("{r:?}")),
        })
        .collect();

    AutoPlanResponse {
        epsilon,
        metrics,
        cold_only,
        applied: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planning::plan_sketches_for_queries;

    #[test]
    fn sample_p_follows_epsilon_floor() {
        // p = 1/(1+ε²·rate); ε=0.05, rate=1000 → 1/(1+2.5) = 0.2857…
        let p = derive_sample_p(0.05, 1000.0);
        assert!((p - 1.0 / 3.5).abs() < 1e-9, "got {p}");
    }

    #[test]
    fn split_budget_linear_peel_and_limits() {
        // Exact linear composition: √(ε_sk² + ε_sa²) + ε_st = ε_q.
        let (eq, esk) = (0.1, 0.03);
        let (sa, st) = split_budget(eq, esk, 1.0, 1.0);
        assert!(((esk * esk + sa * sa).sqrt() + st - eq).abs() < 1e-12);
        // No edge-CPU concern ⇒ no sampling (ε_sa=0), all headroom to staleness.
        let (sa, st) = split_budget(eq, esk, 0.0, 1.0);
        assert!(sa.abs() < 1e-12);
        assert!((st - (eq - esk)).abs() < 1e-12);
        // No comm concern ⇒ no staleness, ε_sa = √(ε_q²−ε_sk²).
        let (sa, st) = split_budget(eq, esk, 1.0, 0.0);
        assert_eq!(st, 0.0);
        assert!((sa - (eq * eq - esk * esk).sqrt()).abs() < 1e-12);
        // Higher edge weight ⇒ more of the budget to sampling (larger ε_sa).
        let (sa_hi, _) = split_budget(eq, esk, 4.0, 1.0);
        let (sa_lo, _) = split_budget(eq, esk, 1.0, 1.0);
        assert!(sa_hi > sa_lo);
        // Sketch consumes the whole budget ⇒ nothing left.
        assert_eq!(split_budget(0.03, 0.05, 1.0, 1.0), (0.0, 0.0));
    }

    #[test]
    fn sample_p_degenerate_inputs_disable_sampling() {
        assert_eq!(derive_sample_p(0.0, 1000.0), 1.0); // ε=0
        assert_eq!(derive_sample_p(0.05, 0.0), 1.0); // rate=0
    }

    #[test]
    fn sample_p_monotonic_decreasing_in_rate_and_epsilon() {
        assert!(derive_sample_p(0.05, 100.0) > derive_sample_p(0.05, 10_000.0));
        assert!(derive_sample_p(0.01, 1000.0) > derive_sample_p(0.10, 1000.0));
    }

    #[test]
    fn delta_threshold_is_even_split_of_epsilon_tau() {
        // ε·τ/k = 0.05 * 7000 / 5 = 70
        assert!((derive_delta_threshold(0.05, 7000.0, 5) - 70.0).abs() < 1e-9);
        // k clamps to ≥ 1
        assert!((derive_delta_threshold(0.05, 7000.0, 0) - 350.0).abs() < 1e-9);
    }

    #[test]
    fn allocate_knobs_attaches_p_and_optional_delta() {
        let plan = plan_sketches_for_queries(["quantile_over_time(0.99, latency_ms[5m])"]);
        // latency_ms has a monitored threshold τ=7000 over k=4 sites; rate 2000/win
        let allocated = allocate_knobs(
            0.05,
            &plan,
            |_m| 2000.0,
            |m| {
                if m == "latency_ms" {
                    Some((7000.0, 4))
                } else {
                    None
                }
            },
        );
        let a = allocated
            .iter()
            .find(|a| a.metric_name == "latency_ms")
            .expect("metric present");
        // p = 1/(1+0.0025*2000) = 1/6 = 0.1667…
        assert!(
            (a.knobs.sample_p - 1.0 / 6.0).abs() < 1e-9,
            "p={}",
            a.knobs.sample_p
        );
        // δ = 0.05*7000/4 = 87.5
        assert_eq!(a.knobs.delta_threshold, Some(87.5));
        // the sketch set survived from slice 2
        assert!(
            a.sketches.contains(&SketchType::DDSketch) || a.sketches.contains(&SketchType::KLL)
        );
    }

    #[test]
    fn build_auto_plan_end_to_end_over_query_set() {
        // (ε, queries) → full response. A quantile read + a monitored threshold.
        let mut monitors = HashMap::new();
        monitors.insert("latency_ms".to_string(), (7000.0, 4));
        let resp = build_auto_plan(
            0.05,
            &[
                "quantile_over_time(0.99, latency_ms[5m])".to_string(),
                "not valid promql @@@".to_string(),
            ],
            2000.0,
            |_m, _s| None, // no telemetry → default_rate
            &monitors,
        );
        assert_eq!(resp.epsilon, 0.05);
        // the quantile metric is planned with a quantile sketch + ε-floor p + δ
        let m = resp
            .metrics
            .iter()
            .find(|m| m.metric == "latency_ms")
            .expect("latency_ms planned");
        assert!(
            m.sketches.contains(&SketchType::DDSketch) || m.sketches.contains(&SketchType::KLL)
        );
        assert!((m.sample_p - 1.0 / 6.0).abs() < 1e-9); // 1/(1+0.0025*2000)
        assert_eq!(m.delta_threshold, Some(87.5)); // 0.05*7000/4
                                                   // the unparseable query falls through to cold
        assert_eq!(resp.cold_only.len(), 1);
        // and the response serializes to JSON cleanly (the handler's job)
        let v = serde_json::to_value(&resp).expect("serializes");
        assert!((v["metrics"][0]["sample_p"].as_f64().unwrap() - 1.0 / 6.0).abs() < 1e-9);
    }

    #[test]
    fn metric_without_monitor_gets_no_delta() {
        let plan = plan_sketches_for_queries(["quantile_over_time(0.99, latency_ms[5m])"]);
        let allocated = allocate_knobs(0.05, &plan, |_m| 500.0, |_m| None);
        let a = &allocated[0];
        assert_eq!(a.knobs.delta_threshold, None);
        assert!(a.knobs.sample_p < 1.0); // sampling still applies
    }

    #[test]
    fn telemetry_rate_read_by_sketch_family() {
        use crate::runtime_samples::{RuntimeRecord, RuntimeSamplesStore, SampleKey};
        let store = RuntimeSamplesStore::new(8);
        // seed a ddsketch throughput of 4000 items/sec from one source
        store.append_for_test(RuntimeRecord {
            source: "dc-a".into(),
            sketch: "ddsketch".into(),
            impl_name: "oxide".into(),
            schema_version: 1,
            payload: serde_json::json!({
                "bench": { "throughput_items_per_sec": { "mean": 4000.0, "stddev": 0.0 } }
            }),
        });
        // a quantile metric is allocated DDSketch → telemetry rate found
        let r = metric_rate_from_telemetry(&store, &[SketchType::DDSketch]);
        assert_eq!(r, Some(4000.0));
        // a metric allocated only HLL has no matching telemetry → None (→ default)
        assert_eq!(metric_rate_from_telemetry(&store, &[SketchType::HLL]), None);
        let _ = SampleKey {
            source: "dc-a".into(),
            sketch: "ddsketch".into(),
            impl_name: "oxide".into(),
        };
    }

    #[test]
    fn build_auto_plan_uses_telemetry_rate_when_available() {
        use crate::runtime_samples::{RuntimeRecord, RuntimeSamplesStore};
        let store = RuntimeSamplesStore::new(8);
        store.append_for_test(RuntimeRecord {
            source: "dc-a".into(),
            sketch: "ddsketch".into(),
            impl_name: "oxide".into(),
            schema_version: 1,
            payload: serde_json::json!({
                "bench": { "throughput_items_per_sec": { "mean": 2000.0, "stddev": 0.0 } }
            }),
        });
        let monitors = HashMap::new();
        // default_rate is a tiny 1.0, but telemetry says 2000 → p must reflect 2000
        let resp = build_auto_plan(
            0.05,
            &["quantile_over_time(0.99, latency_ms[5m])".to_string()],
            1.0,
            |_m, sks| metric_rate_from_telemetry(&store, sks),
            &monitors,
        );
        let m = resp
            .metrics
            .iter()
            .find(|m| m.metric == "latency_ms")
            .unwrap();
        // p = 1/(1+0.0025*2000) = 1/6 (telemetry), NOT 1/(1+0.0025*1) (default)
        assert!((m.sample_p - 1.0 / 6.0).abs() < 1e-9, "p={}", m.sample_p);
    }
}
