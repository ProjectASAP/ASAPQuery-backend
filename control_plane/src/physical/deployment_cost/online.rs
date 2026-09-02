//! SP-5 online profiling — EMA-adjusted physical cost constants.
//!
//! The static benchmark table in [`cost_model`] seeds initial estimates.
//! As [`monitor::Scraper`] collects actual observations from running agents,
//! this module blends them in via an Exponential Moving Average (EMA) so the
//! planner's cost estimates converge toward real-world behaviour.
//!
//! # Blending weight
//!
//! After `MIN_OBS` observations the online weight reaches `MAX_ONLINE_WEIGHT`
//! (70 %).  Below that the benchmark retains the majority share so a single
//! noisy scrape cannot destabilise the planner.
//!
//! ```text
//! online_weight = min(observations / MIN_OBS, 1.0) × MAX_ONLINE_WEIGHT
//! effective     = online_weight × observed + (1 − online_weight) × benchmark
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::physical::deployment_cost::{benchmark_table_pub, SketchCosts};
use crate::types::SketchType;

// ── Tuning constants ──────────────────────────────────────────────────────────

/// EMA smoothing factor α.  Smaller → slower adaptation, more stable.
const EMA_ALPHA: f64 = 0.15;

/// Minimum number of observations before online data gets majority weight.
const MIN_OBS: f64 = 5.0;

/// Maximum fraction of weight given to online (observed) data.
const MAX_ONLINE_WEIGHT: f64 = 0.70;

// ── Types ─────────────────────────────────────────────────────────────────────

/// EMA-smoothed cost estimates for one sketch type.
#[derive(Debug, Clone)]
pub struct OnlineSketchCosts {
    /// EMA of observed bytes/series/sec transmitted by agents using this sketch.
    pub bw_bytes_per_series_per_sec: f64,
    /// EMA of observed CPU µs/sample at the agent collector.
    pub cpu_micros_per_sample: f64,
    /// Number of observations incorporated so far.
    pub observations: u64,
}

impl OnlineSketchCosts {
    fn from_benchmark(base: &SketchCosts) -> Self {
        Self {
            bw_bytes_per_series_per_sec: base.bytes_per_series_per_sec,
            cpu_micros_per_sample: base.cpu_micros_per_sample,
            observations: 0,
        }
    }

    /// Incorporate a new observation.
    pub fn update(&mut self, observed_bw: f64, observed_cpu: f64) {
        self.bw_bytes_per_series_per_sec =
            EMA_ALPHA * observed_bw + (1.0 - EMA_ALPHA) * self.bw_bytes_per_series_per_sec;
        self.cpu_micros_per_sample =
            EMA_ALPHA * observed_cpu + (1.0 - EMA_ALPHA) * self.cpu_micros_per_sample;
        self.observations += 1;
    }

    /// Returns a `SketchCosts` that blends the EMA observation with `base`.
    /// The online weight grows with observation count, capping at 70 %.
    pub fn effective_costs(&self, base: &SketchCosts) -> SketchCosts {
        let online_w = (self.observations as f64 / MIN_OBS).min(1.0) * MAX_ONLINE_WEIGHT;
        let bench_w = 1.0 - online_w;
        SketchCosts {
            bytes_per_series_per_sec: online_w * self.bw_bytes_per_series_per_sec
                + bench_w * base.bytes_per_series_per_sec,
            cpu_micros_per_sample: online_w * self.cpu_micros_per_sample
                + bench_w * base.cpu_micros_per_sample,
            base_memory_bytes: base.base_memory_bytes,
            relative_error_at_default: base.relative_error_at_default,
        }
    }
}

// ── Store ─────────────────────────────────────────────────────────────────────

/// Thread-safe map of sketch type → online EMA costs.
/// Shared between the `Scraper`'s `on_metrics` callback and `DeploymentCostPlanner`.
pub type OnlineMetricsStore = Arc<RwLock<HashMap<SketchType, OnlineSketchCosts>>>;

/// Initialise the store from the static benchmark table.
pub fn init_store() -> OnlineMetricsStore {
    let map = benchmark_table_pub()
        .iter()
        .map(|(st, c)| (st.clone(), OnlineSketchCosts::from_benchmark(c)))
        .collect();
    Arc::new(RwLock::new(map))
}

/// Update the EMA for `sketch_type` with a new bandwidth/cpu observation.
/// If `sketch_type` is not yet in the store it is inserted from the benchmark.
pub async fn update(
    store: &OnlineMetricsStore,
    sketch_type: &SketchType,
    observed_bw: f64,
    observed_cpu: f64,
) {
    let mut map = store.write().await;
    let entry = map.entry(sketch_type.clone()).or_insert_with(|| {
        let base = benchmark_table_pub();
        let costs = base.get(sketch_type).cloned().unwrap_or(SketchCosts {
            bytes_per_series_per_sec: 200.0,
            cpu_micros_per_sample: 1.0,
            base_memory_bytes: 4096.0,
            relative_error_at_default: 0.01,
        });
        OnlineSketchCosts::from_benchmark(&costs)
    });
    entry.update(observed_bw, observed_cpu);
}

/// Returns an effective cost table that blends online EMA data with benchmarks.
/// Uses `try_read()` so callers in hot paths never block.
pub fn effective_table(store: &OnlineMetricsStore) -> HashMap<SketchType, SketchCosts> {
    let base = benchmark_table_pub();
    match store.try_read() {
        Ok(map) => base
            .iter()
            .map(|(st, b)| {
                let eff = map
                    .get(st)
                    .map(|o| o.effective_costs(b))
                    .unwrap_or_else(|| b.clone());
                (st.clone(), eff)
            })
            .collect(),
        Err(_) => base, // fall back to benchmark if lock is contended
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SketchType;

    fn base() -> SketchCosts {
        SketchCosts {
            bytes_per_series_per_sec: 100.0,
            cpu_micros_per_sample: 1.0,
            base_memory_bytes: 4096.0,
            relative_error_at_default: 0.01,
        }
    }

    #[test]
    fn no_observations_returns_benchmark() {
        let o = OnlineSketchCosts::from_benchmark(&base());
        let eff = o.effective_costs(&base());
        assert_eq!(eff.bytes_per_series_per_sec, 100.0);
    }

    #[test]
    fn after_min_obs_online_dominates() {
        let mut o = OnlineSketchCosts::from_benchmark(&base());
        for _ in 0..10 {
            o.update(200.0, 2.0); // observed = 2× benchmark
        }
        let eff = o.effective_costs(&base());
        // Online weight = 1.0 × 0.70 = 0.70
        // effective = 0.70 × online_ema + 0.30 × benchmark
        // online_ema after 10 EMA steps from 100→200: converging toward 200
        assert!(
            eff.bytes_per_series_per_sec > 100.0,
            "effective bw should exceed benchmark after high observations: {}",
            eff.bytes_per_series_per_sec
        );
    }

    #[test]
    fn single_observation_has_low_weight() {
        let mut o = OnlineSketchCosts::from_benchmark(&base());
        o.update(1000.0, 10.0); // extreme spike
        let eff = o.effective_costs(&base());
        // online_weight = (1/5).min(1.0) × 0.70 = 0.14
        // should only move a little from benchmark
        assert!(
            eff.bytes_per_series_per_sec < 200.0,
            "single spike should not dominate: {}",
            eff.bytes_per_series_per_sec
        );
    }

    #[tokio::test]
    async fn store_update_and_effective_table() {
        let store = init_store();
        update(&store, &SketchType::DDSketch, 200.0, 2.0).await;
        let table = effective_table(&store);
        let eff = table.get(&SketchType::DDSketch).unwrap();
        assert!(eff.bytes_per_series_per_sec > 0.0);
    }

    #[tokio::test]
    async fn effective_table_falls_back_on_benchmark() {
        // Even without any updates the store returns sensible values.
        let store = init_store();
        let table = effective_table(&store);
        assert!(table.contains_key(&SketchType::HLL));
        assert!(table.contains_key(&SketchType::KLL));
    }
}
