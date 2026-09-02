//! Physical deployment total-cost-of-ownership estimator.

use serde::{Deserialize, Serialize};

/// Cloud pricing configuration (loaded from YAML or defaults).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CloudPricing {
    /// Grafana Cloud: $/1000 active series at 1 DPM (data point per minute).
    pub grafana_per_1k_series_1dpm: f64,
    /// AWS S3: $/GB stored per month.
    pub s3_storage_per_gb_month: f64,
    /// AWS S3: $/1000 PUT requests.
    pub s3_put_per_1k: f64,
    /// AWS S3: $/1000 GET requests.
    pub s3_get_per_1k: f64,
    /// AWS S3: $/GB data transfer out.
    pub s3_transfer_per_gb: f64,
    /// EC2 compute cost for sketch processing ($/hour for the instance type).
    pub ec2_sketch_instance_per_hour: f64,
}

impl Default for CloudPricing {
    fn default() -> Self {
        Self {
            grafana_per_1k_series_1dpm: 6.50,
            s3_storage_per_gb_month: 0.023,
            s3_put_per_1k: 0.005,
            s3_get_per_1k: 0.0004,
            s3_transfer_per_gb: 0.09,
            ec2_sketch_instance_per_hour: 0.384, // c6i.xlarge
        }
    }
}

/// Workload parameters for TCO estimation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TcoWorkload {
    /// Number of active time series.
    pub series_count: u64,
    /// Samples per second per series.
    pub samples_per_sec: f64,
    /// Bytes per raw sample (metric name + labels + value + timestamp).
    pub bytes_per_sample: u64,
    /// Scrape interval in seconds.
    pub scrape_interval_secs: u64,
    /// Average query rate (queries per second).
    pub queries_per_sec: f64,
    /// Average query window in seconds.
    pub query_window_secs: u64,
    /// Retention period in days.
    pub retention_days: u64,
    /// Sketch compression ratio (sketch bytes / raw bytes). Typically 0.03-0.10.
    pub sketch_compression_ratio: f64,
    /// Delta compression ratio (delta bytes / full sketch bytes). Typically 0.2-0.5.
    pub delta_compression_ratio: f64,
}

impl Default for TcoWorkload {
    fn default() -> Self {
        Self {
            series_count: 100_000,
            samples_per_sec: 1.0,
            bytes_per_sample: 100,
            scrape_interval_secs: 15,
            queries_per_sec: 1.0,
            query_window_secs: 300,
            retention_days: 30,
            sketch_compression_ratio: 0.05,
            delta_compression_ratio: 0.3,
        }
    }
}

/// TCO estimation result.
#[derive(Debug, Clone, Serialize)]
pub struct TcoEstimate {
    /// Monthly cost breakdown for traditional pipeline.
    pub before: TcoBefore,
    /// Monthly cost breakdown for sketch pipeline.
    pub after: TcoAfter,
    /// Absolute monthly savings in dollars.
    pub monthly_savings_dollars: f64,
    /// Savings as a percentage of the before cost.
    pub savings_percent: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TcoBefore {
    pub ingestion_dollars: f64,
    pub query_dollars: f64,
    pub storage_dollars: f64,
    pub total_dollars: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TcoAfter {
    pub sketch_ingestion_dollars: f64,
    pub s3_ingestion_dollars: f64,
    pub sketch_query_dollars: f64,
    pub s3_query_dollars: f64,
    pub sketch_storage_dollars: f64,
    pub s3_storage_dollars: f64,
    pub compute_dollars: f64,
    pub total_dollars: f64,
}

/// Seconds in a 30-day month.
const SECS_PER_MONTH: f64 = 86_400.0 * 30.0;
/// Hours in a 30-day month.
const HOURS_PER_MONTH: f64 = 24.0 * 30.0;
/// Bytes per gigabyte.
const BYTES_PER_GB: f64 = 1_000_000_000.0;
/// Grafana bundled storage cost approximation ($/GB/month for active series).
const GRAFANA_STORAGE_PER_GB_MONTH: f64 = 0.10;
/// Approximate Grafana query cost per 1000 queries (bundled in series pricing).
const GRAFANA_QUERY_COST_PER_1K: f64 = 0.10;
/// Series capacity per EC2 instance for sketch processing.
const SERIES_PER_INSTANCE: f64 = 100_000.0;

/// Compute a TCO estimate comparing traditional TSDB vs sketch-based pipeline.
pub fn estimate_tco(workload: &TcoWorkload, pricing: &CloudPricing) -> TcoEstimate {
    let before = compute_before(workload, pricing);
    let after = compute_after(workload, pricing);

    let savings = before.total_dollars - after.total_dollars;
    let savings_pct = if before.total_dollars > 0.0 {
        savings / before.total_dollars * 100.0
    } else {
        0.0
    };

    TcoEstimate {
        before,
        after,
        monthly_savings_dollars: savings,
        savings_percent: savings_pct,
    }
}

fn compute_before(w: &TcoWorkload, p: &CloudPricing) -> TcoBefore {
    // Data points per minute multiplier: if scrape_interval is 15s, that's 4 DPM.
    let dpm_multiplier = if w.scrape_interval_secs > 0 {
        60.0 / w.scrape_interval_secs as f64
    } else {
        1.0
    };

    // Ingestion: Grafana charges per 1000 active series at 1 DPM.
    let ingestion =
        (w.series_count as f64 / 1000.0) * p.grafana_per_1k_series_1dpm * dpm_multiplier;

    // Storage: raw bytes over retention period.
    let samples_per_day = w.samples_per_sec * 86_400.0;
    let total_bytes = w.bytes_per_sample as f64
        * samples_per_day
        * w.series_count as f64
        * w.retention_days as f64;
    let storage = (total_bytes / BYTES_PER_GB) * GRAFANA_STORAGE_PER_GB_MONTH;

    // Query: approximate cost based on query rate.
    let total_queries_per_month = w.queries_per_sec * SECS_PER_MONTH;
    let query = (total_queries_per_month / 1000.0) * GRAFANA_QUERY_COST_PER_1K;

    let total = ingestion + storage + query;
    TcoBefore {
        ingestion_dollars: ingestion,
        query_dollars: query,
        storage_dollars: storage,
        total_dollars: total,
    }
}

fn compute_after(w: &TcoWorkload, p: &CloudPricing) -> TcoAfter {
    let series = w.series_count as f64;

    // Sketch bytes per second per series after both compression stages.
    let sketch_bps = w.samples_per_sec
        * w.bytes_per_sample as f64
        * w.sketch_compression_ratio
        * w.delta_compression_ratio;

    // -- Sketch ingestion: transfer cost for sketch data --
    let sketch_bytes_per_month = series * sketch_bps * SECS_PER_MONTH;
    let sketch_ingestion = (sketch_bytes_per_month / BYTES_PER_GB) * p.s3_transfer_per_gb;

    // -- S3 ingestion: sketch-compressed backup (storage + PUT costs) --
    // In the sketch pipeline, S3 stores compressed sketches, not raw data.
    let sketch_gb_per_month = sketch_bytes_per_month / BYTES_PER_GB;
    // PUTs are batched: one PUT per flush interval (scrape_interval) containing
    // all series in a single object, not one PUT per series.
    // No PUTs needed if there are no series to back up.
    let puts_per_month = if w.scrape_interval_secs > 0 && w.series_count > 0 {
        SECS_PER_MONTH / w.scrape_interval_secs as f64
    } else {
        0.0
    };
    let s3_ingestion = sketch_gb_per_month * p.s3_storage_per_gb_month
        + (puts_per_month / 1000.0) * p.s3_put_per_1k;

    // -- Sketch query: near-zero (local sketch eval) --
    let sketch_query = 0.0;

    // -- S3 query: only for ad-hoc queries (assume 1% of total) --
    let total_queries_per_month = w.queries_per_sec * SECS_PER_MONTH;
    let s3_query = 0.01 * (total_queries_per_month / 1000.0) * p.s3_get_per_1k;

    // -- Compute: EC2 for sketch processing --
    let num_instances = (series / SERIES_PER_INSTANCE).ceil().max(1.0);
    let compute = p.ec2_sketch_instance_per_hour * HOURS_PER_MONTH * num_instances;

    // -- Sketch storage: in-memory sketches (small, included in compute) --
    // Approximate: sketch data retained for query_window only, not full retention.
    let sketch_mem_bytes = series * sketch_bps * w.query_window_secs as f64;
    let sketch_storage = (sketch_mem_bytes / BYTES_PER_GB) * p.s3_storage_per_gb_month;

    // -- S3 storage: sketch-compressed data over retention --
    let s3_storage_bytes = sketch_bytes_per_month * w.retention_days as f64 / 30.0;
    let s3_storage = (s3_storage_bytes / BYTES_PER_GB) * p.s3_storage_per_gb_month;

    let total = sketch_ingestion
        + s3_ingestion
        + sketch_query
        + s3_query
        + sketch_storage
        + s3_storage
        + compute;

    TcoAfter {
        sketch_ingestion_dollars: sketch_ingestion,
        s3_ingestion_dollars: s3_ingestion,
        sketch_query_dollars: sketch_query,
        s3_query_dollars: s3_query,
        sketch_storage_dollars: sketch_storage,
        s3_storage_dollars: s3_storage,
        compute_dollars: compute,
        total_dollars: total,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_pricing_is_reasonable() {
        let p = CloudPricing::default();
        assert!((p.grafana_per_1k_series_1dpm - 6.50).abs() < f64::EPSILON);
        assert!((p.s3_storage_per_gb_month - 0.023).abs() < f64::EPSILON);
        assert!((p.s3_put_per_1k - 0.005).abs() < f64::EPSILON);
        assert!((p.s3_get_per_1k - 0.0004).abs() < f64::EPSILON);
        assert!((p.s3_transfer_per_gb - 0.09).abs() < f64::EPSILON);
        assert!((p.ec2_sketch_instance_per_hour - 0.384).abs() < f64::EPSILON);
    }

    #[test]
    fn test_100k_series_15s_scrape() {
        let workload = TcoWorkload {
            series_count: 100_000,
            samples_per_sec: 1.0,
            bytes_per_sample: 100,
            scrape_interval_secs: 15,
            queries_per_sec: 1.0,
            query_window_secs: 300,
            retention_days: 30,
            sketch_compression_ratio: 0.05,
            delta_compression_ratio: 0.3,
        };
        let pricing = CloudPricing::default();
        let est = estimate_tco(&workload, &pricing);

        assert!(
            est.before.total_dollars > 0.0,
            "before total should be positive"
        );
        assert!(
            est.after.total_dollars > 0.0,
            "after total should be positive"
        );
        assert!(
            est.savings_percent > 50.0,
            "expected >50% savings for 100K series, got {:.1}%",
            est.savings_percent
        );
        assert!(est.monthly_savings_dollars > 0.0, "should save money");
    }

    #[test]
    fn test_1m_series_high_cardinality() {
        let workload = TcoWorkload {
            series_count: 1_000_000,
            samples_per_sec: 1.0,
            bytes_per_sample: 150,
            scrape_interval_secs: 15,
            queries_per_sec: 5.0,
            query_window_secs: 600,
            retention_days: 90,
            sketch_compression_ratio: 0.04,
            delta_compression_ratio: 0.25,
        };
        let pricing = CloudPricing::default();
        let est = estimate_tco(&workload, &pricing);

        assert!(est.before.total_dollars > est.after.total_dollars);
        assert!(
            est.savings_percent > 50.0,
            "expected >50% savings at 1M series, got {:.1}%",
            est.savings_percent
        );
        // At 1M series we need ceil(1M/100K) = 10 instances.
        let expected_compute = pricing.ec2_sketch_instance_per_hour * HOURS_PER_MONTH * 10.0;
        assert!(
            (est.after.compute_dollars - expected_compute).abs() < 0.01,
            "compute should be ~${:.2}, got ${:.2}",
            expected_compute,
            est.after.compute_dollars
        );
    }

    #[test]
    fn zero_series_returns_zero() {
        let workload = TcoWorkload {
            series_count: 0,
            ..Default::default()
        };
        let pricing = CloudPricing::default();
        let est = estimate_tco(&workload, &pricing);

        // Before costs should be zero (no series).
        assert!(
            est.before.ingestion_dollars.abs() < f64::EPSILON,
            "ingestion should be 0"
        );
        assert!(
            est.before.storage_dollars.abs() < f64::EPSILON,
            "storage should be 0"
        );
        // After still has a minimum 1-instance compute cost.
        assert!(
            est.after.compute_dollars > 0.0,
            "compute has a 1-instance minimum"
        );
        // But sketch/s3 ingestion should be zero.
        assert!(
            est.after.sketch_ingestion_dollars.abs() < f64::EPSILON,
            "sketch ingestion should be 0 for 0 series"
        );
        assert!(
            est.after.s3_ingestion_dollars.abs() < f64::EPSILON,
            "s3 ingestion should be 0 for 0 series"
        );
    }

    #[test]
    fn sketch_compression_ratio_affects_result() {
        let pricing = CloudPricing::default();

        let low_ratio = TcoWorkload {
            series_count: 100_000,
            sketch_compression_ratio: 0.03,
            ..Default::default()
        };
        let high_ratio = TcoWorkload {
            series_count: 100_000,
            sketch_compression_ratio: 0.10,
            ..Default::default()
        };

        let est_low = estimate_tco(&low_ratio, &pricing);
        let est_high = estimate_tco(&high_ratio, &pricing);

        // Higher compression ratio means more sketch bytes, so higher after cost.
        assert!(
            est_high.after.sketch_ingestion_dollars > est_low.after.sketch_ingestion_dollars,
            "higher sketch ratio should cost more: {:.4} vs {:.4}",
            est_high.after.sketch_ingestion_dollars,
            est_low.after.sketch_ingestion_dollars
        );
        // Before cost should be the same (independent of sketch ratio).
        assert!(
            (est_high.before.total_dollars - est_low.before.total_dollars).abs() < f64::EPSILON,
            "before cost should not change with sketch ratio"
        );
    }

    #[test]
    fn delta_compression_ratio_affects_result() {
        let pricing = CloudPricing::default();

        let low_delta = TcoWorkload {
            series_count: 100_000,
            delta_compression_ratio: 0.2,
            ..Default::default()
        };
        let high_delta = TcoWorkload {
            series_count: 100_000,
            delta_compression_ratio: 0.5,
            ..Default::default()
        };

        let est_low = estimate_tco(&low_delta, &pricing);
        let est_high = estimate_tco(&high_delta, &pricing);

        assert!(
            est_high.after.sketch_ingestion_dollars > est_low.after.sketch_ingestion_dollars,
            "higher delta ratio should increase sketch ingestion cost"
        );
    }

    #[test]
    fn retention_days_affects_storage() {
        let pricing = CloudPricing::default();

        let short = TcoWorkload {
            retention_days: 7,
            ..Default::default()
        };
        let long = TcoWorkload {
            retention_days: 90,
            ..Default::default()
        };

        let est_short = estimate_tco(&short, &pricing);
        let est_long = estimate_tco(&long, &pricing);

        assert!(
            est_long.before.storage_dollars > est_short.before.storage_dollars,
            "longer retention should cost more storage"
        );
        assert!(
            est_long.after.s3_storage_dollars > est_short.after.s3_storage_dollars,
            "longer retention should cost more S3 storage"
        );
    }
}
