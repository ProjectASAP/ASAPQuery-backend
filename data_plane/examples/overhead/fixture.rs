use anyhow::{ensure, Result};
use asap_sketchlib::{DdSketch, MessagePackCodec};
use data_plane::drivers::query::{
    adapters::AdapterConfig, servers::http::validate_and_build_runtime_plan,
};
use data_plane::query_engines::{
    routing::query_engine_routing::QueryEngine, QueryForwardingPolicy, QueryResult,
};
use data_plane::storage_engines::{
    sketch_db::index::*,
    types::{ActivePhysicalPlanHandle, BackendStorageRouting},
};
use data_plane::{ASAPQueryEngine, HttpServer, HttpServerConfig};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

#[path = "../../tests/support/physical_fixture.rs"]
#[allow(dead_code)]
mod physical_fixture;

pub const QUERY: &str = "quantile_over_time(0.5, overhead_values[1s])";
pub const TIME: u64 = 600_000;
pub struct Fixture {
    pub raw: Vec<f64>,
    pub sketch: DdSketch,
    pub expected: f64,
    pub exact_expected: f64,
    pub engine: Arc<ASAPQueryEngine>,
    pub server: HttpServer,
    pub setup_seconds: BTreeMap<&'static str, f64>,
    pub installed_plan: serde_json::Value,
}
pub fn exact(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    let middle = sorted.len() / 2;
    let (_, upper, _) = sorted.select_nth_unstable_by(middle, f64::total_cmp);
    let upper = *upper;
    if sorted.len().is_multiple_of(2) {
        (sorted[..middle]
            .iter()
            .copied()
            .max_by(f64::total_cmp)
            .unwrap()
            + upper)
            / 2.0
    } else {
        upper
    }
}
impl Fixture {
    pub fn new(samples: usize) -> Result<Self> {
        ensure!(samples > 0, "samples must be positive");
        let mut setup_seconds = BTreeMap::new();
        let started = Instant::now();
        // Deliberately unsorted, reproducible input; exact includes scratch allocation
        // and selection, while prebuilt sketch construction is outside measurement.
        let raw: Vec<_> = (0..samples)
            .map(|i| 1.0 + ((i * 7919) % 10007) as f64)
            .collect();
        setup_seconds.insert("fixture.raw_input", started.elapsed().as_secs_f64());
        let started = Instant::now();
        let mut sketch = DdSketch::new(0.01);
        for value in &raw {
            sketch.update(*value);
        }
        setup_seconds.insert(
            "fixture.direct_sketch_build",
            started.elapsed().as_secs_f64(),
        );
        let expected = sketch.quantile(0.5).unwrap();
        let exact_expected = exact(&raw);
        ensure!(
            (expected - exact(&raw)).abs() <= exact(&raw) * 0.02,
            "sketch accuracy gate failed"
        );
        let config: asap_types::PrecomputeMaterialization = serde_json::from_value(
            serde_json::json!({
                "aggregation_type":"DDSketch", "aggregation_sub_type":"", "metric":"overhead_values",
                "window_size":1,"slide_interval":1,"window_type":"tumbling","num_aggregates_to_retain":10,
                "parameters":{"alpha":0.01},"pane_origin_ms":0,"partitioning":"per_entity",
                "window_layout":{"kind":"pane","pane_secs":1},"grouping_labels":{"labels":[]},
                "aggregated_labels":{"labels":[]},"rollup_labels":{"labels":[]},
                "spatial_filter":"","spatial_filter_normalized":"","original_yaml":""
            }),
        )?;
        let started = Instant::now();
        let plan = physical_fixture::artifact_from_materializations(vec![config.clone()]);
        setup_seconds.insert("fixture.bound_plan", started.elapsed().as_secs_f64());
        let installed_plan = serde_json::to_value(&plan)?;
        let started = Instant::now();
        let store = Arc::new(SketchStore::new());
        store
            .install_precompute_plan(
                Arc::new(plan.summary_catalog.clone()),
                &plan.precompute_plan,
            )
            .map_err(anyhow::Error::msg)?;
        setup_seconds.insert("sds.install_catalog", started.elapsed().as_secs_f64());
        let started = Instant::now();
        let skconfig = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        store.register(SummarySeriesMetadata {
            storage_handle: 1,
            metric_name: "overhead_values".into(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch))),
            accuracy: Some(AccuracyBound::from_config(&skconfig)),
            agg_kind: AggKind::Sketch {
                algorithm: SketchAlgorithm::DDSketch,
                config: skconfig,
                spatial_filter_canonical: String::new(),
            },
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: config.policy_fingerprint(),
        });
        ensure!(
            store.append_sample(
                1,
                BTreeMap::new(),
                (TIME - 1000, TIME),
                SketchSampleState {
                    bytes: sketch.to_msgpack().map_err(|e| anyhow::anyhow!("{e}"))?,
                    encoding: SketchEncoding::MsgpackFull,
                }
            ),
            "sample admission failed"
        );
        setup_seconds.insert("sds.register_and_publish", started.elapsed().as_secs_f64());
        let started = Instant::now();
        let active = ActivePhysicalPlanHandle::new(
            validate_and_build_runtime_plan(plan, Arc::new(BackendStorageRouting::empty()))
                .map_err(anyhow::Error::msg)?,
        );
        setup_seconds.insert(
            "deployment.validate_and_activate",
            started.elapsed().as_secs_f64(),
        );
        let engine = Arc::new(
            ASAPQueryEngine::new(1000)
                .with_sketch_index(store.clone())
                .with_active_physical_plan(active)
                .with_query_forwarding_policy(QueryForwardingPolicy::Disabled),
        );
        let server = HttpServer::new(
            HttpServerConfig {
                port: 0,
                handle_http_requests: true,
                adapter_config: AdapterConfig::prometheus_promql(String::new(), false)
                    .with_query_forwarding_policy(QueryForwardingPolicy::Disabled),
            },
            engine.clone(),
            store,
        );
        Ok(Self {
            raw,
            sketch,
            expected,
            exact_expected,
            engine,
            server,
            setup_seconds,
            installed_plan,
        })
    }
    pub async fn backend(&self) -> Result<f64> {
        match self.engine.execute_at(QUERY, TIME).await? {
            QueryResult::Vector(v) if v.values.len() == 1 => Ok(v.values[0].value),
            other => anyhow::bail!("unexpected result {other:?}"),
        }
    }
}
