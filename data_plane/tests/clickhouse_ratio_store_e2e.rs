use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::array::{Float64Array, StringArray};
use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
use control_plane::{
    clickhouse::{compile_clickhouse_workload, ClickHouseSqlWorkload, ClickHouseSqlWorkloadEntry},
    physical::{
        compiler::{PlanEnvelope, PrecomputePlan, TransmissionPlan},
        summary_catalog::SummaryCatalog,
    },
    query_plan::ExecutableQueryPlan,
};
use data_plane::{
    precompute_engine::operators::IncreaseAccumulator,
    query_engines::asap_clickhouse_query_engine::execution::{
        execute_sql_dag, ClickHouseDagOutcome,
    },
    storage_engines::{
        sketch_db::index::{AggKind, Capability, SketchInstanceMetadata, SketchStore},
        types::Measurement,
    },
};
use planner_types::pre_asap::{Column, DataType, Schema};

#[tokio::test]
async fn compiled_ratio_bundle_executes_two_real_store_summaries() {
    let configs = ["errors_total", "requests_total"].map(|metric| {
        let mut config = PrecomputeMaterialization::new(
            AggregationType::Increase,
            String::new(),
            Default::default(),
            KeyByLabelNames::new(vec!["labels".into()]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            300,
            300,
            WindowKind::Tumbling,
            String::new(),
            metric.into(),
            None,
            Some("raw_samples".into()),
            Some("value".into()),
        );
        config.pane_origin_ms = Some(0);
        config
    });
    let sds = SummaryCatalog::from_materializations(76, 1, &configs).unwrap();
    let envelope = PlanEnvelope {
        plan_id: 76,
        plan_version: 1,
        generated_at_unix_ms: 0,
        activation_unix_ms: 0,
        expiry_unix_ms: None,
        backend_compat: "test".into(),
        planner_revision: "test".into(),
        capability_snapshot_id: "test".into(),
    };
    let mut precompute =
        PrecomputePlan::build_backend_local(envelope.clone(), configs.to_vec()).unwrap();
    precompute.summary_catalog = Some(sds.reference().unwrap());
    let mut transmission =
        TransmissionPlan::build(envelope, &precompute, &Default::default()).unwrap();
    transmission.summary_catalog = precompute.summary_catalog.clone();
    let sql = "SELECT a.labels, a.v / b.v AS ratio FROM \
        (SELECT labels, asap_rate(value, ts_ms, 300000) AS v FROM raw_samples WHERE metric='errors_total' GROUP BY labels) a \
        INNER JOIN \
        (SELECT labels, asap_rate(value, ts_ms, 300000) AS v FROM raw_samples WHERE metric='requests_total' GROUP BY labels) b \
        ON a.labels=b.labels";
    let compiled = compile_clickhouse_workload(&ClickHouseSqlWorkload {
        sds: sds.clone(),
        precompute_plan: precompute,
        transmission_plan: transmission,
        tables: HashMap::from([(
            "raw_samples".into(),
            Schema::with_time_index(
                vec![
                    Column::new("metric", DataType::Utf8, false),
                    Column::new("labels", DataType::Utf8, false),
                    Column::new("ts_ms", DataType::Timestamp, false),
                    Column::new("value", DataType::Float64, false),
                ],
                2,
                vec![vec![2, 1]],
            ),
        )]),
        accuracy: planner_types::types::AccuracyTarget::Exact,
        queries: vec![ClickHouseSqlWorkloadEntry {
            sql: sql.into(),
            planning_sql: None,
            start_ms: 0,
            end_ms: 300_000,
            cumulative: true,
        }],
    })
    .await
    .unwrap();
    let plan: ExecutableQueryPlan =
        serde_json::from_value(compiled.plans[0]["runtime"]["executable"].clone()).unwrap();

    let store = SketchStore::new();
    store
        .install_summary_catalog(std::sync::Arc::new(sds.clone()))
        .unwrap();
    let group = BTreeMap::from([("labels".into(), "api".into())]);
    for (sid, config, end_value) in [(11, &configs[0], 60.0), (12, &configs[1], 300.0)] {
        store.register(SketchInstanceMetadata {
            sid,
            metric_name: config.metric.clone(),
            group_by_keys: BTreeSet::from(["labels".into()]),
            capability: Some(Capability::ExactAgg(AggregationType::Increase)),
            agg_kind: AggKind::ExactAgg {
                agg_type: AggregationType::Increase,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: None,
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: config.policy_fingerprint(),
        });
        store.append_precompute(
            sid,
            group.clone(),
            (0, 300_000),
            Box::new(IncreaseAccumulator::new(
                Measurement::new(0.0),
                0,
                Measurement::new(end_value),
                300_000,
            )),
        );
    }

    let ClickHouseDagOutcome::Accelerated(result) =
        execute_sql_dag(&store, &plan, &sds, 0, 300_000, true)
    else {
        panic!("compiled ratio plan did not execute warm")
    };
    let batch = &result.batches[0];
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "api"
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.2
    );
}
