//! Verify the §6.4 accuracy envelope lands on the Prometheus
//! HTTP response as an ASAP-extension `accuracy` field plus a
//! human-readable `infos` mirror, in the A+C schema agreed in
//! the design discussion.
//!
//! * **A** — top-level `accuracy: { epsilon, delta, kind, per_segment }`
//! * **C** — `infos: ["accuracy: ε=..., δ=..., kind=..."]`
//!
//! Standard PromQL clients ignore both fields; Grafana 11+
//! renders `infos` inline; custom panels read `accuracy`.

#[cfg(test)]
use crate::drivers::query::adapters::PrometheusResponse;
use crate::storage_engines::sketch_db::{
    AccuracyEnvelope, AccuracyKind, AccuracyProfile, PerSegmentAccuracy,
};
use serde_json::Value;

fn hll_profile() -> AccuracyProfile {
    AccuracyProfile {
        epsilon: 0.008125,
        delta: None,
        kind: AccuracyKind::RelativeCardinality,
    }
}

#[test]
fn prometheus_response_carries_accuracy_top_level_and_infos_mirror() {
    let envelope = AccuracyEnvelope::single(hll_profile());
    let resp = PrometheusResponse::success(serde_json::json!({
        "resultType": "vector",
        "result": [],
    }))
    .with_accuracy(envelope.clone());

    let json: Value = serde_json::to_value(&resp).unwrap();

    // A: structured accuracy top-level.
    let accuracy = &json["accuracy"];
    assert!(!accuracy.is_null(), "accuracy field should be present");
    assert_eq!(accuracy["epsilon"], 0.008125);
    assert!(accuracy["delta"].is_null());
    assert_eq!(accuracy["kind"], "relative_cardinality");
    assert!(
        accuracy
            .get("per_segment")
            .map(|v| v.as_array().unwrap().is_empty())
            .unwrap_or(true),
        "per_segment must be absent / empty for single-schema queries"
    );

    // C: infos mirror.
    let infos = json["infos"].as_array().expect("infos should be an array");
    assert_eq!(infos.len(), 1);
    let line = infos[0].as_str().unwrap();
    assert!(
        line.contains("ε=0.008125")
            && line.contains("δ=unknown")
            && line.contains("relative_cardinality"),
        "infos summary must include ε, δ, kind: got {line}"
    );
}

#[test]
fn prometheus_response_without_accuracy_skips_both_fields() {
    let resp = PrometheusResponse::success(serde_json::json!({
        "resultType": "vector",
        "result": [],
    }));
    let json = serde_json::to_string(&resp).unwrap();
    // Both top-level extensions must be absent so the wire shape
    // stays byte-identical to standard Prometheus when accuracy
    // isn't populated (e.g. fallback-only responses).
    assert!(
        !json.contains("\"accuracy\""),
        "accuracy must be omitted: {json}"
    );
    assert!(!json.contains("\"infos\""), "infos must be omitted: {json}");
}

#[test]
fn prometheus_response_per_segment_contains_all_segments_with_worst_case_top() {
    let segments = vec![
        PerSegmentAccuracy {
            agg_id: 1,
            range_ms: [1_000, 2_000],
            profile: AccuracyProfile {
                epsilon: 0.01,
                delta: Some(0.0),
                kind: AccuracyKind::RelativeQuantile,
            },
        },
        PerSegmentAccuracy {
            agg_id: 2,
            range_ms: [2_000, 3_000],
            profile: AccuracyProfile {
                epsilon: 0.05,
                delta: Some(0.01),
                kind: AccuracyKind::RankQuantile,
            },
        },
    ];
    let envelope = AccuracyEnvelope::from_segments(segments).unwrap();
    // Worst-case envelope = (max ε=0.05, max δ=0.01, first
    // non-Exact kind wins for `kind`).
    assert_eq!(envelope.profile.epsilon, 0.05);
    assert_eq!(envelope.profile.delta, Some(0.01));
    assert_eq!(envelope.profile.kind, AccuracyKind::RelativeQuantile);

    let resp = PrometheusResponse::success(serde_json::json!({
        "resultType": "vector",
        "result": [],
    }))
    .with_accuracy(envelope);
    let json: Value = serde_json::to_value(&resp).unwrap();

    assert_eq!(json["accuracy"]["epsilon"], 0.05);
    let per_seg = json["accuracy"]["per_segment"].as_array().unwrap();
    assert_eq!(per_seg.len(), 2);
    assert_eq!(per_seg[0]["agg_id"], 1);
    assert_eq!(per_seg[0]["range_ms"], serde_json::json!([1_000, 2_000]));
    assert_eq!(per_seg[1]["agg_id"], 2);

    // `infos` summary flags the multi-segment shape.
    let infos = json["infos"].as_array().unwrap();
    assert!(infos[0]
        .as_str()
        .unwrap()
        .contains("schema-timeline segments"));
}

#[test]
fn accuracy_coexists_with_warnings_without_interference() {
    // The wire contract: `warnings` stays for partial-result
    // advisories, `accuracy` is orthogonal. Both must coexist.
    let envelope = AccuracyEnvelope::single(hll_profile());
    let resp = PrometheusResponse::success_with_warnings(
        serde_json::json!({
            "resultType": "vector",
            "result": [],
        }),
        vec!["partial: 2 schemas".to_string()],
    )
    .with_accuracy(envelope);
    let json: Value = serde_json::to_value(&resp).unwrap();
    assert_eq!(
        json["warnings"].as_array().unwrap()[0].as_str().unwrap(),
        "partial: 2 schemas"
    );
    assert_eq!(json["accuracy"]["kind"], "relative_cardinality");
    assert_eq!(
        json["infos"].as_array().unwrap().len(),
        1,
        "infos should carry only the accuracy summary, not the warning"
    );
}

#[test]
fn promql_standard_client_can_decode_response_ignoring_extensions() {
    // A minimal "standard Prometheus" response decoder — just
    // the fields the upstream API defines. It must parse our
    // extended response without error (unknown fields
    // ignored). This locks down the "zero-risk extension" claim.
    let envelope = AccuracyEnvelope::single(hll_profile());
    let resp = PrometheusResponse::success(serde_json::json!({
        "resultType": "vector",
        "result": [],
    }))
    .with_accuracy(envelope);
    let wire = serde_json::to_string(&resp).unwrap();

    #[derive(serde::Deserialize)]
    struct StandardPromResponse {
        status: String,
        #[serde(default)]
        _data: Option<serde_json::Value>,
    }
    let standard: StandardPromResponse = serde_json::from_str(&wire).unwrap();
    assert_eq!(standard.status, "success");
}
