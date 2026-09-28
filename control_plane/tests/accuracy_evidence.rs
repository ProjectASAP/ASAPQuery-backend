//! Evidence applicability is independent of prices and observed resource use.
#[path = "support/erp_contract.rs"]
mod fixture;
use control_plane::physical::erp::{ErpAccuracyMode, ErpParameterDecision, ErpPlanningInput};
use planner_types::post_asap::{SketchAlgorithm, SketchParams};
use serde_json::json;

fn decision(policy: &ErpPlanningInput) -> ErpParameterDecision {
    policy.select(
        SketchAlgorithm::Cms,
        0.01,
        SketchParams::Cms {
            width: 4096,
            depth: 5,
        },
    )
}

/// Wrong distribution/implementation/metric, insufficient trials and excessive
/// error reject empirical admission even when that record is nearly free.
#[test]
fn inapplicable_accuracy_cannot_be_rescued_by_a_low_price() {
    assert!(matches!(
        decision(&fixture::policy(ErpAccuracyMode::Empirical)),
        ErpParameterDecision::Empirical { .. }
    ));
    for mismatch in [
        "distribution",
        "implementation",
        "metric",
        "trials",
        "error",
        "parameters",
    ] {
        let mut policy = fixture::policy(ErpAccuracyMode::Empirical);
        let row = &mut policy.artifact.records[0];
        row.resources.update_cpu_seconds = 1e-30;
        row.resources.query_cpu_seconds = 1e-30;
        match mismatch {
            "distribution" => row.distribution = json!({"different_population":true}),
            "implementation" => row.implementation = "different-implementation".into(),
            "metric" => row.error_metrics.clear(),
            "trials" => row.trials = 1,
            "error" => {
                row.error_metrics.insert("relative_error".into(), 0.2);
            }
            "parameters" => row.parameters = json!({"rows":0,"cols":0}),
            _ => unreachable!(),
        }
        assert!(
            matches!(
                decision(&policy),
                ErpParameterDecision::ExactFallback { .. }
            ),
            "{mismatch}"
        );
    }
}

/// Theoretical fallback retains its identity; it is never relabeled empirical.
#[test]
fn missing_evidence_preserves_the_declared_fallback_mode() {
    for mode in [ErpAccuracyMode::Hybrid, ErpAccuracyMode::Empirical] {
        let mut policy = fixture::policy(mode);
        policy.artifact.records.clear();
        match (mode, decision(&policy)) {
            (ErpAccuracyMode::Hybrid, ErpParameterDecision::TheoreticalFallback { params, .. }) => {
                assert_eq!(
                    params,
                    SketchParams::Cms {
                        width: 4096,
                        depth: 5
                    }
                )
            }
            (ErpAccuracyMode::Empirical, ErpParameterDecision::ExactFallback { .. }) => {}
            unexpected => panic!("wrong evidence/fallback provenance: {unexpected:?}"),
        }
    }
}

/// Both parameter choice and error must be traceable to the same admitted row.
#[test]
fn admitted_accuracy_retains_record_identity() {
    let policy = fixture::policy(ErpAccuracyMode::Empirical);
    let ErpParameterDecision::Empirical {
        record_id,
        observed_error,
        params,
        ..
    } = decision(&policy)
    else {
        panic!("matching evidence should be admitted")
    };
    assert_eq!(record_id, policy.artifact.records[0].id);
    assert_eq!(
        observed_error,
        policy.artifact.records[0].error_metrics["relative_error"]
    );
    assert_eq!(
        params,
        SketchParams::Cms {
            width: 512,
            depth: 3
        }
    );
}
