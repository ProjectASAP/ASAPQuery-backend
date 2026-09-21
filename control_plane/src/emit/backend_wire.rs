//! Backend wire construction for a compiled physical plan.
//!
//! Two documents, one classifier each:
//!
//! * the storage-routing table, which maps each metric's materialized summary
//!   families to the query shapes the ASAP tier serves natively versus the
//!   ones that belong to the archive;
//! * the aggregation and readout JSON the backend's `AggregationConfig`
//!   parser consumes.
//!
//! `backend_plan::from_stage_config` reuses [`build_backend_aggregation_json`]
//! rather than re-deriving the field mapping, so `BackendPlan` materializations
//! and the JSON wire format share one `PolicyFingerprint` identity space.

use planner_types::post_asap::{ExactKind, SketchAlgorithm, SketchParams, SummaryFamilyType};
use serde_json::{json, Value as JsonValue};

use crate::physical::backend_stage::{AggregationInput, BackendAggregation};

// ── Window-size clamping (MVP blocker B4) ─────────────────────────────────────
//
// The controller derives `window_secs` from the workload's matrix-selector
// range (`metric[30s]` → 30s). Two bounds keep the emitted value sane:
//
//   * Lower bound 5s — below this the sketch processor mints a new
//     window before it has enough samples for the family's quality
//     guarantees, and the per-flush cardinality on the sid catalog
//     explodes (one (sid, window) row per few seconds).
//   * Upper bound 60s — above this the user's query range no longer
//     contains a closed sketch window, and replay queries return NoData
//     while the warm tier still owns the metric. 60s is also the
//     historical default the legacy single-pipeline emitter shipped with,
//     so clamping here preserves backwards-compat for plans without an
//     explicit range.
//
// `None` means "no `Window` node in the typed L5 — agent runs in batch
// mode, no window_duration in the YAML"; we pass that straight through.
//
// Centralised here so [`build_edge_processor_block`] (sketch processor
// `window_duration`) and the [`BackendAggregation`] consumer
// ([`emit_backend_streaming_config_json`]) clamp to the same bounds. The
// downstream backend's reducer keys windows by the emitted value, so
// the two MUST agree or replay-vs-warm answers go out of sync.

/// Lower bound for [`clamp_window_secs`].
pub const MIN_WINDOW_SECS: u64 = 5;

/// Upper bound for [`clamp_window_secs`]. Matches the legacy default
/// the pre-B4 emitter shipped with.
pub const MAX_WINDOW_SECS: u64 = 60;

/// Cardinality at which a per-series HLL is emitted DENSE rather than sparse
/// (ASAPCollector#472 follow-up to PR #358).
///
/// The sketchlib-go in-memory sparse HLL base (`NewHLLWrapperSparse`)
/// auto-promotes to the dense register array once roughly this many registers
/// become non-zero (the sparse representation stops saving memory past that
/// point). A per-series HLL whose known distinct-key count
/// ([`crate::workload::WorkloadEntry::distinct_keys_per_window`]) is at or
/// above this crossover would promote almost immediately, so starting it sparse
/// only pays one-time promotion churn — we emit it dense instead.
///
/// This is a HEURISTIC: distinct *keys* map to non-zero *registers* only
/// approximately (hash collisions mean registers < keys at high cardinality),
/// so the crossover is fuzzy. Being slightly off has NO correctness or accuracy
/// impact — the sparse base is lossless and serializes byte-identically to
/// dense for the same inputs; an over- or under-estimate at worst costs (or
/// saves) a single in-memory sparse→dense promotion. The value tracks the
/// in-memory promotion threshold (~4096 non-zero registers); the wire-crossover
/// constant the agent uses elsewhere is larger (~6000).
pub const DENSE_CROSSOVER: u64 = 4096;

/// Clamp a derived window size to `[MIN_WINDOW_SECS, MAX_WINDOW_SECS]`.
/// `None` is preserved as `None` so callers can keep the
/// "no-window / batch-mode" branch distinguishable from a clamped value.
pub fn clamp_window_secs(w: Option<u64>) -> Option<u64> {
    w.map(|s| s.clamp(MIN_WINDOW_SECS, MAX_WINDOW_SECS))
}

pub const DEFAULT_TENANT: &str = "default";

pub fn storage_routing_document(
    tenant: &str,
    metric_algorithms: &[(String, Vec<SketchAlgorithm>)],
) -> JsonValue {
    let metrics_json: Vec<JsonValue> = metric_algorithms
        .iter()
        .map(|(metric_name, algorithms)| build_routing_entry(metric_name, algorithms))
        .collect();
    json!({
        "tenant": tenant,
        "default_engine": "asap_query",
        "metrics": metrics_json,
    })
}
// ── Internals ─────────────────────────────────────────────────────────────────

/// Build the JSON `metrics:` entry for one metric — picks per-shape targets
/// from the summary families the plan landed at the backend.
///
/// Returns a JSON object of shape:
/// ```text
/// { "name": <metric>, "targets": [<target>, ...] }
/// ```
/// where each `<target>` is either `{ "engine": <engine>, "applies_to_query_shape": [...] }`
/// or `{ "engine": <engine> }` for the default slot.
fn build_routing_entry(metric_name: &str, algorithms: &[SketchAlgorithm]) -> JsonValue {
    // Sketch-eligible shapes — the ASAP tier serves these natively
    // because we planned a sketch for them.
    let mut warm_shapes: Vec<&'static str> = Vec::new();
    let has_quantile_sketch = algorithms
        .iter()
        .any(|k| matches!(k, SketchAlgorithm::DDSketch | SketchAlgorithm::Kll));
    if has_quantile_sketch {
        warm_shapes.push("quantile");
        warm_shapes.push("quantile_over_time");
    }
    let has_hll = algorithms.iter().any(|k| matches!(k, SketchAlgorithm::Hll));
    if has_hll {
        warm_shapes.push("count");
    }
    // Heap-bearing frequency sketches also contribute their routing capability.
    let has_count_sketch = algorithms.iter().any(|k| {
        matches!(
            k,
            SketchAlgorithm::CountSketch | SketchAlgorithm::CountSketchWithHeap
        )
    });
    if has_count_sketch {
        warm_shapes.push("topk");
    }
    let has_cms = algorithms
        .iter()
        .any(|k| matches!(k, SketchAlgorithm::Cms | SketchAlgorithm::CmsWithHeap));
    if has_cms {
        // CMS's `Estimate` readout serves point-count / count queries.
        // If HLL also planned, `count` is already in the list — push
        // only when not already there (keep order stable).
        if !warm_shapes.contains(&"count") {
            warm_shapes.push("count");
        }
    }
    // Sketch-planned `rate / sum / avg / min / max` over the planned
    // ranges — every sketch family the planner emits also tracks the
    // range aggregation needed to answer these from the ASAP tier
    // (the gateway merge processor produces a windowed accumulator).
    if !algorithms.is_empty() {
        warm_shapes.push("rate");
        warm_shapes.push("sum");
        warm_shapes.push("avg");
        warm_shapes.push("min");
        warm_shapes.push("max");
    }

    // One target: the ASAP tier, as the unfiltered default slot. #746
    // deleted the archive tier, so there is no second engine to claim the
    // shapes the sketches cannot answer — the backend forwards those to its
    // Prometheus fallback instead. We do NOT attach
    // `applies_to_query_shape` to this slot: that would make it
    // shape-specific and leave unanticipated shapes with no target at all.
    let targets: Vec<JsonValue> = vec![json!({
        "engine": "asap_query",
    })];

    // The warm-shape list is informational — surface it on a side
    // field for operators / tests to spot-check what the controller
    // decided the ASAP tier serves natively. The backend ignores
    // unknown fields (`#[serde(default)]` on the parser side).
    let mut entry = json!({
        "name": metric_name,
        "targets": targets,
    });
    if !warm_shapes.is_empty() {
        entry["asap_tier_native_shapes"] = json!(warm_shapes);
    }
    entry
}

pub(crate) fn build_backend_aggregation_json(agg: &BackendAggregation) -> JsonValue {
    // Option B (post-PR-#287): when `agg_type_override` is set, use
    // it as the wire `aggregationType` and emit an empty
    // `parameters` object — bypasses the sketch_kind → backend type
    // mapping for ExactAgg(Sum/Increase/Count) rows the Replanner
    // synthesizes for non-sketch (Sum-shaped) workloads. The
    // `sketch_kind` / `sketch_params` fields carry sentinel values
    // in this case and are not emitted on the wire.
    let (aggregation_type, mut parameters) = match &agg.family {
        SummaryFamilyType::ExactAggregate(kind, _) => (
            match kind {
                ExactKind::Sum => "Sum",
                ExactKind::Count => "Count",
                ExactKind::Min => "Min",
                ExactKind::Max => "Max",
                ExactKind::Increase => "Increase",
                ExactKind::Rate => "Rate",
                ExactKind::IRate => "IRate",
            }
            .to_string(),
            json!({}),
        ),
        SummaryFamilyType::Sketch(kind, _) => (
            sketch_algorithm_to_backend_type(kind.algorithm()).to_string(),
            sketch_params_to_json(kind.params()),
        ),
        other => panic!("backend emitter cannot encode summary family {other:?}"),
    };
    // Carry the per-item dimension (e.g. "endpoint"/"service") into the
    // policy parameters so the data-plane ingest can record it on the CMS
    // sid and answer per-item estimate(key). Only set for item_label-mode
    // frequency sketches; a subset content-match keeps policy resolution
    // working for sketches that don't carry it.
    if let Some(label) = &agg.item_label {
        if let Some(obj) = parameters.as_object_mut() {
            obj.insert("item_label".to_string(), JsonValue::String(label.clone()));
        }
    }
    if let Some(mode) = agg.heap_update_mode {
        if let Some(obj) = parameters.as_object_mut() {
            obj.insert("weight_mode".into(), JsonValue::String(mode.into()));
            if mode == "counter_delta" {
                obj.insert("weight_scale".into(), json!(1_000_000));
            }
        }
    }
    // PromQL range selectors are (start, end]. Encode the boundary convention
    // in state identity so legacy half-open panes cannot satisfy this binding.
    if matches!(agg.aggregation_input, AggregationInput::Raw) {
        parameters["promql_right_closed"] = json!(true);
    }
    let aggregation_input = match agg.aggregation_input {
        AggregationInput::SketchEnvelope => "sketch_envelope",
        AggregationInput::Raw => "raw",
    };
    // MVP blocker B4: clamp `windowSize` so the backend's reducer keys
    // windows by the SAME size the agent's sketch processor uses. The
    // backend's `streaming-config.window_size` must match the agent's
    // `window_duration` exactly — drift here de-syncs the warm tier
    // and replay queries return NoData (the backend's pre-compute
    // engine looks for closed windows at the streaming-config size).
    // `agg.window_secs` is u64 (not Option) here; passing through
    // `clamp_window_secs(Some(_))` and unwrapping keeps the contract
    // explicit.
    let window_size =
        clamp_window_secs(Some(agg.window_secs)).expect("clamp_window_secs preserves Some");
    json!({
        "aggregationType": aggregation_type,
        "aggregationSubType": "",
        "metric": agg.metric_name,
        "labels": {
            "grouping": agg.grouping,
            "rollup": Vec::<String>::new(),
            "aggregated": agg.item_label.iter().cloned().collect::<Vec<_>>(),
        },
        "parameters": parameters,
        "windowSize": window_size,
        "windowType": "tumbling",
        "spatialFilter": agg.spatial_filter,
        "aggregationInput": aggregation_input,
    })
}

/// Map a `SketchAlgorithm` to the backend's `AggregationType::Display`
/// string — the same mapping
/// [`crate::config::asapquery_backend::map_sketch_type_to_agg_type`] uses
/// (the strings must match `AggregationType::FromStr` in the backend's
/// `promql_utilities::query_logics::enums`).
///
/// Heap-bearing is now identity, not a params flag (`SketchAlgorithm::CmsWithHeap`
/// / `CountSketchWithHeap`, set by `BindCountSketchOnTopK` — see
/// `physical::post_asap::rules::bind_cms_topk`), so this maps on `kind` alone;
/// `params` is unused but kept for call-site stability. This is what
/// lets the backend's `policy_capability` lookup return
/// `FrequencyTopk(*WithHeap)` for heap-bearing aggregations — required
/// for `topk(...)` queries to bind to the right sids.
fn sketch_algorithm_to_backend_type(kind: &SketchAlgorithm) -> &'static str {
    match kind {
        SketchAlgorithm::UnivMon => "UnivMon",
        SketchAlgorithm::DDSketch => "DDSketch",
        SketchAlgorithm::Kll => "DatasketchesKLL",
        SketchAlgorithm::Hll => "HLL",
        SketchAlgorithm::CountSketchWithHeap => "CountSketchWithHeap",
        SketchAlgorithm::CountSketch => "CountSketch",
        SketchAlgorithm::CmsWithHeap => "CountMinSketchWithHeap",
        SketchAlgorithm::Cms => "CountMinSketch",
        SketchAlgorithm::Kmv | SketchAlgorithm::Theta => unreachable!(
            "sketch_algorithm_to_backend_type: unsupported SketchAlgorithm; \
             no Bind* rule in this repo produces one"
        ),
    }
}

/// Serialize a `SketchParams` payload to a flat JSON object the backend
/// can read directly without round-tripping through the controller's
/// internally-tagged enum form.
fn sketch_params_to_json(p: &SketchParams) -> JsonValue {
    match p {
        SketchParams::UnivMon {
            heap_size,
            sketch_rows,
            sketch_cols,
            layers,
        } => json!({
            "heap_size": heap_size, "sketch_rows": sketch_rows, "sketch_cols": sketch_cols, "layers": layers,
        }),
        SketchParams::Kll { k } => json!({ "k": k }),
        SketchParams::DDSketch { alpha } => json!({ "alpha": alpha }),
        SketchParams::Hll { precision } => json!({ "precision": precision }),
        SketchParams::Cms { width, depth } => json!({ "w": width, "d": depth }),
        SketchParams::CmsWithHeap {
            width,
            depth,
            heap_size,
        } => json!({
            "w": width,
            "d": depth,
            "with_heap": true,
            "heap_size": heap_size,
        }),
        // CountSketch/CountSketchWithHeap: the old arm always emitted
        // `with_heap` (from `CountSketchParams.with_heap: bool`);
        // that boolean is now the kind identity itself.
        SketchParams::CountSketch { width, depth } => {
            json!({ "w": width, "d": depth, "with_heap": false })
        }
        SketchParams::CountSketchWithHeap {
            width,
            depth,
            heap_size,
        } => json!({
            "w": width,
            "d": depth,
            "with_heap": true,
            "heap_size": heap_size,
        }),
        // Exact accumulators never reach here -- see
        // `sketch_kind_to_backend_type`'s doc.
        SketchParams::Kmv { .. } | SketchParams::Theta { .. } => {
            unreachable!(
                "sketch_params_to_json: non-sketch or unsupported SummaryParams; \
             no Bind* rule in this repo produces one"
            )
        }
    }
}
