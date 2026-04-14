//! Vendored Rust bindings for DataCollector's modified OTLP proto.
//!
//! Adds first-class `DDSketch`, `KLLSketch`, `CountSketch`, `CountMinSketch`,
//! and `HLLSketch` variants to the `Metric.data` oneof on tags 13–17, plus
//! per-sketch encoding enums (with full and `*_DELTA` variants) and a
//! `series_id` field on every data point. See
//! `docs/pipeline-query-catalog.md` §5.4 in the DataCollector repo for the
//! full motivation and the corresponding upstream proto path.
//!
//! The generated Rust modules mirror `opentelemetry_proto::tonic::*` so call
//! sites only need to swap `opentelemetry_proto` for `asap_otel_proto`.

#![allow(clippy::all)]

pub mod tonic {
    pub mod common {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.common.v1");
        }
    }

    pub mod resource {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.resource.v1");
        }
    }

    pub mod metrics {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.metrics.v1");
        }
    }

    pub mod collector {
        pub mod metrics {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.collector.metrics.v1");
            }
        }
    }
}
