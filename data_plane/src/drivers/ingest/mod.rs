pub mod otel;
pub mod series_resolver;

pub use otel::{OtlpReceiver, OtlpReceiverConfig};
pub use series_resolver::{canonical_attrs_fingerprint, SeriesIdResolver};
