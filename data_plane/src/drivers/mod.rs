pub mod control_plane_client;
pub mod ingest;
pub mod query;

// Re-export commonly used types for convenience
pub use control_plane_client::{spawn_capability_miss_notify, ControlPlaneClient, HttpControlPlaneClient};
pub use ingest::{OtlpReceiver, OtlpReceiverConfig};
pub use query::{AdapterConfig, HttpServer, HttpServerConfig};
