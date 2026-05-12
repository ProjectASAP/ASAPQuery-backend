pub mod adapters;
pub mod fallback;
pub mod servers;

// Re-export commonly used types for convenience
pub use adapters::{create_http_adapter, AdapterConfig, HttpProtocolAdapter};
pub use fallback::FallbackClient;
pub use servers::{HttpServer, HttpServerConfig};

// `controller_client` moved up one level in the 2026-05 reorg
// (drivers/query/controller_client.rs → drivers/controller_client/miss_notifier.rs).
// Existing import sites that say `drivers::query::controller_client::X` get
// a redirect alias so the cutover doesn't touch every caller in one PR.
pub use crate::drivers::controller_client;
