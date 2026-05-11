pub mod aggregation_config;
pub mod aggregation_reference;
pub mod enums;
pub mod hot_reload_config;
pub mod inference_config;
pub mod key_by_label_values;
pub mod measurement;
pub mod precomputed_output;
pub mod promql_schema;
pub mod query_config;
pub mod streaming_config;
pub mod traits;

pub use aggregation_config::*;
pub use aggregation_reference::*;
pub use enums::*;
pub use hot_reload_config::*;
pub use inference_config::*;
pub use key_by_label_values::*;
pub use measurement::*;
pub use precomputed_output::*;
pub use promql_schema::*;
pub use query_config::*;
pub use streaming_config::*;
pub use traits::*;

// Step-1 of the JSONL deprecation refactor moved
// `backend_storage_routing` into the new `crate::routing` module
// alongside the engine router. Re-export here to keep
// `crate::data_model::BackendStorageRouting` compiling for any
// transitive caller that hasn't been migrated yet.
pub use crate::routing::{
    classify_query_shape, BackendStorageRouting, HotReloadBackendStorageRouting, QueryShape,
    RoutingTarget,
};
