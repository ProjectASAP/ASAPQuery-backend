//! Controller-client surface.
//!
//! Two cooperating submodules — bidirectional plumbing between the
//! data plane and the control plane (controller crate):
//!
//! * [`config_fetcher`] — outbound `GET /api/v1/plan/:metric` polling.
//!   Fetches plan config from the controller. Optional; today's
//!   binary path does not consume it.
//! * [`miss_notifier`] — outbound capability-miss notifications. The
//!   warm-tier engine fires fire-and-forget POSTs here when a query
//!   has no compatible stored aggregation, so the controller can
//!   generate a new sketch plan and push it back via the streaming
//!   config endpoint.

pub mod config_fetcher;
pub mod miss_notifier;

pub use miss_notifier::{spawn_capability_miss_notify, ControllerClient, HttpControllerClient};
