//! Control-plane-client surface.
//!
//! Two cooperating submodules — bidirectional plumbing between the
//! data plane and the control plane (`control_plane` crate):
//!
//!   Fetches plan config from the control plane. Optional; today's
//!   binary path does not consume it.
//! * [`miss_notifier`] — outbound capability-miss notifications. The
//!   ASAP-tier engine fires fire-and-forget POSTs here when a query
//!   has no compatible stored aggregation, so the control plane can
//!   generate a new sketch plan and push it back via the streaming
//!   config endpoint.

pub mod miss_notifier;

pub use miss_notifier::{spawn_capability_miss_notify, ControlPlaneClient, HttpControlPlaneClient};
