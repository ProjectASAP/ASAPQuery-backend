//! Backend coordination of per-edge update sampling.
//!
//! Edges periodically report observation rates and receive a whole-sketch
//! ε-floor grant, `p_i = 1/(1+ε²·rate_i)`. Each grant depends only on that
//! edge's rate. Alert decisions belong to query-time reads of backend state.
//!
//! * [`coordinator`] maintains edge sampling state.
//! * [`epoch`] aligns tumbling epochs.
//! * [`server`] implements the bidirectional monitor service.
//! * [`sampling_alloc`] defines the sampling law and eligible sketch families.

pub mod coordinator;
pub mod epoch;
pub mod sampling_alloc;
pub mod server;

pub use coordinator::{Action, Functional, Monitor, MonitorConfig};
pub use sampling_alloc::epsilon_sample_floor;
pub use server::{MonitorCoordinator, MonitorServiceImpl};
