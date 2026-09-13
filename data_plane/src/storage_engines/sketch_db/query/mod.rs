//! Sketch query state decoding, composition, and result types.
//!
//! [`ASAPTierResult`] carries timestamped scalar samples from summary execution
//! to the query engine. Query planning and execution live outside this module.

pub mod asap_tier_result;
pub mod decoders;
pub mod delta_apply;
pub mod timeline;
pub mod timeline_dispatch;
pub mod window_merger;

pub use asap_tier_result::ASAPTierResult;
