//! The data plane's ingest side: routing samples to the plan's raw data and
//! closing windows ([`ingestion`]), building views by push pipelines
//! ([`push`]), and the threads that run them ([`engine`]).

pub mod engine;
pub mod ingestion;
pub mod push;
