//! The data plane's ingest side: routing samples to the plan's raw data and
//! closing windows ([`ingestion`]), building views by push pipelines
//! ([`push`]), taking in the views a collector builds ([`collector`]), and
//! the threads that run them ([`engine`]).

pub mod collector;
pub mod engine;
pub mod ingestion;
pub mod push;
