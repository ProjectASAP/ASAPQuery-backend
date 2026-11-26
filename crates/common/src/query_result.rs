//! A query's answer. Every variant carries its own label or column names.

use std::sync::Arc;

use crate::LabelSet;

/// The result of a query. A response adapter returns an error for a
/// variant its protocol cannot express.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Vector(InstantVector),
    Matrix(RangeVector),
    /// One value at one time (a PromQL scalar).
    Scalar(Sample),
    /// Named columns and rows (a SQL result set).
    Table(Table),
}

/// One value per series, all at `timestamp_ms`.
#[derive(Debug, Clone, PartialEq)]
pub struct InstantVector {
    pub timestamp_ms: i64,
    pub series: Vec<InstantVectorElement>,
}

/// One series of an instant vector: its labels and its value.
#[derive(Debug, Clone, PartialEq)]
pub struct InstantVectorElement {
    pub labels: LabelSet,
    pub value: f64,
}

/// Samples over a time range, per series.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeVector {
    pub series: Vec<RangeVectorElement>,
}

/// One series of a range vector: its labels and its samples, oldest first.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeVectorElement {
    pub labels: LabelSet,
    pub samples: Vec<Sample>,
}

/// A value at a time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A table: each row holds one value per column, in column order.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<TableValue>>,
}

/// One table cell.
#[derive(Debug, Clone, PartialEq)]
pub enum TableValue {
    String(String),
    Float(f64),
}

/// One series of an instant answer as the query engine returns it natively
/// to the dispatcher and the Prometheus response writers: unlike
/// [`InstantVectorElement`], its labels are shared, not copied.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultSample {
    /// Shared with the interned series (an input series) or with every
    /// other row of the same result series (`series_ids::result_labels`)
    /// when the row has no other label column, so results and the result
    /// cache hold no copy of it.
    pub labels: Arc<LabelSet>,
    pub value: f64,
}
