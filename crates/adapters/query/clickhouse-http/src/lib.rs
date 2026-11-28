//! ClickHouse's HTTP interface as the data plane speaks it, on ASAPQuery's
//! route (`/clickhouse/query`): the query, TabSeparated result
//! bodies, and the [`fallback`] client that sends a query the data plane
//! does not answer to a ClickHouse server. An adapter: it depends on
//! `common` only.

use std::collections::BTreeSet;
use std::io::Write as _;

use common::{QueryEval, QueryLanguage, QueryRequest, QueryResult, SqlDialect, TableValue};

pub mod fallback;

/// The request has no query (400).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("missing parameter \"query\"")]
pub struct MissingQuery;

/// The `Content-Type` of a TabSeparated body, as ClickHouse sends it.
pub const TAB_SEPARATED_CONTENT_TYPE: &str = "text/tab-separated-values; charset=UTF-8";

/// The ClickHouse SQL query of a request, evaluated at `now_ms` (ClickHouse
/// queries name no evaluation time): the `query` parameter of the URL query
/// or of a form body, else, as ASAPQuery reads it, the whole body (the
/// ClickHouse HTTP interface's plain SQL `POST`).
pub fn parse_query(
    url_query: Option<&str>,
    body: &[u8],
    now_ms: i64,
) -> Result<QueryRequest, MissingQuery> {
    let parameter = form_urlencoded::parse(url_query.unwrap_or_default().as_bytes())
        .chain(form_urlencoded::parse(body))
        .find(|(name, _)| name == "query");
    let query = match parameter {
        Some((_, query)) => query.into_owned(),
        None if !body.is_empty() => std::str::from_utf8(body)
            .map_err(|_| MissingQuery)?
            .to_string(),
        None => return Err(MissingQuery),
    };
    Ok(QueryRequest {
        language: QueryLanguage::SQL(SqlDialect::ClickhouseSQL),
        query,
        eval: QueryEval::Instant { time_ms: now_ms },
    })
}

/// A result a TabSeparated body cannot hold (a range or a scalar).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a ClickHouse response cannot hold a {0} result")]
pub struct UnsupportedResult(&'static str);

/// `result` in ClickHouse's default format, TabSeparated: one line per row,
/// values separated by tabs, with `\`, tab, newline, carriage return and
/// NUL escaped. A table
/// writes its rows; an instant vector one row per series: its label values
/// (every label any series has, by name; empty where it has none), then its
/// value.
pub fn tab_separated(result: &QueryResult) -> Result<Vec<u8>, UnsupportedResult> {
    let mut body = Vec::new();
    match result {
        QueryResult::Table(table) => {
            for row in &table.rows {
                for (index, cell) in row.iter().enumerate() {
                    if index > 0 {
                        body.push(b'\t');
                    }
                    match cell {
                        TableValue::String(text) => write_escaped(&mut body, text),
                        TableValue::Float(value) => write_float(&mut body, *value),
                    }
                }
                body.push(b'\n');
            }
        }
        QueryResult::Vector(vector) => {
            let names: BTreeSet<&String> = vector
                .series
                .iter()
                .flat_map(|series| series.labels.keys())
                .collect();
            for series in &vector.series {
                for name in &names {
                    write_escaped(
                        &mut body,
                        series.labels.get(*name).map_or("", String::as_str),
                    );
                    body.push(b'\t');
                }
                write_float(&mut body, series.value);
                body.push(b'\n');
            }
        }
        QueryResult::Matrix(_) => return Err(UnsupportedResult("range")),
        QueryResult::Scalar(_) => return Err(UnsupportedResult("scalar")),
    }
    Ok(body)
}

/// TabSeparated's escapes for the characters that end a value or a row.
fn write_escaped(body: &mut Vec<u8>, text: &str) {
    for byte in text.bytes() {
        match byte {
            b'\\' => body.extend_from_slice(b"\\\\"),
            b'\t' => body.extend_from_slice(b"\\t"),
            b'\n' => body.extend_from_slice(b"\\n"),
            b'\r' => body.extend_from_slice(b"\\r"),
            b'\0' => body.extend_from_slice(b"\\0"),
            _ => body.push(byte),
        }
    }
}

/// As ClickHouse writes a `Float64`: shortest round-trip digits, `nan`,
/// `inf` and `-inf`.
fn write_float(body: &mut Vec<u8>, value: f64) {
    if value.is_nan() {
        body.extend_from_slice(b"nan");
    } else if value.is_infinite() {
        body.extend_from_slice(if value > 0.0 { b"inf" } else { b"-inf" });
    } else {
        write!(body, "{value}").expect("writing to a Vec does not fail");
    }
}

#[cfg(test)]
mod tests {
    use common::{InstantVector, InstantVectorElement, LabelSet, RangeVector, Table};

    use super::*;

    fn clickhouse(query: &str, now_ms: i64) -> QueryRequest {
        QueryRequest {
            language: QueryLanguage::SQL(SqlDialect::ClickhouseSQL),
            query: query.into(),
            eval: QueryEval::Instant { time_ms: now_ms },
        }
    }

    /// From ASAPQuery's `test_parse_get_request`, `test_parse_post_request_form`
    /// and `test_missing_query_parameter`: the query comes from the URL
    /// query, the form body or the plain body and is evaluated now; without
    /// one the request is refused.
    #[test]
    fn parses_the_query_parameter() {
        assert_eq!(
            parse_query(Some("query=SELECT%201"), b"", 7),
            Ok(clickhouse("SELECT 1", 7))
        );
        assert_eq!(
            parse_query(None, b"query=SELECT+*+FROM+table", 7),
            Ok(clickhouse("SELECT * FROM table", 7))
        );
        assert_eq!(
            parse_query(None, b"SELECT count() FROM t WHERE a=1", 7),
            Ok(clickhouse("SELECT count() FROM t WHERE a=1", 7))
        );
        assert_eq!(parse_query(Some("database=d"), b"", 7), Err(MissingQuery));
        assert_eq!(parse_query(None, b"\xff", 7), Err(MissingQuery));
        assert_eq!(MissingQuery.to_string(), "missing parameter \"query\"");
    }

    /// ASAPQuery's `test_format_success_response`: one row per series, its
    /// label values then its value.
    #[test]
    fn writes_an_instant_vector_as_rows() {
        let series = |host: &str, value| InstantVectorElement {
            labels: LabelSet::from([("hostname".to_string(), host.to_string())]),
            value,
        };
        let result = QueryResult::Vector(InstantVector {
            timestamp_ms: 1000,
            series: vec![series("host1", 91.0), series("host2", 77.5)],
        });
        assert_eq!(tab_separated(&result).unwrap(), b"host1\t91\nhost2\t77.5\n");
    }

    /// A table writes its rows in column order, with values escaped and
    /// floats as ClickHouse writes them; a series lacking a label another
    /// has gets an empty value there.
    #[test]
    fn writes_tables_and_escapes_values() {
        let table = QueryResult::Table(Table {
            columns: vec!["job".into(), "p99".into()],
            rows: vec![
                vec![
                    TableValue::String("a\tb\\c\nd".into()),
                    TableValue::Float(0.25),
                ],
                vec![TableValue::String("e".into()), TableValue::Float(f64::NAN)],
                vec![
                    TableValue::String("f".into()),
                    TableValue::Float(f64::NEG_INFINITY),
                ],
            ],
        });
        assert_eq!(
            tab_separated(&table).unwrap(),
            b"a\\tb\\\\c\\nd\t0.25\ne\tnan\nf\t-inf\n"
        );
        let labels = |pairs: &[(&str, &str)]| -> LabelSet {
            pairs
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect()
        };
        let vector = QueryResult::Vector(InstantVector {
            timestamp_ms: 0,
            series: vec![
                InstantVectorElement {
                    labels: labels(&[("a", "1"), ("b", "2")]),
                    value: 1.0,
                },
                InstantVectorElement {
                    labels: labels(&[("b", "3")]),
                    value: 2.0,
                },
            ],
        });
        assert_eq!(tab_separated(&vector).unwrap(), b"1\t2\t1\n\t3\t2\n");
    }

    /// Ranges and scalars have no ClickHouse form (ASAPQuery answered 501).
    #[test]
    fn refuses_ranges_and_scalars() {
        let range = QueryResult::Matrix(RangeVector { series: vec![] });
        assert_eq!(
            tab_separated(&range).unwrap_err().to_string(),
            "a ClickHouse response cannot hold a range result"
        );
        let scalar = QueryResult::Scalar(common::Sample {
            timestamp_ms: 0,
            value: 1.0,
        });
        assert!(tab_separated(&scalar).is_err());
    }
}
