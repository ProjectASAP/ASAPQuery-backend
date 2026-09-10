use axum::{
    body::Bytes,
    http::{HeaderMap, Method},
};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct ClickHouseQueryRequest {
    pub method: Method,
    pub sql: String,
    pub body: Bytes,
    pub parameters: BTreeMap<String, String>,
    pub headers: HeaderMap,
}

impl ClickHouseQueryRequest {
    pub fn database(&self) -> Option<&str> {
        self.parameters.get("database").map(String::as_str)
    }
    pub fn query_id(&self) -> Option<&str> {
        self.parameters.get("query_id").map(String::as_str)
    }

    pub fn format(&self) -> Option<&str> {
        self.parameters
            .get("default_format")
            .map(String::as_str)
            .or_else(|| {
                let upper = self.sql.to_ascii_uppercase();
                upper
                    .rfind(" FORMAT ")
                    .map(|index| self.sql[index + " FORMAT ".len()..].trim())
            })
    }
}
