/// Normalize spatial filter for PromQL queries
pub fn normalize_spatial_filter(filter: &str) -> String {
    if filter.is_empty() {
        return String::new();
    }

    let trimmed = filter.trim().strip_prefix('{').unwrap_or(filter.trim());
    let trimmed = trimmed.strip_suffix('}').unwrap_or(trimmed);
    let trimmed = trimmed.trim();

    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, ch) in trimmed.char_indices() {
        if escaped {
            escaped = false;
        } else if ch == '\\' && quoted {
            escaped = true;
        } else if ch == '"' {
            quoted = !quoted;
        } else if ch == ',' && !quoted {
            parts.push(trimmed[start..index].trim());
            start = index + 1;
        }
    }
    parts.push(trimmed[start..].trim());
    parts.sort();

    format!("{{{}}}", parts.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_spatial_filter() {
        assert_eq!(normalize_spatial_filter("").as_str(), "");

        let result = normalize_spatial_filter("instance=\"localhost:9090\"");
        assert_eq!(result, "{instance=\"localhost:9090\"}");

        let result = normalize_spatial_filter("{instance=\"localhost:9090\"}");
        assert_eq!(result, "{instance=\"localhost:9090\"}");

        let result = normalize_spatial_filter("{job=\"prometheus\",instance=\"localhost:9090\"}");
        assert_eq!(result, "{instance=\"localhost:9090\",job=\"prometheus\"}");

        let result = normalize_spatial_filter("job=\"prometheus\",instance=\"localhost:9090\"");
        assert_eq!(result, "{instance=\"localhost:9090\",job=\"prometheus\"}");

        let result = normalize_spatial_filter(r#"{status=~"5(00,01)",job="api"}"#);
        assert_eq!(result, r#"{job="api",status=~"5(00,01)"}"#);
    }
}
