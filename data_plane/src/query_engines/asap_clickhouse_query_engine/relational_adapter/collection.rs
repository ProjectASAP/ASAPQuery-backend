//! Native collection metadata is checked against the existing shared schema.

use planner_types::pre_asap::Column;

/// Split native type arguments without splitting nested types or quoted names.
pub(super) fn arguments(input: &str) -> Option<Vec<&str>> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut depth = 0_usize;
    let mut quote = None;
    let mut chars = input.char_indices().peekable();
    while let Some((position, ch)) = chars.next() {
        if let Some(delimiter) = quote {
            if ch == '\\' {
                chars.next()?;
            } else if ch == delimiter {
                if chars.peek().is_some_and(|(_, next)| *next == delimiter) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match ch {
            '\'' | '`' | '"' => quote = Some(ch),
            '(' => depth = depth.checked_add(1)?,
            ')' => depth = depth.checked_sub(1)?,
            ',' if depth == 0 => {
                result.push(input[start..position].trim());
                start = position + 1;
            }
            _ => {}
        }
    }
    if depth != 0 || quote.is_some() {
        return None;
    }
    if !input.is_empty() {
        result.push(input[start..].trim());
    }
    (!result.iter().any(|argument| argument.is_empty())).then_some(result)
}

/// Anonymous native Tuple fields have explicit one-based names in the shared
/// Struct schema. Named fields must match their native names exactly. Other
/// Arrow names are never interpreted as an anonymous Tuple.
pub(super) fn tuple_field_types<'a>(native: &'a str, fields: &[Column]) -> Option<Vec<&'a str>> {
    let inner = native.strip_prefix("Tuple(")?.strip_suffix(')')?;
    let args = arguments(inner)?;
    if args.len() != fields.len() {
        return None;
    }
    let mut types = Vec::with_capacity(fields.len());
    for (index, (argument, field)) in args.into_iter().zip(fields).enumerate() {
        if field.table.is_some() {
            return None;
        }
        if field.name == (index + 1).to_string()
            && super::clickhouse_type_matches(Some(argument), &field.dtype, field.nullable)
        {
            types.push(argument);
            continue;
        }
        // Initial named transport accepts ordinary identifiers. Quoted names
        // remain unsupported until a native identifier-decoding contract exists.
        let (name, native_type) = argument.split_once(char::is_whitespace)?;
        if name != field.name
            || name.is_empty()
            || !name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            || !super::clickhouse_type_matches(
                Some(native_type.trim()),
                &field.dtype,
                field.nullable,
            )
        {
            return None;
        }
        types.push(native_type.trim());
    }
    Some(types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::pre_asap::DataType;

    #[test]
    fn tuple_metadata_preserves_names_order_and_nested_types() {
        let fields = vec![
            Column::new("ts", DataType::Int64, false),
            Column::new(
                "samples",
                DataType::List {
                    element: Box::new(Column::new("item", DataType::Float64, true)),
                },
                false,
            ),
        ];
        assert_eq!(
            tuple_field_types("Tuple(ts Int64, samples Array(Nullable(Float64)))", &fields),
            Some(vec!["Int64", "Array(Nullable(Float64))"])
        );
        assert!(
            tuple_field_types("Tuple(samples Int64, ts Array(Nullable(Float64)))", &fields)
                .is_none()
        );
        assert!(tuple_field_types("Tuple(Int64, Array(Nullable(Float64)))", &fields).is_none());
        let anonymous = vec![
            Column::new("1", DataType::Int64, false),
            Column::new("2", DataType::Utf8, false),
        ];
        assert!(tuple_field_types("Tuple(Int64, String)", &anonymous).is_some());
        assert!(arguments("Map(String, Tuple(Int64, String)), DateTime64(3, 'UTC')").is_some());
        assert!(arguments("Map(String, Tuple(Int64, String)").is_none());
    }
}
