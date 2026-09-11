//! Typed query-time reductions over relational rows, independent of summary storage.
use super::*;
use planner_types::pre_asap::{AggIntent, Predicate, Reduction};

fn unsupported(detail: &str) -> ClickHouseRelationalError {
    ClickHouseRelationalError::Unsupported(detail.into())
}

fn group_cmp(left: &[Cell], right: &[Cell], keys: &[usize]) -> Ordering {
    for key in keys {
        let order = match (&left[*key], &right[*key]) {
            (Cell::Null, Cell::Null) => Ordering::Equal,
            (Cell::Null, _) => Ordering::Less,
            (_, Cell::Null) => Ordering::Greater,
            (left, right) => cell_cmp(left, right).expect("validated grouping cells"),
        };
        if order != Ordering::Equal {
            return order;
        }
    }
    Ordering::Equal
}

fn measure(
    intent: &AggIntent,
    rows: &[Vec<Cell>],
    fields: &[(String, DataType, bool)],
) -> Result<Cell, ClickHouseRelationalError> {
    if matches!(intent, AggIntent::Count { .. }) {
        return i64::try_from(rows.len())
            .map(Cell::Int64)
            .map_err(|_| unsupported("row count exceeds Int64"));
    }
    let column = match intent {
        AggIntent::Sum { col }
        | AggIntent::Avg { col }
        | AggIntent::Min { col }
        | AggIntent::Max { col } => {
            col.ok_or_else(|| unsupported("aggregate lacks input column"))?
        }
        _ => return Err(unsupported("relational aggregate intent")),
    };
    let (_, dtype, nullable) = fields
        .get(column)
        .ok_or_else(|| unsupported("aggregate input column is out of range"))?;
    // Nullable aggregate empty/default semantics need an explicit SQL policy.
    if *nullable || !matches!(dtype, DataType::Int64 | DataType::Float64) {
        return Err(unsupported("nullable or nonnumeric aggregate input"));
    }
    let values = rows.iter().map(|row| &row[column]).collect::<Vec<_>>();
    if values.iter().any(|value| {
        !matches!(value, Cell::Int64(_))
            && !matches!(value, Cell::Float64(value) if value.is_finite())
    }) {
        return Err(unsupported("nonfinite or invalid aggregate value"));
    }
    if matches!(intent, AggIntent::Min { .. } | AggIntent::Max { .. }) {
        let maximum = matches!(intent, AggIntent::Max { .. });
        return values
            .into_iter()
            .reduce(|left, right| {
                let order = cell_cmp(left, right).expect("validated numeric aggregate");
                if (maximum && order.is_lt()) || (!maximum && order.is_gt()) {
                    right
                } else {
                    left
                }
            })
            .cloned()
            .ok_or_else(|| unsupported("empty min/max SQL default"));
    }
    if matches!(intent, AggIntent::Sum { .. }) && *dtype == DataType::Int64 {
        let mut sum = 0i64;
        for value in values {
            let Cell::Int64(value) = value else {
                return Err(unsupported("integer aggregate received noninteger"));
            };
            sum = sum
                .checked_add(*value)
                .ok_or_else(|| unsupported("integer sum overflow"))?;
        }
        return Ok(Cell::Int64(sum));
    }
    let mut sum = 0.0;
    for value in values {
        sum += match value {
            Cell::Int64(value) => *value as f64,
            Cell::Float64(value) => *value,
            _ => unreachable!("validated numeric aggregate"),
        };
    }
    if matches!(intent, AggIntent::Avg { .. }) {
        sum /= rows.len() as f64;
    }
    if !sum.is_finite() {
        return Err(unsupported("nonfinite aggregate output"));
    }
    Ok(Cell::Float64(sum))
}

pub(super) fn apply(
    reduction: &Reduction,
    measures: &[AggIntent],
    having: Option<&Predicate>,
    output: &SummarySchema,
    mut input: ClickHouseRelation,
) -> Result<ClickHouseRelation, ClickHouseRelationalError> {
    let Reduction::Reduce(keys) = reduction else {
        return Err(unsupported("per-entity relational reduction"));
    };
    if keys.is_without() {
        return Err(unsupported("relational grouping without"));
    }
    let keys = keys.keys();
    for row in &input.rows {
        for key in keys {
            let value = row
                .get(*key)
                .ok_or_else(|| unsupported("group column out of range"))?;
            if !matches!(value, Cell::Null) && cell_cmp(value, value).is_none() {
                return Err(unsupported("unordered grouping value"));
            }
        }
    }
    input
        .rows
        .sort_by(|left, right| group_cmp(left, right, keys));
    let mut rows = Vec::new();
    let mut start = 0;
    while start < input.rows.len() || (start == 0 && input.rows.is_empty() && keys.is_empty()) {
        let mut end = (start + 1).min(input.rows.len());
        while end < input.rows.len()
            && group_cmp(&input.rows[start], &input.rows[end], keys) == Ordering::Equal
        {
            end += 1;
        }
        let mut row = keys
            .iter()
            .map(|key| input.rows[start][*key].clone())
            .collect::<Vec<_>>();
        for intent in measures {
            row.push(measure(intent, &input.rows[start..end], &input.fields)?);
        }
        rows.push(row);
        if end == start {
            break;
        }
        start = end;
    }
    input.rows = rows;
    input.fields = fields_from_schema(output);
    if input.rows.iter().any(|row| row.len() != input.fields.len()) {
        return Err(unsupported("aggregate output width mismatch"));
    }
    if let Some(predicate) = having {
        input = ClickHouseRelationalAdapter.apply_filter(predicate, input)?;
    }
    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::{post_asap::SummaryField, pre_asap::GroupKeys};

    fn output(fields: &[(&str, DataType)]) -> SummarySchema {
        SummarySchema {
            fields: fields
                .iter()
                .map(|(name, dtype)| SummaryField {
                    name: (*name).into(),
                    dtype: SummaryFamilyType::Plain(dtype.clone()),
                    nullable: false,
                })
                .collect(),
            time_index: None,
        }
    }

    #[test]
    fn typed_grouped_sum_reduces_nonadjacent_rows_without_label_conversion() {
        let input = ClickHouseRelation {
            fields: vec![
                ("group".into(), DataType::Int64, false),
                ("value".into(), DataType::Int64, false),
            ],
            rows: vec![
                vec![Cell::Int64(2), Cell::Int64(4)],
                vec![Cell::Int64(1), Cell::Int64(9_007_199_254_740_993)],
                vec![Cell::Int64(2), Cell::Int64(7)],
            ],
            coverage: Some((10, 20)),
        };
        let result = apply(
            &Reduction::Reduce(GroupKeys::by(vec![0])),
            &[AggIntent::Sum { col: Some(1) }],
            None,
            &output(&[("group", DataType::Int64), ("total", DataType::Int64)]),
            input,
        )
        .unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![Cell::Int64(1), Cell::Int64(9_007_199_254_740_993)],
                vec![Cell::Int64(2), Cell::Int64(11)]
            ]
        );
        assert_eq!(result.coverage, Some((10, 20)));
    }

    #[test]
    fn global_empty_sum_and_overflow_have_explicit_semantics() {
        let input = ClickHouseRelation {
            fields: vec![("value".into(), DataType::Int64, false)],
            rows: vec![],
            coverage: None,
        };
        let result = apply(
            &Reduction::Reduce(GroupKeys::none()),
            &[AggIntent::Sum { col: Some(0) }],
            None,
            &output(&[("sum", DataType::Int64)]),
            input.clone(),
        )
        .unwrap();
        assert_eq!(result.rows, vec![vec![Cell::Int64(0)]]);
        let mut overflow = input;
        overflow.rows = vec![vec![Cell::Int64(i64::MAX)], vec![Cell::Int64(1)]];
        assert!(apply(
            &Reduction::Reduce(GroupKeys::none()),
            &[AggIntent::Sum { col: Some(0) }],
            None,
            &output(&[("sum", DataType::Int64)]),
            overflow
        )
        .is_err());
    }
}
