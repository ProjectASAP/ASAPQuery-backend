//! Integration tests bind relational reductions to Planner-owned operators.
use super::*;
use planner_types::pre_asap::{AggIntent, Predicate, Reduction};
fn apply(
    reduction: &Reduction,
    measures: &[AggIntent],
    having: Option<&Predicate>,
    output: &SummarySchema,
    input: ClickHouseRelation,
) -> Result<ClickHouseRelation, ClickHouseRelationalError> {
    let groups = reduction.group_keys().map_or(0, |keys| keys.keys().len());
    ClickHouseRelationalAdapter.apply_operation(
        &ValueOperation::Exact(planner_types::post_asap::ExactOperation::Aggregate {
            reduction: reduction.clone(),
            measures: measures.to_vec(),
            having: having.cloned(),
            output_names: output
                .fields
                .iter()
                .skip(groups)
                .map(|field| field.name.clone())
                .collect(),
        }),
        output,
        input,
    )
}
fn measure(
    intent: &AggIntent,
    rows: &[Vec<Cell>],
    fields: &[(String, DataType, bool)],
) -> Result<Cell, ClickHouseRelationalError> {
    let mut output = intent.output_column(&planner_types::pre_asap::Column::new(
        fields[0].0.clone(),
        fields[0].1.clone(),
        fields[0].2,
    ));
    if matches!(intent, AggIntent::Min { .. } | AggIntent::Max { .. }) {
        output.nullable = true;
    }
    let output = SummarySchema {
        fields: vec![planner_types::post_asap::SummaryField {
            name: output.name,
            dtype: SummaryFamilyType::Plain(output.dtype),
            nullable: output.nullable,
        }],
        time_index: None,
    };
    let result = apply(
        &Reduction::by(vec![]),
        &[intent.clone()],
        None,
        &output,
        ClickHouseRelation {
            rows: rows.to_vec(),
            fields: fields.to_vec(),
            coverage: None,
        },
    )?;
    Ok(result.rows[0][0].clone())
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

    #[test]
    fn integer_average_preserves_cancellation_before_float_conversion() {
        let fields = vec![("value".into(), DataType::Int64, false)];
        let rows = vec![
            vec![Cell::Int64(9_007_199_254_740_993)],
            vec![Cell::Int64(-9_007_199_254_740_992)],
        ];
        assert_eq!(
            measure(&AggIntent::Avg { col: Some(0) }, &rows, &fields).unwrap(),
            Cell::Float64(0.5)
        );
    }

    #[test]
    fn numeric_reductions_preserve_nullable_planner_inputs() {
        let fields = vec![("value".into(), DataType::Float64, false)];
        let rows = vec![vec![Cell::Float64(2.0)], vec![Cell::Float64(8.0)]];
        for (intent, expected) in [
            (AggIntent::Sum { col: Some(0) }, Cell::Float64(10.0)),
            (AggIntent::Avg { col: Some(0) }, Cell::Float64(5.0)),
            (AggIntent::Min { col: Some(0) }, Cell::Float64(2.0)),
            (AggIntent::Max { col: Some(0) }, Cell::Float64(8.0)),
            (
                AggIntent::Count {
                    accuracy: planner_types::types::AccuracyTarget::Exact,
                },
                Cell::Int64(2),
            ),
        ] {
            assert_eq!(measure(&intent, &rows, &fields).unwrap(), expected);
        }
        let nullable = vec![("value".into(), DataType::Float64, true)];
        assert!(measure(&AggIntent::Sum { col: Some(0) }, &rows, &nullable).is_ok());
    }
}
