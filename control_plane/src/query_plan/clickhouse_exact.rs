//! Render a supported canonical relational cut without changing its row population.
//! Unsupported operators remain admission errors, never guessed SQL semantics.
use planner_types::pre_asap::{
    AggIntent, ArithmeticOpKind, CompareOpKind, QueryExpr, Reduction, ScalarValue, Schema, Source,
};

fn quoted(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}
fn column(index: usize, schema: &Schema) -> Result<String, String> {
    schema
        .columns
        .get(index)
        .map(|c| quoted(&c.name))
        .ok_or_else(|| format!("unresolved exact column {index}"))
}
fn scalar(expr: &QueryExpr, schema: &Schema) -> Result<String, String> {
    Ok(match expr {
        QueryExpr::Column(id) => column(*id, schema)?,
        QueryExpr::Literal(value) => match value {
            ScalarValue::Int64(v) => v.to_string(),
            ScalarValue::Float64(v) if v.is_finite() => format!("toFloat64('{}')", v),
            ScalarValue::Utf8(v) => format!("'{}'", v.replace('\\', "\\\\").replace('\'', "\\'")),
            ScalarValue::Boolean(v) => if *v { "true" } else { "false" }.into(),
            ScalarValue::Null => "NULL".into(),
            _ => return Err("nonfinite exact literal".into()),
        },
        QueryExpr::Arithmetic { op, left, right } => {
            let op = match op {
                ArithmeticOpKind::Add => "+",
                ArithmeticOpKind::Sub => "-",
                ArithmeticOpKind::Mul => "*",
                ArithmeticOpKind::Div => "/",
                ArithmeticOpKind::Mod => "%",
                _ => return Err("unsupported exact arithmetic".into()),
            };
            format!(
                "({} {op} {})",
                scalar(left, schema)?,
                scalar(right, schema)?
            )
        }
        QueryExpr::Compare { op, left, right } => {
            let op = match op {
                CompareOpKind::Eq => "=",
                CompareOpKind::Ne => "!=",
                CompareOpKind::Lt => "<",
                CompareOpKind::Le => "<=",
                CompareOpKind::Gt => ">",
                CompareOpKind::Ge => ">=",
                _ => return Err("unsupported exact comparison".into()),
            };
            format!(
                "({} {op} {})",
                scalar(left, schema)?,
                scalar(right, schema)?
            )
        }
        QueryExpr::BoolAnd(args) | QueryExpr::BoolOr(args) => {
            let and = matches!(expr, QueryExpr::BoolAnd(_));
            if args.is_empty() {
                if and { "true" } else { "false" }.into()
            } else {
                format!(
                    "({})",
                    args.iter()
                        .map(|e| scalar(e, schema))
                        .collect::<Result<Vec<_>, _>>()?
                        .join(if and { " AND " } else { " OR " })
                )
            }
        }
        QueryExpr::Not(arg) => format!("NOT ({})", scalar(arg, schema)?),
        QueryExpr::IsNull(arg) => format!("({} IS NULL)", scalar(arg, schema)?),
        QueryExpr::IsNotNull(arg) => format!("({} IS NOT NULL)", scalar(arg, schema)?),
        QueryExpr::FunctionCall { name, args } => {
            let function = match name.as_str() {
                "map" => "map",
                "mapconcat" => "mapConcat",
                "asap_map_access" | "asap_element_access" => "arrayElement",
                "asap_struct_field" => "tupleElement",
                _ => return Err(format!("unsupported exact scalar function {name}")),
            };
            expr.scalar_type(schema).map_err(|e| e.to_string())?;
            format!(
                "{function}({})",
                args.iter()
                    .map(|e| scalar(e, schema))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ")
            )
        }
        _ => return Err("unsupported exact scalar expression".into()),
    })
}
fn aggregate(intent: &AggIntent, schema: &Schema) -> Result<String, String> {
    if let Some((arg, order)) = intent.arg_selector_columns(schema)? {
        let AggIntent::Extension { ext_kind, .. } = intent else {
            unreachable!()
        };
        let function = if ext_kind == "arg_max" {
            "argMax"
        } else {
            "argMin"
        };
        return Ok(format!(
            "{function}({}, {})",
            column(arg, schema)?,
            column(order, schema)?
        ));
    }
    let (function, col) = match intent {
        AggIntent::Count { .. } => return Ok("count()".into()),
        AggIntent::Sum { col } => ("sum", col),
        AggIntent::Min { col } => ("min", col),
        AggIntent::Max { col } => ("max", col),
        AggIntent::Avg { col } => ("avg", col),
        _ => return Err("unsupported exact aggregate contract".into()),
    };
    Ok(format!(
        "{function}({})",
        column(
            col.ok_or("SQL aggregate requires explicit input column")?,
            schema
        )?
    ))
}
/// A closed table population verifies the native table's complete row schema,
/// including columns not used by a quantile but returned by SELECT * TopK.
pub(super) fn render_population_snapshot(expr: &QueryExpr) -> Result<String, String> {
    let QueryExpr::Scan {
        source: Source::Table { table_ref },
        predicates,
        schema,
    } = expr
    else {
        return Err("table population requires a direct table scan".into());
    };
    if !schema.closed {
        return Err("table population requires a closed source schema".into());
    }
    let table = table_ref
        .split('.')
        .map(quoted)
        .collect::<Vec<_>>()
        .join(".");
    let filters = predicates
        .iter()
        .map(|p| scalar(&p.0, schema))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!(
        "SELECT * FROM {table}{}",
        if filters.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", filters.join(" AND "))
        }
    ))
}

/// Composite cuts retain their own canonical predicates; caller bounds are not
/// injected into descendant scans (which may belong to independent windows).
pub(super) fn render(expr: &QueryExpr) -> Result<String, String> {
    let output = expr.output_schema().map_err(|e| e.to_string())?;
    // SQL names must identify a unique positional field at each nested boundary.
    let mut names = std::collections::BTreeSet::new();
    if output.columns.iter().any(|c| !names.insert(&c.name)) {
        return Err("ambiguous exact output column names".into());
    }
    match expr {
        QueryExpr::Scan {
            source: Source::Table { table_ref },
            predicates,
            schema,
        } => {
            let table = table_ref
                .split('.')
                .map(quoted)
                .collect::<Vec<_>>()
                .join(".");
            let columns = schema
                .columns
                .iter()
                .map(|c| quoted(&c.name))
                .collect::<Vec<_>>()
                .join(", ");
            let filters = predicates
                .iter()
                .map(|p| scalar(&p.0, schema))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!(
                "SELECT {columns} FROM {table}{}",
                if filters.is_empty() {
                    String::new()
                } else {
                    format!(" WHERE {}", filters.join(" AND "))
                }
            ))
        }
        QueryExpr::Filter { pred, child } => {
            let schema = child.output_schema().map_err(|e| e.to_string())?;
            Ok(format!(
                "SELECT * FROM ({}) WHERE {}",
                render(child)?,
                scalar(&pred.0, &schema)?
            ))
        }
        QueryExpr::Project { cols, child, .. } => {
            let schema = child.output_schema().map_err(|e| e.to_string())?;
            if cols.len() != output.columns.len() {
                return Err("exact projection width mismatch".into());
            }
            let columns = cols
                .iter()
                .zip(&output.columns)
                .map(|(item, col)| {
                    Ok(format!(
                        "{} AS {}",
                        scalar(&item.expr, &schema)?,
                        quoted(&col.name)
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(format!(
                "SELECT {} FROM ({})",
                columns.join(", "),
                render(child)?
            ))
        }
        QueryExpr::Aggregate {
            reduction: Reduction::Reduce(keys),
            measures,
            having: None,
            child,
            ..
        } if !keys.is_without() => {
            let schema = child.output_schema().map_err(|e| e.to_string())?;
            let groups = keys
                .keys()
                .iter()
                .map(|id| column(*id, &schema))
                .collect::<Result<Vec<_>, _>>()?;
            let mut values = groups.clone();
            values.extend(
                measures
                    .iter()
                    .map(|m| aggregate(m, &schema))
                    .collect::<Result<Vec<_>, _>>()?,
            );
            if values.len() != output.columns.len() {
                return Err("exact aggregate width mismatch".into());
            }
            let values = values
                .iter()
                .zip(&output.columns)
                .map(|(v, c)| format!("{v} AS {}", quoted(&c.name)))
                .collect::<Vec<_>>();
            Ok(format!(
                "SELECT {} FROM ({}){}",
                values.join(", "),
                render(child)?,
                if groups.is_empty() {
                    String::new()
                } else {
                    format!(" GROUP BY {}", groups.join(", "))
                }
            ))
        }
        QueryExpr::Sort {
            keys,
            partition_by,
            child,
        } if partition_by.keys().is_empty() && !partition_by.is_without() => {
            let schema = child.output_schema().map_err(|e| e.to_string())?;
            let keys = keys
                .iter()
                .map(|key| {
                    Ok(format!(
                        "{} {} NULLS {}",
                        scalar(&key.expr, &schema)?,
                        if key.ascending { "ASC" } else { "DESC" },
                        if key.nulls_first { "FIRST" } else { "LAST" }
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            if keys.is_empty() {
                return Err("empty exact sort".into());
            }
            Ok(format!(
                "SELECT * FROM ({}) ORDER BY {}",
                render(child)?,
                keys.join(", ")
            ))
        }
        QueryExpr::Limit { n, offset, child } => Ok(format!(
            "SELECT * FROM ({}) LIMIT {n} OFFSET {offset}",
            render(child)?
        )),
        _ => Err("unsupported canonical ClickHouse exact subtree".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::pre_asap::{Column, DataType, Predicate, ProjectItem};
    use std::rc::Rc;
    #[test]
    fn composite_cut_preserves_branch_time_and_positional_projection() {
        let scan = Rc::new(QueryExpr::Scan {
            source: Source::Table {
                table_ref: "db.samples".into(),
            },
            schema: Schema::new(vec![
                Column::new("ts", DataType::Int64, false),
                Column::new("v", DataType::Float64, false),
            ]),
            predicates: vec![Predicate(Rc::new(QueryExpr::Compare {
                left: Rc::new(QueryExpr::Column(0)),
                op: CompareOpKind::Lt,
                right: Rc::new(QueryExpr::Literal(ScalarValue::Int64(-100))),
            }))],
        });
        let project = QueryExpr::Project {
            cols: vec![ProjectItem {
                alias: Some("result".into()),
                expr: QueryExpr::Column(1),
            }],
            qualifier: None,
            child: scan,
        };
        let sql = render(&project).unwrap();
        assert!(sql.contains("`ts` < -100"));
        assert!(sql.starts_with("SELECT `v` AS `result`"));
        assert!(!sql.contains("{from:"));
        assert!(!sql.contains("{to:"));
    }
    #[test]
    fn typed_list_access_renders_native_element_lookup() {
        let schema = Schema::new(vec![Column::new(
            "samples",
            DataType::List {
                element: Box::new(Column::new("item", DataType::Float64, false)),
            },
            false,
        )]);
        let expr = QueryExpr::FunctionCall {
            name: "asap_element_access".into(),
            args: vec![
                QueryExpr::Column(0),
                QueryExpr::Literal(ScalarValue::Int64(-1)),
            ],
        };
        assert_eq!(
            scalar(&expr, &schema).unwrap(),
            "arrayElement(`samples`, -1)"
        );
    }

    #[test]
    fn typed_struct_field_renders_native_lookup() {
        let schema = Schema::new(vec![Column::new(
            "sample",
            DataType::Struct {
                fields: vec![
                    Column::new("ts", DataType::Int64, false),
                    Column::new("value", DataType::Float64, true),
                ],
            },
            false,
        )]);
        let expr = QueryExpr::FunctionCall {
            name: "asap_struct_field".into(),
            args: vec![
                QueryExpr::Column(0),
                QueryExpr::Literal(ScalarValue::Utf8("value".into())),
            ],
        };
        assert_eq!(
            scalar(&expr, &schema).unwrap(),
            "tupleElement(`sample`, 'value')"
        );
    }

    #[test]
    fn unsupported_scalar_is_not_forwarded_as_arbitrary_native_code() {
        let schema = Schema::new(vec![]);
        let expr = QueryExpr::FunctionCall {
            name: "unreviewedFunction".into(),
            args: vec![],
        };
        assert!(scalar(&expr, &schema).is_err());
    }
}

#[cfg(test)]
mod original_tests {
    use super::*;
    use asap_frontend_sql::{lower_sql_dialect, SqlCatalog};
    use planner_types::{
        pre_asap::{Column, DataType},
        types::AccuracyTarget,
        workload::SqlDialect,
    };
    #[tokio::test]
    async fn original_exact_shapes_retain_native_aggregates_and_bounds() {
        let catalog = SqlCatalog::new().with_table(
            "raw_samples",
            Schema::new(vec![
                Column::new("metric", DataType::Utf8, false),
                Column::new("ts_ms", DataType::Int64, false),
                Column::new("value", DataType::Float64, false),
                Column::new(
                    "labels",
                    DataType::Map {
                        key: Box::new(DataType::Utf8),
                        value: Box::new(DataType::Utf8),
                        value_nullable: false,
                    },
                    false,
                ),
            ]),
        );
        for sql in [
            include_str!("../../tests/fixtures/sql_exact_cuts/q07.sql"),
            include_str!("../../tests/fixtures/sql_exact_cuts/q09.sql"),
            include_str!("../../tests/fixtures/sql_exact_cuts/q12.sql"),
            include_str!("../../tests/fixtures/sql_exact_cuts/q27.sql"),
        ] {
            let canonical = lower_sql_dialect(
                sql,
                &catalog,
                SqlDialect::ClickhouseSQL,
                AccuracyTarget::Exact,
            )
            .await
            .unwrap();
            let rendered = render(&canonical).unwrap();
            assert!(rendered.contains("1788891296000"));
            assert!(rendered.contains("`ts_ms`"));
            assert!(!rendered.contains("{from:"));
            if sql.contains("sum(value)") && sql.contains("argMax") {
                use crate::physical::post_asap::{PhysicalExpr, PostAsapPlan};
                use crate::query_plan::{
                    FallbackPolicy, FixedEvaluationRange, InstantExecution, QueryPlanError,
                    QueryPlanNode,
                };
                let planned =
                    crate::clickhouse::plan_clickhouse_sql(sql, &catalog, AccuracyTarget::Exact)
                        .await
                        .unwrap();
                let PhysicalExpr::Committed(PostAsapPlan::Summary(root)) = planned.physical else {
                    panic!("missing selected SQL DAG")
                };
                let entry = crate::query_plan::compile_bound_relational(
                    "test".into(),
                    planned.canonical_sql,
                    &root,
                    FixedEvaluationRange {
                        start_ms: 1788890996000,
                        end_ms: 1788891296000,
                        cumulative: false,
                    },
                    InstantExecution {
                        lookback_ms: 300000,
                        full_history: false,
                        cumulative_readout: false,
                    },
                    FallbackPolicy::ExactBackend,
                    |_, _| Err(QueryPlanError::Invalid("unexpected summary binding".into())),
                )
                .unwrap();
                assert!(
                    !entry
                        .nodes
                        .values()
                        .any(|node| matches!(node, QueryPlanNode::Logical { .. })),
                    "SQL must not acquire PromQL operators"
                );
                assert!(entry.nodes.values().any(|node| match node {
                    QueryPlanNode::Relational { operation, .. } => matches!(
                        serde_json::from_value::<planner_types::post_asap::ValueOperation>(
                            operation.clone()
                        )
                        .unwrap(),
                        planner_types::post_asap::ValueOperation::Exact(
                            planner_types::post_asap::ExactOperation::Aggregate { .. }
                        )
                    ),
                    _ => false,
                }));
            }
        }
    }
    #[test]
    fn literal_quotes_and_backslashes_are_escaped_independently() {
        let rendered = scalar(
            &QueryExpr::Literal(ScalarValue::Utf8("a\\'b\n".into())),
            &Schema::new(vec![]),
        )
        .unwrap();
        assert_eq!(rendered, "'a\\\\\\'b\n'");
    }
}
