//! Executable exact kernels for the PromQL float-sample surface.
//!
//! Summary binding cannot implement ordered-window reducers or label-producing
//! operators with the existing five accumulator families. This plan binds each
//! operator to a backend kernel and explicitly requires raw timestamped samples.
use std::rc::Rc;

use planner_types::pre_asap::{
    AggIntent, CompareOpKind, GroupKeys, QueryExpr, Reduction, SampleKind, ScalarValue, Source,
    VectorMatchKind,
};
use planner_types::types::AccuracyTarget;
use promql_parser::label::Matcher;
use promql_parser::parser::token;

macro_rules! kernels {
    ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name { $($variant),+ }
        impl std::str::FromStr for $name {
            type Err = anyhow::Error;
            fn from_str(name: &str) -> anyhow::Result<Self> {
                match name { $($text => Ok(Self::$variant),)+ _ => anyhow::bail!("no exact kernel for {name}") }
            }
        }
    }
}
kernels!(AggregateKernel {
    Sum => "sum", Avg => "avg", Count => "count", Min => "min", Max => "max",
    Group => "group", Stddev => "stddev", Stdvar => "stdvar", TopK => "topk",
    BottomK => "bottomk", CountValues => "count_values", Quantile => "quantile",
    LimitK => "limitk", LimitRatio => "limit_ratio",
});
kernels!(RangeKernel {
    Avg => "avg_over_time", Min => "min_over_time", Max => "max_over_time",
    Sum => "sum_over_time", Count => "count_over_time", Quantile => "quantile_over_time",
    Stddev => "stddev_over_time", Stdvar => "stdvar_over_time", Last => "last_over_time",
    Present => "present_over_time", Absent => "absent_over_time", Changes => "changes",
    Delta => "delta", Deriv => "deriv", IDelta => "idelta", Increase => "increase",
    IRate => "irate", PredictLinear => "predict_linear", Rate => "rate", Resets => "resets",
    Smoothing => "double_exponential_smoothing", Mad => "mad_over_time",
    TsMin => "ts_of_min_over_time", TsMax => "ts_of_max_over_time", TsLast => "ts_of_last_over_time",
});
kernels!(BinaryKernel {
    Add => "+", Sub => "-", Mul => "*", Div => "/", Mod => "%", Pow => "^",
    And => "and", Or => "or", Unless => "unless",
});

#[derive(Debug, Clone)]
pub struct Grouping {
    pub labels: Vec<String>,
    pub without: bool,
}

#[derive(Debug, Clone)]
pub struct ExactSelector {
    pub metric: String,
    pub matchers: Vec<Matcher>,
}

#[derive(Debug, Clone)]
pub enum ExactExpr {
    Scalar(f64),
    Select {
        selector: ExactSelector,
        range_seconds: Option<f64>,
    },
    Aggregate {
        kernel: AggregateKernel,
        parameter: Option<f64>,
        label: Option<String>,
        grouping: Grouping,
        input: Box<ExactExpr>,
    },
    Range {
        kernel: RangeKernel,
        parameters: Vec<f64>,
        input: Box<ExactExpr>,
    },
    Binary {
        kernel: BinaryKernel,
        lhs: Box<ExactExpr>,
        rhs: Box<ExactExpr>,
    },
}

#[derive(Debug, Clone)]
pub struct ExactPromqlPlan {
    /// The canonical frontend result is retained for review and semantic regression checks.
    pub canonical: Rc<QueryExpr>,
    root: ExactExpr,
}

impl ExactPromqlPlan {
    pub fn bind(query: &str) -> anyhow::Result<Self> {
        let canonical = Rc::new(crate::query_parser::parse_query_expr_canonical(
            query,
            AccuracyTarget::Exact,
        )?);
        Self::from_canonical(canonical)
    }

    /// Bind the planner's canonical tree; execution never reparses the query text.
    pub fn from_canonical(canonical: Rc<QueryExpr>) -> anyhow::Result<Self> {
        let root = bind_expr(&canonical)?;
        anyhow::ensure!(
            !matches!(
                &root,
                ExactExpr::Scalar(_)
                    | ExactExpr::Select {
                        range_seconds: Some(_),
                        ..
                    }
            ),
            "exact endpoint requires an instant-vector result"
        );
        Ok(Self { canonical, root })
    }

    pub fn root(&self) -> &ExactExpr {
        &self.root
    }
}

fn grouping(keys: &GroupKeys, child: &QueryExpr) -> anyhow::Result<Grouping> {
    let schema = child.output_schema()?;
    let labels = keys
        .keys()
        .iter()
        .map(|index| {
            let column = schema
                .columns
                .get(*index)
                .ok_or_else(|| anyhow::anyhow!("invalid grouping column {index}"))?;
            anyhow::ensure!(
                column.name != "ts" && column.name != "value",
                "grouping requires label columns"
            );
            Ok(column.name.clone())
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Grouping {
        labels,
        without: keys.is_without(),
    })
}

fn bind_expr(expr: &QueryExpr) -> anyhow::Result<ExactExpr> {
    match expr {
        QueryExpr::PromqlScalarBridge(child) => bind_expr(child),
        QueryExpr::Literal(ScalarValue::Float64(value)) => Ok(ExactExpr::Scalar(*value)),
        QueryExpr::Literal(ScalarValue::Int64(value)) => Ok(ExactExpr::Scalar(*value as f64)),
        QueryExpr::Scan {
            source: Source::TimeSeries { metric },
            predicates,
            schema,
        } => {
            let mut matchers = Vec::new();
            for predicate in predicates {
                let QueryExpr::Compare { left, op, right } = predicate.0.as_ref() else {
                    anyhow::bail!("unsupported exact scan predicate")
                };
                let (QueryExpr::Column(index), QueryExpr::Literal(ScalarValue::Utf8(value))) =
                    (left.as_ref(), right.as_ref())
                else {
                    anyhow::bail!("exact scan requires literal label matchers")
                };
                let column = schema
                    .columns
                    .get(*index)
                    .ok_or_else(|| anyhow::anyhow!("invalid matcher column"))?;
                let token = match op {
                    CompareOpKind::Eq => token::T_EQL,
                    CompareOpKind::Ne => token::T_NEQ,
                    CompareOpKind::Regex => token::T_EQL_REGEX,
                    CompareOpKind::NotRegex => token::T_NEQ_REGEX,
                    _ => anyhow::bail!("unsupported label comparison"),
                };
                matchers.push(
                    Matcher::new_matcher(token, column.name.clone(), value.clone())
                        .map_err(anyhow::Error::msg)?,
                );
            }
            Ok(ExactExpr::Select {
                selector: ExactSelector {
                    metric: metric.clone(),
                    matchers,
                },
                range_seconds: None,
            })
        }
        QueryExpr::TimeRange { range, child } => {
            let mut input = bind_expr(child)?;
            let ExactExpr::Select { range_seconds, .. } = &mut input else {
                anyhow::bail!("exact range currently requires a raw selector")
            };
            anyhow::ensure!(
                range_seconds.is_none(),
                "nested raw ranges are not supported"
            );
            *range_seconds = Some(range.as_secs_f64());
            Ok(input)
        }
        QueryExpr::Aggregate {
            reduction,
            measures,
            having: None,
            child,
            ..
        } if measures.len() == 1 => {
            let intent = &measures[0];
            let input = Box::new(bind_expr(child)?);
            match reduction {
                Reduction::PerEntity => {
                    anyhow::ensure!(
                        matches!(
                            input.as_ref(),
                            ExactExpr::Select {
                                range_seconds: Some(_),
                                ..
                            }
                        ),
                        "exact rollup needs a raw range input"
                    );
                    let (kernel, parameters) = range_kernel(intent)?;
                    Ok(ExactExpr::Range {
                        kernel,
                        parameters,
                        input,
                    })
                }
                Reduction::Reduce(keys) => {
                    let (kernel, parameter, label) = aggregate_kernel(intent)?;
                    Ok(ExactExpr::Aggregate {
                        kernel,
                        parameter,
                        label,
                        grouping: grouping(keys, child)?,
                        input,
                    })
                }
            }
        }
        QueryExpr::Limit {
            n,
            offset: 0,
            child,
        } => {
            let QueryExpr::Sort {
                keys,
                partition_by,
                child: input,
            } = child.as_ref()
            else {
                anyhow::bail!("limit requires a bound value sort")
            };
            anyhow::ensure!(keys.len() == 1, "exact topk requires one value sort key");
            let QueryExpr::Column(index) = keys[0].expr else {
                anyhow::bail!("topk must sort sample values")
            };
            anyhow::ensure!(
                input
                    .output_schema()?
                    .columns
                    .get(index)
                    .is_some_and(|c| c.name == "value"),
                "topk must sort sample values"
            );
            Ok(ExactExpr::Aggregate {
                kernel: if keys[0].ascending {
                    AggregateKernel::BottomK
                } else {
                    AggregateKernel::TopK
                },
                parameter: Some(*n as f64),
                label: None,
                grouping: grouping(partition_by, input)?,
                input: Box::new(bind_expr(input)?),
            })
        }
        QueryExpr::PromqlSeriesSample { by, kind, child } => {
            let (kernel, parameter) = match kind {
                SampleKind::LimitK(k) => (AggregateKernel::LimitK, *k as f64),
                SampleKind::LimitRatio(r) => (AggregateKernel::LimitRatio, *r),
            };
            Ok(ExactExpr::Aggregate {
                kernel,
                parameter: Some(parameter),
                label: None,
                grouping: grouping(by, child)?,
                input: Box::new(bind_expr(child)?),
            })
        }
        QueryExpr::BinaryOp {
            op,
            lhs,
            rhs,
            vector_match,
        } => {
            if let Some(m) = vector_match {
                anyhow::ensure!(
                    m.kind == VectorMatchKind::Ignoring
                        && m.labels.is_empty()
                        && m.grouping.is_none(),
                    "exact kernel does not implement explicit vector matching"
                );
            }
            Ok(ExactExpr::Binary {
                kernel: op.to_string().to_lowercase().parse()?,
                lhs: Box::new(bind_expr(lhs)?),
                rhs: Box::new(bind_expr(rhs)?),
            })
        }
        _ => anyhow::bail!("no executable exact kernel for canonical node {expr:?}"),
    }
}

fn aggregate_kernel(
    intent: &AggIntent,
) -> anyhow::Result<(AggregateKernel, Option<f64>, Option<String>)> {
    use AggregateKernel as K;
    let (kernel, parameter, label) = match intent {
        AggIntent::Sum { col: None } => (K::Sum, None, None),
        AggIntent::Avg { col: None } => (K::Avg, None, None),
        AggIntent::Count { .. } => (K::Count, None, None),
        AggIntent::Min { col: None } => (K::Min, None, None),
        AggIntent::Max { col: None } => (K::Max, None, None),
        AggIntent::Group => (K::Group, None, None),
        AggIntent::StdDev {
            population: true,
            col: None,
        } => (K::Stddev, None, None),
        AggIntent::Variance {
            population: true,
            col: None,
        } => (K::Stdvar, None, None),
        AggIntent::Quantile { q, col: None, .. } => (K::Quantile, Some(*q), None),
        AggIntent::CountValues { label } => (K::CountValues, None, Some(label.clone())),
        _ => anyhow::bail!("no exact aggregation kernel for {intent:?}"),
    };
    Ok((kernel, parameter, label))
}

fn range_kernel(intent: &AggIntent) -> anyhow::Result<(RangeKernel, Vec<f64>)> {
    use RangeKernel as K;
    Ok(match intent {
        AggIntent::Sum { col: None } => (K::Sum, vec![]),
        AggIntent::Avg { col: None } => (K::Avg, vec![]),
        AggIntent::Count { .. } => (K::Count, vec![]),
        AggIntent::Min { col: None } => (K::Min, vec![]),
        AggIntent::Max { col: None } => (K::Max, vec![]),
        AggIntent::StdDev {
            population: true,
            col: None,
        } => (K::Stddev, vec![]),
        AggIntent::Variance {
            population: true,
            col: None,
        } => (K::Stdvar, vec![]),
        AggIntent::Quantile { q, col: None, .. } => (K::Quantile, vec![*q]),
        AggIntent::LastOverTime => (K::Last, vec![]),
        AggIntent::PresentOverTime => (K::Present, vec![]),
        AggIntent::AbsentOverTime => (K::Absent, vec![]),
        AggIntent::Changes => (K::Changes, vec![]),
        AggIntent::Delta => (K::Delta, vec![]),
        AggIntent::Deriv => (K::Deriv, vec![]),
        AggIntent::IDelta => (K::IDelta, vec![]),
        AggIntent::Increase => (K::Increase, vec![]),
        AggIntent::IRate => (K::IRate, vec![]),
        AggIntent::Rate => (K::Rate, vec![]),
        AggIntent::Resets => (K::Resets, vec![]),
        AggIntent::PredictLinear { seconds } => (K::PredictLinear, vec![*seconds]),
        AggIntent::DoubleExpSmoothing { smoothing, trend } => {
            (K::Smoothing, vec![*smoothing, *trend])
        }
        AggIntent::MadOverTime => (K::Mad, vec![]),
        AggIntent::TsOfMinOverTime => (K::TsMin, vec![]),
        AggIntent::TsOfMaxOverTime => (K::TsMax, vec![]),
        AggIntent::TsOfLastOverTime => (K::TsLast, vec![]),
        _ => anyhow::bail!("no exact range kernel for {intent:?}"),
    })
}
