//! Execute the control plane's bound exact PromQL kernels over raw float samples.
//! No query is forwarded and no sketch value is substituted for a raw observation.
use std::collections::{BTreeMap, BTreeSet};

use control_plane::physical::promql_exact::{
    AggregateKernel as A, BinaryKernel as B, ExactExpr, ExactPromqlPlan, Grouping, RangeKernel as R,
};
use serde::{Deserialize, Serialize};

pub type Labels = BTreeMap<String, String>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RawSeries {
    pub labels: Labels,
    /// Unix seconds, strictly increasing; missing samples are absent from this list.
    pub samples: Vec<(f64, f64)>,
}

#[derive(Debug, Clone)]
pub struct ExactSample {
    pub labels: Labels,
    pub value: f64,
}

enum Value {
    Scalar(f64),
    Vector(Vec<ExactSample>),
    Matrix {
        series: Vec<RawSeries>,
        seconds: f64,
    },
}
impl Value {
    fn vector(self) -> anyhow::Result<Vec<ExactSample>> {
        match self {
            Self::Vector(v) => Ok(v),
            _ => anyhow::bail!("expected an instant vector"),
        }
    }
}

/// Evaluate only the bound program. Raw data must already be a consistent snapshot.
pub fn execute(
    plan: &ExactPromqlPlan,
    data: &[RawSeries],
    evaluation: f64,
    lookback: f64,
) -> anyhow::Result<Vec<ExactSample>> {
    anyhow::ensure!(
        evaluation.is_finite() && lookback.is_finite() && lookback > 0.0,
        "invalid evaluation time/lookback"
    );
    let mut seen = BTreeSet::new();
    for series in data {
        anyhow::ensure!(seen.insert(&series.labels), "duplicate input label set");
        anyhow::ensure!(
            series.samples.iter().all(|(t, _)| t.is_finite())
                && series.samples.windows(2).all(|p| p[0].0 < p[1].0),
            "raw samples must have unique increasing finite timestamps"
        );
    }
    let out = eval(plan.root(), data, evaluation, lookback)?.vector()?;
    let mut seen = BTreeSet::new();
    anyhow::ensure!(
        out.iter().all(|s| seen.insert(&s.labels)),
        "duplicate output label set"
    );
    Ok(out)
}

fn eval(expr: &ExactExpr, data: &[RawSeries], time: f64, lookback: f64) -> anyhow::Result<Value> {
    match expr {
        ExactExpr::Scalar(value) => Ok(Value::Scalar(*value)),
        ExactExpr::Select {
            selector,
            range_seconds,
        } => {
            let mut selected = Vec::new();
            for series in data {
                if series.labels.get("__name__") != Some(&selector.metric) {
                    continue;
                }
                if !selector.matchers.iter().all(|m| {
                    m.is_match(series.labels.get(&m.name).map(String::as_str).unwrap_or(""))
                }) {
                    continue;
                }
                let seconds = range_seconds.unwrap_or(lookback);
                let mut samples: Vec<_> = series
                    .samples
                    .iter()
                    .copied()
                    .filter(|(t, _)| *t > time - seconds && *t <= time)
                    .collect();
                if range_seconds.is_none() && !samples.is_empty() {
                    samples = vec![*samples.last().unwrap()];
                }
                if !samples.is_empty() {
                    selected.push(RawSeries {
                        labels: series.labels.clone(),
                        samples,
                    });
                }
            }
            selected.sort_by(|a, b| a.labels.cmp(&b.labels));
            Ok(match range_seconds {
                Some(seconds) => Value::Matrix {
                    series: selected,
                    seconds: *seconds,
                },
                None => Value::Vector(
                    selected
                        .into_iter()
                        .map(|s| ExactSample {
                            labels: s.labels,
                            value: s.samples[0].1,
                        })
                        .collect(),
                ),
            })
        }
        ExactExpr::Aggregate {
            kernel,
            parameter,
            label,
            grouping,
            input,
        } => {
            let rows = eval(input, data, time, lookback)?.vector()?;
            Ok(Value::Vector(aggregate(
                *kernel,
                *parameter,
                label.as_deref(),
                grouping,
                rows,
            )?))
        }
        ExactExpr::Range {
            kernel,
            parameters,
            input,
        } => {
            let Value::Matrix { series, seconds } = eval(input, data, time, lookback)? else {
                anyhow::bail!("range kernel needs raw range samples")
            };
            if *kernel == R::Absent {
                let labels = absent_labels(input);
                return Ok(Value::Vector(if series.is_empty() {
                    vec![ExactSample { labels, value: 1.0 }]
                } else {
                    vec![]
                }));
            }
            let mut out = Vec::new();
            for series in series {
                if let Some(value) = rollup(*kernel, parameters, &series.samples, time, seconds)? {
                    let mut labels = series.labels;
                    if *kernel != R::Last {
                        labels.remove("__name__");
                    }
                    out.push(ExactSample { labels, value });
                }
            }
            Ok(Value::Vector(out))
        }
        ExactExpr::Binary { kernel, lhs, rhs } => binary(
            *kernel,
            eval(lhs, data, time, lookback)?,
            eval(rhs, data, time, lookback)?,
        ),
    }
}

fn absent_labels(input: &ExactExpr) -> Labels {
    let mut labels = Labels::new();
    if let ExactExpr::Select { selector, .. } = input {
        // Derive a label only when it has a single equality matcher.
        let mut counts = BTreeMap::new();
        for m in &selector.matchers {
            *counts.entry(&m.name).or_insert(0) += 1;
        }
        for m in &selector.matchers {
            if m.name != "__name__"
                && m.op.to_string() == "="
                && counts[&m.name] == 1
                && !m.value.is_empty()
            {
                labels.insert(m.name.clone(), m.value.clone());
            }
        }
    }
    labels
}

fn group_key(labels: &Labels, grouping: &Grouping) -> Labels {
    labels
        .iter()
        .filter(|(key, _)| {
            let included = grouping.labels.contains(key);
            if grouping.without {
                key.as_str() != "__name__" && !included
            } else {
                included
            }
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn aggregate(
    kernel: A,
    parameter: Option<f64>,
    label: Option<&str>,
    grouping: &Grouping,
    rows: Vec<ExactSample>,
) -> anyhow::Result<Vec<ExactSample>> {
    let mut groups: BTreeMap<Labels, Vec<ExactSample>> = BTreeMap::new();
    for mut row in rows {
        let mut key = group_key(&row.labels, grouping);
        if kernel == A::CountValues {
            let label = label.ok_or_else(|| anyhow::anyhow!("count_values requires a label"))?;
            // The output value label participates in grouping even with no 'by'.
            row.labels.insert(label.into(), float_label(row.value));
            key.insert(label.into(), float_label(row.value));
        }
        groups.entry(key).or_default().push(row);
    }
    let mut out = Vec::new();
    for (labels, mut rows) in groups {
        match kernel {
            A::TopK | A::BottomK | A::LimitK => {
                let k = parameter.unwrap_or(0.0);
                anyhow::ensure!(k.is_finite() && k >= 0.0, "invalid selection count");
                if kernel != A::LimitK {
                    rows.sort_by(|a, b| {
                        if a.value.is_nan() {
                            return if b.value.is_nan() {
                                std::cmp::Ordering::Equal
                            } else {
                                std::cmp::Ordering::Greater
                            };
                        }
                        if b.value.is_nan() {
                            return std::cmp::Ordering::Less;
                        }
                        if kernel == A::TopK {
                            b.value.total_cmp(&a.value)
                        } else {
                            a.value.total_cmp(&b.value)
                        }
                    });
                }
                out.extend(rows.into_iter().take(k as usize));
            }
            A::LimitRatio => {
                let ratio = parameter.unwrap_or(0.0).clamp(-1.0, 1.0);
                anyhow::ensure!(ratio.is_finite(), "invalid sampling ratio");
                out.extend(rows.into_iter().filter(|row| {
                    let mut bytes = Vec::new();
                    for (key, value) in &row.labels {
                        bytes.extend(key.as_bytes());
                        bytes.push(255);
                        bytes.extend(value.as_bytes());
                        bytes.push(255);
                    }
                    let offset = xxhash_rust::xxh64::xxh64(&bytes, 0) as f64 / u64::MAX as f64;
                    if ratio < 0.0 {
                        offset >= 1.0 + ratio
                    } else {
                        offset < ratio
                    }
                }));
            }
            _ => {
                let values: Vec<_> = rows.iter().map(|s| s.value).collect();
                let value = match kernel {
                    A::Sum => values.iter().sum(),
                    A::Avg => mean(&values),
                    A::Count | A::CountValues => values.len() as f64,
                    A::Min => minimum(&values),
                    A::Max => maximum(&values),
                    A::Group => 1.0,
                    A::Stdvar => variance(&values),
                    A::Stddev => variance(&values).sqrt(),
                    A::Quantile => quantile(&values, parameter.unwrap_or(f64::NAN)),
                    _ => unreachable!(),
                };
                out.push(ExactSample { labels, value });
            }
        }
    }
    Ok(out)
}

fn float_label(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value == f64::INFINITY {
        "+Inf".into()
    } else if value == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        value.to_string()
    }
}
fn minimum(values: &[f64]) -> f64 {
    values.iter().copied().reduce(f64::min).unwrap_or(f64::NAN)
}
fn maximum(values: &[f64]) -> f64 {
    values.iter().copied().reduce(f64::max).unwrap_or(f64::NAN)
}
fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}
fn variance(values: &[f64]) -> f64 {
    let mut mean = 0.0;
    let mut m2 = 0.0;
    for (i, value) in values.iter().enumerate() {
        let d = value - mean;
        mean += d / (i + 1) as f64;
        m2 += d * (value - mean);
    }
    m2 / values.len() as f64
}
fn quantile(values: &[f64], phi: f64) -> f64 {
    if phi.is_nan() || values.is_empty() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| match (a.is_nan(), b.is_nan()) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.total_cmp(b),
    });
    let rank = phi * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let weight = rank - lower as f64;
    sorted[lower] * (1.0 - weight) + sorted[upper] * weight
}

fn rollup(
    kernel: R,
    parameters: &[f64],
    samples: &[(f64, f64)],
    time: f64,
    seconds: f64,
) -> anyhow::Result<Option<f64>> {
    if samples.is_empty() {
        return Ok(None);
    }
    let values: Vec<_> = samples.iter().map(|s| s.1).collect();
    let first = samples[0];
    let last = *samples.last().unwrap();
    let needs_two = matches!(
        kernel,
        R::Delta
            | R::Deriv
            | R::IDelta
            | R::Increase
            | R::IRate
            | R::PredictLinear
            | R::Rate
            | R::Smoothing
    );
    if needs_two && samples.len() < 2 {
        return Ok(None);
    }
    Ok(Some(match kernel {
        R::Avg => mean(&values),
        R::Min => minimum(&values),
        R::Max => maximum(&values),
        R::Sum => values.iter().sum(),
        R::Count => values.len() as f64,
        R::Quantile => quantile(&values, parameters[0]),
        R::Stddev => variance(&values).sqrt(),
        R::Stdvar => variance(&values),
        R::Last => last.1,
        R::Present => 1.0,
        R::Changes => values
            .windows(2)
            .filter(|p| p[0] != p[1] && !(p[0].is_nan() && p[1].is_nan()))
            .count() as f64,
        R::Resets => values.windows(2).filter(|p| p[1] < p[0]).count() as f64,
        R::IDelta => last.1 - samples[samples.len() - 2].1,
        R::IRate => {
            let previous = samples[samples.len() - 2];
            let delta = if last.1 < previous.1 {
                last.1
            } else {
                last.1 - previous.1
            };
            delta / (last.0 - previous.0)
        }
        R::Rate | R::Increase | R::Delta => {
            let counter = kernel != R::Delta;
            let mut difference = last.1 - first.1;
            if counter {
                for pair in values.windows(2) {
                    if pair[1] < pair[0] {
                        difference += pair[0];
                    }
                }
            }
            let observed = last.0 - first.0;
            let interval = observed / (samples.len() - 1) as f64;
            let mut before = first.0 - (time - seconds);
            let mut after = time - last.0;
            if before >= interval * 1.1 {
                before = interval / 2.0;
            }
            if after >= interval * 1.1 {
                after = interval / 2.0;
            }
            if counter && difference > 0.0 && first.1 >= 0.0 {
                before = before.min(observed * first.1 / difference);
            }
            difference * ((observed + before + after) / observed)
                / if kernel == R::Rate { seconds } else { 1.0 }
        }
        R::Deriv | R::PredictLinear => {
            // Center timestamps near the window to avoid losing precision on Unix time.
            let xs: Vec<_> = samples.iter().map(|s| s.0 - time).collect();
            let mx = mean(&xs);
            let my = mean(&values);
            let slope = xs
                .iter()
                .zip(&values)
                .map(|(x, y)| (x - mx) * (y - my))
                .sum::<f64>()
                / xs.iter().map(|x| (x - mx).powi(2)).sum::<f64>();
            if kernel == R::Deriv {
                slope
            } else {
                my + slope * (parameters[0] - mx)
            }
        }
        R::Smoothing => {
            let (sf, tf) = (parameters[0], parameters[1]);
            anyhow::ensure!(
                sf > 0.0 && sf < 1.0 && tf > 0.0 && tf < 1.0,
                "invalid smoothing/trend factor"
            );
            let mut level = first.1;
            let mut previous = 0.0;
            let mut trend = values[1] - values[0];
            for (i, value) in values.iter().enumerate().skip(1) {
                if i > 1 {
                    trend = tf * (level - previous) + (1.0 - tf) * trend;
                }
                previous = level;
                level = sf * value + (1.0 - sf) * (level + trend);
            }
            level
        }
        R::Mad => {
            let median = quantile(&values, 0.5);
            let deviations: Vec<_> = values.iter().map(|v| (v - median).abs()).collect();
            quantile(&deviations, 0.5)
        }
        R::TsLast => last.0,
        R::TsMin | R::TsMax => {
            let mut selected = first;
            for sample in samples.iter().copied().skip(1) {
                if selected.1.is_nan()
                    || (kernel == R::TsMin && sample.1 <= selected.1)
                    || (kernel == R::TsMax && sample.1 >= selected.1)
                {
                    selected = sample;
                }
            }
            selected.0
        }
        R::Absent => unreachable!(),
    }))
}

fn arithmetic(kernel: B, a: f64, b: f64) -> anyhow::Result<f64> {
    Ok(match kernel {
        B::Add => a + b,
        B::Sub => a - b,
        B::Mul => a * b,
        B::Div => a / b,
        B::Mod => a % b,
        B::Pow => a.powf(b),
        _ => anyhow::bail!("set operator needs two vectors"),
    })
}
fn matching_key(labels: &Labels) -> Labels {
    let mut key = labels.clone();
    key.remove("__name__");
    key
}
fn binary(kernel: B, lhs: Value, rhs: Value) -> anyhow::Result<Value> {
    let vector = match (lhs, rhs) {
        (Value::Scalar(a), Value::Scalar(b)) => {
            return Ok(Value::Scalar(arithmetic(kernel, a, b)?))
        }
        (Value::Vector(rows), Value::Scalar(scalar))
        | (Value::Scalar(scalar), Value::Vector(rows))
            if matches!(kernel, B::Add | B::Mul) =>
        {
            rows.into_iter()
                .map(|mut row| {
                    row.value = arithmetic(kernel, row.value, scalar)?;
                    row.labels.remove("__name__");
                    Ok(row)
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        }
        (Value::Vector(rows), Value::Scalar(scalar)) => rows
            .into_iter()
            .map(|mut row| {
                row.value = arithmetic(kernel, row.value, scalar)?;
                row.labels.remove("__name__");
                Ok(row)
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        (Value::Scalar(scalar), Value::Vector(rows)) => rows
            .into_iter()
            .map(|mut row| {
                row.value = arithmetic(kernel, scalar, row.value)?;
                row.labels.remove("__name__");
                Ok(row)
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        (Value::Vector(left), Value::Vector(right)) => {
            let left_keys: BTreeSet<_> = left.iter().map(|s| matching_key(&s.labels)).collect();
            let right_keys: BTreeSet<_> = right.iter().map(|s| matching_key(&s.labels)).collect();
            match kernel {
                B::Or => left
                    .into_iter()
                    .chain(
                        right
                            .into_iter()
                            .filter(|r| !left_keys.contains(&matching_key(&r.labels))),
                    )
                    .collect(),
                B::And => left
                    .into_iter()
                    .filter(|s| right_keys.contains(&matching_key(&s.labels)))
                    .collect(),
                B::Unless => left
                    .into_iter()
                    .filter(|s| !right_keys.contains(&matching_key(&s.labels)))
                    .collect(),
                _ => {
                    anyhow::ensure!(
                        left_keys.len() == left.len() && right_keys.len() == right.len(),
                        "non-unique vector match"
                    );
                    let right: BTreeMap<_, _> = right
                        .into_iter()
                        .map(|s| (matching_key(&s.labels), s.value))
                        .collect();
                    let mut out = Vec::new();
                    for row in left {
                        let key = matching_key(&row.labels);
                        if let Some(value) = right.get(&key) {
                            out.push(ExactSample {
                                labels: key,
                                value: arithmetic(kernel, row.value, *value)?,
                            });
                        }
                    }
                    out
                }
            }
        }
        _ => anyhow::bail!("binary operator cannot consume a range vector"),
    };
    Ok(Value::Vector(vector))
}
