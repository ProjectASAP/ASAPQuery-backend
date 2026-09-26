use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Suite {
    pub name: String,
    #[serde(default)]
    pub comparison_defaults: Policy,
    pub queries: Vec<Query>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub name: String,
    pub expr: String,
    #[serde(default)]
    pub instant_offsets_seconds: Vec<f64>,
    pub range: Option<Range>,
    pub comparison: Option<Policy>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Range {
    pub start_offset_seconds: f64,
    pub end_offset_seconds: f64,
    pub step_seconds: f64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub value_tolerance: Option<Tolerance>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tolerance {
    pub relative: Option<f64>,
    pub absolute: Option<f64>,
}
impl Query {
    pub fn policy(&self, defaults: &Policy) -> Policy {
        let base = defaults.value_tolerance.clone().unwrap_or_default();
        let own = self
            .comparison
            .as_ref()
            .and_then(|p| p.value_tolerance.clone())
            .unwrap_or_default();
        Policy {
            value_tolerance: Some(Tolerance {
                relative: own.relative.or(base.relative),
                absolute: own.absolute.or(base.absolute),
            }),
        }
    }
}
impl Suite {
    pub fn load(path: &Path) -> Result<Self> {
        Self::parse(&std::fs::read_to_string(path)?)
    }
    pub fn parse(text: &str) -> Result<Self> {
        let suite: Self = yaml(text)?;
        ensure!(
            !suite.name.is_empty() && !suite.queries.is_empty(),
            "empty suite"
        );
        let mut names = BTreeSet::new();
        for q in &suite.queries {
            ensure!(
                !q.name.is_empty() && !q.expr.is_empty() && names.insert(&q.name),
                "empty/duplicate query"
            );
            ensure!(
                !q.instant_offsets_seconds.is_empty() || q.range.is_some(),
                "query {} has no evaluations",
                q.name
            );
            if let Some(r) = &q.range {
                ensure!(
                    [r.start_offset_seconds, r.end_offset_seconds, r.step_seconds]
                        .iter()
                        .all(|n| n.is_finite())
                        && r.end_offset_seconds > r.start_offset_seconds
                        && r.step_seconds > 0.,
                    "invalid query range"
                );
            }
            for t in &q.instant_offsets_seconds {
                ensure!(
                    t.is_finite()
                        && q.range.as_ref().is_none_or(
                            |r| *t >= r.start_offset_seconds && *t <= r.end_offset_seconds
                        ),
                    "invalid instant time"
                );
            }
            let t = q
                .policy(&suite.comparison_defaults)
                .value_tolerance
                .unwrap();
            ensure!(
                [t.relative, t.absolute]
                    .into_iter()
                    .flatten()
                    .all(|n| n.is_finite() && n >= 0.),
                "invalid tolerance"
            );
        }
        Ok(suite)
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    pub name: String,
    pub series: Vec<Series>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Series {
    pub metric: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub samples: Vec<Sample>,
    pub generated_samples: Option<Generated>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sample {
    pub offset_seconds: f64,
    pub value: f64,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Generated {
    pub start_offset_seconds: f64,
    pub end_offset_seconds: f64,
    pub step_seconds: f64,
    pub multiplier: f64,
    pub base: f64,
    pub modulo: f64,
}
impl Dataset {
    pub fn load(path: &Path) -> Result<Self> {
        Self::parse(&std::fs::read_to_string(path)?)
    }
    pub fn parse(text: &str) -> Result<Self> {
        let mut data: Self = yaml(text)?;
        ensure!(
            !data.name.is_empty() && !data.series.is_empty(),
            "empty dataset"
        );
        let mut seen = BTreeSet::new();
        for s in &mut data.series {
            ensure!(
                !s.metric.is_empty()
                    && !s.labels.contains_key("__name__")
                    && seen.insert((s.metric.clone(), s.labels.clone())),
                "invalid/duplicate series"
            );
            if let Some(g) = s.generated_samples.take() {
                ensure!(
                    s.samples.is_empty(),
                    "explicit and generated samples both supplied"
                );
                ensure!(
                    [
                        g.start_offset_seconds,
                        g.end_offset_seconds,
                        g.step_seconds,
                        g.multiplier,
                        g.base,
                        g.modulo
                    ]
                    .iter()
                    .all(|n| n.is_finite())
                        && g.end_offset_seconds >= g.start_offset_seconds
                        && g.step_seconds > 0.
                        && g.modulo > 0.,
                    "invalid generated samples"
                );
                let count = (g.end_offset_seconds - g.start_offset_seconds) / g.step_seconds;
                ensure!(
                    count < 10_000_000. && (count - count.round()).abs() < 1e-9,
                    "invalid generated sample grid"
                );
                s.samples = (0..=count.round() as usize)
                    .map(|i| {
                        let offset = g.start_offset_seconds + i as f64 * g.step_seconds;
                        Sample {
                            offset_seconds: offset,
                            value: g.multiplier * (g.base + offset % g.modulo),
                        }
                    })
                    .collect();
            }
            ensure!(!s.samples.is_empty(), "empty series");
            let mut previous = None;
            for sample in &s.samples {
                ensure!(
                    sample.offset_seconds.is_finite() && sample.value.is_finite(),
                    "nonfinite sample"
                );
                let ms = offset_ms(sample.offset_seconds)?;
                ensure!(
                    previous.is_none_or(|p| ms > p),
                    "samples collide or are unordered at millisecond precision"
                );
                previous = Some(ms);
            }
        }
        Ok(data)
    }
    pub fn uniform_demand(&self) -> Result<(f64, u64, usize)> {
        let mut cadence = None;
        let mut count = 0;
        for s in &self.series {
            ensure!(
                s.samples.len() >= 2,
                "source {} needs two samples for cadence",
                s.metric
            );
            for pair in s.samples.windows(2) {
                let step = offset_ms(pair[1].offset_seconds)? - offset_ms(pair[0].offset_seconds)?;
                ensure!(
                    step > 0 && cadence.is_none_or(|p| p == step),
                    "benefit fixture needs uniform source cadence"
                );
                cadence = Some(step);
            }
            count += s.samples.len();
        }
        let ms = cadence.context("empty data population")? as u64;
        Ok((self.series.len() as f64 * 1000. / ms as f64, ms, count))
    }
}
pub fn offset_ms(seconds: f64) -> Result<i64> {
    let ms = (seconds * 1000.).round();
    ensure!(
        ms.is_finite() && ms > i64::MIN as f64 && ms < i64::MAX as f64,
        "timestamp overflow"
    );
    Ok(ms as i64)
}
pub fn at_ms(base: i64, seconds: f64) -> Result<i64> {
    base.checked_add(offset_ms(seconds)?)
        .context("timestamp overflow")
}

fn yaml<T: serde::de::DeserializeOwned>(text: &str) -> Result<T> {
    let mut value: serde_yaml::Value = serde_yaml::from_str(text)?;
    value.apply_merge()?;
    Ok(serde_yaml::from_value(value)?)
}
