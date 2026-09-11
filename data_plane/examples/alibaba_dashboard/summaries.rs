use super::input::Event;
use asap_sketchlib::{CountMinSketch, CountMinSketchWithHeap, CountSketchWithHeap, DDSketch, KLL};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Config {
    Cms {
        depth: usize,
        width: usize,
        heap: usize,
    },
    Cs {
        depth: usize,
        width: usize,
        heap: usize,
    },
    Kll {
        capacity: usize,
    },
    Dd {
        milli_alpha: u32,
    },
    Exact,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Workload {
    Service,
    Edge,
    Latency,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Query {
    Count,
    Top3,
    Quantile(f64),
    Ratio,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Panel {
    pub window: usize,
    pub query: Query,
}
pub fn panels(workload: Workload) -> Vec<Panel> {
    [1, 10, 60]
        .into_iter()
        .flat_map(|window| {
            let queries = if workload == Workload::Latency {
                vec![
                    Query::Quantile(0.5),
                    Query::Quantile(0.75),
                    Query::Quantile(0.9),
                    Query::Quantile(0.95),
                    Query::Quantile(0.99),
                    Query::Ratio,
                ]
            } else {
                vec![Query::Count, Query::Top3]
            };
            queries
                .into_iter()
                .map(move |query| Panel { window, query })
        })
        .collect()
}
pub fn grid(w: Workload) -> Vec<Config> {
    if w == Workload::Latency {
        [128, 256, 512, 1024, 2048]
            .into_iter()
            .map(|capacity| Config::Kll { capacity })
            .chain(
                [5, 10, 20, 50, 100]
                    .into_iter()
                    .map(|milli_alpha| Config::Dd { milli_alpha }),
            )
            .collect()
    } else {
        let mut out = Vec::new();
        for depth in [3, 5, 7] {
            for width in [128, 256, 512, 1024] {
                out.push(Config::Cms {
                    depth,
                    width,
                    heap: 0,
                });
                for heap in [16, 32, 64] {
                    out.push(Config::Cms { depth, width, heap });
                    out.push(Config::Cs { depth, width, heap });
                }
            }
        }
        out
    }
}
pub fn family(c: Config) -> &'static str {
    match c {
        Config::Cms { heap: 0, .. } => "CmsPoint",
        Config::Cms { .. } => "Cms",
        Config::Cs { .. } => "CountSketch",
        Config::Kll { .. } => "Kll",
        Config::Dd { .. } => "DdSketch",
        Config::Exact => "Exact",
    }
}
pub fn coordinates(c: Config) -> Vec<usize> {
    match c {
        Config::Cms {
            depth,
            width,
            heap: 0,
        } => vec![depth, width],
        Config::Cms { depth, width, heap } | Config::Cs { depth, width, heap } => {
            vec![depth, width, heap]
        }
        Config::Kll { capacity } => vec![capacity],
        Config::Dd { milli_alpha } => vec![milli_alpha as usize],
        Config::Exact => vec![],
    }
}

#[derive(Clone)]
pub enum Quantiles {
    Kll(KLL<f64>),
    Dd { positive: DDSketch, zeros: u64 },
    Exact { values: Vec<f64>, sorted: bool },
}
impl Quantiles {
    fn new(c: Config, seed: u64) -> Self {
        match c {
            Config::Kll { capacity } => Self::Kll(KLL::init_kll_with_seed(capacity as i32, seed)),
            Config::Dd { milli_alpha } => Self::Dd {
                positive: DDSketch::new(milli_alpha as f64 / 1000.),
                zeros: 0,
            },
            Config::Exact => Self::Exact {
                values: vec![],
                sorted: true,
            },
            _ => unreachable!(),
        }
    }
    fn update(&mut self, value: f64) -> anyhow::Result<()> {
        anyhow::ensure!(value.is_finite() && value >= 0., "invalid latency");
        match self {
            Self::Kll(s) => s.update(&value),
            Self::Dd { positive, zeros } => {
                if value == 0. {
                    *zeros += 1;
                } else {
                    let before = positive.get_count();
                    positive.add(&value);
                    anyhow::ensure!(
                        positive.get_count() == before + 1,
                        "DDSketch rejected positive latency"
                    );
                }
            }
            Self::Exact { values, sorted } => {
                values.push(value);
                *sorted = false;
            }
        }
        Ok(())
    }
    fn merge(&mut self, other: &Self) -> anyhow::Result<()> {
        match (self, other) {
            (Self::Kll(a), Self::Kll(b)) => a.merge(b),
            (
                Self::Dd {
                    positive: a,
                    zeros: x,
                },
                Self::Dd {
                    positive: b,
                    zeros: y,
                },
            ) => {
                a.merge(b).map_err(anyhow::Error::msg)?;
                *x += y;
            }
            (Self::Exact { values: a, sorted }, Self::Exact { values: b, .. }) => {
                a.extend_from_slice(b);
                *sorted = false;
            }
            _ => anyhow::bail!("incompatible quantile summaries"),
        };
        Ok(())
    }
    fn count(&self) -> usize {
        match self {
            Self::Kll(s) => s.count(),
            Self::Dd { positive, zeros } => positive.get_count() as usize + *zeros as usize,
            Self::Exact { values, .. } => values.len(),
        }
    }
    fn at_rank(&mut self, rank: usize) -> f64 {
        match self {
            Self::Kll(s) => s.quantile_cached((rank + 1) as f64 / s.count() as f64),
            Self::Dd { positive, zeros } => {
                if rank < *zeros as usize {
                    0.
                } else {
                    let index = rank - *zeros as usize;
                    let n = positive.get_count() as usize;
                    positive
                        .get_value_at_quantile(if index + 1 == n {
                            1.
                        } else {
                            (index as f64 + 0.5) / n as f64
                        })
                        .unwrap()
                }
            }
            Self::Exact { values, sorted } => {
                if !*sorted {
                    values.sort_unstable_by(f64::total_cmp);
                    *sorted = true;
                }
                values[rank]
            }
        }
    }
    pub fn quantile(&mut self, q: f64) -> f64 {
        let n = self.count();
        if n == 0 {
            return f64::NAN;
        }
        let rank = q * (n - 1) as f64;
        let low = rank.floor() as usize;
        let high = rank.ceil() as usize;
        let a = self.at_rank(low);
        let b = self.at_rank(high);
        a + (b - a) * (rank - low as f64)
    }
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Kll(s) => s.wire_items().len() * 8 + s.wire_levels().len() * 4,
                Self::Dd { positive, .. } => positive.store_counts().len() * 8,
                Self::Exact { values, .. } => values.len() * 8,
            }
    }
}

#[derive(Clone)]
pub enum State {
    CmsPoint {
        sketch: CountMinSketch,
        keys: HashSet<u64>,
        config: Config,
    },
    Cms {
        sketch: CountMinSketchWithHeap,
        keys: HashSet<u64>,
        config: Config,
    },
    Cs {
        sketch: CountSketchWithHeap,
        keys: HashSet<u64>,
        config: Config,
    },
    Counts(HashMap<u64, u64>),
    Quantiles {
        groups: HashMap<u64, Quantiles>,
        config: Config,
        seed: u64,
    },
}
pub type Answer = HashMap<u64, f64>;
pub type Cdfs = HashMap<u64, Vec<(f64, u64)>>;
impl State {
    pub fn exact_cdfs(&mut self) -> anyhow::Result<Cdfs> {
        let Self::Quantiles { groups, .. } = self else {
            return Ok(HashMap::new());
        };
        let mut out = HashMap::new();
        for (key, summary) in groups {
            let Quantiles::Exact { values, sorted } = summary else {
                anyhow::bail!("CDF export requires exact values");
            };
            if !*sorted {
                values.sort_unstable_by(f64::total_cmp);
                *sorted = true;
            }
            let mut entries: Vec<(f64, u64)> = Vec::new();
            for (i, value) in values.iter().enumerate() {
                if let Some(last) = entries.last_mut().filter(|last| last.0 == *value) {
                    last.1 = i as u64 + 1;
                } else {
                    entries.push((*value, i as u64 + 1));
                }
            }
            out.insert(*key, entries);
        }
        Ok(out)
    }
    pub fn new(c: Config, w: Workload, seed: u64) -> Self {
        if w == Workload::Latency {
            return Self::Quantiles {
                groups: HashMap::new(),
                config: c,
                seed,
            };
        }
        match c {
            Config::Cms {
                depth,
                width,
                heap: 0,
            } => Self::CmsPoint {
                sketch: CountMinSketch::new(depth, width),
                keys: HashSet::new(),
                config: c,
            },
            Config::Cms { depth, width, heap } => Self::Cms {
                sketch: CountMinSketchWithHeap::new(depth, width, heap),
                keys: HashSet::new(),
                config: c,
            },
            Config::Cs { depth, width, heap } => Self::Cs {
                sketch: CountSketchWithHeap::new(depth, width, heap),
                keys: HashSet::new(),
                config: c,
            },
            Config::Exact => Self::Counts(HashMap::new()),
            _ => unreachable!(),
        }
    }
    pub fn update(&mut self, e: Event, w: Workload, enumerate: bool) -> anyhow::Result<()> {
        let key = if w == Workload::Edge {
            if e.upstream == u32::MAX {
                return Ok(());
            }
            ((e.upstream as u64) << 32) | e.downstream as u64
        } else {
            e.downstream as u64
        };
        match self {
            Self::CmsPoint { sketch, keys, .. } => {
                sketch.update(&key.to_string(), 1.);
                keys.insert(key);
            }
            Self::Cms { sketch, keys, .. } => {
                sketch.update(&key.to_string(), 1.);
                if enumerate {
                    keys.insert(key);
                }
            }
            Self::Cs { sketch, keys, .. } => {
                sketch.update(&key.to_string(), 1.);
                if enumerate {
                    keys.insert(key);
                }
            }
            Self::Counts(counts) => *counts.entry(key).or_default() += 1,
            Self::Quantiles {
                groups,
                config,
                seed,
            } => {
                if e.latency.is_finite() && e.latency >= 0. {
                    groups
                        .entry(key)
                        .or_insert_with(|| Quantiles::new(*config, *seed ^ key))
                        .update(e.latency)?;
                }
            }
        }
        Ok(())
    }
    pub fn merge(&mut self, b: &Self) -> anyhow::Result<()> {
        match (self, b) {
            (
                Self::CmsPoint {
                    sketch: a, keys: x, ..
                },
                Self::CmsPoint {
                    sketch: b, keys: y, ..
                },
            ) => {
                a.merge(b).map_err(anyhow::Error::msg)?;
                x.extend(y);
            }
            (
                Self::Cms {
                    sketch: a, keys: x, ..
                },
                Self::Cms {
                    sketch: b, keys: y, ..
                },
            ) => {
                a.merge(b).map_err(anyhow::Error::msg)?;
                x.extend(y);
            }
            (
                Self::Cs {
                    sketch: a, keys: x, ..
                },
                Self::Cs {
                    sketch: b, keys: y, ..
                },
            ) => {
                a.merge(b).map_err(anyhow::Error::msg)?;
                x.extend(y);
            }
            (Self::Counts(a), Self::Counts(b)) => {
                for (k, v) in b {
                    *a.entry(*k).or_default() += v;
                }
            }
            (
                Self::Quantiles {
                    groups: a,
                    config,
                    seed,
                },
                Self::Quantiles { groups: b, .. },
            ) => {
                for (k, v) in b {
                    a.entry(*k)
                        .or_insert_with(|| Quantiles::new(*config, *seed ^ *k))
                        .merge(v)?;
                }
            }
            _ => anyhow::bail!("incompatible summaries"),
        }
        Ok(())
    }
    pub fn answer(&mut self, q: Query) -> Answer {
        if matches!(q, Query::Quantile(_) | Query::Ratio) {
            if let Self::Quantiles { groups, .. } = self {
                return groups
                    .iter_mut()
                    .map(|(k, s)| {
                        (
                            *k,
                            match q {
                                Query::Quantile(p) => s.quantile(p),
                                Query::Ratio => s.quantile(0.9) / s.quantile(0.5),
                                _ => unreachable!(),
                            },
                        )
                    })
                    .collect();
            }
        }
        let mut values: Answer = match self {
            Self::CmsPoint { sketch, keys, .. } => keys
                .iter()
                .map(|k| (*k, sketch.estimate(&k.to_string())))
                .collect(),
            Self::Counts(h) => h.iter().map(|(k, v)| (*k, *v as f64)).collect(),
            Self::Cms { sketch, keys, .. } => {
                if matches!(q, Query::Count) {
                    keys.iter()
                        .map(|k| (*k, sketch.estimate(&k.to_string())))
                        .collect()
                } else {
                    sketch
                        .topk_heap_items()
                        .into_iter()
                        .map(|x| (x.key.parse().unwrap(), x.value))
                        .collect()
                }
            }
            Self::Cs { sketch, keys, .. } => {
                if matches!(q, Query::Count) {
                    keys.iter()
                        .map(|k| (*k, sketch.estimate(&k.to_string())))
                        .collect()
                } else {
                    sketch
                        .topk_heap_items()
                        .into_iter()
                        .map(|x| (x.key.parse().unwrap(), x.value))
                        .collect()
                }
            }
            _ => unreachable!(),
        };
        if matches!(q, Query::Top3) {
            let mut items: Vec<_> = values.drain().collect();
            items.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            items.truncate(3);
            items.into_iter().collect()
        } else {
            values
        }
    }
    pub fn bytes_without_enumeration(&self) -> usize {
        // Heap TopK does not need the external key universe used by count-by.
        // Point CMS always requires its universe, including for TopK readout.
        let removable = match self {
            Self::Cms { keys, .. } | Self::Cs { keys, .. } => keys.len() * 8,
            _ => 0,
        };
        self.bytes() - removable
    }
    pub fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::CmsPoint { keys, config, .. }
                | Self::Cms { keys, config, .. }
                | Self::Cs { keys, config, .. } => {
                    let (Config::Cms { depth, width, heap } | Config::Cs { depth, width, heap }) =
                        *config
                    else {
                        unreachable!()
                    };
                    depth * width * 8 + heap * 32 + keys.len() * 8
                }
                Self::Counts(h) => h.len() * 16,
                Self::Quantiles { groups, .. } => groups.values().map(|s| 8 + s.bytes()).sum(),
            }
    }
}

/// Normalize each query's explicit error target to loss <= 1. Non-finite
/// ratios are matched by IEEE class, not converted into finite values.
pub fn loss(pred: &Answer, truth: &Answer, q: Query) -> f64 {
    if truth.is_empty() {
        return if pred.is_empty() { 0. } else { 1e12 };
    }
    match q {
        Query::Top3 => {
            // Caller supplies all exact counts, preserving boundary ties.
            let mut ranked: Vec<_> = truth.values().copied().collect();
            ranked.sort_by(|a, b| b.total_cmp(a));
            let n = ranked.len().min(3);
            let cutoff = ranked[n - 1];
            let required = truth.iter().filter(|(_, v)| **v > cutoff).count();
            let strict = truth
                .iter()
                .filter(|(k, v)| **v > cutoff && pred.contains_key(k))
                .count();
            let ties = truth
                .iter()
                .filter(|(k, v)| **v == cutoff && pred.contains_key(k))
                .count();
            (1. - (strict + ties.min(n - required)) as f64 / n as f64) / 0.2
        }
        Query::Count => {
            truth
                .iter()
                .map(|(k, v)| (pred.get(k).copied().unwrap_or(0.) - v).abs())
                .sum::<f64>()
                / truth.values().sum::<f64>().max(1.)
                / 0.02
        }
        Query::Quantile(_) | Query::Ratio => truth
            .iter()
            .map(|(k, t)| {
                let Some(p) = pred.get(k) else {
                    return 1e12;
                };
                if !t.is_finite() || !p.is_finite() {
                    return if (t.is_nan() && p.is_nan()) || (t == p) {
                        0.
                    } else {
                        1e12
                    };
                }
                (p - t).abs() / t.abs().max(1.) / if matches!(q, Query::Ratio) { 0.2 } else { 0.1 }
            })
            .fold(0., f64::max),
    }
}

/// Rank distance outside the empirical tie interval, with 1/n tolerance for
/// linear interpolation of the exact finite-sample quantile convention.
pub fn rank_error(pred: &Answer, cdfs: &Cdfs, q: f64) -> (f64, f64) {
    let mut max = 0f64;
    let mut sum = 0.;
    for (key, cdf) in cdfs {
        let error = if let Some(value) = pred.get(key).filter(|v| v.is_finite()) {
            let n = cdf.last().unwrap().1 as f64;
            let left = cdf.partition_point(|(v, _)| v < value);
            let right = cdf.partition_point(|(v, _)| v <= value);
            let lower = if left == 0 {
                0.
            } else {
                cdf[left - 1].1 as f64 / n
            };
            let upper = if right == 0 {
                0.
            } else {
                cdf[right - 1].1 as f64 / n
            };
            ((lower - q).max(q - upper).max(0.) - 1. / n).max(0.)
        } else {
            1.
        };
        max = max.max(error);
        sum += error;
    }
    (max, sum / cdfs.len().max(1) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dd_preserves_zeros_and_interpolates() {
        let mut s = Quantiles::new(Config::Dd { milli_alpha: 10 }, 1);
        for v in [0., 0., 10., 10.] {
            s.update(v).unwrap();
        }
        assert_eq!(s.quantile(0.5), 5.);
        let mut other = Quantiles::new(Config::Dd { milli_alpha: 10 }, 1);
        other.update(0.).unwrap();
        s.merge(&other).unwrap();
        assert_eq!(s.quantile(0.5), 0.);
    }
    #[test]
    fn topk_ties_do_not_replace_heavy_key() {
        let truth = HashMap::from([(1, 10.), (2, 1.), (3, 1.), (4, 1.)]);
        let pred = HashMap::from([(2, 1.), (3, 1.), (4, 1.)]);
        assert!(loss(&pred, &truth, Query::Top3) > 1.);
    }
    #[test]
    fn zero_ratio_classes_are_not_silently_dropped() {
        let t = HashMap::from([(1, f64::NAN)]);
        assert_eq!(loss(&t, &t, Query::Ratio), 0.);
        assert!(loss(&HashMap::from([(1, 0.)]), &t, Query::Ratio) > 1.);
    }
}
