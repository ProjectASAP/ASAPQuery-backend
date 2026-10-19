//! A deterministic, seeded generator of the Kubernetes control-plane series
//! the rule set reads, and the `DataWorkload` facts it implies.
//!
//! Every metric family is a cross product of label dimensions (target
//! labels, then the metric's own), plus labels picked per series from a
//! list. Dimensions marked as scaled grow with [`Scale::factor`]. Values
//! depend only on the seed, the series and the sample time, so the same
//! scale and seed always produce the same samples.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use serde_json::{json, Value};

/// How large a generated dataset is.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Scale {
    /// Multiplies the cardinality of every scaled dimension (apiserver and
    /// etcd instances, nodes, resources).
    pub factor: usize,
    /// Milliseconds between two samples of a series.
    pub sample_interval_ms: i64,
    /// First sample time (Unix ms).
    pub start_ms: i64,
    /// Samples are generated for `[start_ms, start_ms + duration_ms]`.
    pub duration_ms: i64,
    pub seed: u64,
    /// Probability that a counter resets to 0 at one sample.
    pub counter_reset_probability: f64,
}

/// How a family's samples evolve.
#[derive(Debug, Clone)]
enum Shape {
    /// A counter whose per-series rate (per second) is log-uniform in the range.
    Counter { rate: (f64, f64) },
    /// The same value at every sample.
    Const(f64),
    /// 1 with this probability at each sample, else 0.
    Flag(f64),
    /// A fresh log-normal value at every sample around a per-series median.
    LogNormal { median: (f64, f64), sigma: f64 },
    /// A random walk clamped to `[lo, hi]`.
    Walk { lo: f64, hi: f64 },
    /// Starts in `start` and changes by a per-series slope per second.
    Linear {
        start: (f64, f64),
        slope: (f64, f64),
    },
    /// `kube_node_status_condition`: 1 for `Ready/true` and for every other
    /// condition's `false`, else 0.
    Condition,
    /// A classic histogram: `_bucket` (with `le`), `_sum` and `_count`.
    /// Observations arrive at a per-series rate and are log-normal around a
    /// per-series median.
    Histogram {
        rate: (f64, f64),
        median: (f64, f64),
        sigma: f64,
        buckets: &'static [f64],
    },
}

#[derive(Debug, Clone)]
enum DimValues {
    Fixed(Vec<String>),
    /// `prefix{i}{suffix}` for `i < base * factor`.
    Scaled {
        prefix: &'static str,
        suffix: &'static str,
        base: usize,
    },
}

#[derive(Debug, Clone)]
enum DimKind {
    /// Multiplies the series count.
    Cross,
    /// One value per series, picked by hash.
    Pick,
    /// The value at the same index as the named cross dimension.
    Follows(&'static str),
}

#[derive(Debug, Clone)]
struct Dim {
    name: &'static str,
    values: DimValues,
    kind: DimKind,
}

fn fixed(name: &'static str, values: &[&str]) -> Dim {
    Dim {
        name,
        values: DimValues::Fixed(values.iter().map(|v| v.to_string()).collect()),
        kind: DimKind::Cross,
    }
}

fn pick(name: &'static str, values: &[&str]) -> Dim {
    Dim {
        kind: DimKind::Pick,
        ..fixed(name, values)
    }
}

fn scaled(name: &'static str, prefix: &'static str, suffix: &'static str, base: usize) -> Dim {
    Dim {
        name,
        values: DimValues::Scaled {
            prefix,
            suffix,
            base,
        },
        kind: DimKind::Cross,
    }
}

fn follows(
    name: &'static str,
    of: &'static str,
    prefix: &'static str,
    suffix: &'static str,
) -> Dim {
    Dim {
        name,
        values: DimValues::Scaled {
            prefix,
            suffix,
            base: usize::MAX,
        },
        kind: DimKind::Follows(of),
    }
}

#[derive(Debug, Clone)]
struct Family {
    name: &'static str,
    shape: Shape,
    dims: Vec<Dim>,
}

const RESOURCES: &[&str] = &[
    "pods",
    "nodes",
    "services",
    "configmaps",
    "secrets",
    "deployments",
    "events",
    "leases",
];
const APISERVER_LATENCY: &[f64] = &[
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.0,
    4.0,
    8.0,
    15.0,
    30.0,
    60.0,
    f64::INFINITY,
];
const CERT_EXPIRY: &[f64] = &[
    600.0,
    1800.0,
    3600.0,
    14400.0,
    86400.0,
    172800.0,
    345600.0,
    604800.0,
    2592000.0,
    7776000.0,
    15552000.0,
    31104000.0,
    f64::INFINITY,
];
const ETCD_PEER_RTT: &[f64] = &[
    0.0001,
    0.0002,
    0.0004,
    0.0008,
    0.0016,
    0.0032,
    0.0064,
    0.0128,
    0.0256,
    0.0512,
    0.1024,
    0.2048,
    0.4096,
    0.8192,
    f64::INFINITY,
];
const ETCD_DISK: &[f64] = &[
    0.001,
    0.002,
    0.004,
    0.008,
    0.016,
    0.032,
    0.064,
    0.128,
    0.256,
    0.512,
    1.024,
    2.048,
    4.096,
    8.192,
    f64::INFINITY,
];
const SECONDS: &[f64] = &[
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    f64::INFINITY,
];

fn apiserver(mut own: Vec<Dim>) -> Vec<Dim> {
    let mut dims = vec![
        fixed("cluster", &["c1"]),
        fixed("job", &["apiserver"]),
        scaled("instance", "10.0.0.", ":6443", 3),
    ];
    dims.append(&mut own);
    dims
}

fn etcd(mut own: Vec<Dim>) -> Vec<Dim> {
    let mut dims = vec![
        fixed("cluster", &["c1"]),
        fixed("job", &["kube-etcd"]),
        fixed("endpoint", &["http-metrics"]),
        scaled("instance", "10.0.1.", ":2381", 3),
        follows("pod", "instance", "etcd-", ""),
    ];
    dims.append(&mut own);
    dims
}

fn kubelet(mut own: Vec<Dim>) -> Vec<Dim> {
    let mut dims = vec![
        fixed("cluster", &["c1"]),
        fixed("job", &["kubelet"]),
        fixed("metrics_path", &["/metrics"]),
        scaled("instance", "node-", ":10250", 10),
        follows("node", "instance", "node-", ""),
    ];
    dims.append(&mut own);
    dims
}

fn kube_state_metrics(mut own: Vec<Dim>) -> Vec<Dim> {
    let mut dims = vec![
        fixed("cluster", &["c1"]),
        fixed("job", &["kube-state-metrics"]),
        fixed("instance", &["kube-state-metrics:8080"]),
        scaled("node", "node-", "", 10),
    ];
    dims.append(&mut own);
    dims
}

fn counter(lo: f64, hi: f64) -> Shape {
    Shape::Counter { rate: (lo, hi) }
}

fn histogram(rate: (f64, f64), median: (f64, f64), buckets: &'static [f64]) -> Shape {
    Shape::Histogram {
        rate,
        median,
        sigma: 0.8,
        buckets,
    }
}

/// Every family the apiserver, etcd and kubelet rules read.
fn catalog() -> Vec<Family> {
    let f = |name, shape, dims| Family { name, shape, dims };
    let verbs = || {
        fixed(
            "verb",
            &["GET", "LIST", "WATCH", "POST", "PUT", "PATCH", "DELETE"],
        )
    };
    let resource = || Dim {
        name: "resource",
        values: DimValues::Fixed(RESOURCES.iter().map(|r| r.to_string()).collect()),
        kind: DimKind::Cross,
    };
    let peers = || scaled("To", "member-", "", 3);
    let grpc_methods = || {
        fixed(
            "grpc_method",
            &["Range", "Put", "Txn", "LeaseGrant", "Watch", "Defragment"],
        )
    };
    let grpc_service = || {
        pick(
            "grpc_service",
            &[
                "etcdserverpb.KV",
                "etcdserverpb.Lease",
                "etcdserverpb.Watch",
                "etcdserverpb.Maintenance",
            ],
        )
    };
    let grpc_type = || pick("grpc_type", &["unary", "unary", "unary", "bidi_stream"]);
    vec![
        // apiserver
        f(
            "apiserver_request_total",
            counter(0.01, 20.0),
            apiserver(vec![
                verbs(),
                resource(),
                fixed("code", &["200", "201", "404", "409", "500", "503"]),
                pick("scope", &["resource", "namespace", "cluster"]),
                fixed("component", &["apiserver"]),
            ]),
        ),
        f(
            "apiserver_request_sli_duration_seconds",
            histogram((0.05, 10.0), (0.02, 0.6), APISERVER_LATENCY),
            apiserver(vec![
                fixed("verb", &["GET", "LIST", "POST", "PUT", "PATCH", "DELETE"]),
                resource(),
                pick("scope", &["resource", "namespace", "cluster"]),
                pick("subresource", &["", "", "", "status", "log", "exec"]),
                fixed("component", &["apiserver"]),
            ]),
        ),
        f(
            "apiserver_request_terminations_total",
            counter(0.0005, 0.05),
            apiserver(vec![
                fixed("verb", &["GET", "LIST", "POST", "PUT"]),
                resource(),
                pick("code", &["429", "504"]),
            ]),
        ),
        f(
            "apiserver_client_certificate_expiration_seconds",
            histogram((0.01, 0.5), (3.0e6, 3.0e7), CERT_EXPIRY),
            apiserver(vec![]),
        ),
        f(
            "aggregator_unavailable_apiservice",
            Shape::Flag(0.02),
            apiserver(vec![
                fixed(
                    "name",
                    &[
                        "v1beta1.metrics.k8s.io",
                        "v1.apps",
                        "v1.batch",
                        "v1.coordination.k8s.io",
                        "v1.networking.k8s.io",
                    ],
                ),
                pick("namespace", &["kube-system", "monitoring", ""]),
            ]),
        ),
        f(
            "aggregator_unavailable_apiservice_total",
            counter(0.0001, 0.01),
            apiserver(vec![
                fixed(
                    "name",
                    &[
                        "v1beta1.metrics.k8s.io",
                        "v1.apps",
                        "v1.batch",
                        "v1.coordination.k8s.io",
                        "v1.networking.k8s.io",
                    ],
                ),
                pick("reason", &["MissingEndpoints", "FailedDiscoveryCheck"]),
            ]),
        ),
        f("up", Shape::Flag(0.995), apiserver(vec![])),
        // etcd
        f("up", Shape::Flag(0.995), etcd(vec![])),
        f("etcd_server_has_leader", Shape::Flag(0.999), etcd(vec![])),
        f(
            "etcd_server_leader_changes_seen_total",
            counter(0.00001, 0.001),
            etcd(vec![]),
        ),
        f(
            "etcd_server_proposals_failed_total",
            counter(0.00001, 0.01),
            etcd(vec![]),
        ),
        f(
            "etcd_network_peer_sent_failures_total",
            counter(0.0001, 0.05),
            etcd(vec![peers()]),
        ),
        f(
            "etcd_network_peer_round_trip_time_seconds",
            histogram((0.1, 2.0), (0.002, 0.02), ETCD_PEER_RTT),
            etcd(vec![peers()]),
        ),
        f(
            "etcd_disk_wal_fsync_duration_seconds",
            histogram((5.0, 50.0), (0.002, 0.05), ETCD_DISK),
            etcd(vec![]),
        ),
        f(
            "etcd_disk_backend_commit_duration_seconds",
            histogram((1.0, 20.0), (0.005, 0.1), ETCD_DISK),
            etcd(vec![]),
        ),
        f(
            "etcd_mvcc_db_total_size_in_bytes",
            Shape::Linear {
                start: (5.0e8, 2.0e9),
                slope: (10.0, 2000.0),
            },
            etcd(vec![]),
        ),
        f(
            "etcd_mvcc_db_total_size_in_use_in_bytes",
            Shape::Linear {
                start: (2.0e8, 1.5e9),
                slope: (-500.0, 1000.0),
            },
            etcd(vec![]),
        ),
        f(
            "etcd_server_quota_backend_bytes",
            Shape::Const(8_589_934_592.0),
            etcd(vec![]),
        ),
        f(
            "grpc_server_handled_total",
            counter(0.001, 50.0),
            etcd(vec![
                grpc_type(),
                grpc_service(),
                grpc_methods(),
                fixed(
                    "grpc_code",
                    &[
                        "OK",
                        "Unknown",
                        "Unavailable",
                        "DeadlineExceeded",
                        "ResourceExhausted",
                    ],
                ),
            ]),
        ),
        f(
            "grpc_server_handling_seconds",
            histogram((0.1, 50.0), (0.001, 0.05), SECONDS),
            etcd(vec![grpc_type(), grpc_service(), grpc_methods()]),
        ),
        // kubelet
        f("up", Shape::Flag(0.995), kubelet(vec![])),
        f("kubelet_node_name", Shape::Const(1.0), kubelet(vec![])),
        f(
            "kubelet_running_pods",
            Shape::Walk { lo: 5.0, hi: 110.0 },
            kubelet(vec![]),
        ),
        f(
            "kubelet_pleg_relist_duration_seconds",
            histogram((0.5, 1.5), (0.005, 0.05), SECONDS),
            kubelet(vec![]),
        ),
        f(
            "kubelet_pod_worker_duration_seconds",
            histogram((0.01, 1.0), (0.01, 0.5), SECONDS),
            kubelet(vec![fixed("operation_type", &["create", "update", "sync"])]),
        ),
        f(
            "kubelet_evictions",
            counter(0.00001, 0.0005),
            kubelet(vec![fixed(
                "eviction_signal",
                &["memory.available", "nodefs.available", "imagefs.available"],
            )]),
        ),
        f(
            "kubelet_certificate_manager_client_ttl_seconds",
            Shape::Linear {
                start: (1.0e6, 3.0e7),
                slope: (-1.0, -1.0),
            },
            kubelet(vec![]),
        ),
        f(
            "kubelet_certificate_manager_server_ttl_seconds",
            Shape::Linear {
                start: (1.0e6, 3.0e7),
                slope: (-1.0, -1.0),
            },
            kubelet(vec![]),
        ),
        f(
            "kubelet_certificate_manager_client_expiration_renew_errors",
            counter(0.000001, 0.0001),
            kubelet(vec![]),
        ),
        f(
            "kubelet_server_expiration_renew_errors",
            counter(0.000001, 0.0001),
            kubelet(vec![]),
        ),
        // The planner-layering examples' metrics.
        f(
            "http_requests_total",
            counter(0.1, 100.0),
            vec![
                fixed("job", &["api", "web", "worker", "db", "cache"]),
                scaled("instance", "10.1.0.", ":8080", 20),
            ],
        ),
        f(
            "latency_ms",
            Shape::LogNormal {
                median: (5.0, 200.0),
                sigma: 0.6,
            },
            vec![
                scaled("service", "svc-", "", 10),
                fixed(
                    "instance",
                    &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
                ),
            ],
        ),
        // kube-state-metrics, joined with kubelet rules on node
        f(
            "kube_node_status_condition",
            Shape::Condition,
            kube_state_metrics(vec![
                fixed(
                    "condition",
                    &[
                        "Ready",
                        "MemoryPressure",
                        "DiskPressure",
                        "PIDPressure",
                        "NetworkUnavailable",
                    ],
                ),
                fixed("status", &["true", "false", "unknown"]),
            ]),
        ),
        f(
            "kube_node_spec_unschedulable",
            Shape::Flag(0.01),
            kube_state_metrics(vec![]),
        ),
        f(
            "kube_node_spec_taint",
            Shape::Const(1.0),
            kube_state_metrics(vec![
                pick(
                    "key",
                    &[
                        "node-role.kubernetes.io/control-plane",
                        "node.kubernetes.io/unreachable",
                        "dedicated",
                    ],
                ),
                pick("effect", &["NoSchedule", "NoExecute", "PreferNoSchedule"]),
            ]),
        ),
        f(
            "kube_node_status_capacity",
            Shape::Const(110.0),
            kube_state_metrics(vec![fixed("resource", &["cpu", "memory", "pods"])]),
        ),
    ]
}

/// SplitMix64: a stateless hash of `x`, uniform over `u64`.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// A uniform number in `[0, 1)` determined by its arguments.
fn uniform(seed: u64, a: u64, b: u64) -> f64 {
    let h = mix(seed ^ mix(a.wrapping_mul(0x1000_0000_01b3) ^ mix(b)));
    (h >> 11) as f64 / (1u64 << 53) as f64
}

fn log_uniform(u: f64, (lo, hi): (f64, f64)) -> f64 {
    (lo.ln() + u * (hi.ln() - lo.ln())).exp()
}

/// The standard normal CDF (Abramowitz and Stegun 7.1.26).
fn normal_cdf(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.327_591_1 * x.abs() / std::f64::consts::SQRT_2);
    let y = 1.0
        - (((((1.061_405_429 * t - 1.453_152_027) * t) + 1.421_413_741) * t - 0.284_496_736) * t
            + 0.254_829_592)
            * t
            * (-(x * x) / 2.0).exp();
    if x >= 0.0 {
        0.5 * (1.0 + y)
    } else {
        0.5 * (1.0 - y)
    }
}

/// One generated base series: a plain series, or a histogram's buckets,
/// `_sum` and `_count`.
struct BaseSeries {
    id: u64,
    shape: Shape,
    /// Output series: one, or `buckets + 2` for a histogram.
    outputs: Vec<Arc<BTreeMap<String, String>>>,
    rate_per_s: f64,
    median: f64,
    start: f64,
    slope: f64,
    /// Running state: counter value, or the histogram's cumulative bucket
    /// counts then sum then count.
    state: Vec<f64>,
}

/// A series' labels and value at one sample time.
pub struct GeneratedSample<'a> {
    pub labels: &'a Arc<BTreeMap<String, String>>,
    pub value: f64,
}

pub struct Generator {
    pub scale: Scale,
    series: Vec<BaseSeries>,
    /// Series count per metric name, after expanding histograms.
    pub series_per_metric: BTreeMap<String, u64>,
    /// Declared type per metric name (`counter` or `gauge`).
    pub metric_types: BTreeMap<String, &'static str>,
}

impl Generator {
    /// Generates only the families whose output metric names `wanted`
    /// accepts.
    pub fn new(scale: Scale, wanted: impl Fn(&str) -> bool) -> Self {
        let mut series = Vec::new();
        let mut series_per_metric = BTreeMap::new();
        let mut metric_types = BTreeMap::new();
        for (family_index, family) in catalog().into_iter().enumerate() {
            let names: Vec<(String, &'static str)> = match &family.shape {
                Shape::Histogram { .. } => ["_bucket", "_sum", "_count"]
                    .iter()
                    .map(|s| (format!("{}{s}", family.name), "counter"))
                    .collect(),
                Shape::Counter { .. } => vec![(family.name.to_string(), "counter")],
                _ => vec![(family.name.to_string(), "gauge")],
            };
            if !names.iter().any(|(n, _)| wanted(n)) {
                continue;
            }
            for (name, kind) in &names {
                metric_types.insert(name.clone(), *kind);
            }
            for (index, labels) in expand(&family.dims, scale.factor).into_iter().enumerate() {
                let id = ((family_index as u64) << 40) | index as u64;
                let u = |k: u64| uniform(scale.seed, id, k);
                let (rate_per_s, median) = match &family.shape {
                    Shape::Counter { rate } => (log_uniform(u(1), *rate), 0.0),
                    Shape::Histogram { rate, median, .. } => {
                        (log_uniform(u(1), *rate), log_uniform(u(2), *median))
                    }
                    Shape::LogNormal { median, .. } => (0.0, log_uniform(u(2), *median)),
                    _ => (0.0, 0.0),
                };
                let (start, slope) = match &family.shape {
                    Shape::Linear { start, slope } => (
                        start.0 + u(3) * (start.1 - start.0),
                        slope.0 + u(4) * (slope.1 - slope.0),
                    ),
                    Shape::Walk { lo, hi } => (lo + u(3) * (hi - lo), 0.0),
                    _ => (0.0, 0.0),
                };
                let with_name = |name: &str, le: Option<f64>| {
                    let mut l = labels.clone();
                    l.insert("__name__".into(), name.into());
                    if let Some(le) = le {
                        l.insert("le".into(), format_le(le));
                    }
                    Arc::new(l)
                };
                let (outputs, state) = match &family.shape {
                    Shape::Histogram { buckets, .. } => {
                        let mut outputs: Vec<_> = buckets
                            .iter()
                            .map(|le| with_name(&names[0].0, Some(*le)))
                            .collect();
                        outputs.push(with_name(&names[1].0, None));
                        outputs.push(with_name(&names[2].0, None));
                        let n = outputs.len();
                        (outputs, vec![0.0; n])
                    }
                    _ => (vec![with_name(&names[0].0, None)], vec![start]),
                };
                for (name, _) in &names {
                    let count = if name.ends_with("_bucket") {
                        outputs.len() as u64 - 2
                    } else {
                        1
                    };
                    *series_per_metric.entry(name.clone()).or_insert(0) += count;
                }
                series.push(BaseSeries {
                    id,
                    shape: family.shape.clone(),
                    outputs,
                    rate_per_s,
                    median,
                    start,
                    slope,
                    state,
                });
            }
        }
        Self {
            scale,
            series,
            series_per_metric,
            metric_types,
        }
    }

    /// Sample times: `start_ms`, then every `sample_interval_ms` through
    /// `start_ms + duration_ms`.
    pub fn ticks(&self) -> impl Iterator<Item = i64> {
        let s = self.scale;
        (0..=s.duration_ms / s.sample_interval_ms)
            .map(move |k| s.start_ms + k * s.sample_interval_ms)
    }

    /// Advances every series to `time_ms` (ticks must be visited in order)
    /// and returns their samples.
    pub fn step(&mut self, time_ms: i64) -> Vec<GeneratedSample<'_>> {
        let scale = self.scale;
        let tick = ((time_ms - scale.start_ms) / scale.sample_interval_ms) as u64;
        let dt = scale.sample_interval_ms as f64 / 1000.0;
        let elapsed = (time_ms - scale.start_ms) as f64 / 1000.0;
        let mut out = Vec::new();
        for s in &mut self.series {
            let u = |k: u64| uniform(scale.seed ^ tick.wrapping_mul(0x9e37), s.id, k);
            let reset = tick > 0 && u(10) < scale.counter_reset_probability;
            match &s.shape {
                Shape::Counter { .. } => {
                    if tick > 0 {
                        s.state[0] = if reset {
                            0.0
                        } else {
                            s.state[0] + (s.rate_per_s * dt * (0.5 + u(11))).round()
                        };
                    }
                }
                Shape::Const(v) => s.state[0] = *v,
                Shape::Flag(p) => s.state[0] = f64::from(u(12) < *p),
                Shape::LogNormal { sigma, .. } => {
                    // Box-Muller from two uniforms of this series and tick.
                    let z = (-2.0 * (1.0 - u(15)).ln()).sqrt()
                        * (2.0 * std::f64::consts::PI * u(16)).cos();
                    s.state[0] = (s.median * (sigma * z).exp() * 1000.0).round() / 1000.0;
                }
                Shape::Walk { lo, hi } => {
                    if tick > 0 {
                        s.state[0] =
                            (s.state[0] + (u(13) - 0.5) * (hi - lo) * 0.02).clamp(*lo, *hi);
                    }
                }
                Shape::Linear { .. } => s.state[0] = (s.start + s.slope * elapsed).max(0.0),
                Shape::Condition => {
                    let labels = &s.outputs[0];
                    let ready = labels["condition"] == "Ready";
                    let status = labels["status"].as_str();
                    s.state[0] = f64::from(if ready {
                        status == "true"
                    } else {
                        status == "false"
                    });
                }
                Shape::Histogram { buckets, sigma, .. } => {
                    if tick > 0 {
                        let n_buckets = buckets.len();
                        if reset {
                            s.state.iter_mut().for_each(|v| *v = 0.0);
                        } else {
                            let n = (s.rate_per_s * dt * (0.5 + u(11))).round();
                            for (i, le) in buckets.iter().enumerate() {
                                let fraction = if le.is_infinite() {
                                    1.0
                                } else {
                                    normal_cdf((le / s.median).ln() / sigma)
                                };
                                s.state[i] += (n * fraction).round();
                            }
                            let mean = s.median * (sigma * sigma / 2.0).exp();
                            s.state[n_buckets] += n * mean * (0.8 + 0.4 * u(14));
                            s.state[n_buckets + 1] += n;
                        }
                    }
                }
            }
            for (labels, value) in s.outputs.iter().zip(&s.state) {
                out.push(GeneratedSample {
                    labels,
                    value: *value,
                });
            }
        }
        out
    }

    /// The Planner's `DataWorkload` for the metrics in `metrics`: series
    /// count, declared type and sample interval from the generator, and
    /// the distinct-count bound per evaluation (every series of a metric
    /// exists for the whole run, so a distinct count over its rows counts
    /// at most its series).
    pub fn data_workload(&self, metrics: &[String]) -> Value {
        let mut sources = serde_json::Map::new();
        let mut series_total = 0u64;
        for metric in metrics {
            let Some(&count) = self.series_per_metric.get(metric) else {
                continue;
            };
            series_total += count;
            sources.insert(
                metric.clone(),
                json!({
                    "series_count": count,
                    "metric_type": self.metric_types[metric],
                    "sample_interval": self.scale.sample_interval_ms,
                    "stream_norms": null,
                    "max_distinct_per_evaluation": count,
                }),
            );
        }
        let rows_per_second = series_total as f64 * 1000.0 / self.scale.sample_interval_ms as f64;
        json!({
            "arrival": {"ingesting": {"rows_per_second": rows_per_second}},
            "sources": sources,
        })
    }
}

/// Every metric name the catalog can generate.
pub fn catalog_metrics() -> Vec<String> {
    let mut names = Vec::new();
    for family in catalog() {
        match family.shape {
            Shape::Histogram { .. } => {
                for s in ["_bucket", "_sum", "_count"] {
                    names.push(format!("{}{s}", family.name));
                }
            }
            _ => names.push(family.name.to_string()),
        }
    }
    names.sort();
    names.dedup();
    names
}

/// Prometheus' `le` label format: `+Inf`, and integers without a decimal
/// point (`1`, as the rules' `le=~"1(\\.0)?"` expects).
fn format_le(le: f64) -> String {
    if le.is_infinite() {
        "+Inf".into()
    } else {
        format!("{le}")
    }
}

/// The label sets of a family's series: the cross product of its cross
/// dimensions, with followed and picked labels added.
fn expand(dims: &[Dim], factor: usize) -> Vec<BTreeMap<String, String>> {
    let values = |d: &Dim| -> Vec<String> {
        match &d.values {
            DimValues::Fixed(v) => v.clone(),
            DimValues::Scaled {
                prefix,
                suffix,
                base,
            } => (0..base * factor)
                .map(|i| format!("{prefix}{i}{suffix}"))
                .collect(),
        }
    };
    type Row<'a> = (BTreeMap<String, String>, BTreeMap<&'a str, usize>);
    let mut rows: Vec<Row> = vec![(BTreeMap::new(), BTreeMap::new())];
    for dim in dims.iter().filter(|d| matches!(d.kind, DimKind::Cross)) {
        let vals = values(dim);
        rows = rows
            .into_iter()
            .flat_map(|(labels, indices)| {
                vals.iter().enumerate().map(move |(i, v)| {
                    let mut labels = labels.clone();
                    let mut indices = indices.clone();
                    labels.insert(dim.name.to_string(), v.clone());
                    indices.insert(dim.name, i);
                    (labels, indices)
                })
            })
            .collect();
    }
    rows.into_iter()
        .enumerate()
        .map(|(row, (mut labels, indices))| {
            for dim in dims {
                match (&dim.kind, &dim.values) {
                    (DimKind::Follows(of), DimValues::Scaled { prefix, suffix, .. }) => {
                        labels.insert(dim.name.into(), format!("{prefix}{}{suffix}", indices[of]));
                    }
                    (DimKind::Pick, DimValues::Fixed(v)) => {
                        let name_hash = dim.name.bytes().fold(0u64, |h, b| mix(h ^ u64::from(b)));
                        let h = mix(row as u64 ^ name_hash);
                        labels.insert(dim.name.into(), v[(h % v.len() as u64) as usize].clone());
                    }
                    _ => {}
                }
            }
            labels.retain(|_, v| !v.is_empty());
            labels
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scale() -> Scale {
        Scale {
            factor: 1,
            sample_interval_ms: 15_000,
            start_ms: 0,
            duration_ms: 600_000,
            seed: 7,
            counter_reset_probability: 0.0,
        }
    }

    /// The same seed and scale give the same samples; counters never
    /// decrease without a reset, and histogram buckets are cumulative.
    #[test]
    fn deterministic_counters_and_cumulative_buckets() {
        let run = || {
            let mut g = Generator::new(scale(), |_| true);
            let ticks: Vec<i64> = g.ticks().collect();
            ticks
                .into_iter()
                .map(|t| g.step(t).iter().map(|s| s.value).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        };
        let (a, b) = (run(), run());
        assert_eq!(a, b);
        let mut g = Generator::new(scale(), |m| {
            m == "apiserver_request_sli_duration_seconds_bucket"
        });
        let mut previous: Option<Vec<f64>> = None;
        for t in g.ticks().collect::<Vec<_>>() {
            let samples = g.step(t);
            let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
            // Buckets of one histogram are cumulative in `le`.
            for chunk in samples.chunks(APISERVER_LATENCY.len() + 2) {
                let buckets: Vec<f64> = chunk[..APISERVER_LATENCY.len()]
                    .iter()
                    .map(|s| s.value)
                    .collect();
                assert!(buckets.windows(2).all(|w| w[0] <= w[1]), "{buckets:?}");
                assert_eq!(
                    buckets.last(),
                    Some(&chunk[APISERVER_LATENCY.len() + 1].value)
                );
            }
            if let Some(previous) = &previous {
                assert!(previous.iter().zip(&values).all(|(p, v)| v >= p));
            }
            previous = Some(values);
        }
    }
}
