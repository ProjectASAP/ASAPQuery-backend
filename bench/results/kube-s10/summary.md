Scale: factor 10, 22860 series, one sample every 15000 ms, 90 min of data, seed 1.

| Run | Process | Cores | CPU core-seconds | Peak RSS GiB | Mean RSS GiB | Wall s |
|---|---|---|---|---|---|---|
| baseline | prometheus | 0-3 | 144.5900 | 0.2351 | 0.1998 | 119.9 |
| asap | prometheus | 0-3 | 152.9300 | 0.2387 | 0.1974 | 379.5 |
| asap | data_plane | 4-7 | 265.8700 | 1.2796 | 0.8595 | 379.5 |

| Query | Evals | By plan | Forwarded | Baseline = reference | Plan = reference | ASAP meets target | Labels differ | Max abs err | Max rel err | Baseline p50/p99 ms | ASAP p50/p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cluster_verb_scope_le:apiserver_request_sli_duration_seconds_bucket:increase1h | 27 | 27 | 0 | 27 | 27 | 27 | 0 | 0.000e0 | 0.000e0 | 469.13 / 678.08 | 6331.85 / 12154.87 |
| KubeAggregatedAPIErrors | 161 | 161 | 0 | 161 | 161 | 161 | 0 | 0.000e0 | 0.000e0 | 2.31 / 4.29 | 55.70 / 109.88 |
| KubeAggregatedAPIDown | 161 | 161 | 0 | 161 | 161 | 161 | 0 | 0.000e0 | 0.000e0 | 3.00 / 4.02 | 87.61 / 153.32 |
| KubeNodeNotReady | 161 | 161 | 0 | 161 | 161 | 161 | 0 | 0.000e0 | 0.000e0 | 2.34 / 4.20 | 55.95 / 92.83 |
| KubeletTooManyPods | 161 | 161 | 0 | 161 | 161 | 161 | 0 | 0.000e0 | 0.000e0 | 4.77 / 12.99 | 39.41 / 61.96 |
| KubeNodeEviction | 161 | 161 | 0 | 161 | 161 | 161 | 0 | 0.000e0 | 0.000e0 | 8.40 / 21.08 | 128.30 / 202.61 |

Stage 3 selected `P159-m485` ({"MatAtQuery":1,"MatAtQueryKeptAcrossEvaluations":92,"NotMaterialized":232}). Predicted over the 90 min of data, against the measured run:

| Plan | Predicted CPU core-seconds | Predicted mean memory GiB | Predicted peak memory GiB |
|---|---|---|---|
| selected (`P159-m485`) | 0.6653 | 0.002555 | 0.002831 |
| nothing materialized (`P159`) | 7.4144 | 0.000000 | 0.002832 |
