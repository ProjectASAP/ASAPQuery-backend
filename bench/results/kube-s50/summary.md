Scale: factor 50, 114300 series, one sample every 15000 ms, 90 min of data, seed 1.

| Run | Process | Cores | CPU core-seconds | Peak RSS GiB | Mean RSS GiB | Wall s |
|---|---|---|---|---|---|---|
| baseline | prometheus | 0-3 | 796.6000 | 0.4414 | 0.3629 | 569.2 |
| asap | prometheus | 0-3 | 883.9900 | 0.6562 | 0.4360 | 2218.6 |
| asap | data_plane | 4-7 | 1628.4300 | 6.7147 | 4.8123 | 2218.6 |

| Query | Evals | By plan | Forwarded | Baseline = reference | Plan = reference | ASAP meets target | Labels differ | Max abs err | Max rel err | Baseline p50/p99 ms | ASAP p50/p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cluster_verb_scope_le:apiserver_request_sli_duration_seconds_bucket:increase1h | 27 | 27 | 0 | 27 | 27 | 27 | 0 | 0.000e0 | 0.000e0 | 2206.93 / 2983.75 | 39025.37 / 63243.15 |
| KubeAggregatedAPIErrors | 161 | 161 | 0 | 161 | 161 | 161 | 0 | 0.000e0 | 0.000e0 | 6.71 / 11.32 | 255.58 / 560.98 |
| KubeAggregatedAPIDown | 161 | 161 | 0 | 161 | 161 | 161 | 0 | 0.000e0 | 0.000e0 | 9.88 / 16.26 | 665.14 / 1054.13 |
| KubeNodeNotReady | 161 | 161 | 0 | 161 | 161 | 161 | 0 | 0.000e0 | 0.000e0 | 8.78 / 19.49 | 267.05 / 336.10 |
| KubeletTooManyPods | 161 | 161 | 0 | 161 | 161 | 161 | 0 | 0.000e0 | 0.000e0 | 19.63 / 55.07 | 159.07 / 240.90 |
| KubeNodeEviction | 161 | 161 | 0 | 161 | 161 | 161 | 0 | 0.000e0 | 0.000e0 | 36.31 / 65.27 | 880.09 / 1250.94 |

Stage 3 selected `P159-m485` ({"MatAtQuery":1,"MatAtQueryKeptAcrossEvaluations":92,"NotMaterialized":232}). Predicted over the 90 min of data, against the measured run:

| Plan | Predicted CPU core-seconds | Predicted mean memory GiB | Predicted peak memory GiB |
|---|---|---|---|
| selected (`P159-m485`) | 3.3255 | 0.012773 | 0.014124 |
| nothing materialized (`P159`) | 37.0707 | 0.000000 | 0.014125 |
