Scale: factor 10, 2000 series, one sample every 15000 ms, 90 min of data, seed 1.

| Run | Process | Cores | CPU core-seconds | Peak RSS GiB | Mean RSS GiB | Wall s |
|---|---|---|---|---|---|---|
| baseline | prometheus | 0-3 | 21.6900 | 0.0928 | 0.0867 | 19.3 |
| asap | prometheus | 0-3 | 22.0000 | 0.1288 | 0.0992 | 72.0 |
| asap | data_plane | 4-7 | 53.3500 | 0.3355 | 0.1114 | 72.0 |

| Query | Evals | By plan | Forwarded | Baseline = reference | Plan = reference | ASAP meets target | Labels differ | Max abs err | Max rel err | Baseline p50/p99 ms | ASAP p50/p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| example1/q1 | 481 | 481 | 0 | 481 | 481 | 481 | 0 | 0.000e0 | 0.000e0 | 10.79 / 22.44 | 46.64 / 63.21 |
| example1/q2 | 481 | 481 | 0 | 481 | 481 | 481 | 481 | 0.000e0 | 0.000e0 | 11.04 / 20.04 | 46.41 / 64.15 |
| example3b/q1 | 81 | 81 | 0 | 81 | 81 | 81 | 0 | 3.247e2 | 2.045e-1 | 22.61 / 37.32 | 139.22 / 168.13 |
| example4b/q1 | 9 | 9 | 0 | 9 | 9 | 9 | 0 | 7.859e2 | 1.384e0 | 97.98 / 127.34 | 1216.54 / 1389.98 |

Stage 3 selected `P141-m8` ({"MatAtQueryKeptAcrossEvaluations":12,"NotMaterialized":43}). Predicted over the 90 min of data, against the measured run:

| Plan | Predicted CPU core-seconds | Predicted mean memory GiB | Predicted peak memory GiB |
|---|---|---|---|
| selected (`P141-m8`) | 0.0719 | 0.000075 | 0.026868 |
| nothing materialized (`P141`) | 0.1281 | 0.000000 | 0.026868 |
