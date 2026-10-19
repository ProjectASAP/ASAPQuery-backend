Scale: factor 50, 10000 series, one sample every 15000 ms, 90 min of data, seed 1.

| Run | Process | Cores | CPU core-seconds | Peak RSS GiB | Mean RSS GiB | Wall s |
|---|---|---|---|---|---|---|
| baseline | prometheus | 0-3 | 98.5600 | 0.1422 | 0.1217 | 86.5 |
| asap | prometheus | 0-3 | 32.9900 | 0.1352 | 0.1201 | 277.9 |
| asap | data_plane | 4-7 | 249.9600 | 1.6942 | 1.3586 | 277.9 |

| Query | Evals | By plan | Forwarded | Baseline = reference | Plan = reference | ASAP meets target | Labels differ | Max abs err | Max rel err | Baseline p50/p99 ms | ASAP p50/p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| example1/q1 | 481 | 481 | 0 | 481 | 481 | 481 | 0 | 0.000e0 | 0.000e0 | 46.62 / 74.17 | 168.78 / 1700.55 |
| example1/q2 | 481 | 481 | 0 | 481 | 481 | 481 | 481 | 0.000e0 | 0.000e0 | 46.10 / 74.32 | 134.55 / 202.66 |
| example3b/q1 | 81 | 81 | 0 | 81 | 81 | 81 | 0 | 3.474e2 | 2.045e-1 | 99.91 / 156.85 | 373.91 / 545.72 |
| example4b/q1 | 9 | 4 | 5 | 9 | 4 | 9 | 0 | 1.542e2 | 2.610e-1 | 363.20 / 533.20 | 436.93 / 665.98 |

Stage 3 selected `P195-m40` ({"MatAtIngestion":23,"NotMaterialized":61}). Predicted over the 90 min of data, against the measured run:

| Plan | Predicted CPU core-seconds | Predicted mean memory GiB | Predicted peak memory GiB |
|---|---|---|---|
| selected (`P195-m40`) | 0.1622 | 0.922605 | 1.064542 |
| nothing materialized (``) | 0.0000 | - | - |
