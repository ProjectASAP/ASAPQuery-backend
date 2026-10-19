Scale: factor 50, 10000 series, one sample every 15000 ms, 90 min of data, seed 1.

| Run | Process | Cores | CPU core-seconds | Peak RSS GiB | Mean RSS GiB | Wall s |
|---|---|---|---|---|---|---|
| baseline | prometheus | 0-3 | 99.0900 | 0.1423 | 0.1226 | 86.6 |
| asap | prometheus | 0-3 | 103.5400 | 0.3038 | 0.1655 | 361.8 |
| asap | data_plane | 4-7 | 272.6100 | 1.6875 | 0.7443 | 361.8 |

| Query | Evals | By plan | Forwarded | Baseline = reference | Plan = reference | ASAP meets target | Labels differ | Max abs err | Max rel err | Baseline p50/p99 ms | ASAP p50/p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| example1/q1 | 481 | 481 | 0 | 481 | 481 | 481 | 0 | 0.000e0 | 0.000e0 | 46.67 / 72.27 | 248.70 / 328.06 |
| example1/q2 | 481 | 481 | 0 | 481 | 481 | 481 | 481 | 0.000e0 | 0.000e0 | 46.77 / 75.10 | 237.99 / 304.52 |
| example3b/q1 | 81 | 81 | 0 | 81 | 81 | 81 | 0 | 3.474e2 | 2.045e-1 | 103.59 / 142.44 | 722.35 / 1141.93 |
| example4b/q1 | 9 | 9 | 0 | 9 | 9 | 9 | 0 | 1.587e3 | 1.640e0 | 406.85 / 531.11 | 4699.11 / 6399.12 |

Stage 3 selected `P141-m8` ({"MatAtQueryKeptAcrossEvaluations":12,"NotMaterialized":43}). Predicted over the 90 min of data, against the measured run:

| Plan | Predicted CPU core-seconds | Predicted mean memory GiB | Predicted peak memory GiB |
|---|---|---|---|
| selected (`P141-m8`) | 0.3605 | 0.000373 | 0.142384 |
| nothing materialized (`P141`) | 0.6413 | 0.000000 | 0.142384 |
