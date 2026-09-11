## Why

Preserve the real Alibaba dashboard measurements collected before the user requested stopping the experiment.

## What

Add the Rust comparison harness, data preparation and evaluation design, plus **15 completed service runs out of 54 planned runs**. Include raw plans/measurements, dataset provenance, a partial report and performance plots. Execution is stopped; this is not the completed experiment.

## How

Prepare all 240 official three-minute archives (12 hours, 2,646,269,187 retained observations). Calibrate on a 10M-event reservoir from hours 0–6; replay hours 6–12 without sampling after one hour of warmup. Compare six baselines on count and Top-3 dashboards with 1m/10m/60m windows refreshed every minute.

## Before this PR

The dashboard comparison branch had no executable Alibaba observation-contract experiment or these real-data measurements.

## After this PR

Reviewers can inspect the [partial report](https://github.com/ProjectASAP/ASAPQuery-backend/blob/eval/alibaba-dashboard-observations/tools/autosketch-comparison/results/alibaba-dashboard-v1/partial-report.md), plots and raw per-query samples, including planning, update, merge, readout, payload memory and accuracy violations. This PR is stacked on #602.

Trial 0 total CPU seconds:

| Baseline | Update | Merge | Readout | Accuracy violations / 2160 |
|---|---:|---:|---:|---:|
| Exact-pane | 66.95 | 10.35 | 4.07 | 0 |
| Exact-scan | 7.90 | 0 | 2270.60 | 0 |
| Analytical sizing | 1076.88 | 10.39 | 9.68 | 1102 |
| AutoSketch CPU extension | 3287.68 | 10.72 | 1.15 | 378 |
| ERP-NoSharing | 3289.36 | 10.69 | 1.15 | 378 |
| ERP shared-or-local → Exact fallback | 67.03 | 10.23 | 3.93 | 0 |

## Verification

- All 15 completed runs pass artifact checks for event counts, endpoints, plan identity, timers, violations and shared-merge accounting; completed exact references have zero accuracy violations.
- Seven Rust correctness tests passed before execution; preparation tests are separate from real performance evidence.
- Four reporting-validation tests pass, including a regression for insignificant JSON floating-point round-trip differences; integer configuration fields remain exact.
- No performance rerun was started after the stop request. The interrupted AutoSketch third run is excluded.
- Product screenshots: not applicable; measured plots are included instead.

## Limitations

Service repetitions are incomplete/unbalanced. Edge and latency catalogs exist, but their held-out runs were not started. The displayed table is consistently trial 0, not a three-trial aggregate. ERP uses a custom calibration catalog, not synthetic nearest-shape matching; the shared ERP result is exact fallback, not a successful approximate-sketch result. AutoSketch is a CPU adaptation with extensions. Analytical sizing is not full cost-model optimization. Timers measure operator regions, not deployed backend/network latency; memory is logical payload, not isolated RSS. Original datasets and oracle caches are not vendored into Git.
