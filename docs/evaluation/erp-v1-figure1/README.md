# ERP analytical versus empirical cost: Figure 1 v1

These are ASAPQuery-backend evaluation artifacts produced on 2026-09-10 by
the sketch-bench `scripts/run_erp_v1_figure1.sh` runner at commit `013a70b`.
They are stored here because Figure 1 evaluates the backend/planner cost-model
decision, while sketch-bench is only the measurement producer. They are not
smoke-test output.

## Workload and query

Each point inserts a one-million-event `i64` key stream into
`sketch_oxide::frequency::CountMinSketch`, merges two identically configured
sketches, and issues point-frequency queries against the observed keys. The
accuracy pass compares every point-frequency estimate with exact counts.

The 18 scenarios cross CMS rows `{3,5}`, columns `{256,1024,4096}`, and three
input shapes:

- uniform over 10,000 keys;
- Zipf power law with `s=1.1` over 10,000 keys;
- the same Zipf stream with two seeded random 100,000-row intervals receiving
  50% additional resampled traffic (1.1M effective events).

Seed is 42. Each cost point has 2 warmup runs and 7 measured runs. Accuracy has
1 warmup and 1 deterministic measured pass. `BENCH_WARMUP_SECS=1`; compilation
uses Cargo release mode with native CPU tuning. The raw artifact contains 72
JSONL records: 54 atomic cost records and 18 accuracy records.

## Models

The analytical baseline sees only algorithmic work: update/query proportional
to CMS rows and merge proportional to rows times columns. One coefficient per
operation is calibrated at the smallest steady uniform point. It has no
distribution or burst input. The empirical model is the measured update,
merge, and query CPU per operation stored in ERP.

## Result

Across 54 plotted atomic-operation points, analytical cost has 11.55% median
absolute percentage error and 0.984 Spearman rank correlation with measured
cost. The median errors are 6.25% for update, 73.21% for merge, and 3.93% for
query. The merge result is the important v1 discrepancy: asymptotic counter
count alone poorly predicts the fixed and cache-sensitive merge cost, which is
then multiplied by every pane merge in a sliding-window plan.

This figure validates the narrower Section 2 claim: an analytical model is a
useful fallback and often preserves broad ordering, while measured atomic ERP
costs materially improve quantitative window-cost estimates. It does not yet
claim an end-to-end planner speedup or accuracy improvement over AutoSketch;
those require the shared multi-query workload experiment.

Artifacts:

- `docs/evaluation/erp-v1-figure1/raw.jsonl`: immutable benchmark records with configs, workload, samples,
  CPU/wall/memory, and accuracy;
- `docs/evaluation/erp-v1-figure1/figure1-points.csv`: every plotted analytical/measured point;
- `docs/evaluation/erp-v1-figure1/figure1-analytical-vs-empirical.png`: Figure 1 v1;
- `docs/evaluation/erp-v1-figure1/summary.json`: aggregate result and execution provenance.
