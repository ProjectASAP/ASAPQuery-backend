# Accuracy metadata

The control plane and data plane adapt sketch parameters to ASAPPlanner's
`DefaultAccuracyModel::sketch_guarantee`. The backend workspace's
`asap_types::accuracy` module only projects that guarantee into the existing
`epsilon`, `delta`, and `kind` response fields; it does not own family formulas.

CMS, HLL, KLL (including HydraKLL), and DDSketch use this shared adapter.
Parameter aliases and defaults remain at the source boundary. The installed
`AggregationConfig` and its fingerprinted wire fields are unchanged.
`AccumulatorSpec` is not used for accuracy extraction because its construction
normalization differs (for example, KLL's integer range and DDSketch's accepted
aliases and validation).

Compared with the previous metadata:

- CMS failure probability now follows Planner's `exp(-depth)` convention.
- KLL uses Planner's DataSketches empirical 99th-percentile rank-error fit.
- HLL's epsilon is relative standard error, and its failure probability is
  unknown: JSON contains `"delta": null`, and the text summary says `δ=unknown`.
  Clients must not interpret null as zero. Numeric delta values remain numbers.

GOS still adds its staleness allowance to approximate epsilon without changing
failure probability. Exact aggregates are unaffected. CountSketch and heap
retention remain backend-specific; CMS-with-heap uses the shared CMS guarantee
before applying the retention allowance. An accuracy envelope containing a
segment with unknown confidence also reports unknown confidence.

The legacy `/api/v1/plan` response no longer includes the disconnected
`plan_summary` estimate. Its `transmission_costs` remain available. Removing the
summary planner does not implement resource-budget-aware placement in v2.
