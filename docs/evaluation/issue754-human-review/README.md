# Issue #754 plans for human review

These are actual plans exported by the Level 1 fixture. No human approval is
recorded. Selected plans and successfully compiled candidate plans are retained
as JSON/DOT; each page shows accuracy rejections and deployment cost decisions.

Backend source revision is in [source-commit.txt](source-commit.txt). Planner:
`3ebe195260fa6e16e226183ee50938078bb8359f`.

## Current implementation boundary

These exports describe the current Backend implementation. They do **not** prove
completion of the Planner physical-candidate installation/execution handoff.
Logical provenance and installed native physical programs are shown separately.
Spatial TopK binds a complete current-series snapshot and runs a persisted native
Sort → Limit program. Planner also exposes a CountSketch-with-heap physical
candidate over the same snapshot. This fixture has no enforced distinct-item
bound or score-separation proof, so its heap candidate is explicitly rejected
before pricing. The inherited `native_snapshot_topk` tests cover certified heap
admission, cost-dependent selection and analytical workspace costing; a separate
real-process test covers Remote Write and heap HTTP results. Signed samples do
not authorize CMS. Rate → heap storage E2E and the complete physical-candidate
handoff remain outstanding. Other PromQL paths still use the documented Backend
adapter representation.

Candidate discovery preserves each root's admitted computations. Deployment
currently evaluates single-root substitutions in a preferred workload context;
it does not exhaustively enumerate joint workload combinations. Fixture costs
are not evidence of production-optimal placement.

## Review order

1. Check source, value transformations, grouping, windows and readouts.
2. Compare candidate rejection reasons with the query's accuracy requirement.
3. Check persisted boundaries, definition IDs and requested pane coverage.
4. For topk-rate, check the actual rate-value sort expression and partition keys.
5. Compare costs and rejected candidates before reviewing the selected adapter.

The strict spatial-quantile fixture rejects the default KLL guarantee. A separate
assertion with relaxed accuracy verifies that both KLL and DDSketch reach costing
when admitted. The grouped-temporal-sum test checks cost-dependent placement;
it does not establish this for every query. Sort-key mutation tests reject ranking
by timestamp or label instead of the finalized rate value.

| Query | PromQL |
| --- | --- |
| [grouped-rate](grouped-rate.md) | `sum by (label_0) (rate(data[1m]))` |
| [grouped-temporal-sum](grouped-temporal-sum.md) | `sum by (label_0) (sum_over_time(data[1m]))` |
| [quantile-ratio](quantile-ratio.md) | `quantile_over_time(0.9, data[1m]) / quantile_over_time(0.5, data[1m])` |
| [spatial-quantile](spatial-quantile.md) | `quantile by (label_0) (0.9, data)` |
| [spatial-sum](spatial-sum.md) | `sum by (label_0) (data)` |
| [spatial-topk](spatial-topk.md) | `topk by (label_0) (3, data)` |
| [temporal-quantile](temporal-quantile.md) | `quantile_over_time(0.9, data[1m])` |
| [temporal-rate](temporal-rate.md) | `rate(data[1m])` |
| [temporal-sum](temporal-sum.md) | `sum_over_time(data[1m])` |
| [topk-rate](topk-rate.md) | `topk by (label_0) (3, rate(data[1m]))` |

## Reproduce

From the #728 worktree:

```sh
ASAP_LEVEL1_ARTIFACT_DIR=/tmp/issue754-plans \
  cargo test -p control_plane --test issue754_level1 --locked -- --test-threads=1
```

The test also exports enumerated candidates into `candidates/`; the ten selected
plans are at the top level. Copy those `.json` and `.dot` files here and run
`python3 docs/evaluation/issue754-human-review/render.py` to regenerate the pages.
Actual execution and recovery evidence is maintained in the downstream
[bound SDS validation report](https://github.com/ProjectASAP/ASAPQuery-backend/blob/test/issue754-level3/docs/evaluation/bound-sds-2026-09-26/README.md).
