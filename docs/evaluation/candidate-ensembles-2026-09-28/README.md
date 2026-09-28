# Individual and ensemble candidate execution

Every admitted executable candidate passed selection, typed deployment installation,
Remote Write ingestion, precompute drain and comparison with Prometheus. This is
fixture execution evidence, not production cost validation or human plan approval.

| Workload | Candidate runs |
| --- | --- |
| query:spatial-sum | 1 |
| query:spatial-topk | 1 |
| query:spatial-quantile | 1 |
| query:temporal-sum | 1 |
| query:temporal-quantile | 1 |
| query:temporal-rate | 1 |
| query:grouped-rate | 3 |
| query:grouped-temporal-sum | 2 |
| query:topk-rate | 2 |
| query:quantile-ratio | 1 |
| shared-rate | 2 |
| shared-quantiles | 1 |
| full-ensemble | 3 |

Total: 20 candidate runs; 52 query comparisons.

For each target, only synthetic prices change. Production selection must return
its exact manifest. An ensemble is installed once and ingested once per candidate;
all member queries run against that installation. Rejected candidates remain in
`results.json.gz`. Coverage is the exposed, admitted fixture inventory, not an
exhaustive Cartesian product of theoretical physical plans.

Validation also passed: three Level 1 tests, the complete Level 2 ranking test,
14 harness contract tests, three additional harness tests, and strict all-target Clippy.

## Reproduce

Run `make candidates` from `promql-compliance/runner`. The committed Compose
configuration enables durable state required by maintenance-stage candidates.
The initial in-memory run failed three candidate trials with
`immutable maintenance requires durable input state`. The isolated replay and
complete durable rerun pass; no candidate was skipped or tolerance relaxed.

## Provenance and artifacts

- Runner code built at `b2d521fe8ca6eed95d1f3e8431a7b1df670b7658`.
- Durable Compose fixture committed at `d393b1bf`; local run mounted the existing
  binary instead of rebuilding the image. The mounted data-plane binary was
  built at `ed944356`; its runtime source is unchanged in the tested revision.
- Planner: `176c1bd565e0c400f9a35996c2bd65de32475e72`.
- Data-plane SHA256: `32a89c8f2091633a629e7fd1a49dc264447069bb95cfd123554f1db9ad8311e8`.
- Runner SHA256: `3bb04c99d9e382d54945efbefb316edce9305b7ea8c2bed1643da0ee80e6d5f0`.

Files retain quoted snapshots, selected plans, typed installations, responses and
service logs. Decompress with `gzip -dc <file>`. Paths inside raw reports refer
to the original run directory; the same relative paths are preserved here.
`SHA256SUMS` covers the retained compressed artifacts.
