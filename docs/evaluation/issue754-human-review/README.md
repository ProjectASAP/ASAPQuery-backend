# Issue #754 plans for human review

These are the ten **actual selected plans** exported by #728's existing Level 1
fixture, not hand-written expected plans. This artifact does not record human
approval. No plan behavior or test assertion was changed for this export.

Backend source revision is in [source-commit.txt](source-commit.txt); Planner is
pinned to `c27cd14b8e052ce1f4641c619488ad539ad71f56`. The source revision differs
from the export execution revision only by inherited documentation merges.

Each page contains the full Planner operation payloads, dependency edges, output
column indices/types, timing, sort expressions, persisted boundaries, maintenance
windows and the installed query adapter. Raw JSON and DOT are retained beside it.
`fallback` in a Planner source payload is an IR tag: it does not by itself prove
that execution forwards to an exact backend. Inspect the bound QueryPlan and
Level 2 provenance to determine execution behavior.

## Review order

1. Follow source → transformation → grouping/window → readout for each query.
2. Check persisted producer nodes against read bindings and pane coverage.
3. For topk-rate, inspect the Sort input and column index, not just descending.
4. Check guarantees and admission evidence in raw JSON for approximate queries.

The topk-rate export explicitly sorts `Column(1)` descending, partitioned by
column 2 (`label_0`). Its producer is `FinalizeExactAccumulator` over per-series
Rate state, with column 1 named `value`. The QueryPlan adapter has an implicit
value sort and does not itself serialize an explicit sort-key expression.
This exposes the relevant mapping for review; it is not an additional assertion.

The grouped-temporal-sum cost-reversal test covers only that query. The other
selected plans use fixture costs; these exports do not prove production-optimal
placement or an exhaustive search over maintenance/query-time alternatives.

Legacy `residual` module/type names still exist in code. They are not presented
here as a new architectural layer; renaming or removing that adapter is separate
from the bound-query SDS migration.

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
