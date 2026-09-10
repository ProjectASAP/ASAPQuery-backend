# AutoSketch versus ASAPPlanner: recurring Top-K dashboard

These are measured release-build results, not smoke-test output. The raw
per-trial JSON and plotting program are committed beside this report.

## Workload

The dashboard evaluates `topk(10, count_over_time(events[W]))` for
`W = {1m, 5m, 15m, 60m}`. A new 30-second pane arrives before every refresh,
so the 100 executions are 100 advancing dashboard refreshes rather than 100
reads of frozen state. The first 60 minutes (120 panes) are calibration and
the next 50 minutes are held out. Each approximate query merges all pane
sketches covering its window and performs one Top-K readout on the merged
state. Ties at the tenth exact rank are treated as equally valid members.

The synthetic workload has exactly 10,000,000 events, 100,000 possible keys,
and a truncated Zipf distribution with exponent 1.1. The trace workload uses
the official Google Cluster Data 2019 `instance_usage` shard, maps
`collection_id` to the Top-K key, uses `start_time` for 30-second panes, and
selects the densest consecutive 220-pane interval (1,971 events and 683 keys).
The latter choice avoids evaluating an arbitrary mostly empty interval while
preserving chronological order.

All approximate methods share a 128 KiB per-sketch constraint and use the
same `asap_sketchlib` CMS/CountSketch-with-heap implementations. AutoSketch is
run independently for all four queries. ASAP-NoSharing maintains four
independent ERP configurations; ASAP-ERP maintains one shared pane stream.
The analytical baseline calls ASAPPlanner's theoretical Top-K sizing function.

## Median of three trials

| Dataset | Method | Planning (s) | Dashboard runtime (s) | Payload (MiB) | Recall@10 | NDCG@10 | Violations / 400 |
|---|---|---:|---:|---:|---:|---:|---:|
| Synthetic | AutoSketch-PerQuery | 22.644 | 8.674 | 1.329 | 0.952 | 0.993 | 27 |
| Synthetic | ASAP-ERP-NoSharing | 0.000020 | 7.370 | 3.322 | 1.000 | 1.000 | 0 |
| Synthetic | ASAP-ERP | 0.000020 | 2.241 | 2.461 | 1.000 | 1.000 | 0 |
| Synthetic | ASAP-Analytical | 0.000002 | 1.867 | 1.282 | 1.000 | 1.000 | 0 |
| Synthetic | Exact-Production | <0.000001 | 14.605 | 75.947 | 1.000 | 1.000 | 0 |
| Google | AutoSketch-PerQuery | 0.222 | 0.913 | 1.325 | 0.917 | 0.958 | 39 |
| Google | ASAP-ERP-NoSharing | 0.000012 | 0.307 | 3.322 | 0.997 | 0.999 | 0 |
| Google | ASAP-ERP | 0.000012 | 0.305 | 2.461 | 0.997 | 0.999 | 0 |
| Google | ASAP-Analytical | 0.000001 | 0.108 | 1.282 | 0.988 | 0.994 | 3 |
| Google | Exact-Production | <0.000001 | 0.008 | 0.015 | 1.000 | 1.000 | 0 |

“Dashboard runtime” is update + eviction + merge + Top-K readout for sketch
methods and grouped exact-query time for Exact-Production. Exact truth
computation used only for scoring is recorded separately in JSON and excluded
from sketch runtime. Logical payload excludes allocator and string overhead.

## Interpretation and limitations

The repeated-query benefit appears in maintenance: on synthetic data, sharing
reduces the ERP runtime from 7.37s to 2.24s and retained payload from 3.32 MiB
to 2.46 MiB without changing accuracy. AutoSketch spends 22.64s planning
because candidate benchmarking is repeated for each window. Its individually
smaller configurations save memory but violate the 0.8 Recall@10 target on 27
of 400 held-out refresh/query pairs at the median trial.

This v1 runner exercises real sketch update/merge/readout and ASAPPlanner's
analytical sizing. Its ERP profile is a fixed catalog entry selected under the
resource constraint; it does not yet invoke the complete backend control-plane
compiler to choose pane width or reconstruct a sketch-bench artifact. Therefore
the measurements validate dashboard execution and sharing, but must not yet be
claimed as an end-to-end validation of shape matching or automatic window-layout
selection. The Google result covers one dense interval of one shard, not the
entire 2019 trace.
