# Google ClusterData 2019 evaluation plan

## Status and reproducibility

The first official data shard has been downloaded and verified at
`/mydata/datasets/google-cluster-data-2019`. It is not committed because it is a
36.2 MB third-party binary. The checked-in [manifest](google-cluster-data-2019.json)
records its immutable GCS generation, byte size, SHA-256 digest, Parquet metadata,
and observed cardinalities. Run:

```sh
tools/autosketch-comparison/download_google_cluster_data_2019.sh
```

to reproduce the download and checksum verification. The source dataset is
licensed CC-BY-4.0. The official trace documentation describes eight Borg cells
covering May 2019 and about 2.4 TiB compressed in total. Downloading the complete
corpus is neither necessary for the first experiment nor possible with the
currently available 98 GiB workspace capacity.

## Included data

The initial corpus is one complete official Parquet object:

- cell: `a`;
- table: `instance_usage`;
- object: `instance_usage-000000000000.parquet.gz`;
- size: 36,204,879 bytes;
- rows: 626,337 in two row groups;
- trace-time coverage: start timestamps 300,000,000 through 2,678,998,000,000;
- identities: 44,806 collection IDs, 16,126 instance indexes, and 9,990 machine
  IDs.

Despite the `.gz` suffix, this particular official object is directly readable
Apache Parquet, not a gzip stream. The official `instance_usage` schema is also
downloaded. Its 21 physical columns cover interval timestamps and instance,
collection, allocation, and machine identities; average, maximum, and randomly
sampled CPU/memory usage; assigned/page-cache memory; CPI/MAI; sample rate; and
coarse/tail CPU distributions. Usage records normally represent non-overlapping
five-minute measurement windows, although lifecycle boundaries can shorten them.

The first experiment deliberately excludes `collection_events`,
`instance_events`, `machine_events`, and `machine_attributes`: none is needed to
construct usage-window aggregates, and joining them would confound the initial
sketch/window comparison. It also excludes the other shards and cells. Results
must therefore be labeled **cell-a/shard-0**, not “the full Google trace.”

## Data mapping

Rows are ordered by `(start_time, collection_id, instance_index)` before replay.
Trace timestamps are relative microseconds and are converted to event-time
seconds. Null metric values are dropped for that metric only; identity/timestamp
nulls are rejected. No synthetic reshuffling is used in the primary run.

The dataset drives three query classes:

| Query class | Key/value mapping | Sketch candidates | Accuracy oracle |
|---|---|---|---|
| Frequency/top-k | key=`collection_id` or `machine_id`, update=one usage record | CMS, Count Sketch | exact per-window counts/top-k |
| Quantile | value=`average_usage.cpus`, `maximum_usage.cpus`, or `average_usage.memory` | KLL, DDSketch where semantics permit | exact sorted values per window |
| Membership | key=`(collection_id, instance_index)` active in a window | Bloom | exact set; FPR on absent IDs and zero false negatives |

`sample_rate` is a measurement frequency, not a row-sampling probability, so it
is retained as a possible grouping/filter field and is not used as an inverse
probability weight. Normalized CPU and memory values remain in their published
units.

## Workload and comparisons

The primary repeated workload issues 5m, 15m, 1h, 6h, and 24h queries. Each
query fires at its window slide (5m for the first three, 30m for 6h, and 1h for
24h) after the window has complete coverage. A second workload replays a fixed
query set every five minutes to isolate recurrence. For every class and error
target, all methods receive identical rows, event-time boundaries, candidate
families, configurations, and hard total-memory budgets.

Comparisons are:

1. AutoSketch-adapted per query versus ASAP-NoSharing, isolating family and
   parameter selection.
2. ASAP-NoSharing versus ASAP-Full, isolating pane reuse across recurring
   windows.
3. Grid oracle and Exact, establishing configuration regret and accuracy.
4. Empirical ERP versus theoretical sizing, plus Hybrid profile-miss and
   distribution-drift fallbacks.

For each method, report held-out error/FPR, top-k precision and recall where
applicable, retained payload and peak RSS, sketch updates, merges, maintenance
CPU, query latency (median/p95/p99), planning/calibration cost, and fallback
rate. Trials split by time rather than randomly: earlier complete windows train
ERP, later windows evaluate it. Report both first-use cost and amortized cost.

## Expansion gates

The shard is expanded only after the pipeline passes answer-equivalence and
window-boundary tests. Expansion order is additional `instance_usage` Parquet
shards from cell `a`, then the same deterministic shard IDs from other cells.
Every expansion updates the manifest with object generations, hashes, rows,
time range, and cardinalities. Cross-cell evaluation is reported separately to
test whether an ERP learned on one distribution transfers or correctly triggers
Hybrid fallback.
