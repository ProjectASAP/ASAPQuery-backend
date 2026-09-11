# Cost-selected HLL evaluation against VictoriaMetrics

A fresh evaluation of the control plane's selected HLL plan reduced query latency
and CPU for one generated `distinct_over_time` workload. It did **not** reduce
memory. This is a sensitivity experiment, not coverage of the original o11ybench
queries.

The input contains 1,440,040 samples: 40 series, one hour, 100 ms sampling. The
query selects 28 held-out series:

```promql
distinct_over_time(data{label_0=~"g00000[3-9]"}[1h])
```

Calibration used separate hash populations and a predeclared HLL precision grid
of 10, 12, and 14. The measured accuracy evidence admitted precision 12 for an
empirical 5% relative-error target. This is not an epsilon-delta guarantee.
Complete candidate CPU calibration included the backend and its external exact
service. The ordinary control-plane compiler selected HLL precision 12 with an
estimated cost of 14.478 CPU seconds versus 20.920 seconds for its exact
alternative, for a demand of 60 query evaluations. The benchmark did not select
the winner.

An independent run installed that selected plan in fresh processes and compared
it with a fresh native VictoriaMetrics v1.126.0 process:

| Metric | ASAP backend + external VM | Native VM |
| --- | ---: | ---: |
| Median query latency | 13.174 ms | 62.564 ms |
| Query CPU, 60 evaluations | 0.690 s | 13.790 s |
| Full child-process lifecycle CPU (`wait4`) | 15.264 s | 16.735 s |
| Query-phase RSS | 138.6 MB combined; 84.1 MB backend | 63.7 MB |
| Observed process peak RSS | 182.2 MB backend; 72.1 MB external VM | 85.9 MB |
| Result completeness | 28/28 series, 60/60 evaluations | Reference |
| Maximum relative result error | 4.557% | Reference |

All 60 selected-plan evaluations reported warm execution, without exact subtree
RPCs. The latency improvement was 4.75×, query CPU about 20× lower, and combined
finite-run lifecycle CPU 8.79% lower. Sum of separate process peaks is not a
simultaneous deployment peak. SummaryStore reported 274,466 approximate resident
bytes; that number is not a durable storage footprint.

Both deployments used the same four CPU affinity cores in sequential quiet
measurement windows. Result caches were disabled. Input visibility validation
ran before workload queries and warmed data caches. All repetitions used the
same evaluation timestamp; this does not measure moving windows, concurrent
throughput, cold storage, or one hour of wall-clock residency. The ASAP
configuration retained and ingested a complete external VM, whose CPU and memory
are included above. Driver and separate correctness-oracle resources are
excluded. The VM cache budget was 256 MiB, not a process RSS limit.

## Reproduction artifacts

The dataset is at
`/mydata/univmon-benefit-study/datasets/100ms-1h-10g4m/`.
The artifact root is `/mydata/univmon-benefit-study/`:

- `distinct-revision-aligned-costed.json`: input to normal compiler selection.
- `distinct-revision-aligned-selected.json`: selected plan, alternatives, costs,
  catalog, and executable installation request.
- `calibration-revision-aligned-cache-off/`: fresh candidate calibration.
- `independent-cost-selected-cache-off/`: selected-plan independent execution.
- `independent-native-cache-off/`: adjacent native baseline.
- `HLL_SELECTED_RESULT.json`: machine-readable results and limitations.

Export and runtime revision:
`dd102e07fb274725edf9819ff202864878633d2d`.
Planner revision: `7e7931b581f4941ad02e4c6d90d41f2e95b2d7fc`.
Sketch library revision: `079ba44936bb74f7e0f4374936e8846266b35243`.
The result artifact also records both executable digests and the input digest.
Earlier measurements with mismatched candidate-export provenance remain in the
artifact directory; they were not reused as cost quotes for this result.

See [the calibration workflow](../../tools/o11y-execution/CALIBRATION.md).
