# Repeated-window performance results

Executed in release mode at backend revision `04e9ff7b590aa072917446d793c7aefce50147b6`.
The workload contains 120 one-minute panes, 5,000 updates per pane, 1,000 keys,
and recurring 1m/5m/15m/60m queries. All sketch methods use the same CMS
`width=256, depth=4`; each trial repeats all four queries ten times. Values below
are medians over seven independently generated workload trials.

| Method | Update wall time | Query wall time | Counter updates | Retained state | Logical bytes | Max normalized error |
|---|---:|---:|---:|---:|---:|---:|
| AutoSketch-PerQuery | 0.7629 s | 0.1158 s | 2,400,000 | 81 sketches | 663,552 | 0.0066 |
| ASAP-NoSharing | 0.7624 s | 0.1159 s | 2,400,000 | 81 sketches | 663,552 | 0.0066 |
| ASAP-Full | 0.1900 s | 0.1159 s | 600,000 | 60 sketches | 491,520 | 0.0066 |
| Exact raw | 0.0004 s | 0.0026 s | 600,000 appends | 300,000 keys | 2,400,000 | 0 |

ASAP-Full performs 4x fewer sketch updates than the query-local layouts and
uses 25.9% fewer logical sketch bytes, while returning the same estimates and
error. The measured update wall-time speedup is 4.02x. ASAP-NoSharing and
AutoSketch-PerQuery intentionally converge: this is the control showing that
the result comes from cross-window sharing, not a different sketch.

The exact baseline is faster for this in-memory synthetic point-query kernel but
retains 4.88x the logical bytes of ASAP-Full. It omits parsing, grouping, network,
and storage-engine costs, so it is not a database latency result. Likewise, this
runner evaluates the latest complete windows rather than launching the backend
or Planner. Raw JSON is generated outside the repository with the README command.

## Backend E2E calibration observation

A separate real Prometheus/backend run over `/mydata/metrics.txt` successfully
discovered nine candidates, calibrated all nine, and selected one materialization
shared by two queries. Its three evaluation trials correctly served all 40
occurrences through external-exact fallback: the input covers 24h minus 30s,
while the selected 24h query requires complete 30-second pane coverage. The
runtime rejected the missing earliest pane with `counter SDS requires full-pane
query coverage`. This is a useful fail-closed test, not a warm-path performance
measurement, and is therefore not mixed into the table above.
