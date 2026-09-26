# spatial-topk

`topk by (label_0) (3, data)`

[Raw selected plan](spatial-topk.json) · [DAG DOT](spatial-topk.dot)

## Planner-selected computation

IDs below are Planner node IDs; QueryPlan adapter IDs are shown separately.

Root: `2`.

```mermaid
flowchart LR
  N0["0: Source / time range"]
  N1["1: MaintainPopulation"]
  N2["2: ReadPopulation"]
  N0 --> N1
  N1 --> N2
```

| Node | Dependencies (producer, edge role) | Timing | Operation | Output fields (index: name/type) |
| --- | --- | --- | --- | --- |
| 0 | `[]` | `{"timing":"ingestion_time","primitive":"Raw"}` | `{"source":{"TimeSeries":{"metric":"data"}},"predicates":[],"range":{"nanos":0,"secs":5}}` | `["0: ts/timestamp","1: value/float64","2: label_0/utf8"]` |
| 1 | `[[0,"Input"]]` | `{"timing":"ingestion_time","primitive":"Raw"}` | `{"kind":"value","operation":{"MaintainPopulation":{"population":{"input":{"CurrentSeries":{"grouping":["label_0"],"lookback_ms":5000,"matchers":[],"metric":"data","without":false}},"max_k":3,"quantiles":false}}}}` | `["0: ts/timestamp","1: value/float64","2: label_0/utf8"]` |
| 2 | `[[1,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"value","operation":{"ReadPopulation":{"readout":{"TopK":{"k":3}}}}}` | `["0: ts/timestamp","1: value/float64","2: label_0/utf8"]` |

### Sort expressions

No standalone Sort node in this selected DAG; any ranking readout is shown in the operation table.

## Persisted boundaries

No precompute executable DAG is installed for this selected candidate.

No summary stored-output binding. Maintained current-series input, when present, is visible in the DAG and QueryPlan.

### Maintenance configuration

```json
[]
```

## Bound query execution

This is the emitted adapter representation, including dependencies, pane size, readout lookback and stored-output references. Legacy wire names are preserved so the export remains auditable.

```json
{
  "language": "prom_ql",
  "query_id": "compat-query-0",
  "canonical_query": "topk by (label_0) (3, data)",
  "root": 0,
  "nodes": {
    "0": {
      "op": "logical",
      "operator": {
        "kind": "current_series",
        "population": {
          "metric": "data",
          "matchers": [],
          "grouping": {
            "labels": [
              "label_0"
            ],
            "without": false
          },
          "lookback_ms": 5000,
          "max_input_lag_ms": 5000,
          "history_retention_ms": 0,
          "max_series": 100000,
          "max_bytes": 1073741824,
          "max_k": 3,
          "quantiles": false
        },
        "readout": {
          "kind": "top_k",
          "k": 3
        }
      },
      "inputs": []
    }
  },
  "instant": {
    "lookback_ms": 5000,
    "full_history": false,
    "cumulative_readout": true
  },
  "fallback": "exact_backend"
}
```
