# topk-rate

`topk by (label_0) (3, rate(data[1m]))`

[Raw selected plan](topk-rate.json) · [DAG DOT](topk-rate.dot)

## Planner-selected computation

IDs below are Planner node IDs; QueryPlan adapter IDs are shown separately.

Root: `4`.

```mermaid
flowchart LR
  N0["0: Source / time range"]
  N1["1: SummaryAgg {&quot;ExactAggregate&quot;:[&quot;Rate&quot;,&quot;Rate&quot;]}"]
  N2["2: FinalizeExactAccumulator"]
  N3["3: Sort"]
  N4["4: Limit"]
  N0 --> N1
  N1 --> N2
  N2 --> N3
  N3 --> N4
```

| Node | Dependencies (producer, edge role) | Timing | Operation | Output fields (index: name/type) |
| --- | --- | --- | --- | --- |
| 0 | `[]` | `{"timing":"ingestion_time","primitive":"Raw"}` | `{"source":{"TimeSeries":{"metric":"data"}},"predicates":[],"range":{"nanos":0,"secs":60}}` | `["0: ts/timestamp","1: value/float64","2: label_0/utf8"]` |
| 1 | `[[0,"Input"]]` | `{"timing":"ingestion_time","primitive":"SummaryState"}` | `{"family":{"ExactAggregate":["Rate","Rate"]},"grouping":"PerSubpopulationInstance","input":{"item":null,"weight":{"Column":"SampleValue"},"weight_domain":{"kind":"unknown_or_signed"}},"kind":"summary_agg","reduction":"PerEntity"}` | `["0: ts/timestamp","1: value/{\"ExactAggregate\":[\"Rate\",\"Rate\"]}","2: label_0/utf8"]` |
| 2 | `[[1,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"value","operation":"FinalizeExactAccumulator"}` | `["0: ts/timestamp","1: value/float64","2: label_0/utf8"]` |
| 3 | `[[2,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"value","operation":{"Sort":{"keys":[{"ascending":false,"expr":{"Column":1},"nulls_first":false}],"partition_by":[2]}}}` | `["0: ts/timestamp","1: value/float64","2: label_0/utf8"]` |
| 4 | `[[3,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"value","operation":{"Limit":{"n":3,"offset":0,"partition_by":[2]}}}` | `["0: ts/timestamp","1: value/float64","2: label_0/utf8"]` |

### Sort expressions

Node `3`: `{"keys":[{"ascending":false,"expr":{"Column":1},"nulls_first":false}],"partition_by":[2]}`

Its input is node `2`: `{"kind":"value","operation":"FinalizeExactAccumulator"}`. Column indices refer to that producer's output schema above.

## Persisted boundaries

Binding for `compat-query-0`:

```json
{
  "nodes": {
    "0": {
      "placement": "maintenance_input"
    },
    "1": {
      "placement": "materialization",
      "stored_output": 14425999489689100447
    }
  },
  "query_sink": 4,
  "query_plan_sink": 0,
  "precompute_sinks": [
    1
  ]
}
```

Stored output `14425999489689100447` → semantic definition `sds-v1:528d2a7adb528822d205d1e239daf9904249ae30c5c50c6f7288f286b451ea6c`.

### Maintenance configuration

```json
[
  {
    "aggregation_type": "Rate",
    "aggregation_sub_type": "",
    "parameters": {
      "promql_right_closed": true
    },
    "grouping_labels": {
      "labels": []
    },
    "partitioning": "per_entity",
    "aggregated_labels": {
      "labels": []
    },
    "rollup_labels": {
      "labels": []
    },
    "window_size": 60,
    "slide_interval": 10,
    "window_type": "sliding",
    "window_layout": {
      "kind": "pane",
      "pane_secs": 10
    },
    "pane_origin_ms": 0,
    "spatial_filter": "",
    "spatial_filter_normalized": "",
    "metric": "data",
    "num_aggregates_to_retain": 7,
    "table_name": null,
    "value_projection": null
  }
]
```

## Bound query execution

This is the emitted adapter representation, including dependencies, pane size, readout lookback and stored-output references. Legacy wire names are preserved so the export remains auditable.

```json
{
  "language": "prom_ql",
  "query_id": "compat-query-0",
  "canonical_query": "topk by (label_0) (3, rate(data[1m]))",
  "root": 0,
  "nodes": {
    "0": {
      "op": "logical",
      "operator": {
        "kind": "limit",
        "n": 3,
        "offset": 0,
        "grouping": {
          "labels": [
            "label_0"
          ],
          "without": false
        }
      },
      "inputs": [
        1
      ]
    },
    "1": {
      "op": "logical",
      "operator": {
        "kind": "sort",
        "descending": true,
        "grouping": {
          "labels": [
            "label_0"
          ],
          "without": false
        }
      },
      "inputs": [
        2
      ]
    },
    "2": {
      "op": "exact_readout",
      "input": 3,
      "readout": "rate"
    },
    "3": {
      "op": "read_materialization",
      "binding": {
        "stored_output_reference": {
          "stored_output_id": 14425999489689100447,
          "definition_id": "sds-v1:528d2a7adb528822d205d1e239daf9904249ae30c5c50c6f7288f286b451ea6c"
        },
        "materialization": 14425999489689100447,
        "output_grouping": {
          "mode": "per_entity"
        },
        "window_ms": 10000,
        "pane_origin_ms": 0,
        "readout_lookback_ms": 60000
      }
    }
  },
  "instant": {
    "lookback_ms": 60000,
    "full_history": false,
    "cumulative_readout": true
  },
  "fallback": "exact_backend"
}
```
