# spatial-quantile

`quantile by (label_0) (0.9, data)`

[Raw selected plan](spatial-quantile.json) · [DAG DOT](spatial-quantile.dot)

## Planner-selected computation

IDs below are Planner node IDs; QueryPlan adapter IDs are shown separately.

Root: `2`.

| Node | Dependencies (producer, edge role) | Timing | Operation | Output fields (index: name/type) |
| --- | --- | --- | --- | --- |
| 0 | `[]` | `{"timing":"ingestion_time","primitive":"Raw"}` | `{"source":{"TimeSeries":{"metric":"data"}},"predicates":[],"range":{"nanos":0,"secs":5}}` | `["0: ts/timestamp","1: value/float64","2: label_0/utf8"]` |
| 1 | `[[0,"Input"]]` | `{"timing":"ingestion_time","primitive":"SummaryState"}` | `{"family":{"Sketch":[{"algorithm":"DDSketch","category":"Quantile","params":{"DDSketch":{"alpha":0.01}}},"PerSubpopulationInstance"]},"grouping":"PerSubpopulationInstance","input":{"item":null,"weight":{"Column":"SampleValue"},"weight_domain":{"kind":"unknown_or_signed"}},"kind":"summary_agg","reduction":{"Reduce":[2]}}` | `["0: label_0/utf8","1: quantile_0_9/{\"Sketch\":[{\"algorithm\":\"DDSketch\",\"category\":\"Quantile\",\"params\":{\"DDSketch\":{\"alpha\":0.01}}},\"PerSubpopulationInstance\"]}"]` |
| 2 | `[[1,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"summary_estimate","query":{"Quantile":{"q":0.9}}}` | `["0: label_0/utf8","1: quantile_0_9/float64"]` |

### Sort expressions

No standalone Sort node in this selected DAG; any ranking readout is shown in the operation table.

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
      "stored_output": 1584935399223751265
    }
  },
  "query_sink": 2,
  "query_plan_sink": 0,
  "precompute_sinks": [
    1
  ]
}
```

Stored output `1584935399223751265` → semantic definition `sds-v1:1fd0b827ec010a13a2709eacb72138e3723d1823a6eaf9aa7547679020b3b502`.

### Maintenance configuration

```json
[
  {
    "aggregation_type": "DDSketch",
    "aggregation_sub_type": "",
    "parameters": {
      "alpha": 0.01,
      "promql_right_closed": true
    },
    "grouping_labels": {
      "labels": [
        "label_0"
      ]
    },
    "partitioning": "grouped",
    "aggregated_labels": {
      "labels": []
    },
    "rollup_labels": {
      "labels": []
    },
    "window_size": 5,
    "slide_interval": 10,
    "window_type": "sliding",
    "window_layout": {
      "kind": "full_window"
    },
    "pane_origin_ms": 5000,
    "spatial_filter": "",
    "spatial_filter_normalized": "",
    "metric": "data",
    "num_aggregates_to_retain": 1,
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
  "canonical_query": "quantile by (label_0) (0.9, data)",
  "root": 0,
  "nodes": {
    "0": {
      "op": "summary_estimate",
      "input": 1,
      "query": {
        "kind": "quantile",
        "q": 0.9
      }
    },
    "1": {
      "op": "read_materialization",
      "binding": {
        "stored_output_reference": {
          "stored_output_id": 1584935399223751265,
          "definition_id": "sds-v1:1fd0b827ec010a13a2709eacb72138e3723d1823a6eaf9aa7547679020b3b502"
        },
        "full_window_slide_ms": 10000,
        "materialization": 1584935399223751265,
        "output_grouping": {
          "mode": "reduce",
          "keys": [
            "label_0"
          ]
        },
        "window_ms": 5000,
        "pane_origin_ms": 5000,
        "readout_lookback_ms": 5000
      }
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
