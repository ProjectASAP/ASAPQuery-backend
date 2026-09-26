# quantile-ratio

`quantile_over_time(0.9, data[1m]) / quantile_over_time(0.5, data[1m])`

[Raw selected plan](quantile-ratio.json) · [DAG DOT](quantile-ratio.dot)

## Planner-selected computation

IDs below are Planner node IDs; QueryPlan adapter IDs are shown separately.

Root: `4`.

| Node | Dependencies (producer, edge role) | Timing | Operation | Output fields (index: name/type) |
| --- | --- | --- | --- | --- |
| 0 | `[]` | `{"timing":"ingestion_time","primitive":"Raw"}` | `{"source":{"TimeSeries":{"metric":"data"}},"predicates":[],"range":{"nanos":0,"secs":60}}` | `["0: ts/timestamp","1: value/float64"]` |
| 1 | `[[0,"Input"]]` | `{"timing":"ingestion_time","primitive":"SummaryState"}` | `{"family":{"Sketch":[{"algorithm":"DDSketch","category":"Quantile","params":{"DDSketch":{"alpha":0.0049751243781094535}}},"PerSubpopulationInstance"]},"grouping":"PerSubpopulationInstance","input":{"item":null,"weight":{"Column":"SampleValue"},"weight_domain":{"kind":"unknown_or_signed"}},"kind":"summary_agg","reduction":"PerEntity"}` | `["0: ts/timestamp","1: value/{\"Sketch\":[{\"algorithm\":\"DDSketch\",\"category\":\"Quantile\",\"params\":{\"DDSketch\":{\"alpha\":0.0049751243781094535}}},\"PerSubpopulationInstance\"]}"]` |
| 2 | `[[1,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"summary_estimate","query":{"Quantile":{"q":0.9}}}` | `["0: ts/timestamp","1: value/float64"]` |
| 3 | `[[1,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"summary_estimate","query":{"Quantile":{"q":0.5}}}` | `["0: ts/timestamp","1: value/float64"]` |
| 4 | `[[2,"Left"],[3,"Right"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"binary","operator":{"checked_finite_division":false,"checked_relative_division":false,"kind":{"Arithmetic":"Div"},"vector_match":null}}` | `["0: ts/timestamp","1: value/float64"]` |

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
      "stored_output": 2776719732314936823
    }
  },
  "query_sink": 4,
  "query_plan_sink": 0,
  "precompute_sinks": [
    1
  ]
}
```

Stored output `2776719732314936823` → semantic definition `sds-v1:f20a6a984012173d96b103e5ca9cc19db0140c191f7f40b1f323f2d48c1a5744`.

### Maintenance configuration

```json
[
  {
    "aggregation_type": "DDSketch",
    "aggregation_sub_type": "",
    "parameters": {
      "promql_right_closed": true,
      "alpha": 0.0049751243781094535
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
  "canonical_query": "quantile_over_time(0.9, data[1m]) / quantile_over_time(0.5, data[1m])",
  "root": 0,
  "nodes": {
    "0": {
      "op": "binary",
      "inputs": [
        1,
        3
      ],
      "operator": "Div"
    },
    "1": {
      "op": "summary_estimate",
      "input": 2,
      "query": {
        "kind": "quantile",
        "q": 0.9
      }
    },
    "2": {
      "op": "read_materialization",
      "binding": {
        "stored_output_reference": {
          "stored_output_id": 2776719732314936823,
          "definition_id": "sds-v1:f20a6a984012173d96b103e5ca9cc19db0140c191f7f40b1f323f2d48c1a5744"
        },
        "materialization": 2776719732314936823,
        "output_grouping": {
          "mode": "per_entity"
        },
        "window_ms": 10000,
        "pane_origin_ms": 0,
        "readout_lookback_ms": 60000
      }
    },
    "3": {
      "op": "summary_estimate",
      "input": 2,
      "query": {
        "kind": "quantile",
        "q": 0.5
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
