# grouped-rate

`sum by (label_0) (rate(data[1m]))`

[Raw selected plan](grouped-rate.json) · [DAG DOT](grouped-rate.dot)

## Selected computation: logical provenance

IDs below are Planner node IDs; QueryPlan adapter IDs are shown separately.

Root: `3`.

```mermaid
flowchart LR
  N0["0: Source / time range"]
  N1["1: SummaryAgg {&quot;ExactAggregate&quot;:[&quot;Rate&quot;,&quot;Rate&quot;]}"]
  N2["2: FinalizeExactAccumulator"]
  N3["3: Exact"]
  N0 --> N1
  N1 --> N2
  N2 --> N3
```

| Node | Dependencies (producer, edge role) | Timing | Operation | Output fields (index: name/type) |
| --- | --- | --- | --- | --- |
| 0 | `[]` | `{"timing":"ingestion_time","primitive":"Raw"}` | `{"source":{"TimeSeries":{"metric":"data"}},"predicates":[],"range":{"nanos":0,"secs":60}}` | `["0: ts/timestamp","1: value/float64","2: label_0/utf8"]` |
| 1 | `[[0,"Input"]]` | `{"timing":"ingestion_time","primitive":"SummaryState"}` | `{"family":{"ExactAggregate":["Rate","Rate"]},"grouping":"PerSubpopulationInstance","input":{"item":null,"weight":{"Column":"SampleValue"},"weight_domain":{"kind":"unknown_or_signed"}},"kind":"summary_agg","reduction":"PerEntity"}` | `["0: ts/timestamp","1: value/{\"ExactAggregate\":[\"Rate\",\"Rate\"]}","2: label_0/utf8"]` |
| 2 | `[[1,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"value","operation":"FinalizeExactAccumulator"}` | `["0: ts/timestamp","1: value/float64","2: label_0/utf8"]` |
| 3 | `[[2,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"value","operation":{"Exact":{"Aggregate":{"having":null,"measures":[{"col":null,"kind":"sum"}],"output_names":[""],"reduction":{"Reduce":[2]}}}}}` | `["0: label_0/utf8","1: sum/float64"]` |

### Sort expressions

No standalone Sort node in this selected DAG; any ranking readout is shown in the operation table.

## Candidate admission and costing

Costs below come from the controlled Level 1 fixture, not production measurements.

```json
{
  "deployment_evaluation": "single_root_substitutions_in_preferred_workload",
  "inventory": "all_root_candidates",
  "joint_workload_search_exhaustive": false
}
```

### Planner physical candidate

Logical root: `"asap-explain-v1:root:0e5bd63bf2b058035044a1c50ec5912afbfbd59dd75611d27ff23b9420d09851"`.

Sum { col: None } realizes as an exact Sum accumulator — the only realization realizations_for_intent produces for this intent (no approximate candidate applies)

Guarantee: `{"bound":{"op":"zero"},"failure_probability":{"op":"zero"},"metric":"absolute_value","provenance":[{"kind":"exact","reason":"ExactAggregate(Sum)"},{"guarantee":{"bound":{"op":"zero"},"failure_probability":{"op":"zero"},"metric":"absolute_value","provenance":[{"kind":"exact","reason":"ExactAggregate(Rate)"},{"guarantee":{"bound":{"op":"zero"},"failure_probability":{"op":"zero"},"metric":"absolute_value","provenance":[{"kind":"exact","reason":"KeepPreAsap"}]},"input_index":0,"kind":"child_guarantee"},{"kind":"composition_step","operator":{"op":"counter_rate"},"rule":"exact_input"}]},"input_index":0,"kind":"child_guarantee"},{"kind":"composition_step","operator":{"op":"exact_sum"},"rule":"exact_input"}]}`

| Node | Dependencies | Native operator / input |
| --- | --- | --- |
| 2 | `[]` | `{"Input":{"properties":{"boundedness":"Bounded","emission":"Unknown"},"schema":{"fields":[{"dtype":{"Plain":"timestamp"},"name":"ts","nullable":false},{"dtype":{"Plain":"float64"},"name":"value","nullable":false},{"dtype":{"Plain":"utf8"},"name":"label_0","nullable":true},{"dtype":{"Plain":"utf8"},"name":"$promql_series_identity","nullable":false}],"time_index":0}}}` |
| 3 | `[2]` | `{"SummaryBuild":{"family":{"ExactAggregate":["Sum","Sum"]},"groups":[2],"time":0,"value":1}}` |

Roots: `[3]`.


| Candidate | Logical root IDs | Status | Fixture cost | Rejection / unavailable reason |
| --- | --- | --- | --- | --- |
| 0 | `["asap-explain-v1:root:0e31a8d3445c715a2134c5af0a5ea3f922da28603611abeb514a6bd3a11c083e"]` | `"selected"` | `125.0` | `null` |
| 1 | `["asap-explain-v1:root:643999c2c793eda9409a53042cc0ff84502915e9de4dc74029bf589e91917ff7"]` | `"unselected"` | `61000000000000.0` | `null` |
| 2 | `["asap-explain-v1:root:81569c887cc72d5c00aafe4f5a9840b55e15409c4224e7e879ce7d698bb0a4d1"]` | `"bind_failed"` | `null` | `"failed to construct QueryPlan: invalid QueryPlan: Planner residual does not match any original query subtree"` |
| 3 | `["asap-explain-v1:root:0e5bd63bf2b058035044a1c50ec5912afbfbd59dd75611d27ff23b9420d09851"]` | `"bind_failed"` | `null` | `"failed to construct QueryPlan: invalid QueryPlan: physical vector program loses complete identity, timestamp or value"` |

Successfully compiled candidate plans: [grouped-rate-0](candidates/grouped-rate-0.json), [grouped-rate-1](candidates/grouped-rate-1.json)

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
      "stored_output": 11966640087163441478
    }
  },
  "query_sink": 3,
  "query_plan_sink": 0,
  "precompute_sinks": [
    1
  ]
}
```

Stored output `11966640087163441478` → semantic definition `sds-v1:528d2a7adb528822d205d1e239daf9904249ae30c5c50c6f7288f286b451ea6c`.

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
      "labels": [
        "label_0"
      ]
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
  "canonical_query": "sum by (label_0) (rate(data[1m]))",
  "root": 0,
  "nodes": {
    "0": {
      "op": "logical",
      "operator": {
        "kind": "aggregate",
        "operation": "sum",
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
      "op": "exact_readout",
      "input": 2,
      "readout": "rate"
    },
    "2": {
      "op": "read_materialization",
      "binding": {
        "stored_output_reference": {
          "stored_output_id": 11966640087163441478,
          "definition_id": "sds-v1:528d2a7adb528822d205d1e239daf9904249ae30c5c50c6f7288f286b451ea6c"
        },
        "materialization": 11966640087163441478,
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
