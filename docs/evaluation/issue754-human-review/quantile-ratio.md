# quantile-ratio

`quantile_over_time(0.9, data[1m]) / quantile_over_time(0.5, data[1m])`

[Raw selected plan](quantile-ratio.json) · [DAG DOT](quantile-ratio.dot)

## Selected computation: logical provenance

IDs below are Planner node IDs; QueryPlan adapter IDs are shown separately.

Root: `4`.

```mermaid
flowchart LR
  N0["0: Source / time range"]
  N1["1: SummaryAgg {&quot;Sketch&quot;:[{&quot;algorithm&quot;:&quot;DDSketch&quot;,&quot;category&quot;:&quot;Quantile&quot;,&quot;params&quot;:{&quot;DDSketch&quot;:{&quot;alpha&quot;:0.0049751243781094535}}},&quot;PerSubpopulationInstance&quot;]}"]
  N2["2: summary_estimate"]
  N3["3: summary_estimate"]
  N4["4: binary"]
  N0 --> N1
  N1 --> N2
  N1 --> N3
  N2 --> N4
  N3 --> N4
```

| Node | Dependencies (producer, edge role) | Timing | Operation | Output fields (index: name/type) |
| --- | --- | --- | --- | --- |
| 0 | `[]` | `{"timing":"ingestion_time","primitive":"Raw"}` | `{"source":{"TimeSeries":{"metric":"data"}},"predicates":[],"range":{"nanos":0,"secs":60}}` | `["0: ts/timestamp","1: value/float64"]` |
| 1 | `[[0,"Input"]]` | `{"timing":"ingestion_time","primitive":"SummaryState"}` | `{"family":{"Sketch":[{"algorithm":"DDSketch","category":"Quantile","params":{"DDSketch":{"alpha":0.0049751243781094535}}},"PerSubpopulationInstance"]},"grouping":"PerSubpopulationInstance","input":{"item":null,"weight":{"Column":"SampleValue"},"weight_domain":{"kind":"unknown_or_signed"}},"kind":"summary_agg","reduction":"PerEntity"}` | `["0: ts/timestamp","1: value/{\"Sketch\":[{\"algorithm\":\"DDSketch\",\"category\":\"Quantile\",\"params\":{\"DDSketch\":{\"alpha\":0.0049751243781094535}}},\"PerSubpopulationInstance\"]}"]` |
| 2 | `[[1,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"summary_estimate","query":{"Quantile":{"q":0.9}}}` | `["0: ts/timestamp","1: value/float64"]` |
| 3 | `[[1,"Input"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"summary_estimate","query":{"Quantile":{"q":0.5}}}` | `["0: ts/timestamp","1: value/float64"]` |
| 4 | `[[2,"Left"],[3,"Right"]]` | `{"timing":"query_time","primitive":"Raw"}` | `{"kind":"binary","operator":{"checked_finite_division":false,"checked_relative_division":false,"kind":{"Arithmetic":"Div"},"vector_match":null}}` | `["0: ts/timestamp","1: value/float64"]` |

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

- quantile(q=0.9) realizes as a Kll sketch: composed guarantee (Rank, bound Some(0.013294757464848584), failure probability Some(0.01)) does not satisfy EpsilonDelta { epsilon: 0.01, delta: 0.01 }
- quantile(q=0.5) realizes as a Kll sketch: composed guarantee (Rank, bound Some(0.013294757464848584), failure probability Some(0.01)) does not satisfy EpsilonDelta { epsilon: 0.01, delta: 0.01 }

| Candidate | Logical root IDs | Status | Fixture cost | Rejection / unavailable reason |
| --- | --- | --- | --- | --- |
| 0 | `["asap-explain-v1:root:8fcad76c8faf47f60741d4888d55323486f842f5086dc3ecd39b493b6a9e12e2"]` | `"selected"` | `155.0` | `null` |
| 1 | `["asap-explain-v1:root:994e1b491dc33fab7e19c7b0b7eb8080bfd17bd1e7af0222eefd190b7eef0ac0"]` | `"unselected"` | `61000000000000.0` | `null` |
| 2 | `["asap-explain-v1:root:94bf296517fa559b33eefab876aa1b77c4bae34b93477b567541571979fa006d"]` | `"unselected"` | `61000000000000.0` | `null` |

Successfully compiled candidate plans: [quantile-ratio-0](candidates/quantile-ratio-0.json), [quantile-ratio-1](candidates/quantile-ratio-1.json), [quantile-ratio-2](candidates/quantile-ratio-2.json)

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
      "alpha": 0.0049751243781094535,
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
