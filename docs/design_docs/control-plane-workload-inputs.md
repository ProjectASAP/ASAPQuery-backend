# Query and data workload inputs

> Status: active with evolving empirical inputs

## Responsibility

The control plane converts observed/query-declared demand into the workload
input expected by ASAPPlanner. It keeps query workload, data workload, and the
planning horizon separate; it does not infer a summary solely from one query or
one metric sample.

## Inputs

| Input | Required evidence |
| --- | --- |
| Query workload | Normalized query, frequency/recurrence, time selection, latency and accuracy target |
| Data workload | Arrival rate, cardinality, label distributions, value/range evidence, current materializations |
| Planning horizon | Time interval over which build, maintenance, read, retention, and retirement costs are compared |
| Deployment inventory | Collector/backend instances, capacities, network paths, supported runtime capabilities |

The Planner design at commit
[`d31a566`](https://github.com/ProjectASAP/ASAPPlanner/tree/d31a5667ea2d2bd5f2823cc01115e74b9895bd0f/docs/design_docs)
is authoritative for the semantic workload model. This component owns
collection and translation into that interface, not Planner candidate logic.

## Flow

```text
query registrations/API observations ----+
                                          +-> workload snapshot -> ASAPPlanner
metric/runtime inventory ----------------+
planning horizon and objectives ----------+
```

Snapshots are versioned and timestamped. Missing or stale data remains unknown;
the control plane must not turn it into a zero-cost or zero-cardinality fact.
Sensitive query/label evidence is minimized and tenant-isolated.

## Outputs

The output binds each query demand to its accuracy/freshness/latency requirement
and supplies compatible data evidence and horizon. Planner returns a selected
logical Post-ASAP plan plus guarantees, assumptions, costs, and rejected
alternatives. Physical compilation is a separate component.

## Acceptance behavior

Given a fixed workload snapshot, Planner version, capability inventory, and
horizon, the adapter emits deterministic input. Tests distinguish one-time from
recurring queries, historical from continuously arriving data, stale from fresh
evidence, and unknown from zero.

See [Developing workload inputs](../developer_docs/control-plane-workload-inputs.md).
