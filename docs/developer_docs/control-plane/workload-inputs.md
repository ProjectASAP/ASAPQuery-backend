# Developing query and data workload inputs

## Architecture

The control-plane workload registry and analyzer normalize query registrations,
runtime query demand, data statistics, deployment inventory, and planning
horizon into ASAPPlanner input. Planner's workload model at commit `d31a566` is
the semantic source; local code adapts transport and runtime evidence.

## Interfaces

An input snapshot includes query expression/normalized identity, recurrence,
time scope, accuracy/freshness/latency requirements, data arrival/cardinality/
distribution evidence, horizon, timestamp, and evidence version. Unknown fields
remain explicit.

## Extension and verification

Adding an input requires provenance, freshness, tenant scope, serialization,
and deterministic normalization. Tests cover one-time/recurring demand,
historical/live data, unknown versus zero, stale evidence, duplicate
registrations, and fixed-snapshot reproducibility.

See [workload input design](workload-inputs-design.md).
