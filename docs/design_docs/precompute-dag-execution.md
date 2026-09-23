# Precompute DAG execution

Audience: backend designers and developers.

This document specifies the precompute engine design: how it receives a
Planner-selected computation, runs it across data partitions, and publishes the
stored outputs consumed by query plans. The [plan split](asapplanner-integration.md)
and [SDS contract](summary-catalog-sds-architecture.md) define the compiler and
storage contracts used here.

## 1. Intuition: compute once, reuse across queries, amortize the cost

Precompute performs shared work ahead of query execution and stores its selected
outputs. Later queries reuse those outputs repeatedly, amortizing the cost of
constructing and maintaining them across the queries they serve. The purpose is
to reduce both total resource cost and query latency: repeated queries avoid
repeating expensive input scans and computation, while the work left on the
query path is a stored-state read and the remaining query operators.

For example, repeated p50 and p99 requests for request latency by service over
the same five-minute range can share one Planner-selected KLL producer.
Precompute builds the KLL state once per service and scheduled window. Every
compatible request then reads that stored state and applies its percentile
readout, rather than scanning the samples and rebuilding the summary for each
request. Reuse applies both to repeated executions of a query and to different
queries that share the selected output.

Over a workload, the resource cost is the cost of construction, maintenance and
storage plus the reads and remaining query computation. Reuse must save enough
repeated work to offset those costs; this is what makes precomputation worthwhile.
Once the required output is ready, query latency excludes its construction work.
“Compute once” refers to a particular output, population and window: subsequent
windows still require construction or updates under the selected schedule.

The physical compiler splits the selected DAG at its stored outputs:

```mermaid
flowchart LR
  subgraph P[PrecomputePlan]
    I[Read latency samples] --> G[Group by service]
    G --> K[Build KLL state]
    K --> W[Write latency-kll]
  end
  W --> S[SummaryStore]
  subgraph Q[QueryPlans]
    R[Read latency-kll] --> P50[Estimate p50]
    R --> P99[Estimate p99]
  end
  S --> R
```

These labels describe the computation, not a separate backend operator language.
The executable nodes preserve Planner operator semantics and their provenance;
the compiler adds concrete source and storage bindings.

A worker runs the **whole precompute subgraph for its assigned data partition**.
For this example, one worker handles `service=api` and another can handle
`service=worker`. Both execute the same graph. Each worker runs its nodes
sequentially in dependency order; workers process independent partitions
concurrently. A worker is an execution task, not necessarily a dedicated OS
thread.

Only outputs selected for persistence become stored summaries. Intermediate
values remain in worker memory unless the plan explicitly stores them. A single
precompute subgraph can have several stored outputs, and several query plans
can read the same output.

## 2. Inputs and outputs

The physical compiler receives the selected post-ASAP DAG and the selected
deployment guarantee and schedule/retention. It emits one coherent plan version
containing definition rows, a PrecomputePlan, and matching QueryPlans.

| Input to the precompute engine | Purpose |
| --- | --- |
| `PrecomputePlan` | Defines the precompute subgraph, node IDs, operator parameters, dependency edges, input bindings and stored-output bindings. Carries the selected deployment guarantee and schedule/retention. |
| Definition rows | Describe the input, Planner family, parameters, grouping and time semantics of each selected stored output. Installed in `SummaryStore.summary_definitions`. |
| Source data | Raw samples or rows, or explicitly referenced committed summary state. The plan determines which input type each node accepts. |
| Execution triggers | Input arrival, scheduled evaluation endpoints, and input-completion signals, according to the selected maintenance mode. |
| `PrecomputeEngineConfig` | Supplies worker count, queue capacity and operational settings such as flush polling interval. These settings cannot change computation, logical windows or the selected schedule/retention. |

The engine produces committed `StoredSummary` records for the selected outputs.
It also reports completion or failure for scheduled work and drain requests.
Installing a plan does not imply that its output records are ready.

A QueryPlan consumes these records through a `StoredOutputReference`. That
reference names a stored output and definition within the installed plan
version; population and window selection identify the records to read.

## 3. Install the plan once

The backend derives an immutable `InstalledPrecomputePlan` from the supplied
PrecomputePlan. This runtime representation avoids validating the graph and
binding operators again for every input batch.

Installation performs four steps:

1. Validate the graph and operator input/output types, including ordered edge
   roles for operators with multiple inputs. Validate source, definition and
   stored-output bindings together with the matching QueryPlans.
2. Bind supported runtime operators and compile a dependency execution order.
   Preserve Planner families and parameters: Rate and Increase remain distinct,
   and grouping does not create a separate backend family.
3. Derive a partitioning rule that keeps all required dependencies and
   reductions local to a worker, as described below. Check that execution can
   satisfy the selected deployment guarantee and schedule/retention.
4. Build node and source-routing indexes, register the validated definition
   rows, and activate the coherent plan version for routing and execution.

Unsupported operators, incompatible bindings, cycles and unsatisfied deployment
requirements fail installation before the plan becomes active.

| Object | Lifetime and contents |
| --- | --- |
| `PrecomputePlan` | Compiler-supplied computation and bindings for one plan version. |
| `InstalledPrecomputePlan` | Backend-derived execution order, immutable operator programs, bindings, routing indexes and validated partitioning rule. Shared by the router and workers. |
| Worker execution state | Mutable node state, input ordering buffers, windows and intermediate results for that worker's assigned partitions. |

`InstalledPrecomputePlan` is internal runtime data, not another serialized
configuration or independently installable plan. All execution lookup tables
come from the validated DAG and its bindings. There is no separately maintained
aggregation list. Operator fusion is permitted only when it preserves the DAG's
semantics and physical-to-semantic provenance.

## 4. Route data so each worker can execute the whole subgraph

Partitioning is a property of the entire precompute subgraph. All inputs that a
node must combine must reach the same worker. The router applies the installed
partitioning rule to canonical input labels and assigns that partition to a
worker for the lifetime of the active execution state.

| Computation | Required data placement |
| --- | --- |
| KLL or Sum grouped by service | Keep all inputs for one service together. |
| Per-series Rate | Keep each series together and preserve its sample-time semantics. |
| Per-series Rate followed by Sum by service | Partition by service; keep separate Rate state for each series within the worker, then reduce locally. |
| Global Sum | Place all inputs on one worker in the simple execution model. |

For Rate followed by Sum, distributing individual series independently could
put two series of the same service on different workers. Neither worker could
then compute the complete service result. Partitioning by service satisfies
both the per-series state requirement and the downstream reduction.

The simple design uses one validated partitioning rule for the installed
precompute subgraph. If independent partitions cannot be established, use one
worker when the deployment requirements permit it; otherwise reject the plan.
There is no implicit shuffle or cross-worker partial-result merge. A hot
partition therefore remains limited by its owning worker.

Partition ownership is separate from stored-output identity. Hashing a stored
output ID does not establish that its input dependencies are local. Multiple
stored outputs can share one data partition and one worker.

```mermaid
flowchart TD
  I[Bound input data] --> R[Route by validated data partition]
  P[InstalledPrecomputePlan] -. shared program .-> A
  P -. shared program .-> B
  R --> A[Worker 0: entire subgraph for service api]
  R --> B[Worker 1: entire subgraph for service worker]
  A --> S[SummaryStore]
  B --> S
```

Worker queues are bounded. Admission applies backpressure when a destination
queue is full; an atomic input batch must reserve its required queue capacity
before being acknowledged. Sequential queue consumption does not itself sort
event timestamps. Input ordering, allowed lateness and counter-reset handling
must follow the bound operators' time semantics.

## 5. Execute ready work inside the owning worker

Each worker holds the same installed program and separate mutable state. An
input batch updates only the state for its assigned data partition. A scheduled
evaluation identifies the logical window whose outputs are due. Nodes run when
the inputs required for that evaluation are available.

The following diagram expands the KLL example for one scheduled window. The
router assigns partition work; each worker reads only its bound partition and
executes every required node of the same installed precompute subgraph. Dashed
arrows denote shared immutable program access; solid arrows denote work or data.

```mermaid
flowchart TB
  D[Selected Planner DAG] --> C[Physical compiler: split at stored outputs]
  C --> P[PrecomputePlan: Read -> Group -> KLL -> Write]
  C --> QP[QueryPlans: Read stored KLL -> Estimate]
  P --> IP[InstalledPrecomputePlan: shared immutable program]
  T[Scheduled window and source partitions] --> R[Route by service]

  subgraph W0[Worker 0: service=api]
    direction TB
    A0[Read partition input] --> A1[Group by service]
    A1 --> A2[Build KLL: private state]
    A2 --> A3[Write latency-kll for api and window]
  end
  subgraph W1[Worker 1: service=worker]
    direction TB
    B0[Read partition input] --> B1[Group by service]
    B1 --> B2[Build KLL: private state]
    B2 --> B3[Write latency-kll for worker and window]
  end

  IP -. same program .-> A0
  IP -. same program .-> B0
  R -->|api partition| A0
  R -->|worker partition| B0
  A3 --> S[SummaryStore: separate committed population/window records]
  B3 --> S
  S --> QR[Query engine: bound stored-state read]
  QP -. query program .-> QR
  QR --> E[Estimate p50 or p99 on each query]
```

Worker 0 and Worker 1 can run concurrently. Within each worker the arrows
establish dependency order: KLL construction follows its input computation,
and publication follows completion of the required state. Neither worker owns
an exclusive operator stage; both execute Read, Group, KLL and Write. They share
the program but not mutable KLL state. The two records use the same plan version,
stored-output ID and definition, with different population keys. Query execution
starts from the committed output rather than running this precompute path again.

The selected maintenance mode determines how state is constructed. An
incremental operator updates its window state as input arrives. A batch operator
reads the bound range and builds its result at the scheduled endpoint. The
runtime cannot replace one mode with another merely because both produce the
same family of summary.

Within one evaluation, the worker follows this workflow:

### Shared physical operator execution

The shared library is introduced by #770, before this integration. Installed
precompute DAGs execute through its `PhysicalDag` runtime. The backend supplies
storage frontiers, declared edge order, window completeness and durable commit
keys. It does not own a second dependency walker.

For completed-window DAGs, SummaryAgg uses the native summary builder,
SummaryMerge uses the native state merge, finalization uses native typed readout,
and Binary lowers aligned rows to native arithmetic Project. Batch conversion
preserves the installed population and timestamp bindings. Native calls receive
the surrounding execution context, so they share its memory budget and
cancellation. Query execution uses the same library's operations.

Raw ingestion retains per-window accumulator state through shared-library
updaters; worker routing and window completion remain backend responsibilities.
Storage publication occurs only after successful DAG execution. It is separate
from the library's request-local caching of intermediate results.

```text
execute(partition, evaluation_window, bound_inputs):
    check input identities and required completeness
    create one intermediate-result map for this evaluation
    for node in installed dependency order:
        if this evaluation does not require node: continue
        obtain inputs in their declared edge-role order
        if a required input is incomplete: keep dependent work pending
        otherwise:
            execute node using this partition's node state
            retain its result for every downstream consumer
    publish completed selected outputs with their stored-output bindings
```

A shared upstream node is evaluated once for the same partition, window and
input revision, even when several downstream sinks consume it. Its result stays
available until those consumers finish. A later input revision or evaluation
window requires fresh evaluation; the cache is not keyed only by node ID across
unrelated work.

Readiness belongs to dependencies, not merely to elapsed wall-clock time. For a
node consuming several sources, all required sources must satisfy the bound
window and population-completion rules. A flush timer cannot make missing input
complete. On a finite input run, admission closes and completion barriers follow
all accepted input; workers then finish eligible downstream work and acknowledge
drain only after the required output commits finish.

A derived summary reads explicitly referenced committed source records and
checks their coverage before executing downstream operators. That read is a
frontier: the worker does not repeat the source records' upstream computation.
Derived work follows the same partition ownership and worker execution model.

Nodes within a worker execute sequentially. Parallelism comes from independent
workers, including when they construct derived summaries. Store commit
coordination protects publication without requiring unrelated workers to hold
one global lock while evaluating their DAGs.

## 6. Publish the selected DAG outputs

One `SummaryStore` owns two logical tables:

| Table | Record contents |
| --- | --- |
| `summary_definitions` | `SummaryDefinition`: canonical input, Planner family, parameters, grouping and time semantics. Shared across compatible records. |
| `stored_summaries` | `StoredSummary`: record key, definition ID, actual format and coverage, and payload. |

The compiler assigns a `stored_output_id` to each output selected for
persistence. The precompute writer and query readers carry the same
`StoredOutputReference`. The reference is a plan binding, not a third catalog
object.

A concrete stored record has the key:

```text
(plan_id, plan_version, stored_output_id, population_key, window)
```

`population_key` preserves canonical label names and values. `window` identifies
the intended time partition and its boundary convention; actual coverage is
validated separately. Worker IDs and transient runtime handles are not record
identities. A store may use a local numeric row handle for indexing or wire
compression; that handle cannot select another output, definition or plan
version. The writer supplies a `StoredSummaryKey` and the reader supplies the
same selected `StoredOutputReference` within its installed plan version.
Durable metadata retains this binding so recovery validates it before exposing
payloads. V1 does not implicitly reuse records from another plan version.
The durable binding format is version 4; earlier SID metadata is rejected rather
than inferred to refer to a selected output.

Before publication, validate that the output matches its bound definition,
format, population and window. Payload and identifying metadata become visible
as one committed record. A reader must not observe metadata pointing to an
uncommitted payload. Repeating the publication of the same completed result must
not add that result twice; conflicting writes must not silently overwrite a
record under the same key. Any permitted replacement follows the selected
update policy and preserves coherent reads.

QueryPlan and derived precompute readers perform indexed lookup using the
installed reference and requested population/window. They check definition,
format, plan version and coverage before consuming a record. Missing or
incomplete state remains unavailable; query fallback is governed by QueryPlan.

## 7. Worked example: one KLL producer, two percentile readers

Use the latency example with this selected deployment:

- Plan version `42`; group input by `service`.
- Build KLL with `k=200` over `(T - 5m, T]`.
- Rebuild from data at rest every minute, aligned to the Unix epoch.
- Retain completed summaries for ten minutes.
- Store output `latency-kll`, defined by `def-api-latency-kll`.

The following YAML illustrates the bindings and stored record, not a Rust wire
schema:

```yaml
plan_version: 42
writer_reference:
  stored_output_id: latency-kll
  definition_id: def-api-latency-kll
reader_references:
  p50: {stored_output_id: latency-kll, definition_id: def-api-latency-kll}
  p99: {stored_output_id: latency-kll, definition_id: def-api-latency-kll}

summary_definitions:
  def-api-latency-kll:
    input: {metric: request_latency_seconds, value: sample_value}
    family: {kind: Sketch, algorithm: KLL, parameters: {k: 200}}
    group_by: [service]
    time_semantics: {range: 5m, bounds: "(start, end]"}
    output_type: kll_state

stored_summaries:
  - key:
      plan_version: 42
      stored_output_id: latency-kll
      population_key: {service: api}
      window: {start_exclusive: '12:00', end_inclusive: '12:05'}
    definition_id: def-api-latency-kll
    format: {schema: kll-v1, encoding: kll-binary-v1}
    coverage: {start_exclusive: '12:00', end_inclusive: '12:05'}
    payload: <encoded KLL state>
```

At the `12:05` evaluation endpoint:

1. The schedule triggers construction for `(12:00, 12:05]`. The bound source
   supplies the required data and completion evidence for that range.
2. Routing sends `service=api` data to its owning worker. That worker executes
   the read, grouping and KLL construction nodes for the partition. Another
   worker can execute the same graph for `service=worker` concurrently.
3. The first worker commits the record illustrated above. The other worker
   commits a separate record with `population_key: {service: worker}`.
4. Both percentile QueryPlans select the `service=api` record for endpoint
   `12:05`. They read the same KLL state and apply their different estimates.

At `12:06`, the same graph builds new records for `(12:01, 12:06]`. The definition,
stored-output ID and plan version stay the same; the window part of the key
changes. Ten-minute retention controls how long completed records remain
available, not how much input each KLL summarizes. Building a percentile result
at query time does not create another stored output.

## 8. Plan changes, recovery and validation

Routing, operator bindings and output references belong to one coherent plan
version. Accepted work retains that version until completion. Activation drains
or isolates incompatible old worker state before new work uses a different
partition assignment. Changing worker count requires coordinated reassignment;
changing a hash modulus while state is active is insufficient.

Recovery validates persisted record identities, definitions, formats and
coverage before exposing them to readers. A matching definition alone does not
authorize reuse across plan versions. Runtime caches and routing indexes are
rebuilt from the installed plan. Input replay and durable operator-state
checkpointing require an explicit source/update contract; record publication
alone does not guarantee exactly-once ingestion.

Acceptance checks follow the workflow:

| Check | Required observation |
| --- | --- |
| Planner-to-runtime binding | Node semantics, edge roles, families and stored-output references agree; invalid plans fail installation. |
| One worker versus several | Identical results for grouped Sum, per-series Rate and Rate followed by service reduction, including counter resets and window boundaries. |
| Partition locality | Every dependency needed for a partition stays with its owner; a rule splitting a required reduction is rejected. |
| Shared computation | A common upstream node runs once per evaluation/input revision across its consumers, including multiple stored sinks. |
| Readiness and backpressure | Incomplete inputs do not produce readable complete records; admission and drain honor queued work and commits. |
| Store and query integration | Writer and reader use the same composite identity; format, coverage and version mismatches fail validation. |
| Restart and activation | Committed records recover coherently; retries do not duplicate completed publication; old and new plan state do not mix. |

The simple execution model does not require node-level parallel scheduling,
cross-worker shuffle, or separate metadata and payload services. Its unit of
parallel work is a complete precompute subgraph over an independent data
partition.
