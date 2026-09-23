# QueryPlan DAG execution

Audience: system designers and deployment implementers.

This document describes how the backend executes an installed `QueryPlan`.
The [plan split](asapplanner-integration.md) defines why query-time work is
separate from ingestion-time work, and the
[SDS contract](summary-catalog-sds-architecture.md) defines the stored records
read by the plan.

## Runtime contract

The physical compiler publishes one `QueryPlanEntry` for each installed query.
An entry contains a root, typed nodes, dependency IDs, evaluation-time rules,
and an explicit fallback policy. The query engine executes the nodes reachable
from that root. It does not parse the incoming expression into another runtime
program and does not execute `selected_dags`; those documents retain Planner
semantic provenance for validation and debugging.

The request first resolves a canonical query identity in the active, immutable
physical-plan snapshot. The Summary Catalog, PrecomputePlan and QueryPlan in
that snapshot have the same plan ID and version. A lookup miss is a capability
miss and follows the installed routing policy.

```mermaid
flowchart LR
  Request[Query request] --> Lookup[Find installed query physical plan]
  Lookup --> Root[Identify query result operation]
  Root --> Walk[Find required physical operations]
  Walk --> Inputs[Obtain operation inputs]
  Inputs --> Execute[Execute physical operation]
  Execute --> Memo[Cache result within this request]
  Memo --> Result[Return query result]
  Store[Stored materializations] --> Read[Read materialization]
  Read --> Inputs
  External[Declared external computation] --> Inputs
```

The installed query physical plan is represented by `QueryPlanEntry`; its `root`
identifies the operation producing the query result. Required input operations
execute before their consumers. The execution adapter invokes the implementation
of each physical operation, and request-local caching avoids repeated evaluation
of shared dependencies. Operations evaluated at multiple query times are cached
separately for each evaluation time.

“Read materialization” is the operation listed in the coverage table. It obtains
stored state from `SummaryStore` through a `StoredOutputReference`. Declared
external computation supplies an explicitly bound input; it is not a local
physical operation implementation.


Installation rejects missing inputs, cycles, unreachable nodes, invalid output
bindings, unsupported provenance versions, and a reader whose window contract
or `StoredOutputReference` differs from its PrecomputePlan writer. Runtime
errors retain the query ID and node ID.

## Shared DAG execution

**Decision: ASAP will independently implement the shared DAG runtime and
physical operators. Both the precompute engine and the query engine will use
this library. DataFusion is a design reference, not the execution framework.**

The shared library executes a physical operator DAG for both the precompute
engine and the query engine. It is designed around that execution contract,
not around the current backend's function or module boundaries. The existing
accumulator library and backend-driven query traversal do not yet provide this
architecture.

```mermaid
flowchart TB
  Planner[Post-ASAP physical plan] --> Bind[Bind and validate physical operators]
  Bind --> DAG[Executable physical operator DAG]
  Precompute[Precompute engine] -->|Ingestion inputs and windows| Run[Shared DAG execution library]
  Query[Query engine] -->|Query inputs and evaluation time| Run
  DAG --> Run
  Run --> States[States and materialized results]
  Run --> Results[Query results]
```

The compiler binds each operation to a concrete implementation and checks its
input and output types before accepting the plan. Both engines execute the
result through the same library. Neither engine supplies a second interpretation
of Filter, Project, Aggregate, SummaryMerge, Sort, or Limit.

An operation defines its computation, typed inputs and outputs, and requirements
such as input ordering and grouping. An execution instance owns the changing
state for one run. Keeping the plan separate from running state allows the same
plan to serve concurrent queries and ingestion windows without sharing mutable
accumulators accidentally.

Operations consume and produce batches incrementally where their semantics
allow it. Filter and Project can emit results as batches arrive. Sorting a
complete group must wait until that group's input is complete. Grouped Limit
counts across batches, not separately within each batch. For ingestion, the
engine supplies window completion; for a query, the engine supplies the
requested input range. These requirements do not make an operator exclusive to
one execution phase.

### Engine responsibilities

| Component | Responsibility |
| --- | --- |
| Shared DAG execution library | Physical operator implementations, typed expressions, state construction/merge/readout, dependency execution, shared intermediate results, cancellation, and execution resource accounting |
| Precompute engine | Connect ingestion sources, assign data to the intended windows, supply completion signals, and persist or restore operator state and materialized results |
| Query engine | Bind request parameters and evaluation times, connect stored or declared external inputs, invoke the shared DAG, and format results |
| Deployment integration | Supply source/sink implementations, storage, scheduling resources, and durability policy |

The library does not depend on either engine. Another deployment such as
asap-fusion can bind its own sources and sinks to the same physical operators.
Every computation operator may run at ingestion time or query time; the plan
chooses when, and the engine supplies the appropriate inputs and execution scope.

### Shared dependencies

A DAG can have several consumers of the same operation. The execution library
must represent that shared identity explicitly. Reusing a plan object alone does
not establish that its computation runs once.

Within a query, **request-local caching of intermediate results** prevents
repeated evaluation of a shared dependency. Time-dependent results are separated
by evaluation time. During ingestion, sharing is scoped to the same execution
and input window. Results from distinct requests or windows are never mixed.

For streaming output, the runtime delivers the same produced batches to each
consumer. Buffering is bounded and participates in the execution memory budget;
a slow consumer cannot cause unlimited retention. Cancelling one consumer does
not stop a producer still needed by another. Cancelling the whole execution
releases its streams, intermediate results, and tasks.

## Shared physical operator library

The library's unit of composition is an executable physical operator. Each
operator exposes its input dependencies, output schema, execution requirements,
and a way to start execution. Filter, Project, Aggregate, Join, Sort, Limit,
and summary operations participate in this same contract.

Typed scalar expressions are separate from operations over batches. A literal,
negation, or comparison can be evaluated inside Project or Filter without
inventing a separate DAG node for every expression. Where Planner represents a
standalone scalar-producing operation, it uses the same expression semantics.
All accepted value types and nullability rules follow Planner's contract.

Existing mathematical algorithms may supply internal kernels, but their current
backend wrappers do not define the new operator API. Moving helper functions
into a crate is not sufficient: both engines must instantiate and execute the
shared operators through the shared DAG runtime. Removed backend-specific
execution paths must not survive as compatibility branches.

## DataFusion reuse vs. independent implementation

The alternatives are to build on DataFusion's execution framework and general
operators, adding ASAP-specific operators, or to implement ASAP's own DAG runtime
and physical operators. This design selects independent implementation.

| Concern | Reuse DataFusion | Independent ASAP implementation |
| --- | --- | --- |
| General computations | Reuse existing expressions, projection, filtering, joins, sorting, and aggregation where their semantics match Planner | Implement and test the supported operations and expression semantics against Planner's contract |
| Execution model | Adopt its physical-plan interfaces and batch streams; integrate ASAP-specific execution requirements | Define node identity, typed edges, execution instances, and multi-consumer behavior as the library's core contract |
| Shared dependencies | Shared references to a plan object do not by themselves guarantee shared execution; additional coordination is needed | One producer execution per node, input partition, and evaluation scope, with explicit result delivery to all consumers |
| Summary state | Supply custom accumulators/operators and integrate the required state lifecycle | Treat summary construction, updates, merge, readout, snapshots, and restoration as native operator capabilities |
| Ingestion and query execution | Adapt both engines to DataFusion while adding ASAP's window and persistence behavior | Use the same runtime and operators in both engines; engines supply their inputs, execution scope, and persistence integration |
| Resource management | Reuse framework facilities where applicable, while accounting for ASAP-specific state and sharing | Implement bounded buffering, memory accounting, backpressure, cancellation, and cleanup |
| Dependencies and maintenance | Accept DataFusion/Arrow interface and version constraints | Own the execution API and its maintenance; accept greater implementation and verification work |

DataFusion's
[ExecutionPlan interface](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)
represents input dependencies through shared plan references and starts execution
by returning a batch stream. This permits shared references in the plan
representation; it does not establish a general execute-once guarantee for a
producer with multiple consumers. ASAP's decision is therefore not based on a
claim that DataFusion cannot represent a shared node. It is based on making
shared execution and the summary-state lifecycle explicit parts of ASAP's own
runtime contract.

ASAP needs both a finite query execution and ingestion execution over successive
windows. The same summary producer may feed several computations or sinks.
The runtime must coordinate those consumers without duplicating updates,
mixing evaluation times, or cancelling work still needed elsewhere. Persisted
state also requires explicit snapshot and restoration semantics. DataFusion's
[Accumulator interface](https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.Accumulator.html)
provides update, merge, and result operations, but its intermediate-state export
can consume state; that interface alone is not an ingestion checkpoint protocol.

Independent implementation gives ASAP direct control over these behaviors and
keeps the shared library usable by other deployments. The cost is substantial:
ASAP must implement and test the general operators, type and null semantics,
stream lifecycle, and resource controls rather than assume a framework supplies
them. The coverage table must continue to report incomplete implementations.

DataFusion remains a reference for separating immutable operator definitions
from execution state, batch-stream processing, typed
[physical expressions](https://docs.rs/datafusion/latest/datafusion/physical_expr/trait.PhysicalExpr.html),
and operator input/output requirements. Existing sketch algorithms and suitable
low-level libraries may be reused internally. This does not authorize retaining
the current backend executor as a second execution path. Choosing a batch memory
format is separate from choosing the DAG runtime; independence does not require
reimplementing every buffer or mathematical primitive.

Acceptance of the new runtime must include a shared producer with two consumers,
consumers progressing at different rates, cancellation of one consumer, failure
propagation, and isolation between query times and ingestion windows. Stateful
operators must also survive snapshot and restoration without applying a committed
input twice. The execute-once guarantee within one execution does not by itself
prove correct recovery after a restart. These tests complement operator result
checks and must run through both engines' integration with the shared library.

## Physical operator coverage and acceptance contract

Coverage describes this PR and the immutable Planner revision in `Cargo.toml`.
**The current code does not yet implement the shared DAG architecture or meet
the universal local-execution contract.**
An exhaustive phase match proves ownership only. A reusable kernel proves an
algorithm implementation exists; neither proves that a concrete installed plan
can obtain its inputs and execute every node locally.

The target contract is: compilation accepts a local query plan only if every
reachable operator has a concrete implementation for its parameters, input and
output schemas/states, grouping, time scope and placement. Raw-only, partial
precomputation and full precomputation must obey the same contract. An explicitly
external plan may remain useful, but is not evidence of local coverage.

### Physical operation coverage

A physical operation defines **what computation happens**. The plan chooses
**ingestion time or query time**. This applies to every computation operation;
ingestion time includes background processing of arriving data. Current adapter
gaps are implementation work, not permanent restrictions on execution time.

The table lists each operation once, regardless of whether the code represents
it inside `ValueOperation`, `Logical`, or another wrapper. “Backend integration”
means an implemented path for the stated subset, not universal support.

| Physical operation | Purpose | Shared library implementation | Backend integration | Missing coverage |
| --- | --- | --- | --- | --- |
| Raw Scan | Read raw input rows | None | Rejected in installed local query plans | Local raw source; deferred from this PR |
| Read materialization | Load previously computed state | State decoding kernels; no storage adapter | Catalog/store binding for compatible populations and windows | General raw input access; unavailable or incompatible state cannot be read |
| Maintain current-series state | Update the maintained values and timestamps for incoming time series. | None | Specialized remote-write ingestion path | General table-row updates and shared implementation |
| Read current-series state | Read values from the maintained time-series state for query execution. | None | Current-series readout using the installed state identity and capacity | Arbitrary raw-table reading |
| Scalar | Produce a scalar value | No separate scalar-source executor | Typed scalar path | General expression evaluation |
| Binary | Combine or compare two inputs | Float64 Add/Sub/Mul/Div/Mod/Pow/Atan2 kernels | Query arithmetic, CheckedDiv/FiniteDiv and comparisons; ingestion arithmetic on immutable completed, aligned rows | General coercion, arbitrary PromQL matching and unsupported value domains |
| Unary negate | Negate a value | No separate adapter | Typed query scalar/vector path | General ingestion adapter |
| Vector to scalar | Convert a vector to a scalar | No separate adapter | Typed query path | General ingestion adapter |
| Exact aggregate | Compute Count/Sum/Avg/Min/Max, including ReduceSum | Exact accumulator kernels | Relation and query aggregate adapters | No universal aggregate implementation; relation numeric measures require non-null Int64/Float64 and finite valid values; relation per-entity reduction and grouping-without unsupported |
| Finalize exact accumulator / ExactReadout | Obtain an exact result from typed state | Exact-family readout kernels | Typed query readout and ingestion finalization of immutable completed windows | Arbitrary state conversion and unsupported exact families |
| Project | Select or calculate output columns | No general expression executor | Query relation adapter | Unsupported expressions/types and general ingestion adapter |
| Filter | Keep rows satisfying a predicate | No general predicate executor | Query relation adapter | Unsupported predicates/types and general ingestion adapter |
| Relational join, including semi-join | Match rows by a predicate; semi-join retains matching left rows | Row membership kernel; general semi-join replacement pending; other joins remain backend-local | Relation adapter supports inner/left/right/full/cross/semi/anti joins within its predicate/schema subset; vector candidate pruning currently uses a dedicated membership adapter; general semi-join replacement pending | General ingestion join adapter and unrestricted SQL/NULL semantics |
| Sort | Order input rows or values | No general sorting adapter | Query relation and logical sorting | Relation partitioned sorting, NaN and unsupported key types; general ingestion adapter |
| Limit | Keep a bounded slice of input | No separate adapter | Query relation offset/limit and specialized logical lowering | General ingestion adapter; does not rank or match candidate keys |
| SummaryAgg | Construct summary state from input | Supported-family construction/update kernels | Raw-ingestion specialization and restricted row-to-state ingestion aggregation | Installed query-time builder; arbitrary item expressions/output populations; typed update evaluation required |
| SummaryMerge | Combine compatible summary states | Compatible-state merge kernels | Ingestion and query state merge | Universal cross-family merge is not supported |
| SummaryEstimate | Query a summary for an approximate result | Family-specific sketch query kernels | Typed stored-state readout | Unsupported family/readout combinations; window/population compatibility and accuracy evidence remain required |
| SummaryJoin | Combine summary inputs using summary join semantics | No registered kernel | No runtime dispatch | Concrete kernel and adapters |
| SummarySubtract | Subtract summary state | No registered kernel | Unsupported in ingestion runtime | Concrete kernel and adapters |
| SummaryDelete | Remove contributions from summary state | No registered kernel | No runtime dispatch | Concrete kernel and adapters |
| Temporal computation | Compute Rate/Increase/Avg/Max/Min/Sum/Count over time | Relevant exact accumulator kernels, not a complete temporal adapter | Query paths over supported inputs | General input/state combinations and ingestion adapter |
| Histogram quantile | Calculate a quantile from histogram buckets | No separate histogram adapter | Query path | General ingestion adapter |
| Subquery | Evaluate an expression over a time grid | Request-local caching of intermediate results; full shared DAG runtime pending | Query path with bounded grids and request-local caching by operation and evaluation time | Unbounded grids and general ingestion adapter |
| Extension | Execute an additional value operation | No general executor | Unsupported operations may route to explicit fallback | A concrete local implementation for each admitted extension |

Grouped TopK is represented in the target plan as Sort followed by Limit within
each group. The current dedicated TopK plan node must be replaced, and Limit
needs an explicit grouping contract. A global Limit is not equivalent. An
optimized kernel may execute the composition without changing its meaning.
Candidate completeness remains a condition on pruning, not on ranking.

The current-series operations maintain and read a set of time series and their
current values; they do not provide a general raw-table scan or full historical
read. Their code names are `MaintainPopulation` for updates and `ReadPopulation`
/ `CurrentSeries` for reads.

External computation (`ExternalExact`, `ExactSubquery`,
`CandidateExactSubquery`) and fallback are routing choices, not local physical
operation implementations. They do not fill any missing coverage in this table.
The shared DAG walker schedules operations and caches intermediate results within each request but still needs backend
adapters; importing the library alone does not provide a complete query engine.

**Deferred raw-data support:** this PR does not implement local raw Scan or
claim complete local execution when only raw data is stored. External fallback
and the independent raw-input kernel tests do not satisfy that capability.

### Candidate pruning is a composed subgraph

The fused candidate-ranking operator is removed from Planner and QueryPlan.
The target graph uses a general semi-join in place of the current dedicated
`MembershipFilter` adapter. That code change is pending separately from this
documentation update. The graph contains these operations:

1. Read membership keys from a summary.
2. Obtain authoritative values, optionally pushing the membership restriction
   into an explicitly bound external request.
3. Apply a general semi-join with explicit matching keys that preserves value-row order and
   multiplicity. Membership scores never replace authoritative values.
4. Sort authoritative values and apply Limit independently within each group.

The semi-join has no k, grouping or ranking behavior. The shared library currently provides `rows::membership_filter` and
`rows::grouped_topk`; the pending change replaces the former with a general
`rows::semi_join` kernel that other deployments can compose with ranking. A missing authoritative value fails a
certified membership plan; best-effort pruning remains explicitly approximate.
The pruning certificate stays on the semi-join. Exact reranking does not prove
that omitted keys could not have won. Planner still rejects uncertified pruning
for an exact request.

External expression binding verifies the selected exact subtree against its
native expression; it does not substitute the original top-level TopK child.
Planner represents and costs filtering and ranking separately. Incompatible
installed plans are rejected; removed operators have no compatibility path.

### Summary-family and readout coverage

The Planner-family factory accepts only `PerSubpopulationInstance` grouping and
matching family/parameter variants. The presence of a low-level accumulator does
not automatically register a Planner binding. Supported kernels expose update,
compatible-state merge and family-specific query operations; decoding, window
coverage and readout compatibility still require the backend adapter.

| Family / algorithm | Factory admission | Intended readout and restrictions |
| --- | --- | --- |
| Exact Sum, Count, Min, Max | Supported matching ExactParams | Corresponding exact readout; keyed/scalar update shape must match |
| Exact Increase, Rate | Supported matching ExactParams | Counter/time-aware readout; not plain scalar sum/division semantics |
| Exact IRate | Unsupported | No matching factory/readout binding |
| KLL | k in 8..65535 | Quantile |
| DDSketch | finite 0 < alpha < 1 | Quantile; backend continuous-percentile adapter uses interpolated readout |
| HLL | precision 4..18, local Regular HLL implementation | Cardinality; kernel availability does not supply an accuracy/failure-probability proof |
| CMS, CountSketch | Positive width/depth, checked allocation size | PointCount: supported key/value shape or sample-total readout; no heap TopK |
| CMSWithHeap, CountSketchWithHeap | Same matrix checks plus positive heap size | PointCount and ranked heap TopK; approximate membership is not guaranteed complete exact TopK |
| UnivMon | Positive heap/columns, rows 1..20, layers 1..64, checked dimensions | Cardinality, FrequencyL2, FrequencyEntropy and sample-total PointCount; no general keyed readout |
| KMV, Theta | Unsupported | Planner algorithm existence is not runtime support |
| Plain, Sample, Wavelet, StatModel | Not summary-factory kernels | Plain rows may be relation values, not a SummaryAgg accumulator |
| Shared grouping / Hydra KLL | Not admitted by current Planner-family factory | Low-level Hydra code exists; no installed shared-grouping coverage claim |

All six SketchQuery variants are accounted for: Quantile, Cardinality,
PointCount, TopK, FrequencyL2 and FrequencyEntropy. They are family-specific,
not a Cartesian product with every sketch. PointCount requires the supported
SampleValue/None or named/qualified-key/Some(value) shape; heapless sketches
cannot enumerate TopK. Native matrix construction checks are distinct from
packed-wire decoder limits. The SummaryAgg capability check does not validate
all subsequent readout combinations or certify approximation guarantees.

### Precomputation boundary: present status and required acceptance

| Precomputation mode | Definition | Current support / remaining gaps |
| --- | --- | --- |
| No precomputation | The query starts from raw data and performs all required computation at query time. | General local raw input and query-time summary construction are not supported in installed plans; deferred from this PR. |
| Partial precomputation | The query reuses previously computed results or states and performs the remaining computation at query time. Inputs may combine stored states, stored values, and raw data. | Supported stored-state and query-time operations can be combined. General plans requiring local raw input or query-time summary construction remain incomplete. |
| Full precomputation | All data-dependent computation needed for the query result has been performed before the query arrives. Query execution retrieves the prepared result and formats the response. | Supported only where the prepared result matches the requested query and time scope and is available. Reading stored summaries followed by merging, estimation, aggregation or ranking is partial precomputation. |

These definitions are independent of any particular algorithm. The KLL consumer
tests are examples of constructing, merging, and querying state across different
precomputation boundaries. They demonstrate reusable kernel behavior, not
complete backend support for all three modes. In particular, a test that queries
a prebuilt KLL state still performs estimation at query time; it does not
demonstrate full precomputation of the query result.

The remaining work below describes the target contract, not a requirement to
implement local raw Scan in this PR. Planner must express valid query-time summary
placement; the backend must bind local raw inputs and query-time builders; and
installation must check every reachable operator against concrete adapter
capabilities, including expressions, family/readout combinations and edge states.
SummaryJoin/Subtract/Delete require actual defined implementations or explicit
compile-time rejection in local plans. External execution must be declared as a
different deployment capability, not silently counted as local support.

Acceptance must execute the same supported query under all three placements with
external forwarding disabled, check results against the raw exact computation
(and declared approximation guarantees where applicable), and reject unsupported
operators/parameters at installation. Include grouped/temporal, empty/missing
window, mixed-state compatibility and query-time summary cases. No percentage
coverage or universal executability is claimed until these tests exist and pass.

### Evidence and verification limits

Shared-library tests cover kernels, a KLL three-boundary consumer, invalid KLL
parameters and native CountSketch dimensions. Backend tests cover supported DAG,
maintenance, readout and relation paths. Passing these suites is not a proof that
every Planner payload or parameter combination is locally executable. Inherited
level-1 grouped-Sum/quantile-ratio failures and #759's strict local-execution gate
remain unresolved; external exact success does not satisfy that gate.

## StoredSummary reads

A `ReadMaterialization` carries the exact `StoredOutputReference` published for
its writer. V1 derives its output ID from the definition ID because the current
store index is definition keyed. The read proceeds as follows:

1. Validate the output reference and its definition identity.
2. Use the active catalog generation and definition to select only visible
   stored summaries. A previous generation is visible only after an explicit
   compatible-definition decision during activation.
3. Reject unpublished input, a request outside the bound pane/full-window
   phase, missing required panes, or an unavailable population.
4. Check the installed catalog descriptor against the requested readout family.
5. Decode payloads using their stored descriptor and merge only compatible
   states according to the plan's grouping contract.

The query engine performs an ID lookup in the SummaryStore index. It does not
search the catalog at serving time for an alternative summary. Instance
metadata and payload are one logical `StoredSummary`; the physical
`SummaryStateReference` used by the storage engine is not a QueryPlan edge.

Readiness is conservative. Until the store represents completed empty panes,
a missing additive pane is not assumed to be zero. A coverage or format miss
therefore triggers the entry's installed fallback behavior instead of returning
a partial accelerated answer.

## Dependency ordering and reuse

Each evaluation derives only the sub-DAG reachable from the requested root.
Dependencies complete before their consumer. Request-local caching of intermediate results makes a diamond
graph execute its shared node once within that evaluation.

PromQL subqueries are the exception to a cache key containing only the operation identity. The same
node has a different result at each evaluation timestamp, so their cache key is
`(node_id, evaluation_time)`. Repeated access at the same timestamp reuses the
value. Prepared external leaves use the same identity and are issued before
local query-time evaluation so network I/O does not hide inside a synchronous
operator.

The cache is request local. It is discarded after the root result is adapted;
the SummaryStore is the cross-request reuse boundary. A summary revision fence
prevents a response from combining payloads changed during one evaluation.

## Exact work and fallback

An `ExternalExact` node is an ordinary typed DAG leaf. Its expression, output
shape, parameters and input contracts are installed with the plan. The engine
may prepare it through Prometheus, MetricsQL or ClickHouse only when forwarding
is enabled. Candidate-dependent leaves first evaluate their installed candidate
sub-DAG and pass the resulting membership set to the exact request.

Fallback is not an implicit parser retry inside the executor. An
`ExactFallback` node fails deliberately, and the entry's `FallbackPolicy`
determines whether the router may call the exact backend. Invalid graphs,
incompatible stored state and unsupported nodes fail closed with a scoped
reason.

## Shared KLL example

Two installed query entries can read one precomputed output while keeping
different query roots:

```mermaid
flowchart LR
  subgraph P[PrecomputePlan]
    Samples[Latency samples] --> KLL[Build KLL by service]
    KLL --> Write[Write output latency-kll]
  end
  Write --> Store[(StoredSummary records)]
  subgraph Q50[QueryPlanEntry p50]
    Read50[Read latency-kll] --> P50[Quantile 0.50]
  end
  subgraph Q99[QueryPlanEntry p99]
    Read99[Read latency-kll] --> P99[Quantile 0.99]
  end
  Store --> Read50
  Store --> Read99
```

`Read50`, `Read99` and `Write` carry the same `StoredOutputReference`. Each
request reads the ready population/window records and executes only its own
readout sub-DAG. No query rebuilds the KLL and no serving-time catalog search
chooses a different summary.

Within one entry, two parents may also share a read or relational node. The
request-local cache returns its existing value to the second parent. Across the
p50 and p99 requests, payload reuse comes from SummaryStore rather than a
cross-request executor cache.

## Concurrency, cancellation and limits

Requests execute concurrently and own separate result caches and intermediate
values. The active physical plan and committed stored summaries are shared
through immutable snapshots or synchronized store indexes. No mutable execution
context is shared between requests.

The current backend evaluates ready nodes sequentially inside one request. Independent requests
still run concurrently. Parallel execution of independent nodes is unnecessary
for correctness and remains future work. External I/O uses the request client's
timeout; cancellation drops the request-local evaluation and its prepared
values. Query-time subqueries enforce depth and evaluation budgets to bound memory
and work.

Large relation intermediates and a unified execution resource budget remain follow-up work. The current implementation also does not cache
root results across requests, add a distributed query scheduler, or reuse stored
payloads across plan versions without the activation-time compatibility check.

## End-to-end sequence

1. The control plane installs and activates one coherent physical plan.
2. The serving endpoint canonicalizes the request only to find its installed
   entry; it does not compile a new execution graph.
3. The engine validates the entry against the active catalog generation.
4. Declared external leaves are prepared when allowed.
5. The target shared runtime executes the reachable physical operator DAG with
   request-local caching of intermediate results.
6. `ReadMaterialization` nodes resolve their bound ready stored summaries.
7. Node failures carry query/node context and follow the installed fallback
   policy.
8. A revision fence confirms that stored input did not change during execution.
9. The root value is adapted to the Prometheus/MetricsQL or ClickHouse response.
