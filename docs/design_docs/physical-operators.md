# Consuming Planner physical operators

## Ownership and contract

The shared physical operator library lives in ASAPPlanner, alongside post-ASAP
IR and physical lowering. Its canonical architecture and acceptance contract are
in [the Planner design](https://github.com/ProjectASAP/ASAPPlanner/blob/feat/shared-physical-operators/docs/design_docs/physical-planning-and-deployment.md).

ASAPQuery-backend depends on `asap-physical-operators`, `asap_sketch_codec` and
Planner IR at the same immutable revision. It does not own a second copy of the
runtime or mathematical kernels. A new IR operation and its implementation can
be changed and tested together in Planner.

The query and precompute integration PRs both use the independent ASAP DAG runtime. Deployment code binds input
sources, storage, ingestion windows, publication and protocol outputs. Execution
phase belongs to the physical node's data state, not the operator payload.
Computation has the same semantics during precomputation and query execution.

Candidate pruning uses a general semi-join with explicit matching keys, followed
by grouped Sort and grouped Limit. The completeness certificate belongs to the
pruning step; sorting exact scores does not prove completeness. There is no
`MembershipFilter` physical operator or compatibility dispatch.

## Acceptance

Planner validates operation, expression, family and parameter support when it
compiles the physical DAG. Backend validates deployment input and storage
contracts before execution. It must not reject a supported computation because
its adapter discarded a type, predicate or join key. An implicit external fallback is not an implementation.
Planner tests cover shared producers, phase assignment, typed batches and
composed candidate pruning. Backend tests cover source binding, installed plan
validation, storage compatibility and query responses.

The migration does not supply a local raw Scan. That deployment capability
remains deferred. Library tests supplied with raw batches are not evidence that
the backend can execute arbitrary raw-only installed plans.

## Stack integration

Planner PR #462 owns the library and depends on Planner #461, including its
composed candidate-pruning API. Backend #774 consumes the pinned library;
#763 integrates precomputation DAG execution and #765 integrates query DAG execution.
The remaining backend stack builds on those integrations. #728 checks candidate
structure, #742 checks selection with synthetic costs, and #775 checks installed
plans against data-plane results. Production evidence and performance validation
remain separate from these correctness tests.

The library also owns stored-summary decoding, delta reconstruction and
family-specific readout kernels. Deployment adapters select compatible panes
and translate inputs and outputs; they do not copy those computations.

The shared `physical_planner::compile` API validates concrete operators and typed
input contracts before opening sources. Deployment resolves those inputs and
instantiates the compiled DAG. `CompiledPhysicalDag::from_operators` supports
adapters that already have a concrete physical fragment. Unsupported deployment
input frontiers are reported to Planner as feasibility evidence, before selection.

## Deployment retention and execution evidence

The backend `DeploymentPlanCompiler` binds declared historical query delay to
retention; it does not change a logical selector's lookback. Current-series
populations retain bounded versions only when the deployment requests historical
coverage. Their current and retained versions share the population memory budget.
Changed inputs require a new output revision; an older common snapshot remains
eligible while it satisfies freshness. Missing or evicted state fails explicitly.
Candidate costing includes version residency, copying and retirement.

Every installed range evaluation uses the shared DAG execution path and reports
its actual local-summary and external-exact work. The differential and benefit
runners require successful local execution evidence as well as matching values.
Routing to the ASAP endpoint alone is insufficient.
Selected Filter, Sort, Limit and semi-join fragments are compiled by Planner and persisted
with typed input contracts. Complete query candidates include Planner readouts
that turn accumulator state into values; internal shared and stored edges retain
their state types. Runtime binds protocol vectors to these contracts;
renamed or multiple join keys retain their original types and positions. External
Prometheus bindings fetch the selected authoritative subquery without rewriting
its labels from the candidate side. The native join performs the comparison.

Physical execution errors retain their original cause. Memory exhaustion and
cancellation terminate both instant and range requests; routing does not try a
second engine or exact fallback. All range steps and nested physical executions share one request budget and
cancellation signal, while retaining separate execution state. Tracked inputs,
workspace and results count against that budget; estimates are not a hard RSS
limit. See [query execution contracts](query-dag-execution.md#6-request-consistency).
