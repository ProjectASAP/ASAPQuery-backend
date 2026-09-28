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
Computation has the same semantics at ingestion time and query time.

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
#763 integrates ingestion DAG execution and #765 integrates query DAG execution.
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

Selected Filter, Sort, Limit and semi-join fragments are compiled by Planner and persisted
with typed input contracts. Complete query candidates include Planner readouts
that turn accumulator state into values; internal shared and stored edges retain
their state types. Runtime binds protocol vectors to these contracts;
renamed or multiple join keys retain their original types and positions. External
Prometheus bindings fetch the selected authoritative subquery without rewriting
its labels from the candidate side. The native join performs the comparison.

Physical execution errors retain their original cause. Memory exhaustion and
cancellation terminate both instant and range requests; routing does not try a
second engine or exact fallback. Physical fragments in one query evaluation share
one run context. This does not yet account for every protocol-buffer allocation
or provide an HTTP-disconnect cancellation mechanism.
