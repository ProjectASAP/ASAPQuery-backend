# Consuming Planner physical operators

## Ownership and contract

The shared physical operator library lives in ASAPPlanner, alongside post-ASAP
IR and physical lowering. Its canonical architecture and acceptance contract are
in [the Planner design](https://github.com/ProjectASAP/ASAPPlanner/blob/feat/shared-physical-operators/docs/design_docs/physical-operators.md).

ASAPQuery-backend depends on `asap-physical-operators`, `asap_sketch_codec` and
Planner IR at the same immutable revision. It does not own a second copy of the
runtime or mathematical kernels. A new IR operation and its implementation can
be changed and tested together in Planner.

Both engines use the independent ASAP DAG runtime. Deployment code binds input
sources, storage, ingestion windows, publication and protocol outputs. Execution
phase belongs to the physical node's data state, not the operator payload.
Computation has the same semantics at ingestion time and query time.

Candidate pruning uses a general semi-join with explicit matching keys, followed
by grouped Sort and grouped Limit. The completeness certificate belongs to the
pruning step; sorting exact scores does not prove completeness. There is no
`MembershipFilter` physical operator or compatibility dispatch.

## Acceptance

Binding must reject an unsupported operation, expression, family, parameter or
schema before execution. An implicit external fallback is not an implementation.
Planner tests cover shared producers, phase assignment, typed batches and
composed candidate pruning. Backend tests cover source binding, installed plan
validation, storage compatibility and query responses.

The migration does not supply a local raw Scan. That deployment capability
remains deferred. Library tests supplied with raw batches are not evidence that
the backend can execute arbitrary raw-only installed plans.
