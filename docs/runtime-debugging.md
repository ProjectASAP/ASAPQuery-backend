# Runtime diagnostics along the execution path

Audience: developers tracing an installed plan from selection to a result.
The [architecture](design_docs/asapplanner-integration.md) and
[SDS contract](design_docs/summary-catalog-sds-architecture.md) define ownership;
diagnostics observe these decisions and never select replacement computations.

## Enable and correlate

Set `RUST_LOG=info,asap_runtime_debug=debug` on controller and backend. Use
`RUST_LOG=info` to hide these events. Logs contain timestamps, target, file/line,
span context and event fields. Controller output goes to stdout; backend output
also goes to `<output_dir>/query_engine.log` without terminal colors.

Correlate by `plan_id` and `plan_version`, then `query_id` and the process-local
`call_id`. A call ID is not a distributed trace ID. For state reads and writes,
use `stored_output_id` for the deployed producer and `definition_id` for its
meaning. A `sid`/storage handle is only a local row locator. Descriptor hashes
are diagnostics, not semantic IDs or authorization tokens.

## Follow the architecture in order

| Step | Owner and operation | Diagnostic evidence |
| --- | --- | --- |
| 1 | Planner selects the Logical Post-ASAP DAG and maintenance requirements | `planner.select` entry and selection errors. `deployment.candidate_inventory`, `deployment.candidate_evaluation` and `deployment.candidate_selected` distinguish compilation/pricing failures from selection within the bounded inventory. Backend spans bracket Planner calls; they do not instrument every internal Planner optimization. |
| 2 | Planner compiles physical operators, dependencies and typed boundaries | Inspect the selected DAG artifact alongside compilation spans. Do not interpret a backend adapter node as a second Planner physical node. |
| 3 | Backend Deployment Plan Compiler binds sources, stored outputs and deployment policy | `deployment.bind`, transmission construction and publication spans; plan generation and query counts. The backend does not reselect operators. |
| 4 | Backend validates, stages and activates one generation | HTTP staging/activation and summary-catalog installation events. Activation does not imply state readiness. |
| 5 | Precompute engine resolves inputs and invokes the shared executor | Worker and precompute DAG spans: node/dependency IDs, execution timing, window, sink and inclusive node duration. Reused inputs and already-committed sinks have separate events. |
| 6 | SummaryStore admits and publishes state | `sds.bind_storage_handle` includes both bound identities; `sds.publish` includes output, plan generation, window and revision. Replay acknowledgment is distinguished from a new publication. |
| 7 | Query engine selects the installed QueryPlan | Query call/preparation events and the selected query DAG. A bound query does not search for semantically similar outputs. |
| 8 | Bound read validates identity, locates records and validates eligibility | `sds.bound_read` carries output ID, definition ID and requested range. `sds.validate_binding` checks the installed reference; `sds.locate_records` reports candidate storage handles; `sds.validate_records` follows format, applicable coverage and stable-revision checks. Failures terminate the read span. |
| 9 | Shared operators compute the result; backend serves or applies installed fallback | Node start/completion/failure, memo hits, query completion and remote/RPC counts. Match fallback evidence to the request rather than assuming a successful response ran locally. |
| 10 | SummaryStore recovers persisted state on restart | `sds.recover` brackets metadata replay and reports eligible restored handles. Existing warnings distinguish provenance mismatch, missing identity and unsupported state. A successful disk read alone does not establish semantic eligibility. |

Step 8 follows the actual implementation: it validates the installed reference
before lookup, then validates concrete state. Lookup is scoped to the selected
output; equal definition IDs never authorize switching to another deployed
output. Coverage rules depend on the selected state family: additive panes need
contiguous coverage, while supported counter state carries sample endpoints.
No diagnostic introduces a universal no-gap rule for every operator.

## Timing and node identity

Node durations are inclusive. Parent and child intervals can overlap and shared
producers can serve multiple consumers, so do not sum them as CPU time. These
logs do not provide exclusive kernel, allocation or lock timing. Pair them with
#766's profiling workflow for overhead attribution.

Backend QueryPlan adapter IDs and Planner DAG IDs occupy different namespaces.
The installed bindings connect their sinks; retain the plan artifact when
following an edge across that boundary. Build, merge and readout are operators,
not fixed precompute/query phases. Interpret placement from the selected DAG.

## Detail and limitations

For storage and worker details, add
`data_plane::precompute_engine=debug,data_plane::storage_engines=debug`.
The diagnostic target omits samples, complete query text and full fallback
expressions from its new structured fields. Existing errors or worker fields can
still contain query text or group labels. Logs are text events, not a versioned
JSON API. Missing a completion event can indicate an interrupted process; it
is not proof that work committed. Publication receipts and recovered state remain
the source of truth.

A missing candidate is not evidence that its placement costs more. For example,
the retained #728 grouped-rate fixture exports per-series Rate state plus
query-time Sum and an exact fallback; it does not export a precomputed grouped
Rate-result candidate. The selected plan therefore cannot establish that query-time
Sum beats that absent candidate. Search coverage is scoped to the declared
inventory, never a claim of exhaustive physical optimization.

### Installed SQL physical DAG stages

SQL plans compile native operators before installation (`physical.compile_install`).
Activation and serving validate the persisted program (`physical.recover_validate`);
this stage decodes physical operators and does not lower logical expressions.
`physical.bind_inputs` attaches the resolved external/SDS batches, and
`physical.execute` covers the request's physical execution path. These debug spans
include the query identity, root or input count, and errors without logging batches
or sketch payloads. They use the existing `asap_runtime_debug` target.

These stages describe the installed SQL path. They do not establish that the
remaining PromQL candidate-selection and maintenance paths have migrated to the
same physical handoff.

### Native SDS publication and recovery

`sds.publish_native` covers complete-cohort validation and immutable publication;
`sds.read_native` covers the installed output lookup and native decoding. Their
fields include the requested window and byte budget; read spans also include the
plan version. Group labels and summary payloads are not logged. Successful return
from an idempotent publication can mean the output was already committed; the
publication receipt remains authoritative.

These spans instrument the native storage API, including its recovery E2E. They
do not imply that all PromQL maintenance candidates use that API yet.
