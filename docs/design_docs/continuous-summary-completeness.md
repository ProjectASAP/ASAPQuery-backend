# Summary publication completeness for accepted input

## Continuous local Remote Write

With bounded revisions enabled, SummaryStore durably captures each accepted input
revision before running the precompute Physical DAG. Replaying retained input
replaces a window result; it never adds the full replacement to its previous state.
Outputs commit independently. The checkpoint retains both input admission revisions
and output snapshots, so same-version recovery can finish a partial publication
without changing already committed sketch bytes.

An installed query pins the latest snapshot with compatible revisions, complete
coverage for its required windows and acceptable freshness. All branches and range
evaluation points share that view. If A:r2 is ready while B:r2 is pending, A+B can
continue reading fresh r1. Publication and reclamation cannot change an already
pinned query. A new plan version has separate history and must warm independently.

The correction horizon uses Backend admission time and applies to the whole input
request, including previously unseen series. A request containing an expired or
future sample is rejected before durable admission or deduplication changes. A
periodic capture evaluates newly elapsed windows without pretending to prove that
upstream sources will never send late data. Resource and cancellation errors stop
execution; they are not capability misses or exact-fallback triggers.

This first implementation serially rebuilds bounded retained input, stores
checkpoints through atomic replacement and fsync, and enforces a single local
writer. It does not add distributed watermark coordination or incremental replay
optimization. Configuration and execution examples are in
[Precompute DAG execution](precompute-dag-execution.md#continuous-local-input-revision-snapshots).

## Queued finite and other ingestion paths

The SummaryStore owns an in-memory admission inventory keyed by catalog generation, summary definition, group population and physical time window. Remote Write reserves every worker queue slot before admitting any coordinates, then sends the same immutable input revision with the queued work. Queue rejection leaves neither messages nor admission records behind.

Workers carry the first and last consumed input revisions through materialized source and maintenance DAG outputs. State writes validate the generation and consumed revisions before modifying SummaryStore; a later output cannot acknowledge an earlier missing update. Corrections within one admitted coordinate are combined before publication. Dropped admitted input leaves the coordinate incomplete. Accepted raw-sample throughput counts once per request, without materialization fanout; materialized-output throughput counts new successful store publications, without receipt retries.

A query rejects overlapping admitted but unpublished input. The whole QueryPlan DAG is fenced by the store revision, so branches cannot combine different publication snapshots. Both direct sketch writes (including OTLP) and exact-state writes (including SQL backfill) participate in the mutation fence; in-flight writes cannot certify a read snapshot. Finite absence proof also requires that every state mutation was admitted. For these paths, the fence is global: unrelated updates can cause conservative exact fallback. This conservative fence is not used to select the pinned continuous-input snapshots described above.

Finite drain closes input, waits for all workers and certifies that every accepted coordinate published. Only this closed-input proof can establish that a known series has no samples in a retained window. Missing state, unknown series, unknown time coverage and incomplete coordinates never imply an empty result. Live Remote Write provides no source watermark, so the backend does not infer global event-time completeness from the fastest series.

Admission metadata has bounded coordinate, pending-revision and byte budgets. Completed receipts expire with configured materialization retention; pending work and the published prefixes of pending admissions remain protected. Already admitted slow-worker outputs can finish behind another worker's maintenance replay frontier. Unsolicited expired input and expired untagged replay remain rejected.

The queued-path admission inventory is not durable and does not establish exactly-once execution across crashes. Startup installs the authoritative catalog before persistence recovery, and current-version persisted series metadata preserves summary definition identity and catalog provenance. Recovery requires the same catalog generation and rejects unsupported persisted metadata formats. A changed catalog generation starts cold and requires fresh state; cross-generation adoption is not supported. General multi-input maintenance transforms and durable producer watermarks remain separate work.

Durable SID bindings also preserve retirement and expiry timestamps and a removal tombstone. Lifecycle changes publish through the same serialized metadata writer as the flusher before changing in-memory visibility. A stale flush snapshot cannot clear those fields. Recovery leaves removed and expired instances unregistered and preserves a still-retired instance's expiry deadline. Tombstones remain until durable state is explicitly reclaimed; this change does not claim automatic tombstone garbage collection or cross-generation reactivation.
