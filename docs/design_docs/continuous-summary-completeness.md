# Summary publication completeness for accepted input

Status: implemented in-memory admission and read-fence contract. Audience:
developers changing Remote Write admission, maintenance publication, SummaryStore
reads or recovery.

## Document map

1. [Contract at a glance](#contract-at-a-glance)
2. [Worked example](#worked-example)
3. [Admission and publication](#admission-and-publication)
4. [Query completeness](#query-completeness)
5. [Finite drain](#finite-drain)
6. [Budgets, recovery and retirement](#budgets-recovery-and-retirement)
7. [Limits](#limits)

## Contract at a glance

```text
Remote Write request
  -> reserve all queue capacity
  -> record one immutable admitted revision
  -> workers publish outputs carrying consumed revisions
  -> SummaryStore advances completeness only for published admitted work
  -> QueryPlan reads one fenced store revision or falls back
```

An accepted request promises queue admission, not completed summary publication.
Missing or unpublished admitted work makes an overlapping query incomplete; it
never means an empty result.

## Worked example

Assume request revision `101` contains samples for two coordinates:

```text
(service=api, window=12:00..12:01)
(service=web, window=12:00..12:01)
```

The receiver reserves both worker slots before acknowledging the request. The
`api` worker publishes state carrying `first_revision=101` and
`last_revision=101`, while the `web` worker has not finished.

```yaml
admission_revision: 101
published:
  api/12:00..12:01: 101
pending:
  web/12:00..12:01: 101
```

A query covering both services at store fence `101` cannot combine the published
`api` state with a missing `web` state. It uses its configured exact fallback or
returns unavailability. After `web` publishes, the same fenced read may use both
states. If queue reservation initially fails, neither coordinate nor admission
record is created and the request is retryable.

## Admission and publication

The in-memory admission inventory is keyed by catalog generation, summary
definition, group population and physical time window.

Here `catalog generation` names the current admission and recovery key. In the
[proposed SDS contract](summary-catalog-sds-architecture.md), the coherent
installed plan bundle uses `plan_version` and a plan-scoped state slot; runtime
instance metadata records actual partition, coverage, format and readiness.
Adapting persisted records requires an explicit mapping, not a silent rename of
the existing generation field. The catalog holds definition semantics rather
than per-instance publication status.

- Remote Write reserves every required worker slot before admitting any work.
- All queued work carries the same immutable input revision.
- Queue rejection leaves no messages or admission records.
- Workers propagate first and last consumed revisions through maintenance DAG
  outputs.
- State writes validate generation and revisions before mutating SummaryStore.
- Corrections within one admitted coordinate combine before publication.
- A later output cannot acknowledge an earlier missing update.

Accepted raw-sample throughput counts once per request, without materialization
fanout. Materialized-output throughput counts new successful publications,
without receipt retries.

## Query completeness

A query rejects any range overlapping admitted but unpublished input. The whole
QueryPlan DAG uses one store revision so branches cannot combine different
publication snapshots.

Direct sketch writes, including OTLP, and exact-state writes, including SQL
backfill, participate in the mutation fence. In-flight writes cannot certify a
read snapshot. Finite absence proof additionally requires that every mutation
was admitted.

The initial fence is global, so unrelated updates may conservatively cause exact
fallback. Read-set-scoped revisions are required before claiming sustained
continuous-query performance.

## Finite drain

A finite drain closes input, waits for workers and verifies that every accepted
coordinate published. Only this closed-input proof can establish that a known
series has no samples in a retained window.

Missing state, unknown series, unknown time coverage and incomplete coordinates
never imply an empty result. Live Remote Write has no authoritative source
watermark, so the fastest series cannot establish global event-time completeness.

## Budgets, recovery and retirement

Admission metadata has bounded coordinate, pending-revision and byte budgets.
Completed receipts expire with the selected producer's state retention; pending
work and its published prefixes remain protected. Admitted slow-worker outputs
may finish behind another worker's replay frontier. Unsolicited expired input
and expired untagged replay are rejected.

Startup installs the authoritative catalog before persistence recovery. Version-3
series metadata preserves summary-definition identity and catalog provenance.
Recovery binds only the same catalog generation; incompatible and legacy records
remain unbound unless an explicit cross-generation compatibility decision exists.

Durable SID bindings preserve retirement/expiry timestamps and removal
tombstones. Lifecycle changes use the same serialized metadata writer as the
flusher before changing in-memory visibility. Stale flush snapshots cannot clear
those fields. Recovery does not register removed or expired instances and
preserves a retired instance's expiry deadline.

## Limits

The admission inventory is not durable and does not provide exactly-once
execution across crashes. Tombstones remain until explicit durable-state
reclamation; automatic garbage collection is not claimed. General multi-input
maintenance transforms and durable producer watermarks remain separate work.
