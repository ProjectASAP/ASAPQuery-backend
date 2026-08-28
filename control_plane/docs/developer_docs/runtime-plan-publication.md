# Runtime plan publication

> Implementation status: target contract; transport exists, but semantic
> CollectorPlan application/reporting is not complete.

## Purpose

Plan publication stages and activates the matching CollectorPlan and BackendPlan
created by the physical compiler. Transport success is not semantic activation.

Design sources:

- [Physical planning](../physical-planning.md)
- [BackendPlan](../backend-plan.md)
- [ASAPCollector plan interface](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/developer_docs/opamp-config-push.md)

## Current code map

| Responsibility | Current entry point |
| --- | --- |
| Runtime-specific emission | [`emit/mod.rs`](../../src/emit/mod.rs) |
| Collector/gateway/backend payloads | [`emit/stage_config.rs`](../../src/emit/stage_config.rs) |
| Backend publication | [`emit/backend_push.rs`](../../src/emit/backend_push.rs) |
| OpAMP collector delivery/status | [`opamp/mod.rs`](../../src/opamp/mod.rs) |
| BackendPlan schema | [`backend_plan/mod.rs`](../../src/backend_plan/mod.rs) |
| BackendPlan protobuf | [`proto/backend_plan.proto`](../../proto/backend_plan.proto) |

## Publication sequence

1. Validate both runtime plans against the capability snapshots used to compile
   them.
2. Stage BackendPlan without routing production queries to it.
3. Deliver CollectorPlan through OpAMP to every required collector.
4. Require backend installation evidence and collector semantic-application
   evidence for the same plan/version.
5. Activate query routing at the declared activation boundary.
6. Drain and retire the previous version after readers and lateness expire.

Any partial failure leaves the previous unexpired plan authoritative.

## Collector transport contract

Do not duplicate the CollectorPlan schema here. The authoritative field and
validation definitions are in ASAPCollector's
[`opamp-config-push.md`](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/developer_docs/opamp-config-push.md).

The backend publisher must preserve these corresponding requirements:

- transport is OpAMP `AgentRemoteConfig`/`AgentConfigMap`;
- the exact entry name is `asap-collector-plan.yaml`;
- the body is the versioned YAML `CollectorPlan`, with
  `content_type: application/yaml`;
- OpAMP `config_hash` identifies delivered bytes and is distinct from the
  cross-runtime `metadata.plan_id`;
- CollectorPlan and BackendPlan share `plan_id`, `plan_version`,
  `backend_compat`, and materialization identities; and
- an OpAMP `RemoteConfigStatus.APPLIED` response does not prove semantic
  activation.

The expected semantic application report uses the collector capability
`io.asap.collector.plan.v1`, message type `application_report`, and reports the
active plan/version, remote-config hash, backend compatibility, active
materializations, effective capability hash, timestamps, and structured
errors. Treat unknown or missing fields according to the versioned collector
contract rather than guessing defaults.

## Evidence contract

The publisher distinguishes:

- transport acknowledgement: bytes reached an endpoint;
- validation acknowledgement: payload schema/capabilities were accepted;
- semantic application: the runtime reports the expected active plan and
  materialization identities; and
- data evidence: emitted/ingested state carries those identities.

Only semantic application plus compatible data evidence can make a plan
queryable.

## Current implementation gap

The corresponding ASAPCollector document records that current OpAMP handling
still applies a complete OTel Collector YAML, identifies it primarily by
`config_hash`, and reports `APPLIED` after syntactic validation/file write. It
does not yet provide the target CollectorPlan parser, atomic in-process apply,
or semantic application report.

Backend code and tests must represent this honestly. Until both sides implement
the target contract, `APPLIED` is delivery evidence only and cannot satisfy the
MVP plan-application gate.

## Versioning and retry

- Re-sending identical `(plan_id, plan_version)` content is idempotent.
- Reusing that pair for different content is an error.
- Older or expired versions cannot replace a newer active version.
- Retry preserves activation and expiry timestamps.
- Rollback selects an explicitly retained compatible version; it does not edit
  an active plan in place.

## Failure handling

Surface collector rejection, backend rejection, timeout, partial rollout, stale
status, and identity mismatch separately. Do not report a successful plan push
when only one runtime side applied it.

## Required tests

- idempotent repeated delivery;
- stale version rejection;
- collector-only and backend-only application remain inactive;
- activation succeeds only for matching identities;
- failed rollout retains the old route;
- rollback restores a complete prior pair; and
- status artifacts contain enough evidence for the MVP harness.
