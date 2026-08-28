# Developing runtime plan publication

> Interface status: target public API. OpAMP transport exists today; semantic
> CollectorPlan application/reporting is still incomplete.

## 1. Code architecture

Publication begins only after physical compilation returns a complete bundle:

```text
CompiledPlanBundle
      |
      v
PlanPublisher
  |             |
  v             v
CollectorClient BackendPlanClient
  |             |
  v             v
CollectorReport BackendPlanReport
       \         /
        v       v
      ActivationResult
```

`CollectorClient` is the ASAPQuery-side counterpart of ASAPCollector's
authoritative
[`opamp-config-push.md`](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/developer_docs/opamp-config-push.md).
This repository does not redefine CollectorPlan fields.

## 2. Public interfaces and definitions

### Runtime clients

```rust
pub trait CollectorPlanClient {
    type Error;

    async fn stage(
        &self,
        target: CollectorTarget,
        plan: CollectorPlan,
    ) -> Result<CollectorApplicationReport, Self::Error>;
}

pub trait BackendPlanClient {
    type Error;

    async fn stage(
        &self,
        target: BackendTarget,
        plan: BackendPlan,
    ) -> Result<BackendApplicationReport, Self::Error>;
}
```

Collector transport requirements come directly from the corresponding
ASAPCollector interface:

- OpAMP `AgentRemoteConfig`/`AgentConfigMap`;
- exact entry name `asap-collector-plan.yaml`;
- YAML CollectorPlan with `content_type: application/yaml`;
- OpAMP `config_hash` identifies bytes, not cross-runtime plan semantics; and
- `RemoteConfigStatus.APPLIED` is delivery/application evidence, not semantic
  activation evidence.

### Application reports

```rust
pub enum ApplicationStatus {
    Rejected,
    Staged,
    Active,
    Expired,
    Failed,
}

pub struct ApplicationError {
    pub code: String,
    pub path: String,
    pub message: String,
}

pub struct CollectorApplicationReport {
    pub plan_id: String,
    pub plan_version: u64,
    pub backend_compat: String,
    pub remote_config_hash: Vec<u8>,
    pub status: ApplicationStatus,
    pub active_materialization_ids: Vec<String>,
    pub effective_capability_hash: String,
    pub observed_at: Timestamp,
    pub activated_at: Option<Timestamp>,
    pub errors: Vec<ApplicationError>,
}

pub struct BackendApplicationReport {
    pub plan_id: String,
    pub plan_version: u64,
    pub backend_compat: String,
    pub status: ApplicationStatus,
    pub active_materialization_ids: Vec<String>,
    pub observed_at: Timestamp,
    pub activated_at: Option<Timestamp>,
    pub errors: Vec<ApplicationError>,
}
```

The collector report corresponds to capability
`io.asap.collector.plan.v1`, message type `application_report`. Unknown report
versions or missing required fields are errors.

Why reports are separate from transport acknowledgement: the MVP must prove the
runtime applied the intended semantic plan, not merely that bytes arrived.

### Publisher

```rust
pub trait PlanPublisher {
    type Error;

    async fn publish(
        &self,
        bundle: CompiledPlanBundle,
    ) -> Result<ActivationResult, Self::Error>;

    async fn rollback(
        &self,
        plan_id: &str,
        plan_version: u64,
    ) -> Result<ActivationResult, Self::Error>;
}

pub struct ActivationResult {
    pub plan_id: String,
    pub plan_version: u64,
    pub status: ApplicationStatus,
    pub collector_reports: Vec<CollectorApplicationReport>,
    pub backend_report: BackendApplicationReport,
}
```

`publish` returns `Active` only when every required runtime reports the same
plan/version/compatibility and expected materializations. Partial staging is an
error result and keeps the prior valid plan authoritative.

## 3. Adding and verifying functionality

### Add another collector transport

1. Implement `CollectorPlanClient`; keep CollectorPlan unchanged.
2. Preserve plan identity separately from transport byte identity.
3. Map transport errors to structured client errors.
4. Verify identical re-delivery is idempotent and conflicting bytes for the same
   plan/version are rejected.

Interpretation: a successful `stage` report is not global activation; only
`PlanPublisher::publish` can return an active bundle.

### Add an application status or report field

1. Version the public report schema/capability.
2. Define required/optional behavior and compatibility.
3. Update collector and backend clients together.
4. Verify older readers reject unknown required semantics rather than defaulting.

### Add rollback policy

1. Select only a retained complete bundle through `rollback`.
2. Stage both runtime sides like a normal publication.
3. Verify the result reports the restored version and all materializations.
4. Verify failed rollback leaves the current active bundle unchanged.

### Required output checks

- reports match bundle identity and expected materialization sets;
- stale/expired/conflicting versions fail;
- collector-only or backend-only success never returns `Active`;
- report artifacts are machine-readable by the MVP harness; and
- post-activation emitted state carries the activated identities.
