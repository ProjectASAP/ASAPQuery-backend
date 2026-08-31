# Developing BackendPlan installation

> Interface status: target public API. Atomic snapshot storage exists; complete
> staging/lifecycle/cross-runtime activation remains partial.

## 1. Code architecture

```text
BackendPlan bytes
      |
      v
BackendPlanDecoder -> BackendPlanValidator -> BackendPlanRuntime
                                                |
                                      BackendPlanSnapshot
                                         /             \
                                      ingest          query
```

The decoder owns wire decoding, the validator owns semantic/capability checks,
and the runtime owns staged/active snapshots. Ingest and query components only
consume immutable snapshots; they do not mutate plans.

## 2. Public interfaces and definitions

```rust
pub trait BackendPlanDecoder {
    type Error;
    fn decode(&self, bytes: &[u8]) -> Result<BackendPlan, Self::Error>;
}

pub trait BackendPlanValidator {
    type Error;
    fn validate(
        &self,
        plan: &BackendPlan,
        capabilities: &BackendCapabilities,
    ) -> Result<ValidatedBackendPlan, Self::Error>;
}
```

`ValidatedBackendPlan` must be constructible only through validation. It proves
schema/lifecycle ordering, unique identities, resolved routes, supported
families/parameters/windows/readouts, and compatible result guarantees.

```rust
pub struct ValidatedBackendPlan {
    pub plan: BackendPlan,
    pub capability_hash: String,
    pub validated_at: Timestamp,
}

pub struct BackendCapabilities {
    pub capability_hash: String,
    pub ingest: Vec<IngestCapability>,
    pub readouts: Vec<ReadoutCapability>,
    pub storage: Vec<StorageCapability>,
}

pub struct BackendPlanSnapshot {
    pub plan: Arc<BackendPlan>,
    pub status: PlanRuntimeStatus,
    pub installed_at: Timestamp,
}

pub enum PlanRuntimeStatus {
    Staged,
    Active,
    Draining,
    Expired,
}
```

Capability entry definitions:

| Type | Definition |
| --- | --- |
| `IngestCapability` | Supported family, algorithm, parameters, encoding version, and full/delta semantics. |
| `ReadoutCapability` | Supported Planner readout/operator and guarantee kinds. |
| `StorageCapability` | Supported materialization representation, merge, window, retention, and durability behavior. |

```rust
pub trait BackendPlanRuntime: Send + Sync {
    type Error;

    fn stage(&self, plan: ValidatedBackendPlan)
        -> Result<BackendApplicationReport, Self::Error>;

    fn activate(&self, plan_id: &str, plan_version: u64)
        -> Result<BackendApplicationReport, Self::Error>;

    fn snapshot(&self) -> BackendPlanSnapshot;

    fn retire(&self, plan_id: &str, plan_version: u64)
        -> Result<BackendApplicationReport, Self::Error>;
}
```

Why these interfaces exist: decoding, validation, and activation have different
failure semantics. A decoded plan must never become queryable before validation
and matching collector evidence.

## 3. Adding and verifying functionality

### Add a BackendPlan field

1. Add it to the public versioned wire/domain structure.
2. Define requiredness, identity impact, and compatibility behavior.
3. Validate it in `BackendPlanValidator`.
4. Expose it through immutable `BackendPlanSnapshot` to its consumer.
5. Verify missing/unknown/incompatible values fail before `stage`.

### Add a runtime lifecycle state

1. Extend `PlanRuntimeStatus` with allowed transitions.
2. Define whether ingest/query may use the state.
3. Return the effective state through `BackendApplicationReport`.
4. Verify invalid transitions do not change `snapshot()`.

### Interpret and verify output

- A `ValidatedBackendPlan` means the plan is deployable by this backend, not
  active.
- A `Staged` report means resources/routes are prepared, not queryable.
- An `Active` report must match the requested plan/version and materializations.
- One request must observe one `BackendPlanSnapshot`, including during swap.
- Re-delivery of identical content is idempotent; conflicting content for the
  same identity fails.
