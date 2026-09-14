# Cross-component data-structure ownership audit

Audience: developers changing the control-plane/data-plane boundary.

This change makes two wire contracts shared without moving compiler or runtime
behavior:

- `control_plane::physical::publication::PhysicalPlanInstallRequest` is the
  single typed install envelope. The control plane constructs it from a
  validated `PhysicalPlanPublication`; the data plane deserializes and validates
  the same type. Publication no longer serializes to dynamic JSON and inserts
  runtime fields by name.
- `asap_types::{AccuracyKind, AccuracyProfile}` owns the serialized accuracy
  contract. Planner-parameter and installed-config derivation remain in their
  owning components through small extension traits.

The data-plane storage-routing classifier is now named `QueryOperatorShape`.
It classifies operators such as `topk`, `rate`, and `quantile`; the control-plane
`QueryShape` describes evaluation lifecycle (`one_shot`, `streaming`, or
`periodic`). The distinct names prevent accidental cross-layer use.

## Shared contracts and compatibility imports

`QueryPlan`, `PrecomputePlan`, `TransmissionPlan`, `CollectorPlan`,
`SummaryCatalog`, and the publication/install envelopes are now defined in
`asap_types`. Control-plane modules retain compatibility re-exports; these are
not independent DTO implementations. Selection and compilation stay in the
control plane, while installation, ingestion, and query execution stay in the
data plane. Shared contracts retain their validation methods.

See [planning terminology](control-plane/planning-terminology.md) for the
compiled-plan/runtime-plan distinction and the wire-preserving naming migration.

Planner's `planner_types::workload::QueryLanguage` and backend
`asap_types::QueryLanguage` also have different scopes. They should remain
explicitly converted at compilation unless the Planner package adopts the
backend wire enum; aliasing them locally would hide a semantic boundary.
