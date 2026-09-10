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

## Remaining migration

`QueryPlan`, `PrecomputePlan`, `TransmissionPlan`, and `CollectorPlan` are wire
contracts currently defined in `control_plane`, so the data-plane crate depends
on the whole control-plane crate to deserialize and execute them. A later change
should move only their serde DTOs and validation-independent identifiers into
`asap_types`, leaving selection, compilation, publication validation, HTTP
handlers, and execution in their current owners. That migration should preserve
the JSON schema and use compile-time conversion at the compiler boundary.

Planner's `planner_types::workload::QueryLanguage` and backend
`asap_types::QueryLanguage` also have different scopes. They should remain
explicitly converted at compilation unless the Planner package adopts the
backend wire enum; aliasing them locally would hide a semantic boundary.
