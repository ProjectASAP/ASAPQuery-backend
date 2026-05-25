# ASAP Developer Documentation

## Getting started

- [Overview & Key Concepts](01-getting-started/overview.md) — what ASAP is
- [Architecture](01-getting-started/architecture.md) — system design & data flows
- [Local Setup](01-getting-started/local-setup.md) — set up a dev environment

## How-to guides

- [Bootstrap Config from Query Log](03-how-to-guides/operations/bootstrap-config-from-query-log.md) — auto-generate sketch configs from Prometheus query traffic

## Design docs

In-depth design notes live as flat `design-*.md` files in this directory. Starting points:

- [Controller-into-backend refactor](design-controller-into-backend.md) — the two-plane (data/control) architecture
- [Sketch DB core](design-sketch-db-core.md) — the warm-tier `SketchStore`
- [Adding a new sketch](adding-a-new-sketch.md) — end-to-end recipe
- [Correctness proofs](proofs.md) — accuracy/lifecycle invariants

See the remaining `design-*.md` files here for sketch-DB persistence, lifecycle, the SID model, and SQL/PromQL planning.
