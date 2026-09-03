# Manual end-to-end tests

Run every component suite and the final repository-wide warm-path test from
the repository root:

```bash
./scripts/e2e.sh
```

The command runs tests serially because several transport tests temporarily
change process environment or still use fixed loopback ports. It covers:

1. shared type and protobuf wire contracts;
2. the real control-plane binary plus HTTP, OpAMP, plan publication, and
   runtime feedback;
3. data-plane ingest adapters, query routing, storage, lifecycle, persistence,
   exact-backend forwarding with controlled peers, and a real data-plane
   process bootstrapped from a file;
4. the monitor coordinator over a real bidirectional gRPC connection;
5. Gorilla fragment ingest, WAL recovery, TSDB block construction, Thanos
   StoreAPI, compaction, and object-store shipping; and
6. the final controller planning -> backend configuration -> modified OTLP
   ingest -> precompute -> SketchStore -> PromQL query path.

The component suites can also be run separately:

```bash
./scripts/e2e.sh contracts
./scripts/e2e.sh control-plane
./scripts/e2e.sh data-plane
./scripts/e2e.sh monitor
./scripts/e2e.sh gorilla-merger
./scripts/e2e.sh whole
```

`whole` is the stable representative DDSketch path. To exercise every
currently checked-in sketch/query combination, including scenarios tracking
known product regressions, run:

```bash
./scripts/e2e.sh whole-matrix
```

Unlike ignored tests, a failing matrix scenario exits non-zero. This target is
diagnostic and is not part of the default `all` acceptance command until those
known query/planner regressions are fixed.

Use `ASAP_E2E_NOCAPTURE=1` to display Rust test output. Build artifacts and
the Go compilation cache are kept below `target/` by default so a full system
disk does not make Go use the home-directory cache.

## Full external system

The backend repository does not contain the producer and collector-agent
binaries. To run the actual multi-node system with those processes, use the
explicit target below. It delegates to the sibling ASAPCollector checkout and
can build images, use SSH, and start containers on the configured nodes:

```bash
ASAP_COLLECTOR_DIR=../ASAPCollector ./scripts/e2e.sh system
```

The local `all` target never performs those external operations.

## Ignored regressions

Tests marked `#[ignore]` are not counted as passing coverage. List the current
known ignored tests and their reasons with:

```bash
./scripts/e2e.sh list
```

In particular, the older `e2e_modified_otlp_sketch_path` cases remain ignored
after the protobuf refactor. The maintained whole-path suite is
`e2e_controller_plans_and_backend_serves`.
