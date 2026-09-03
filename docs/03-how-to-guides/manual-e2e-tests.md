# Manual end-to-end tests

Run every component suite and the final repository-wide warm-path test from
the repository root:

```bash
./scripts/e2e.sh
```

The command runs tests serially because several transport tests temporarily
change process environment or still use fixed loopback ports. It covers:

1. shared type and protobuf wire contracts;
2. the real control-plane binary: HTTP workload planning, configuration
   delivery to a simulated collector over the production OpAMP WebSocket, and
   runtime feedback ingestion over the production gRPC listener;
3. data-plane ingest adapters, query routing, storage, lifecycle, persistence,
   exact-backend forwarding with controlled peers, and a real data-plane
   process that accepts modified OTLP, stores a DDSketch, and returns its
   quantile through the public PromQL endpoint;
4. the monitor coordinator hosted by a real data-plane process, with two edge
   clients exchanging reports and differentiated grants over bidirectional
   gRPC;
5. a real Gorilla merger process that accepts an XOR fragment over HTTP,
   durably writes its TSDB block, and returns the exact chunk over Thanos
   StoreAPI, plus its compaction, recovery, and shipping suites; and
6. the final production-process path: typed physical-plan compilation,
   BackendPlan/precompute installation, collector capability and applied ACK
   over OpAMP, modified-OTLP ingest, SketchStore policy routing, and PromQL
   query readout of that same plan.

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
disk does not make Go use the home-directory cache. Rust incremental builds
are disabled by default to limit disk usage; set `ASAP_E2E_CARGO_INCREMENTAL=1`
to retain that cache when space is available.

## Full external system

The backend repository does not contain the producer and collector-agent
binaries. To run the actual multi-node system with those processes, use the
explicit target below. It delegates to the sibling ASAPCollector checkout and
can build images, use SSH, and start containers on the configured nodes:

```bash
ASAP_COLLECTOR_DIR=../ASAPCollector ./scripts/e2e.sh system
```

The local `all` target never performs those external operations.

## Test hygiene

Tests marked `#[ignore]` are not counted as passing coverage. The repository
does not retain ignored tests for retired or unsupported contracts; those
belong in issue tracking. List suites and verify that no Rust E2E is hidden
behind `#[ignore]` with:

```bash
./scripts/e2e.sh list
```

The final local whole-backend test is
`data_plane/tests/backend_process_e2e.rs`. The environment-gated VictoriaMetrics
comparison in `gorilla-merger/internal/merger/vmload_test.go` remains an
explicit external integration test and reports a Go skip when `VM_ADDR` is not
provided.
