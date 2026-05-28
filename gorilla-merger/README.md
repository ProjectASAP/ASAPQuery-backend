# gorilla-merger

A Thanos-Receive-style component for ASAP edge agents. It:

1. **Ingests** Gorilla XOR-chunk fragments (the `asap-gorilla-go` `ASAPFRG1`
   wire codec) over HTTP `POST /ingest/gorilla` (gzip-aware), **decode-free**:
   it durably logs each raw frame to a block-level WAL (fsync before ack) and
   buffers the raw XOR chunks per time window — NO sample decode/re-encode on
   the hot path.
2. **Builds blocks directly.** On a **small** window's close it sorts each
   series' buffered chunks by time and stitches them straight into a Prometheus
   TSDB block under `<tsdb.path>/pending/` via the low-level `chunks`/`index`
   writers (still no sample decode). The agent's Gorilla-XOR chunk bytes land in
   the block verbatim. The window is small (2m default) so the freshest data is
   queryable within ~window+grace+flush-tick — it is NOT hidden for a full 2h
   block range.
3. **Serves** a Thanos **StoreAPI** (gRPC) over the **union** of `pending/` +
   `shipped/` on-disk blocks so `thanos-query` can union recent data with the S3
   blocks that `thanos-store-gateway` serves — with no gap across a block's
   pending→shipped promotion.
4. **Compacts for ratio.** A background job merges the small per-window pending
   blocks (up to a **decoupled**, wider `-merge.compact-max-span`, 2h default)
   and re-chunks them to Prometheus's ~120 samples/chunk target into
   `<tsdb.path>/shipped/` (the resource-limited agents cannot emit large chunks,
   so the merger does the re-chunking offline/amortized, OFF the ingest path).
   The build window and the compaction span are independent knobs.
5. **Ships** ONLY the compacted blocks: the Thanos shipper watches
   `<tsdb.path>/shipped/` exclusively (with `uploadCompacted=true`), never the
   sibling `pending/` dir, so exactly the ratio-optimized blocks reach the same
   bucket the store-gateway watches (one PUT set per block). A downstream
   `thanos-compact` remains a backstop for cross-merger compaction/dedup.

### On-disk layout (under `-tsdb.path`)

| Dir | Contents | Served? | Shipped? |
|-----|----------|---------|----------|
| `pending/` | per-window Level-1 blocks (built on window close) | yes | no |
| `shipped/` | compacted + re-chunked Level-2 blocks | yes | yes |
| `wal/` | block-level fragment WAL | n/a | n/a |

## Ports / flags

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `-http-address` | `MERGER_HTTP_ADDRESS` | `:10908` | `/ingest/gorilla`, `/metrics`, `/-/healthy`, `/-/ready` |
| `-grpc-address` | `MERGER_GRPC_ADDRESS` | `:10907` | Thanos StoreAPI (the query surface `thanos-query --store=` points at) |
| `-tsdb.path` | `MERGER_TSDB_PATH` | `./data` | data dir root: `pending/` (L1, served not shipped), `shipped/` (L2 compacted, served + shipped), `wal/` |
| `-objstore.config-file` | `MERGER_OBJSTORE_CONFIG_FILE` | _empty_ | Thanos objstore YAML; empty disables the shipper |
| `-external-labels` | `MERGER_EXTERNAL_LABELS` | _empty_ | `k=v,k=v` applied to every uploaded block; distinct mergers MUST carry a distinguishing label |
| `-shipper.interval` | `MERGER_SHIPPER_INTERVAL` | `1m` | block-scan / upload cadence |
| `-tsdb.retention` | `MERGER_TSDB_RETENTION` | `6h` | local on-disk retention (blocks live in S3 once shipped) |
| `-merge.window` | `MERGER_MERGE_WINDOW` | `2m` | buffering/close window; one closed window → one directly-built **pending** block. Small so recent data is visible fast (NOT a 2h block range) |
| `-merge.reorder-grace` | `MERGER_MERGE_REORDER_GRACE` | `1m` | grace after a window's end for late/out-of-order fragments before flushing |
| `-merge.wal-dir` | `MERGER_MERGE_WAL_DIR` | `<tsdb.path>/wal` | block-level fragment WAL directory |
| `-merge.flush-interval` | `MERGER_MERGE_FLUSH_INTERVAL` | `30s` | how often closable windows are flushed into pending blocks |
| `-merge.compact-interval` | `MERGER_MERGE_COMPACT_INTERVAL` | `5m` | background merge + re-chunk + promote-to-shipped cadence |
| `-merge.compact-min-blocks` | `MERGER_MERGE_COMPACT_MIN_BLOCKS` | `1` | min pending source blocks before promotion (1 so even a lone block is re-chunked + shipped) |
| `-merge.compact-max-span` | `MERGER_MERGE_COMPACT_MAX_SPAN` | `2h` | max span a compacted (shipped) block may cover; **decoupled** from `-merge.window` (aligns to the Prometheus/Thanos 2h base) |

## Building the container

The merger imports the **private** Go module
`github.com/ProjectASAP/asap-gorilla-go`, so a naive `go build` in
Docker/CI cannot fetch it (404/auth-prompt on the private repo). The
`Dockerfile` solves this with a **BuildKit secret** carrying a GitHub
token — the token is mounted only for the build `RUN`s that need it and is
never baked into an image layer (unlike a build-arg or a `COPY`ed token
file). The git `url.insteadOf` rewrite happens *inside* the container
build, never on the host.

```sh
# Write a GitHub token to a file. With a modern gh:        gh auth token > /tmp/gh_token
# With gh < 2.x (no `gh auth token` subcommand) read it from the gh config:
python3 -c "import yaml; d=yaml.safe_load(open('$HOME/.config/gh/hosts.yml')); print(d['github.com'].get('oauth_token') or d['github.com'].get('token'), end='')" > /tmp/gh_token
# ...or just:        echo "$GITHUB_TOKEN" > /tmp/gh_token

DOCKER_BUILDKIT=1 docker build \
    --secret id=gh_token,src=/tmp/gh_token \
    -t asap/gorilla-merger:dev \
    .                                    # build context = this directory

rm -f /tmp/gh_token
```

The token needs `repo` read scope on `github.com/ProjectASAP/asap-gorilla-go`.

### CI note (PR #310)

The **same** `GOPRIVATE=github.com/ProjectASAP/*` + token requirement
applies to ASAPQuery-backend CI before PR #310 can merge: the CI runner
must expose a `gh_token` secret (or set `url.insteadOf` with a token) so
`go build` / `go test` of `gorilla-merger/` can fetch the private module.

## Local dev (no container)

`go vet` / `go build` on a host that already has the module in its
`GOMODCACHE` (or with `git` configured for the private repo):

```sh
GOPRIVATE=github.com/ProjectASAP/* go vet ./...
GOPRIVATE=github.com/ProjectASAP/* go build ./cmd/gorilla-merger
```
