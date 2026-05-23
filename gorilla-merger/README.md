# gorilla-merger

A Thanos-Receive-style component for ASAP edge agents. It:

1. **Ingests** Gorilla XOR-chunk fragments (the `asap-gorilla-go` `ASAPFRG1`
   wire codec) over HTTP `POST /ingest/gorilla` (gzip-aware).
2. **Appends** the decoded samples to an embedded Prometheus `tsdb.DB`
   (2h block range + WAL).
3. **Serves** a Thanos **StoreAPI** (gRPC) over the open (`<2h` pending)
   window so `thanos-query` can union recent data with the `>=2h` S3 blocks
   that `thanos-store-gateway` serves.
4. **Ships** completed 2h blocks to object storage via the Thanos shipper
   (one PUT set per block), into the same bucket the store-gateway watches.

## Ports / flags

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `-http-address` | `MERGER_HTTP_ADDRESS` | `:10908` | `/ingest/gorilla`, `/metrics`, `/-/healthy`, `/-/ready` |
| `-grpc-address` | `MERGER_GRPC_ADDRESS` | `:10907` | Thanos StoreAPI (the query surface `thanos-query --store=` points at) |
| `-tsdb.path` | `MERGER_TSDB_PATH` | `./data` | embedded tsdb dir (WAL + unshipped blocks) |
| `-objstore.config-file` | `MERGER_OBJSTORE_CONFIG_FILE` | _empty_ | Thanos objstore YAML; empty disables the shipper |
| `-external-labels` | `MERGER_EXTERNAL_LABELS` | _empty_ | `k=v,k=v` applied to every series + uploaded block; distinct mergers MUST carry a distinguishing label |
| `-shipper.interval` | `MERGER_SHIPPER_INTERVAL` | `1m` | block-scan / upload cadence |
| `-tsdb.retention` | `MERGER_TSDB_RETENTION` | `6h` | local on-disk retention (blocks live in S3 once shipped) |

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
