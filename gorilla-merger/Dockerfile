# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the gorilla-merger (Thanos-Receive-style component).
#
# ## asap-gorilla-go is local source, not the published module
#
# gorilla-merger imports the PRIVATE Go module
# `github.com/ProjectASAP/asap-gorilla-go` (the shared edge<->merger ASAPFRG1
# wire codec) AND its `intchunk` SUBPACKAGE (the cold value-chunk codec the
# internal/coldchunk decode-on-read helper builds on). intchunk does NOT exist
# in the published asap-gorilla-go tag, so the module cannot come from the proxy.
#
# Instead we COPY the in-repo monorepo `asap-gorilla-go` checkout into the build
# as a BuildKit *build-context* and rewrite go.mod's `replace` to point at that
# in-image path. This is the container analogue of the relative
# `../../ASAPCollector/asap-gorilla-go` replace committed in go.mod, and mirrors
# how build_asap_otel.sh injects the same asap-gorilla-go replace for asap-otel:
# the image always compiles against the intchunk-containing source WITHOUT a
# published tag. Because asap-gorilla-go is now local source, no GitHub token is
# required to fetch it; a `gh_token` secret is still accepted (and used if
# mounted) so any OTHER private fetch keeps working, but it is optional.
#
# ## Build invocation
#
#   DOCKER_BUILDKIT=1 docker build \
#       --build-context asap-gorilla-go=/path/ASAPCollector/asap-gorilla-go \
#       -t asap/gorilla-merger:dev \
#       gorilla-merger/
#
# The primary build context is the `gorilla-merger/` module directory (this
# file's directory); the `asap-gorilla-go` build-context supplies the monorepo
# checkout that contains the intchunk subpackage. GOPRIVATE keeps any fetch off
# the public proxy/sumdb; GIT_TERMINAL_PROMPT=0 fails fast instead of hanging.

FROM golang:1.25-bookworm AS build

# git/ca-certificates for any private-module fetch (insteadOf rewrite below).
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Lay the asap-gorilla-go monorepo checkout (with the intchunk subpackage) at
# /ASAPCollector/asap-gorilla-go so it is the sibling the committed relative
# replace `../../ASAPCollector/asap-gorilla-go` resolves to from the module dir
# at /src/gorilla-merger (/src/gorilla-merger/../../ASAPCollector/... == that).
COPY --from=asap-gorilla-go . /ASAPCollector/asap-gorilla-go

WORKDIR /src/gorilla-merger

# Prime the module graph first so dependency downloads cache independently of
# source edits. go.sum is committed, so the public deps resolve normally; with
# the directory replace, asap-gorilla-go itself resolves from the local source.
COPY go.mod go.sum ./
# Repoint the committed relative replace at the in-image checkout. The relative
# path already resolves given the layout above; making it absolute keeps the
# build independent of the WORKDIR depth.
RUN --mount=type=secret,id=gh_token \
    GOPRIVATE=github.com/ProjectASAP/* \
    GONOSUMCHECK=github.com/ProjectASAP/* \
    GOFLAGS=-mod=mod \
    GIT_TERMINAL_PROMPT=0 \
    sh -c 'go mod edit -replace github.com/ProjectASAP/asap-gorilla-go=/ASAPCollector/asap-gorilla-go; \
           if [ -f /run/secrets/gh_token ]; then git config --global url."https://x-access-token:$(cat /run/secrets/gh_token)@github.com/".insteadOf "https://github.com/"; fi; \
           go mod download'

# Now copy the rest of the module and build.
COPY . .
RUN --mount=type=secret,id=gh_token \
    GOPRIVATE=github.com/ProjectASAP/* \
    GONOSUMCHECK=github.com/ProjectASAP/* \
    GOFLAGS=-mod=mod \
    GIT_TERMINAL_PROMPT=0 \
    sh -c 'go mod edit -replace github.com/ProjectASAP/asap-gorilla-go=/ASAPCollector/asap-gorilla-go; \
           CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/gorilla-merger ./cmd/gorilla-merger'

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gorilla-merger /usr/local/bin/gorilla-merger
# HTTP ingest (/ingest/gorilla, /metrics) + Thanos StoreAPI gRPC.
EXPOSE 10908 10907
ENTRYPOINT ["/usr/local/bin/gorilla-merger"]
