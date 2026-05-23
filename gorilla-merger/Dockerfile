# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the gorilla-merger (Thanos-Receive-style component).
#
# ## Why this needs a build secret
#
# gorilla-merger imports the PRIVATE Go module
# `github.com/ProjectASAP/asap-gorilla-go` (the shared edge<->merger
# ASAPFRG1 wire codec). A naive `go build` inside Docker/CI cannot fetch it:
# the module proxy + git fetch get a 404/auth prompt for the private repo.
#
# We solve this with a BuildKit *secret* mount carrying a GitHub token. The
# secret is mounted ONLY for the duration of the build RUN that needs it and
# is NOT baked into any image layer (unlike a build-arg or COPY of a token
# file). Inside that RUN we set a transient `url.insteadOf` git rewrite so
# `go` fetches the private module over HTTPS with the token. The git config
# lives only in the container build layer, never on the host.
#
# `GOPRIVATE=github.com/ProjectASAP/*` keeps the fetch off the public proxy
# and checksum DB; `GIT_TERMINAL_PROMPT=0` makes a missing/incorrect token
# fail fast instead of hanging on an interactive credential prompt.
#
# ## Build invocation
#
# Write a GitHub token (a PAT or `gh auth token`) to a file, then:
#
#   gh auth token > /tmp/gh_token            # or: echo "$GITHUB_TOKEN" > /tmp/gh_token
#   DOCKER_BUILDKIT=1 docker build \
#       --secret id=gh_token,src=/tmp/gh_token \
#       -t asap/gorilla-merger:dev \
#       gorilla-merger/
#   rm -f /tmp/gh_token
#
# The build context is the `gorilla-merger/` module directory (this file's
# directory). The same GOPRIVATE + token requirement applies to
# ASAPQuery-backend CI before PR #310 can be merged (the CI runner must
# expose a `gh_token` secret / configure `url.insteadOf` the same way).

FROM golang:1.25-bookworm AS build
WORKDIR /src

# git is needed for the private-module fetch (insteadOf rewrite below).
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Prime the module graph first so dependency downloads cache independently
# of source edits. go.sum is committed, so the public deps resolve normally;
# only the private module needs the token.
COPY go.mod go.sum ./
RUN --mount=type=secret,id=gh_token \
    GOPRIVATE=github.com/ProjectASAP/* \
    GONOSUMCHECK=github.com/ProjectASAP/* \
    GOFLAGS=-mod=mod \
    GIT_TERMINAL_PROMPT=0 \
    sh -c 'git config --global url."https://x-access-token:$(cat /run/secrets/gh_token)@github.com/".insteadOf "https://github.com/" && go mod download'

# Now copy the rest of the module and build.
COPY . .
RUN --mount=type=secret,id=gh_token \
    GOPRIVATE=github.com/ProjectASAP/* \
    GONOSUMCHECK=github.com/ProjectASAP/* \
    GIT_TERMINAL_PROMPT=0 \
    sh -c 'git config --global url."https://x-access-token:$(cat /run/secrets/gh_token)@github.com/".insteadOf "https://github.com/" && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/gorilla-merger ./cmd/gorilla-merger'

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gorilla-merger /usr/local/bin/gorilla-merger
# HTTP ingest (/ingest/gorilla, /metrics) + Thanos StoreAPI gRPC.
EXPOSE 10908 10907
ENTRYPOINT ["/usr/local/bin/gorilla-merger"]
