# syntax=docker/dockerfile:1

# Builds the load generator of setup/tlmr-loadtest. Build from the repository
# root, which holds go.mod:
#
#   docker build -f tools/loadgen/Dockerfile .

ARG GO_VERSION=1.26

FROM docker.io/library/golang:${GO_VERSION} AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/loadgen ./tools/loadgen

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/loadgen /loadgen
USER nonroot:nonroot
ENTRYPOINT ["/loadgen"]
