# syntax=docker/dockerfile:1

# Builds a static Tram binary and runs it on a minimal, non-root base image.
#
#   task image:build

ARG GO_VERSION=1.27

FROM docker.io/library/golang:${GO_VERSION} AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tram ./cmd/tram

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tram /tram
USER nonroot:nonroot

# HTTP API and gRPC API. OTLP sources open the ports set in their specs.
EXPOSE 8080 50051

ENTRYPOINT ["/tram"]
# Inside a container, the APIs have to listen on all interfaces to be
# reachable through published ports.
CMD ["server", "--http-endpoint=0.0.0.0:8080", "--grpc-endpoint=0.0.0.0:50051"]
