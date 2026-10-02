# Contributing to Tram

Thank you for your interest in contributing to Tram! This document provides guidelines for contributing to the project.

---

## 📋 Table of Contents

- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Project Structure](#project-structure)
- [Adding New Components](#adding-new-components)
- [Pull Request Guidelines](#pull-request-guidelines)
- [Code Style Guidelines](#code-style-guidelines)
- [Testing](#testing)
- [Documentation](#documentation)

---

## 🚀 Getting Started

### Prerequisites

- Go 1.26+
- Docker, optional, to run a NATS server for the `nats` source and sink

Only needed to regenerate code (`go generate ./...`):

- [buf](https://buf.build/) and the protoc plugins used in `buf.gen.yaml`
- [controller-gen](https://book.kubebuilder.io/reference/controller-gen) v0.22.0 for the Kubernetes CRD and deepcopy code

### Install Dependencies

```bash
# Clone the repository
git clone https://codeberg.org/eljoth/tram.git
cd tram

# Install Go dependencies
go mod download

# Code generators (only needed for go generate)
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest
go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.22.0

# Optional: NATS with JetStream for the nats source and sink
docker compose -f setup/nats/docker-compose.yaml up -d
```

---

## 💻 Development Environment

### Build the Project

```bash
# Build the main application
go build -o tram ./cmd/tram

# Build all packages
go build ./...
```

### Run the Service

```bash
# Start the service
./tram server

# Or with file-based config sync
./tram server --enable-file-sync -c config
```

### Generate Code

```bash
# Generate all code: protobuf and swagger (buf), Kubernetes CRD and deepcopy (controller-gen)
go generate ./...
```

### Update API Definitions

When updating the API:

1. Modify the `.proto` files in `api/proto/stream/v1/`
2. Regenerate code:
   ```bash
   go generate ./...
   ```
3. The swagger file in `api/swagger/` is updated automatically

---

## 🗂️ Project Structure

```
tram/
├── api/
│   ├── proto/stream/v1/stream.proto  # API definition
│   └── swagger/                      # Generated OpenAPI description
├── cmd/tram/                         # CLI entry point and `tram server`
├── internal/                         # Private application code
│   ├── adapters/
│   │   ├── api/                      # gRPC API and HTTP gateway
│   │   ├── configsync/               # File sync (--enable-file-sync)
│   │   ├── k8scrd/                   # Kubernetes operator (--enable-k8s-operator)
│   │   ├── source/                   # Sources, base/ and factory.go
│   │   ├── stage/                    # Stages, base/ and factory.go
│   │   └── sink/                     # Sinks, base/ and factory.go
│   ├── core/                         # Builds, runs and stops streams
│   │   └── domain/                   # Events, specs, the stream tree, record filtering
│   ├── ports/                        # Interfaces between core and adapters
│   ├── e2e/                          # End-to-end pipeline tests
│   ├── scenario/                     # End-to-end pipeline benchmarks
│   └── testutil/                     # Test helpers: OTLP payloads (otlpgen), NATS container (natstest)
├── pkg/gen/go/api/proto/stream/v1/   # Generated protobuf, gRPC and gateway code
├── setup/                            # Example configurations: config/, k8s/, nats/
├── tools/benchreport/                # Generates BENCHMARK.md
├── docs/assets/                      # Images used in the documentation
├── buf.yaml, buf.gen.yaml, buf.lock  # Protobuf generation
├── generate.go                       # Entry point for go generate
└── README.md
```

---

## 🧩 Adding New Components

Tram is designed to be easily extensible. Each component is a package in `internal/adapters/{source|stage|sink}/<name>/` that embeds the matching base type and is registered in the factory next to it.

### Common Patterns

1. **Define a `Spec` type** for the configuration. `base.New` decodes the stream's `spec` into it and checks its `validate` tags ([go-playground/validator](https://github.com/go-playground/validator)); there is no `Validate()` method
2. **Implement the component** by embedding `base.Source[Spec]`, `base.Stage[Spec]` or `base.Sink[Spec]`. Checks the tags cannot express, like parsing durations, belong into `New`, so an invalid spec fails when the stream is created
3. **Register it** in the `switch` of `factory.go` in a new branch
4. **Document it** in [COMPONENTS.md](COMPONENTS.md)

Rules all components follow:

- **Events are read-only.** One event is passed to all branches concurrently. To change data, build a new event, e.g. with `domain.FilterLogs`, `FilterMetrics` and `FilterSpans`, which share unchanged records instead of copying them
- **`Notify` returns the delivery result.** Return an error wrapping `domain.ErrEventRejected` when retrying the same data cannot succeed; sources then reject it (OTLP: HTTP 400, NATS: terminate). Any other error is treated as temporary (OTLP: HTTP 500, NATS: redelivery)
- **`Notify` may block** while it delivers the event downstream, or when waiting is its documented purpose (`batch`, `delay`). It has to return when `ctx` is done
- **Lifecycle:** `New` may acquire resources such as connections. `Stop` is called exactly once and must release them; it may be called without `Run`, e.g. when building the stream failed. The core stops the source first, waits for its `Run` to return, and then stops stages and sinks from the root down
- **Updates:** when an update changes only stages or sinks, the source keeps running. The core builds the new stages and sinks, switches the source over with `SetSubscribers`, which waits until no event is delivered to the old ones anymore, and then stops the old stages and sinks. Stages and sinks therefore have to be independent of a particular source instance

### Adding a New Source

Sources have no `Notify`; they implement `Run` and `Stop`. The base type provides `Name`, `Spec`, `Subscribe`, `SetSubscribers` and `NotifySubscribers`.

#### 1. Create the Spec

```go
// internal/adapters/source/heartbeat/spec.go
package heartbeat

type Spec struct {
	Interval string `json:"interval" validate:"required"`
	Message  string `json:"message" validate:"required"`
}
```

#### 2. Implement the Source

```go
// internal/adapters/source/heartbeat/heartbeat.go
package heartbeat

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

const Type = "heartbeat"

type Heartbeat struct {
	*base.Source[Spec]
	interval time.Duration
	shutdown chan struct{}
	stopOnce sync.Once
}

func New(source domain.SourceSpec, validator *validator.Validate) (*Heartbeat, error) {
	// Decodes the spec and checks its validate tags.
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	// Checks the tags cannot express belong here too, so an invalid spec
	// fails when the stream is created.
	interval, err := time.ParseDuration(base.Spec().Interval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %w", err)
	}

	return &Heartbeat{
		Source:   base,
		interval: interval,
		shutdown: make(chan struct{}),
	}, nil
}

// Run produces events until Stop is called or ctx is cancelled.
func (h *Heartbeat) Run(ctx context.Context) error {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.shutdown:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			event := &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
				ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
					TimeUnixNano: uint64(time.Now().UnixNano()),
					Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: h.Spec().Message}},
				}}}},
			}}}}

			// Returns once all children have processed the event.
			if err := h.NotifySubscribers(ctx, event); err != nil {
				log.Printf("heartbeat %s: %v", h.Name(), err)
			}
		}
	}
}

// Stop makes Run return. It may be called without Run, e.g. when building
// the stream failed, and must release everything New acquired.
func (h *Heartbeat) Stop() error {
	h.stopOnce.Do(func() { close(h.shutdown) })
	return nil
}

var _ domain.Source = (*Heartbeat)(nil)
```

#### 3. Register in Factory

```go
// internal/adapters/source/factory.go
func (f *Factory) New(spec domain.SourceSpec) (domain.Source, error) {
	switch spec.Type {
	// ... existing sources
	case heartbeat.Type:
		return heartbeat.New(spec, f.validator)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidSourceType, spec.Type)
	}
}
```

### Adding a New Stage

Stages implement `Notify` and pass events on with `NotifySubscribers`. The base type provides `Name`, `From`, `Spec`, `Subscribe` and a `Stop` that does nothing.

#### 1. Create the Spec

```go
// internal/adapters/stage/severity/spec.go
package severity

type Spec struct {
	Min int `json:"min" validate:"min=0,max=24"`
}
```

#### 2. Implement the Stage

```go
// internal/adapters/stage/severity/severity.go
package severity

import (
	"context"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

const Type = "severity"

type Severity struct {
	*base.Stage[Spec]
}

func New(stage domain.StageSpec, validator *validator.Validate) (*Severity, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}
	return &Severity{Stage: base}, nil
}

// Notify keeps the log records with at least the configured severity. The
// event is shared with sibling branches, so it is never modified; FilterLogs
// builds a new payload and shares the kept records.
func (s *Severity) Notify(ctx context.Context, event *domain.Event) error {
	logs, err := domain.FilterLogs(event.Logs, func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, record *logspb.LogRecord) (bool, error) {
		return int(record.SeverityNumber) >= s.Spec().Min, nil
	})
	if err != nil {
		return err
	}

	filtered := *event
	filtered.Logs = logs
	if filtered.RecordCount() == 0 {
		return nil
	}
	return s.NotifySubscribers(ctx, &filtered)
}

var _ domain.Stage = (*Severity)(nil)
```

#### 3. Register in Factory

```go
// internal/adapters/stage/factory.go
	case severity.Type:
		return severity.New(spec, f.validator)
```

### Adding a New Sink

Sinks implement `Notify`. The base type provides `Name`, `From`, `Spec` and a `Stop` that does nothing.

#### 1. Create the Spec

```go
// internal/adapters/sink/webhook/spec.go
package webhook

type Spec struct {
	URL string `json:"url" validate:"required,url"`
}
```

#### 2. Implement the Sink

```go
// internal/adapters/sink/webhook/webhook.go
package webhook

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"google.golang.org/protobuf/proto"
)

const Type = "webhook"

type Webhook struct {
	*base.Sink[Spec]
	client *http.Client
}

func New(sink domain.SinkSpec, validator *validator.Validate) (*Webhook, error) {
	base, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}
	return &Webhook{
		Sink:   base,
		client: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// Notify posts the logs of an event as OTLP/HTTP protobuf. The returned
// error tells the source whether retrying can help.
func (w *Webhook) Notify(ctx context.Context, event *domain.Event) error {
	if event.Logs == nil {
		return nil
	}
	body, err := proto.Marshal(event.Logs)
	if err != nil {
		return fmt.Errorf("marshal logs: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.Spec().URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("send logs: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 500:
		// Temporary: the source may retry, e.g. the NATS source redelivers.
		return fmt.Errorf("send logs: %s", resp.Status)
	case resp.StatusCode >= 400:
		// Permanent: retrying the same data cannot succeed.
		return fmt.Errorf("%w: %s", domain.ErrEventRejected, resp.Status)
	}
	return nil
}

func (w *Webhook) Stop() error {
	w.client.CloseIdleConnections()
	return nil
}

var _ domain.Sink = (*Webhook)(nil)
```

#### 3. Register in Factory

```go
// internal/adapters/sink/factory.go
	case webhook.Type:
		return webhook.New(spec, f.validator)
```

### Tests and Benchmarks

- Test valid and invalid specs, and `Notify` with payloads from `internal/testutil/otlpgen`
- Check that `Notify` leaves the incoming event unchanged
- Add a benchmark for the per-event path (see [Benchmarks](#benchmarks))

---

## 📝 Pull Request Guidelines

### Before Submitting

1. **Run tests**: Ensure all tests pass
   ```bash
   go test ./...
   ```

2. **Generate code**: Make sure all code is generated
   ```bash
   go generate ./...
   ```

3. **Format code**: Use gofmt for consistent formatting
   ```bash
   gofmt -w .
   ```

4. **Check linting**: Run static analysis
   ```bash
   go vet ./...
   ```

### Pull Request Requirements

1. **Descriptive title**: Clearly describe what the PR does
2. **Detailed description**: Explain the changes and why they're needed
3. **Small, focused changes**: One logical change per PR
4. **Proper commit messages**: Follow [Conventional Commits](https://www.conventionalcommits.org/)
5. **Updated documentation**: Update relevant docs for any API changes
6. **Tests**: Add tests for new functionality

### Commit Message Format

```
feat: add new my-source component

- Add MySource implementation with base.Source
- Register in factory
- Add unit tests for spec validation

Closes #123
```

---

## 🎨 Code Style Guidelines

### General Principles

- Follow [Effective Go](https://go.dev/doc/effective_go)
- Use idiomatic Go patterns
- Keep code simple and readable
- Comment only what's not obvious

### Naming Conventions

- Use **camelCase** for variables and functions
- Use **PascalCase** for types and exported identifiers
- Use **snake_case** for JSON tags
- Type names should be nouns: `UserService`, not `GetUser`
- Interface names should end with `er`: `Logger`, `Writer`

### Formatting

- Use `gofmt` for formatting
- Line length: aim for 80-100 characters
- Indent with tabs, not spaces
- Group related imports and separate with blank lines

### Structure

- Keep functions short (aim for < 50 lines)
- Group related functions together
- Use receiver methods appropriately

### Error Handling

- Always check errors
- Provide context in error messages
- Use `fmt.Errorf` with `%w` for error wrapping
- Return errors, don't panic (except in truly exceptional cases)

```go
// Good
if err := doSomething(); err != nil {
    return fmt.Errorf("failed to do something: %w", err)
}

// Bad
if err := doSomething(); err != nil {
    panic(err)
}
```

---

## 🧪 Testing

### Unit Tests

- Place test files next to the code they test (`*_test.go`)
- Use table-driven tests for similar cases
- Test both happy paths and error cases

```go
func TestMyFunction(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		err      bool
	}{
		{"success", "valid", "result", false},
		{"error", "invalid", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := myFunction(tt.input)
			if (err != nil) != tt.err {
				t.Errorf("expected error: %v, got: %v", tt.err, err)
			}
			if !tt.err && result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}
```

### End-to-End Tests

`internal/e2e` tests complete pipelines without external dependencies: streams are built by the core from the real source and stage factories, OTLP data is sent over HTTP, and capturing sinks record exactly which records arrive where. They cover batching, splitting, cancelled requests, the status codes clients receive, and updates under load, and run with `go test ./...`.

`TestNATS` in the same package runs pipelines against a NATS server in a container (see `internal/testutil/natstest`) and checks what actually ends up in JetStream: subjects, the `Tram-Signal` header, and records with their resource and scope. It also sends data from Tram's NATS sink through Tram's NATS source. It is skipped without Docker or Podman; with Podman on macOS, set the variables described under [Benchmarks](#benchmarks).

When adding a stage or changing how events flow, add a case there that checks which records arrive in which sink.

### Integration Tests

- Use test containers for external dependencies (NATS, etc.), see `internal/testutil/natstest`
- Clean up resources after tests

### Benchmarks

Benchmarks cover the per-event hot path:

| Benchmark | Package | Measures |
|-----------|---------|----------|
| `BenchmarkNotifySubscribers` | `source/base`, `stage/base` | Fan-out cost per event and child |
| `BenchmarkPipeline` | `core` | A full stream with real stages, in records/s |
| `BenchmarkCELValidator` | `stage/celvalidator` | Expression evaluation per payload size |
| `BenchmarkCELFilter` | `stage/celfilter` | Per-record filtering for different keep ratios |
| `BenchmarkFilter` | `core/domain` | The record filtering helpers without CEL |
| `BenchmarkGroupEventsBySubjects` | `sink/nats` | Splitting batches by subject |
| `BenchmarkNotify` | `sink/nats` | Grouping and publishing to NATS (container) |
| `BenchmarkConsume` | `source/nats` | Fetching, decoding and acknowledging from NATS (container) |
| `BenchmarkBatch` | `stage/batch` | Batch overhead and merging with concurrent callers |
| `BenchmarkHandleLogs` | `source/otlp` | Reading and decoding OTLP requests |
| `BenchmarkOTLPDataRoundTrip` | `core/domain` | Serializing events for NATS |
| `BenchmarkScenario` | `scenario` | Complete pipelines end to end: throughput, CPU per record, latency (container) |

[BENCHMARK.md](BENCHMARK.md) presents these results. After performance-relevant changes, regenerate it with `go run ./tools/benchreport` (see the page for details); add new scenarios to `internal/scenario` and new components to the lists in `tools/benchreport`.

Benchmarks marked "container" start a NATS server with [Testcontainers](https://golang.testcontainers.org/) via `internal/testutil/natstest`. They need Docker or Podman and are skipped if neither is running. With Podman on macOS, point Testcontainers to the socket inside the Podman VM:

```bash
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/run/podman/podman.sock
export TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED=true
```

Build payloads with `internal/testutil/otlpgen` and use `otlpgen.Shapes` so results are comparable across benchmarks. Use `b.Loop()`, call `b.ReportAllocs()`, and report `records/s` for payload-dependent benchmarks.

Compare a change against `main` with [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```bash
git switch main && go test -p 1 -run='^$' -bench=. -benchmem -count=10 ./... > old.txt
git switch my-branch && go test -p 1 -run='^$' -bench=. -benchmem -count=10 ./... > new.txt
benchstat old.txt new.txt
```

`-p 1` runs one package at a time. Without it, `go test` benchmarks several packages in parallel, which then compete for CPU and distort each other's results. Other load on the machine, like a container VM that is just starting, has the same effect; check the spread between runs before trusting a result.

---

## 📚 Documentation

### Code Documentation

- Document exported types, functions, and methods
- Use complete sentences for doc comments
- Include examples where helpful

```go
// User represents a system user.
// It contains basic user information and access permissions.
type User struct {
    Name string
    Age  int
}

// Validate checks if the user data is valid.
// Returns an error if any field is invalid.
func (u *User) Validate() error {
    // implementation
}
```

### API Documentation

- The swagger file is automatically generated from protobuf definitions
- No manual updates needed when changing API endpoints

### Component Documentation

When adding new components:

1. Add to [COMPONENTS.md](COMPONENTS.md)
2. Include:
   - Configuration options
   - Usage examples
   - Behavior description
   - Error cases

---

## 🤔 Getting Help

If you have questions or need help:

1. Check existing issues and discussions
2. Open a new discussion for general questions
3. Open an issue for bugs or feature requests

We're happy to help you get started with contributing!

---

## 📜 License

By contributing to Tram, you agree that your contributions will be licensed under the same [MIT License](LICENSE) as the project.
