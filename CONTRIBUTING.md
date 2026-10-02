# Contributing to Stream

Thank you for your interest in contributing to Stream! This document provides guidelines for contributing to the project.

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

- Go 1.21+
- [bufbuild/buf](https://buf.build/) for protobuf management

### Install Dependencies

```bash
# Clone the repository
git clone https://codeberg.org/eljoth/tram.git
cd stream

# Install Go dependencies
go mod download

# Install buf
buf curl --sh | sh
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
# Generate all code (protobuf, etc.)
go generate ./...
```

### Update API Definitions

When updating the API:

1. Modify the `.proto` files in `api/proto/stream/v1/`
2. Regenerate code:
   ```bash
   go generate ./...
   ```
3. The swagger file will be automatically updated

---

## 🗂️ Project Structure

```
stream/
├── api/                      # API definitions (protobuf)
│   └── proto/
│       └── stream/
│           └── v1/
│               ├── stream.proto    # Main service definitions
│               └── ...
├── cmd/                     # Command-line applications
│   └── stream/
│       └── main.go         # Main application entry point
├── internal/               # Private application code
│   ├── adapters/           # Component implementations
│   │   ├── source/
│   │   │   ├── base/        # Base source implementations
│   │   │   └── generator/   # Generator source
│   │   ├── stage/
│   │   │   ├── base/        # Base stage implementations
│   │   │   └── noop/        # NoOp stage
│   │   └── sink/
│   │       ├── base/        # Base sink implementations
│   │       ├── debug/       # Debug sink
│   │       └── ...
│   └── core/               # Core pipeline logic
│       ├── config/         # Configuration parsing
│       ├── domain/         # Domain models and interfaces
│       └── ...
├── pkg/                     # Public library code
│   └── gen/                # Generated code
│       └── go/
│           └── stream/
│               └── v1/       # Generated protobuf code
├── go.mod                  # Go module definition
├── go.sum                  # Go module checksums
├── buf.yaml                # Buf configuration
├── buf.lock                # Buf dependencies lock file
└── README.md               # Project readme
```

---

## 🧩 Adding New Components

Tram is designed to be easily extensible. Follow these steps to add a new source, stage, or sink.

### Common Patterns

All components follow a consistent pattern using the factory:

1. **Define a Spec type** for configuration
2. **Implement the component** using the base class
3. **Add to factory** in a new branch

### Adding a New Source

#### 1. Create the Spec

```go
// internal/adapters/source/my-source/spec.go
package mysource

import (
	"errors"
	"time"
)

type Spec struct {
	Interval string `json:"interval" validate:"required"`
	URL      string `json:"url" validate:"required,url"`
	// Add other configuration fields
}

// Validate implements the validator interface
func (s *Spec) Validate() error {
	if s.Interval == "" {
		return errors.New("interval is required")
	}
	if _, err := time.ParseDuration(s.Interval); err != nil {
		return fmt.Errorf("invalid interval: %w", err)
	}
	if s.URL == "" {
		return errors.New("URL is required")
	}
	return nil
}
```

#### 2. Implement the Source

```go
// internal/adapters/source/my-source/my-source.go
package mysource

import (
	"context"
	"log"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

const Type = "my-source"

type MySource struct {
	*base.Source[Spec]
	interval time.Duration
	client   *http.Client
}

func New(source domain.SourceSpec, validator *validator.Validate) (*MySource, error) {
	// Create base with validation
	baseSource, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	// Parse the interval from validated spec
	interval, err := time.ParseDuration(baseSource.Spec().Interval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %w", err)
	}

	return &MySource{
		Source:   baseSource,
		interval: interval,
		client:   &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (s *MySource) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.Context().Done():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Fetch or generate data
			data, err := s.fetchData(ctx)
			if err != nil {
				log.Printf("Error fetching data: %v", err)
				continue
			}

			event := domain.Event{
				Metadata: map[string]string{"source": s.Name()},
				Data:     data,
			}
			s.NotifySubscribers(ctx, event)
		}
	}
}
```

#### 3. Register in Factory

Add to the factory in your feature branch:

```go
// internal/core/config/factory.go
package config

import (
    "github.com/go-playground/validator/v10"
    mysource "codeberg.org/eljoth/tram/internal/adapters/source/my-source"
)

func (f *Factory) registerComponents() {
    // ... existing registrations

    // Register new source
    f.sources["my-source"] = func(spec domain.SourceSpec, v *validator.Validate) (domain.Source, error) {
        return mysource.New(spec, v)
    }
}
```

### Adding a New Stage

#### 1. Create the Spec

```go
// internal/adapters/stage/my-stage/spec.go
package mystage

type Spec struct {
	Filter string `json:"filter"`
	// Add other fields
}

func (s *Spec) Validate() error {
	// Add validation logic
	return nil
}
```

#### 2. Implement the Stage

```go
// internal/adapters/stage/my-stage/my-stage.go
package mystage

import (
	"context"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

const Type = "my-stage"

type MyStage struct {
	*base.Stage[Spec]
}

func New(stage domain.StageSpec, validator *validator.Validate) (*MyStage, error) {
	baseStage, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}

	return &MyStage{
		Stage: baseStage,
	}, nil
}

func (s *MyStage) Notify(ctx context.Context, event domain.Event) error {
	// Process the event based on spec
	if s.Spec().Filter != "" {
		// Apply filter logic
	}

	// Pass to next component
	return s.NotifySubscribers(ctx, event)
}
```

#### 3. Register in Factory

```go
// internal/core/config/factory.go
import (
    mystage "codeberg.org/eljoth/tram/internal/adapters/stage/my-stage"
)

func (f *Factory) registerComponents() {
    // ... existing registrations

    // Register new stage
    f.stages["my-stage"] = func(spec domain.StageSpec, v *validator.Validate) (domain.Stage, error) {
        return mystage.New(spec, v)
    }
}
```

### Adding a New Sink

#### 1. Create the Spec

```go
// internal/adapters/sink/my-sink/spec.go
package mysink

type Spec struct {
	Endpoint string `json:"endpoint" validate:"required,url"`
	// Add other fields
}

func (s *Spec) Validate() error {
	// Add validation logic
	return nil
}
```

#### 2. Implement the Sink

```go
// internal/adapters/sink/my-sink/my-sink.go
package mysink

import (
	"context"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

const Type = "my-sink"

type MySink struct {
	*base.Sink[Spec]
	client *http.Client
}

func New(sink domain.SinkSpec, validator *validator.Validate) (*MySink, error) {
	baseSink, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}

	return &MySink{
		Sink:   baseSink,
		client: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (s *MySink) Notify(ctx context.Context, event domain.Event) error {
	// Send event to destination
	// err := s.client.Post(s.Spec().Endpoint, "application/json", event.Data)
	// if err != nil {
	//     return fmt.Errorf("failed to send event: %w", err)
	// }
	return nil
}
```

#### 3. Register in Factory

```go
// internal/core/config/factory.go
import (
    mysink "codeberg.org/eljoth/tram/internal/adapters/sink/my-sink"
)

func (f *Factory) registerComponents() {
    // ... existing registrations

    // Register new sink
    f.sinks["my-sink"] = func(spec domain.SinkSpec, v *validator.Validate) (domain.Sink, error) {
        return mysink.New(spec, v)
    }
}
```

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

### Integration Tests

- Test complete pipelines
- Use test containers for external dependencies (NATS, etc.)
- Clean up resources after tests

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

By contributing to Stream, you agree that your contributions will be licensed under the same [MIT License](LICENSE) as the project.
