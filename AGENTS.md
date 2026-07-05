# AI Assistant Guidelines for Stream Project

This file contains project-specific guidelines for AI coding assistants working on the Stream project. All AI agents should follow these instructions to maintain consistency and quality.

## Project Context

Stream is a cloud-native, in-memory data pipeline framework that:
- Uses tree-structured pipelines (1 source → N stages → M sinks)
- Manages configuration entirely via API (no file-based config)
- Uses protobuf for API definitions (swagger auto-generated)
- Follows clean architecture with clear separation between core and adapters

## Instructions for All AI Agents

### General Guidelines

1. **Architecture First**: Always consider the impact on the clean architecture
2. **API-First**: Changes to the API must start with protobuf definitions
3. **Type Safety**: Prefer strongly-typed solutions over dynamic ones
4. **Cloud-Native**: Remember this is a cloud-native, in-memory system

### Code Style

- Follow existing patterns in the codebase
- Use `base.Source[Spec]`, `base.Stage[Spec]`, `base.Sink[Spec]` for components
- Use `base.New[Spec]()` pattern for base instantiation
- Pass `validator *validator.Validate` to New functions
- Register new components in the factory (in feature branches)

### Development Workflow

1. For API changes:
   - Modify `.proto` files first
   - Run `go generate ./...` to regenerate code
   - Swagger file updates automatically

2. For new components:
   - Create spec.go with typed configuration
   - Implement component using base classes
   - Add to factory in a new branch
   - Document in COMPONENTS.md

3. Always:
   - Run `go generate ./...` before committing
   - Run `go test ./...` to verify changes
   - Use `gofmt` for formatting

### Review Focus Areas

When reviewing or generating code, all AI agents should pay special attention to:

1. **Architecture Compliance**:
   - Components use base classes correctly
   - Factory registration follows the pattern
   - No direct dependencies between adapters

2. **API Consistency**:
   - Protobuf definitions are clean
   - Swagger documentation is auto-generated
   - No manual API documentation needed

3. **Error Handling**:
   - Errors are properly wrapped with context
   - Validation happens at component creation
   - Failures don't crash the pipeline

4. **Thread Safety**:
   - Shared state is properly protected
   - Goroutines are properly managed
   - No race conditions in concurrent access

### Component Development

When creating new components (sources, stages, sinks), all AI agents should:

1. **Spec Definition**:
   ```go
   type Spec struct {
       Field1 string `json:"field1" validate:"required"`
       Field2 int    `json:"field2" validate:"min=1"`
   }
   ```

2. **Component Implementation**:
   ```go
   type MyComponent struct {
       *base.Component[Spec]
       // Additional fields
   }
   
   func New(spec domain.ComponentSpec, validator *validator.Validate) (*MyComponent, error) {
       base, err := base.New[Spec](spec.Name, spec.From, spec.Spec, validator)
       if err != nil {
           return nil, err
       }
       return &MyComponent{Component: base}, nil
   }
   ```

3. **Factory Registration**:
   ```go
   // In factory.go (in feature branch)
   func (f *Factory) registerComponents() {
       f.components["my-component"] = func(spec domain.ComponentSpec, v *validator.Validate) (domain.Component, error) {
           return mycomponent.New(spec, v)
       }
   }
   ```

4. **Documentation**:
   - Add to COMPONENTS.md
   - Include configuration options
   - Add usage examples

### Testing Guidelines

All AI agents should ensure:
- Use table-driven tests for component logic
- Test both valid and invalid configurations
- Mock external dependencies in unit tests
- Test pipeline wiring and data flow

### Protobuf Guidelines

All AI agents must follow these protobuf rules:
1. Always use the latest protobuf syntax (proto3)
2. Use `google.protobuf.Struct` for map[string]any fields
3. Include proper validation annotations
4. Keep message definitions clean and focused

### File Structure

All AI agents should respect this project structure:
- All new components go in `internal/adapters/{source|stage|sink}/`
- Base classes are in `internal/adapters/{source|stage|sink}/base/`
- Core logic is in `internal/core/`
- API definitions are in `api/proto/`
- Generated code goes in `pkg/gen/`

### Common Pitfalls to Avoid

All AI agents should be aware of these common issues:
1. **Circular Dependencies**: Don't create circular imports between packages
2. **Global State**: Avoid global variables; use dependency injection
3. **Blocking Operations**: Don't block in Notify methods
4. **Memory Leaks**: Always clean up goroutines and resources
5. **Inconsistent Error Handling**: Always wrap errors with context

## Example Commands

```bash
# Build the project
go build -o stream ./cmd/stream

# Run tests
go test ./...

# Generate code (protobuf, etc.)
go generate ./...

# Format code
gofmt -w .

# Run service
./stream -v
```

## Response Guidelines for All AI Agents

When generating code or providing feedback, all AI agents should:
1. Point out architectural concerns first
2. Suggest idiomatic Go patterns
3. Ensure thread safety is considered
4. Verify error handling is consistent
5. Check that the factory pattern is followed for new components
6. Maintain the existing code style and patterns
7. Provide complete, working examples
8. Explain the reasoning behind suggestions
