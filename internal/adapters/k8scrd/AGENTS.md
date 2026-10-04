# Kubernetes Operator - AI Agent Guide

This adapter embeds a controller-runtime operator into Tram. It was scaffolded with Kubebuilder, but it is not a standalone Kubebuilder project: there is no `Makefile`, no `PROJECT` file, no `cmd/main.go` and no separate deployment. The operator starts inside `tram server --enable-k8s-operator` and passes `Stream` resources to the core through `ports.Core`. The repository-wide rules in the root `AGENTS.md` apply as well.

## Project Structure

```
operator.go                                Creates and runs the controller manager inside Tram; limits its cache to the selected namespaces and labels
api/v1/groupversion_info.go                API group io.tram.core/v1
api/v1/stream_types.go                     Stream schema (+kubebuilder markers)
api/v1/zz_generated.deepcopy.go            Generated (DO NOT EDIT)
internal/controller/stream_controller.go   Reconciliation logic and RBAC markers
config/crd/bases/                          Generated CRD (DO NOT EDIT)
config/rbac/role.yaml                      Generated RBAC (DO NOT EDIT)
hack/boilerplate.go.txt                    Header for generated files
```

Example resources live in `setup/k8s/` at the repository root.

## Critical Rules

### Never Edit Generated Files

`api/v1/zz_generated.deepcopy.go`, `config/crd/bases/*` and `config/rbac/role.yaml` are produced by controller-gen v0.22.0 from the `//go:generate` directives in `operator.go`. After editing `*_types.go` or any marker, regenerate them:

```sh
go generate ./internal/adapters/k8scrd/...
```

Do not use `make manifests`, `make generate` or the `kubebuilder` CLI; they are not set up in this repository.

### Keep the Operator a Driver of the Core

- The reconciler only translates `Stream` resources into `domain.StreamSpec` and calls `ports.Core`. Building, running and stopping streams is the core's job
- Core stream names are `<namespace>.<name>` (see `StreamName`). Namespaces cannot contain dots, so this stays unambiguous
- Component specs are `*runtime.RawExtension` and are passed on unchanged as `map[string]any`, the same way the API and the file sync do

## Reconciliation Rules

- **Idempotent:** reconciling the same resource again must not restart its stream. Compare the desired spec with `Core.GetStream` and only call `UpsertStream` when they differ, because an upsert restarts the stream
- **Trigger on spec changes only:** the controller uses `GenerationChangedPredicate`, so status writes and resyncs do not reconcile. Keep it that way
- **Status:** only write it when it changes (`setActive`)
- **Deletion:** when the resource is gone, delete the stream and treat `domain.ErrStreamNotFound` as success
- **Selection:** `--watch-namespace` and `--stream-selector` only limit the manager's cache (`cacheOptions`). Read `Stream` resources through the manager's client, never an uncached reader, so a resource that leaves the selection is reconciled as deleted
- **Permanent errors:** wrap errors that retrying cannot fix with `reconcile.TerminalError`; other errors are retried with backoff
- **RBAC:** declare permissions with `+kubebuilder:rbac` markers on the reconciler and regenerate

## API Design

Markers for `api/v1/*_types.go`:

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced

// On fields:
// +kubebuilder:validation:Required
// +kubebuilder:validation:Minimum=1
// +kubebuilder:pruning:PreserveUnknownFields
```

- Follow the [Kubernetes API conventions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md): `spec`, `status`, `metadata`
- Prefer `metav1.Condition` for new status fields

## Logging

Use the controller-runtime logger from the context, which Tram routes to `slog`:

```go
log := logf.FromContext(ctx)
log.Info("Upserted Stream into core", "stream", name)
log.Error(err, "Failed to update status", "stream", name)
```

Follow the [Kubernetes logging message style](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md#message-style-guidelines): start with a capital letter, no trailing period, past tense, name the object type (`"Deleted Stream"`), balanced key-value pairs.

## Testing

Unit tests use the fake client from `sigs.k8s.io/controller-runtime/pkg/client/fake` with `WithStatusSubresource`, and a fake `ports.Core`; see `internal/controller/stream_controller_test.go`.

`operator_envtest_test.go` runs the whole operator against a real API server from envtest, connected through a kubeconfig file like `--kubeconfig`. Use it for behavior the fake client does not have: cache selection, watch events and RBAC. Each test starts its own API server, so tests stay parallel and do not need globals. Run them with `task test:envtest`; without `KUBEBUILDER_ASSETS` they are skipped. Several operators in one test binary need `WithoutNameValidation`, since controller-runtime requires unique controller names per process.
