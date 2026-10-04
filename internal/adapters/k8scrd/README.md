# Kubernetes Operator

Lets Tram run streams described by `Stream` custom resources (`io.tram.core/v1`). The operator is part of the Tram binary and starts together with the server; it is not deployed separately.

## Running

1. Install the custom resource definition:

   ```sh
   kubectl apply -f internal/adapters/k8scrd/config/crd/bases/io.tram.core_streams.yaml
   ```

2. Start Tram with the operator enabled:

   ```sh
   ./tram server --enable-k8s-operator
   ```

   Tram connects with the in-cluster configuration when it runs in a pod, otherwise with the `KUBECONFIG` environment variable or `~/.kube/config`. `--kubeconfig <file>` selects a specific file.

3. Create a `Stream`, e.g. the example in `setup/k8s/`:

   ```sh
   kubectl apply -f setup/k8s/generator_delay_debug.yaml
   ```

When Tram runs in a cluster, its service account needs the permissions in `config/rbac/role.yaml` (a `ClusterRole` named `manager-role`), bound with a `ClusterRoleBinding`. With `--watch-namespace`, a `RoleBinding` to that `ClusterRole` in each watched namespace is enough.

## Selecting Streams

By default every Tram instance with the operator enabled runs every `Stream` resource in the cluster. Two flags limit an instance to some of them; when both are set, a resource has to match both:

| Flag | Description |
|------|-------------|
| `--watch-namespace` | Comma-separated namespaces whose `Stream` resources the instance runs, e.g. `tram-validation,tram-bridge` |
| `--stream-selector` | [Label selector](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors) the resources have to match, e.g. `tram.io/validation=true` |

With several Tram deployments in one namespace, give each deployment its own label key. A `Stream` then runs on every deployment whose label it carries:

```yaml
# Deployment "validation": --stream-selector=tram.io/validation=true
# Deployment "bridge":     --stream-selector=tram.io/bridge=true
apiVersion: io.tram.core/v1
kind: Stream
metadata:
  name: shared
  labels:
    tram.io/validation: "true"   # runs on both deployments
    tram.io/bridge: "true"
```

Set-based selectors work as well, e.g. `tram.io/instance in (bridge,shared)`.

- A resource that no instance selects runs nowhere, and its `status.active` is not set
- Removing the matching label from a resource stops its stream on that instance; adding it starts the stream
- The selector only assigns streams to instances, it is no access control. Anyone who can create a `Stream` in a watched namespace can set the labels. Use `--watch-namespace` and RBAC to restrict who can run streams on an instance

## The Stream Resource

```yaml
apiVersion: io.tram.core/v1
kind: Stream
metadata:
  name: test-stream
  namespace: default
spec:
  source:
    name: generator
    type: generator
    spec:
      interval: 2s
  stages:
    - name: delay
      type: delay
      from: generator
      spec:
        delay: 1s
  sinks:
    - name: debug
      type: debug
      from: delay
```

`spec` has the same structure as the `stream` object of the HTTP API, without `name`. Each component's `spec` is passed to the component as it is, so numbers, booleans and nested objects keep their types; see [COMPONENTS.md](../../../COMPONENTS.md) for the options of each component.

## Behavior

- **Names:** a resource runs as the Tram stream `<namespace>.<name>`, e.g. `default.test-stream`, so resources with the same name in different namespaces do not collide. The stream can be read through the API under that name
- **Changes:** the stream is created or updated when the resource's `spec` changes. Status updates and periodic resyncs do not restart it
- **Status:** `status.active` is `true` once the stream runs, and `false` if building it failed, e.g. because of an invalid component spec. Failed builds are retried with increasing delays. If the stream's source stops later, Tram restarts the stream on its own; `status.active` stays `true` meanwhile
- **Deletion:** deleting the resource deletes the stream
- **Scope:** the operator watches `Stream` resources in all namespaces, or in the namespaces from `--watch-namespace`, that match `--stream-selector` if set; see [Selecting Streams](#selecting-streams). There is no leader election, so every Tram instance with the operator enabled runs every `Stream` resource it selects
- **Access:** creating a `Stream` lets its author open ports on the Tram pod and send data anywhere Tram can reach, from any namespace. Grant `create` on `streams.io.tram.core` only to users you would also trust with the Tram deployment; see [Security](../../../README.md#-security)
- **File sync:** with `--enable-file-sync`, streams created by the operator are also written to the configuration directory

## Development

| Path | Content |
|------|---------|
| `api/v1/stream_types.go` | The `Stream` type and its kubebuilder markers |
| `api/v1/zz_generated.deepcopy.go` | Generated |
| `internal/controller/stream_controller.go` | Reconciliation and the RBAC markers |
| `config/crd/bases/`, `config/rbac/` | Generated CRD and RBAC manifests |
| `operator.go` | Starts the controller manager inside Tram |

After changing types or markers, regenerate the deepcopy code and the manifests with [controller-gen](https://book.kubebuilder.io/reference/controller-gen) v0.22.0:

```sh
go generate ./internal/adapters/k8scrd/...
```
