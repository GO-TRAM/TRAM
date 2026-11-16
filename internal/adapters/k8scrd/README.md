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

When Tram runs in a cluster, its service account needs the permissions in `config/rbac/role.yaml` (a `ClusterRole` named `manager-role`), bound with a `ClusterRoleBinding`. With `--watch-namespace`, a `RoleBinding` to that `ClusterRole` in each watched namespace is enough. The role includes reading `Secret` and `ConfigMap` resources, for [references to secrets](#secrets) and [config maps](#config-maps).

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

## Secrets

Any value in a component's `spec` can come from a `Secret` in the namespace of the `Stream`, like environment variables of a pod:

```yaml
  sinks:
    - name: otlp
      type: otlp
      from: in
      spec:
        url: https://collector.example.com:4318
        headers:
          Authorization:
            valueFrom:
              secretKeyRef:
                name: otlp-auth   # Secret in the namespace of the Stream
                key: token        # e.g. "Bearer ..."
```

- The value replaces the whole object as a string, so a header needs its complete value in the secret, e.g. `Bearer ...`
- The stream runs with the values, but Tram shows only the references: the API and the files of the file sync never contain a secret's value. A component may still name a value in an error it logs, e.g. an invalid URL, so keep secrets out of values that components check, like URLs
- Changing a secret restarts the streams that refer to it with the new value. Tram watches only the metadata of secrets and reads a value when it applies a stream, so values are not kept in its cache
- A missing secret or key keeps the stream from starting and sets `status.active` to `false`; the stream starts once the secret is there. A malformed reference, e.g. without `key`, fails for good until the resource is fixed
- References work only in `Stream` resources. Streams from the API or the file sync are passed on unchanged, so they cannot read secrets or config maps

## Config Maps

A value in a component's `spec` can also be the merged `data` of all `ConfigMap` resources in the namespace of the `Stream` that a label selector matches. This keeps large tables, e.g. the table of a [`value_map`](../../../COMPONENTS.md#value-map-stage) stage, out of the `Stream` resource and lets them grow beyond the size limit of a single Kubernetes object:

```yaml
  stages:
    - name: router
      type: value_map
      from: in
      spec:
        source_attr: tenant.id
        target_attr: router.id
        default: unassigned
        table:
          valueFrom:
            configMapSelector:        # like the selector of a Deployment
              matchLabels:
                tram.io/table: tenant-routing
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: tenant-routing-router-1
  labels:
    tram.io/table: tenant-routing
data:
  tenant-a: router-1
  tenant-b: router-1
```

- The selector takes `matchLabels` and `matchExpressions`, at least one of them. The value is an object with the keys and values of `data` of every matching config map; `binaryData` is ignored
- Split a table over as many config maps as needed, e.g. one per router. A key may be in several of them only with the same value. Different values are a conflict: the error names the key and both config maps
- Creating, changing or deleting a matching config map, or changing labels so that it starts or stops matching, applies the new table. Like with a changed spec, only stages and sinks are replaced; the source keeps running. As with secrets, Tram watches only the metadata of config maps and reads their data when it applies a stream
- No matching config map, or a conflict, keeps the stream from starting and sets `status.active` to `false`. A stream that already runs keeps running with its last table until the config maps are fixed. A malformed selector, e.g. an empty one, which would match every config map of the namespace, fails for good until the resource is fixed
- The merged table exists only in Tram's memory: it is never written to a Kubernetes object, so only each config map on its own is bound by the size limit. The API and the file sync show the selector, not the table
- Every Tram instance that runs the stream holds the whole table, and every reconcile reads all matching config maps. Keep tables of hundreds of thousands of entries in mind when sizing memory

## Behavior

- **Names:** a resource runs as the Tram stream `<namespace>.<name>`, e.g. `default.test-stream`, so resources with the same name in different namespaces do not collide. The stream can be read through the API under that name
- **Changes:** the stream is created or updated when the resource's `spec`, a secret it refers to, or a config map it selects changes. Status updates and periodic resyncs do not restart it
- **Status:** `status.active` is `true` once the stream runs, and `false` if building it failed, e.g. because of an invalid component spec. Failed builds are retried with increasing delays. If the stream's source stops later, Tram restarts the stream on its own; `status.active` stays `true` meanwhile
- **Deletion:** deleting the resource deletes the stream
- **Scope:** the operator watches `Stream` resources in all namespaces, or in the namespaces from `--watch-namespace`, that match `--stream-selector` if set; see [Selecting Streams](#selecting-streams). There is no leader election, so every Tram instance with the operator enabled runs every `Stream` resource it selects
- **Access:** creating a `Stream` lets its author open ports on the Tram pod and send data anywhere Tram can reach, from any namespace. Its author can also have Tram read any secret or config map in the `Stream`'s namespace and send the value along, e.g. as a header to their own URL. Grant `create` on `streams.io.tram.core` only to users you would also trust with the Tram deployment and with the secrets and config maps of that namespace; see [Security](../../../README.md#-security)
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
