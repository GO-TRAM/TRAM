# OTLP Sink Kubernetes Examples

This directory contains Kubernetes YAML configurations for deploying Tram streams with OTLP sinks in Kubernetes environments. Each example has a corresponding JSON configuration in the `../config/` directory with the same base filename.

## Quick Start

### Prerequisites
- Kubernetes cluster
- Tram operator installed
- OpenTelemetry Collector deployment (or similar OTLP-compatible endpoint)

### Deploy a Basic OTLP Stream

```bash
# Deploy the basic OTLP forwarding stream
kubectl apply -f otlp_basic.yaml

# Check the stream status
kubectl get streams
```

## Available Examples

### Benchmark Configurations

These configurations match the exact scenarios used in the Tram benchmarks (see BENCHMARK.md).

#### [`benchmark_otlp_nats.yaml`](benchmark_otlp_nats.yaml)
**Purpose**: Exact configuration for the `otlp-nats` benchmark scenario: OTLP → NATS
- Matches the official benchmark configuration
- **JSON version**: [../config/benchmark_otlp_nats.json](../config/benchmark_otlp_nats.json)

#### [`benchmark_otlp_batch_nats.yaml`](benchmark_otlp_batch_nats.yaml)
**Purpose**: Exact configuration for the `otlp-batch-nats` benchmark scenario: OTLP → batch → NATS
- Uses batch with `max_records: 1000`, `timeout: "10ms"` as in benchmarks
- **JSON version**: [../config/benchmark_otlp_batch_nats.json](../config/benchmark_otlp_batch_nats.json)

#### [`benchmark_otlp_split_nats.yaml`](benchmark_otlp_split_nats.yaml)
**Purpose**: Exact configuration for the `otlp-split-nats` benchmark scenario: OTLP → 2 × cel_filter → 2 × NATS
- Splits into team-0 and other teams as specified in benchmarks
- **JSON version**: [../config/benchmark_otlp_split_nats.json](../config/benchmark_otlp_split_nats.json)

#### [`benchmark_nats_filter_nats.yaml`](benchmark_nats_filter_nats.yaml)
**Purpose**: Exact configuration for the `nats-filter-nats` benchmark scenario: NATS → cel_filter → NATS
- Uses durable consumer with workers as in benchmarks
- **JSON version**: [../config/benchmark_nats_filter_nats.json](../config/benchmark_nats_filter_nats.json)

### [`otlp_basic.yaml`](otlp_basic.yaml)
**Purpose**: Basic OTLP to OTLP forwarding
- Receives OTLP data on port 4318
- Forwards to a central OTLP collector
- No processing stages
- **JSON version**: [../config/otlp_basic.json](../config/otlp_basic.json)

### [`otlp_secure.yaml`](otlp_secure.yaml)
**Purpose**: Secure OTLP forwarding with authentication
- HTTPS endpoint
- Bearer token authentication
- Custom headers for metadata
- **JSON version**: [../config/otlp_secure.json](../config/otlp_secure.json)

### [`otl_insecure_dev.yaml`](otlp_insecure_dev.yaml)
**Purpose**: Development configuration with insecure TLS
- HTTPS endpoint with invalid certificate acceptance
- Useful for local development and testing
- Generator source for testing
- **JSON version**: [../config/otlp_insecure_dev.json](../config/otlp_insecure_dev.json)

### [`otlp_batch.yaml`](otlp_batch.yaml)
**Purpose**: High-throughput OTLP forwarding with batching
- Batches events for better throughput
- 1000 records max per batch
- 200ms timeout for batch flush
- **JSON version**: [../config/otlp_batch.json](../config/otlp_batch.json)

### [`otlp_with_filters.yaml`](otlp_with_filters.yaml)
**Purpose**: OTLP forwarding with CEL filtering
- Filters logs based on customer attribute
- Sends filtered data to customer-specific endpoint
- **JSON version**: [../config/otlp_with_filters.json](../config/otlp_with_filters.json)

### [`otlp_with_validation.yaml`](otlp_with_validation.yaml)
**Purpose**: Validated OTLP forwarding with CEL validation
- Validates that all resources have `service.name` attribute
- Rejects invalid data
- **JSON version**: [../config/otlp_with_validation.json](../config/otlp_with_validation.json)

### [`otlp_multi_sink.yaml`](otlp_multi_sink.yaml)
**Purpose**: Multiple OTLP sinks from a single source (fan-out)
- Splits data into different streams using filters
- Sends error/warn logs to one endpoint
- Sends all data to another endpoint
- **JSON version**: [../config/otlp_multi_sink.json](../config/otlp_multi_sink.json)

### [`otlp_nats_bridge.yaml`](otlp_nats_bridge.yaml)
**Purpose**: Bridges NATS JetStream to OTLP HTTP
- Consumes from NATS JetStream
- Uses durable consumer with multiple workers
- Batches messages for efficiency
- Forwards to central OTLP collector
- **JSON version**: [../config/otlp_nats_bridge.json](../config/otlp_nats_bridge.json)

### [`otlp_benchmark.yaml`](otlp_benchmark.yaml)
**Purpose**: High-performance benchmark configuration
- Generator source for load testing
- Large batch sizes (8192 records)
- Short batch timeout (100ms)
- **JSON version**: [../config/otlp_benchmark.json](../config/otlp_benchmark.json)

## Configuration Reference

### Service Discovery

For production environments, consider using Kubernetes service discovery:

```yaml
spec:
  sinks:
    - type: otlp
      name: my_sink
      from: my_source
      spec:
        url: "http://otel-collector-service.namespace.svc.cluster.local:4318"
```

### Environment Variables

Use environment variables for sensitive data:

```yaml
spec:
  sinks:
    - type: otlp
      name: secure_sink
      from: my_source
      spec:
        url: "https://otel-collector:443"
        headers:
          Authorization: "Bearer ${API_KEY}"
```

Deploy with:
```bash
kubectl create secret generic otel-secrets --from-literal=API_KEY=your-api-key-here
kubectl apply -f your-stream.yaml
```

### Resource Management

#### CPU/Memory Requests

For high-throughput OTLP sinks, consider adding resource requests:

```yaml
apiVersion: io.tram.core/v1
kind: Stream
metadata:
  name: high-volume-otlp
  annotations:
    tram.io/cpu-request: "500m"
    tram.io/cpu-limit: "1000m"
    tram.io/memory-request: "512Mi"
    tram.io/memory-limit: "1Gi"
spec:
  # ... your configuration
```

#### Horizontal Pod Autoscaling

For variable workloads, consider HPA:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: tram-otlp-hpa
spec:
  scaleTargetRef:
    apiVersion: io.tram.core/v1
    kind: Stream
    name: otlp-stream
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

## Integration Patterns

### OpenTelemetry Collector Integration

The most common use case is forwarding to an OpenTelemetry Collector:

```yaml
spec:
  sinks:
    - type: otlp
      name: otel_collector_sink
      from: source_or_stage
      spec:
        url: "http://otel-collector:4318"
```

### Multi-Region Deployment

Deploy different streams for different regions:

```yaml
# region-a.yaml
apiVersion: io.tram.core/v1
kind: Stream
metadata:
  name: otlp-region-a
  labels:
    region: a
# ...

# region-b.yaml  
apiVersion: io.tram.core/v1
kind: Stream
metadata:
  name: otlp-region-b
  labels:
    region: b
# ...
```

### Canary Deployment

Deploy a canary stream for testing new configurations:

```bash
# Main production stream
kubectl apply -f otlp_secure.yaml

# Canary stream with new configuration
kubectl apply -f otlp_benchmark.yaml
```

## Monitoring and Observability

### Health Checks

Check stream health:
```bash
kubectl get stream otlp-stream -o yaml
kubectl describe stream otlp-stream
```

### Logs

View stream logs:
```bash
kubectl logs -l app=tram,stream=otlp-stream
```

### Metrics

If your Tram deployment includes Prometheus metrics:
```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: tram-monitor
spec:
  selector:
    matchLabels:
      app: tram
  endpoints:
    - port: metrics
      interval: 30s
```

## Performance Optimization

### Connection Pooling

The OTLP sink uses HTTP connection pooling by default:
- Max idle connections: 100
- Idle connection timeout: 90 seconds
- Max idle connections per host: 100

### Batch Size Tuning

Adjust batch sizes based on your workload:

```yaml
stages:
  - type: batch
    name: batch
    from: source
    spec:
      max_records: 5000    # Increase for high throughput
      timeout: "500ms"    # Increase to allow larger batches
```

### Timeout Configuration

Set appropriate timeouts:

```yaml
sinks:
  - type: otlp
    name: my_sink
    from: batch
    spec:
      url: "http://collector:4318"
      timeout: "60s"  # Longer timeout for large batches
```

## Troubleshooting

### Connection Issues

Check connectivity to your OTLP endpoint:
```bash
# From within the cluster
kubectl exec -it tram-pod -- curl -v http://otel-collector:4318/v1/logs

# Check DNS resolution
kubectl exec -it tram-pod -- nslookup otel-collector
```

### Certificate Issues

For HTTPS endpoints with self-signed certificates:

```yaml
sinks:
  - type: otlp
    name: insecure_sink
    from: source
    spec:
      url: "https://collector:443"
      insecure: true  # Skip certificate validation
```

Or properly configure certificates in your cluster.

### Authentication Issues

Verify your authentication headers are being set correctly:
```bash
# Check environment variables are set
kubectl exec -it tram-pod -- echo $API_KEY

# Check if secrets are mounted
kubectl exec -it tram-pod -- ls /secrets
```

## Security Best Practices

1. **Use HTTPS**: Always use HTTPS in production
2. **TLS Certificates**: Use proper TLS certificates, not `insecure: true`
3. **Secrets Management**: Use Kubernetes Secrets for sensitive data
4. **Network Policies**: Restrict access to your Tram streams
5. **RBAC**: Use appropriate RBAC roles for the Tram operator

Example NetworkPolicy:
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: tram-allow-otel
spec:
  podSelector:
    matchLabels:
      app: tram
  policyTypes:
    - Egress
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: otel-collector
      ports:
        - protocol: TCP
          port: 4318
```

## JSON Configuration Equivalents

All Kubernetes YAML examples in this directory have corresponding JSON configurations in the `../config/` directory with the same base filename:

- `k8s/otlp_basic.yaml` ↔ `config/otlp_basic.json`
- `k8s/otlp_secure.yaml` ↔ `config/otlp_secure.json`
- `k8s/otlp_batch.yaml` ↔ `config/otlp_batch.json`
- `k8s/otlp_with_filters.yaml` ↔ `config/otlp_with_filters.json`
- `k8s/otlp_with_validation.yaml` ↔ `config/otlp_with_validation.json`
- `k8s/otlp_multi_sink.yaml` ↔ `config/otlp_multi_sink.json`
- `k8s/otlp_nats_bridge.yaml` ↔ `config/otlp_nats_bridge.json`
- `k8s/otlp_benchmark.yaml` ↔ `config/otlp_benchmark.json`
- `k8s/otlp_insecure_dev.yaml` ↔ `config/otlp_insecure_dev.json`

See [OTLP JSON Examples](../config/OTLP_EXAMPLES.md) for detailed JSON configuration documentation.