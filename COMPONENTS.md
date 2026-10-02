# Components Documentation

This document provides detailed documentation for all implemented sources, stages, and sinks in Tram.

---

## 📡 Sources

Sources are the entry points of a pipeline that produce or receive data events.

### Available Sources

| Name | Type | Description |
|------|------|-------------|
| `generator` | `generator` | Generates synthetic events at a specified interval for testing |
| `nats` | `nats` | Consumes messages from NATS JetStream |
| `otlp` | `otlp` | Receives protobuf encoded OpenTelemetry Protocol data via HTTP |

### Generator Source

Generates events at a regular interval. Useful for testing and development.

**Type:** `generator`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `interval` | string | Yes | - | Time interval between events (e.g., `"5s"`, `"1m"`) |

**Example:**

```json
{
  "name": "event-generator",
  "type": "generator",
  "spec": {
    "interval": "5s"
  }
}
```

### OTLP Source

Receives OpenTelemetry Protocol data. Supports both HTTP and gRPC endpoints.

**Type:** `otlp`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `http_port` | int | No | - | HTTP port to listen on for OTLP/HTTP |

**Example (HTTP):**

```json
{
  "name": "otlp-receiver",
  "type": "otlp",
  "spec": {
    "http_port": 4318
  }
}
```

### NATS Source

Consumes OTLP-encoded messages from a NATS JetStream stream.

**Type:** `nats`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | string | Yes | - | NATS server URL (e.g., `"nats://localhost:4222"`) |
| `stream` | string | Yes | - | JetStream stream name to consume from |
| `subject` | string | Yes | - | Subject filter for the consumer |
| `queue` | string | No | - | Queue group name for load balancing |
| `durable` | string | No | - | Durable consumer name for resumption |
| `workers` | int | No | 1 | Number of concurrent worker goroutines (1-100) |

**Example:**

```json
{
  "name": "nats-consumer",
  "type": "nats",
  "spec": {
    "url": "nats://localhost:4222",
    "stream": "otlp-data",
    "subject": "logs.*"
  }
}
```

**Example with durable consumer and multiple workers:**

```json
{
  "name": "nats-consumer",
  "type": "nats",
  "spec": {
    "url": "nats://localhost:4222",
    "stream": "otlp-data",
    "subject": "logs.*",
    "durable": "tram-consumer",
    "queue": "tram-queue",
    "workers": 5
  }
}
```

---

## 🔧 Stages

Stages process, transform, or filter data between sources and sinks.

### Available Stages

| Name | Type | Description |
|------|------|-------------|
| `batch` | `batch` | Merges concurrently arriving events into larger batches |
| `cel_validator` | `cel_validator` | Filters events using Common Expression Language (CEL) |
| `delay` | `delay` | Delays events by a specified duration |
| `noop` | `noop` | Pass-through stage that does no processing (useful for testing) |

### CEL Filter Stage

Filters incoming events using Common Expression Language (CEL) expressions. Since an event contains only one of logs, metrics, or traces, separate expressions can be specified for each signal type. Events can be either dropped or rejected based on the evaluation result.

**Type:** `cel_validator`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `logs` | string | No | `"true"` | CEL expression to evaluate for log events. Must return a boolean. Empty string defaults to accepting all logs |
| `metrics` | string | No | `"true"` | CEL expression to evaluate for metric events. Must return a boolean. Empty string defaults to accepting all metrics |
| `traces` | string | No | `"true"` | CEL expression to evaluate for trace events. Must return a boolean. Empty string defaults to accepting all traces |
| `on_mismatch` | string | Yes | - | Action to take when expression evaluates to false. Must be either `"drop"` or `"reject"` |

**CEL Expression Context:**
- Each expression receives the full signal data object: `logs` (LogsData), `metrics` (MetricsData), or `traces` (TracesData)
- Each expression operates only on its respective signal type and must return a boolean
- Use the full protobuf structure, e.g., `logs.resource_logs.all(...)`, `metrics.resource_metrics.all(...)`

**Example:**

```json
{
  "name": "filter-stage",
  "type": "cel_validator",
  "from": "source",
  "spec": {
    "logs": "logs.resource_logs.all(resLog, resLog.scope_logs.all(scopeLog, scopeLog.log_records.exists(record, record.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test')))",
    "on_mismatch": "reject"
  }
}
```

**On Mismatch Behavior:**
- `drop` - Silently drops the event (no error, processing continues)
- `reject` - Rejects the event with an error (may trigger error handling)

**Additional Examples:**

Filter logs by severity:
```json
{
  "logs": "logs.resource_logs.all(resLog, resLog.scope_logs.all(scopeLog, scopeLog.log_records.exists(record, record.severity_text == 'ERROR')))",
  "on_mismatch": "drop"
}
```

Filter metrics by name:
```json
{
  "metrics": "metrics.resource_metrics.all(resMet, resMet.scope_metrics.all(scopeMet, scopeMet.metrics.exists(m, m.name == 'http.request.duration')))",
  "on_mismatch": "drop"
}
```

Accept all, reject nothing (pass-through):
```json
{
  "logs": "",
  "metrics": "",
  "traces": "",
  "on_mismatch": "drop"
}
```

### Batch Stage

Merges events that arrive concurrently into one event, so downstream components handle fewer, larger batches. It batches whatever it receives; use a filter stage before it to batch only some of the data.

**Type:** `batch`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `max_records` | int | No | `8192` | Flushes once the batch holds at least this many records (log records, metric data points and spans). Events are never split, so a batch can exceed it by the size of the last event |
| `timeout` | string | No | `"200ms"` | Flushes a batch this long after its first event arrived |

**Example:**

```json
{
  "name": "batch",
  "type": "batch",
  "from": "otlp-in",
  "spec": {
    "max_records": 1000,
    "timeout": "500ms"
  }
}
```

**Behavior Notes:**
- Batching happens in memory only. Each caller waits until its batch has been delivered and receives the delivery result, so sources keep acknowledging or rejecting exactly what was delivered, and a slow downstream slows the sources down instead of growing the buffer
- Batching only helps when events arrive concurrently, e.g. parallel OTLP requests or a NATS source with several `workers`. A source that sends one event at a time only adds `timeout` to its latency
- If a caller's context is cancelled (e.g. the OTLP client disconnects) before the batch flushes, its event is withdrawn and not delivered. Once the flush has started, the event is delivered and the caller waits for the result
- A delivery error, including a rejection by a later stage, applies to every event in the batch. Place `cel_validator` stages with `on_mismatch: reject` before the batch, so one invalid event does not reject the others
- Events are merged by concatenating their resource entries. Resources and scopes that appear in several events are kept as separate entries, which OTLP allows
- On shutdown, the open batch is delivered before downstream stages and sinks stop

### Delay Stage

Delays the processing of events by a specified duration. Useful for rate limiting, throttling, or creating time-based buffers.

**Type:** `delay`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `delay` | string | Yes | - | Duration to delay events (e.g., `"100ms"`, `"1s"`, `"5m"`) |

**Example:**

```json
{
  "name": "delay-stage",
  "type": "delay",
  "from": "source",
  "spec": {
    "delay": "1s"
  }
}
```

### No-op Stage

A pass-through stage that performs no processing. Useful for testing, debugging, or as a placeholder in pipeline designs.

**Type:** `noop`

**Configuration:** None required.

**Example:**

```json
{
  "name": "passthrough",
  "type": "noop",
  "from": "source"
}
```

---

## 📤 Sinks

Sinks are the terminal points of a pipeline that consume or export data.

### Available Sinks

| Name | Type | Description |
|------|------|-------------|
| `debug` | `debug` | Logs events to stdout for debugging and development |
| `nats` | `nats` | Publishes OTLP-encoded events to NATS JetStream with dynamic subject mapping |

### Debug Sink

Outputs events to stdout in a human-readable format. Useful for development, testing, and debugging pipelines.

**Type:** `debug`

**Configuration:** None required.

**Example:**

```json
{
  "name": "console-output",
  "type": "debug",
  "from": "source"
}
```

### NATS Sink

Publishes OTLP-encoded events to NATS JetStream. Uses dynamic subject mapping with CEL expressions to route different telemetry data to appropriate subjects. Automatically creates or updates the specified stream with the configured settings.

**Type:** `nats`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | string | Yes | - | NATS server URL (e.g., `"nats://localhost:4222"`) |
| `stream` | string | Yes | - | JetStream stream name to publish to |
| `subject` | object | Yes | - | Subject configuration for dynamic subject mapping |
| `retention` | string | No | - | Stream message retention as a Go duration (e.g., `"24h"`, `"168h"`; days are not supported) |
| `size` | int | No | - | Maximum stream size in bytes |

**Subject Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `register` | array[string] | Yes | - | List of subject patterns to register with the stream |
| `logs` | string | One of `logs`, `metrics`, `traces` | - | CEL expression to generate subject for log events. Must return a string. Has access to the `logs` variable |
| `metrics` | string | One of `logs`, `metrics`, `traces` | - | CEL expression to generate subject for metric events. Must return a string. Has access to the `metrics` variable |
| `traces` | string | One of `logs`, `metrics`, `traces` | - | CEL expression to generate subject for trace events. Must return a string. Has access to the `traces` variable |

Signals without an expression are not published by this sink.

**CEL Expression Context:**
- The CEL expressions have access to a custom `get_attr(attributes, key)` function for extracting attribute values
- Each expression operates on its respective signal type and receives a single record/metric/span at a time
- Must return a string that will be used as the NATS subject

**Example:**

```json
{
  "name": "nats_sink",
  "type": "nats",
  "from": "cel_stage",
  "spec": {
    "url": "nats://localhost:4222",
    "stream": "tram-stream",
    "subject": {
      "register": [
        "logs.audit.*",
        "metrics.*",
        "traces.*"
      ],
      "logs": "'logs.audit.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')",
      "metrics": "'metrics.' + metrics.resource_metrics[0].resource.attributes.get_attr('service.name')",
      "traces": "'traces.' + traces.resource_spans[0].resource.attributes.get_attr('service.name')"
    }
  }
}
```

**Example with retention and size limits:**

```json
{
  "name": "nats_sink_with_limits",
  "type": "nats",
  "from": "filter_stage",
  "spec": {
    "url": "nats://localhost:4222",
    "stream": "tram-data",
    "retention": "24h",
    "size": 1073741824,
    "subject": {
      "register": ["logs.*", "metrics.*"],
      "logs": "'logs.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')",
      "metrics": "'metrics.' + metrics.resource_metrics[0].resource.attributes.get_attr('service.name')"
    }
  }
}
```

**Behavior Notes:**
- The sink automatically creates or updates the stream with the specified configuration
- Events are grouped by their resolved subjects and published as individual OTLP messages
- The `register` subjects are used to configure the stream, while the CEL expressions dynamically determine the actual publish subjects per event
- If a CEL expression fails to evaluate or returns a non-string value, the event will be dropped with an error log

---


## 📝 Notes

- **Component Type vs Name:** The `type` field in configuration refers to the component implementation type (e.g., `generator`, `cel_validator`), while the `name` field is a user-defined identifier for the component instance within a stream.
- **Connections:** The `from` field in stages and sinks specifies which component they receive data from.
- **Tree Structure:** Pipelines form a tree with the source as root, stages as intermediate nodes, and sinks as leaves.
