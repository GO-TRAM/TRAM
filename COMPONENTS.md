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
- The expression has access to the specific signal data (e.g., for logs: `resource_logs`, `scope_logs`, `log_records`)
- Each expression operates only on its respective signal type

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
  "logs": "resource_logs[0].scope_logs[0].log_records[0].severity_text == 'ERROR'",
  "on_mismatch": "drop"
}
```

Filter metrics by name:
```json
{
  "metrics": "resource_metrics[0].scope_metrics[0].metrics.exists(m, m.name == 'http.request.duration')",
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
| `retention` | string | No | - | Stream message retention duration (e.g., `"24h"`, `"7d"`) |
| `size` | int | No | - | Maximum stream size in bytes |

**Subject Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `register` | array[string] | Yes | - | List of subject patterns to register with the stream |
| `logs` | string | Yes | - | CEL expression to generate subject for log events. Must return a string. Has access to the `logs` variable |
| `metrics` | string | Yes | - | CEL expression to generate subject for metric events. Must return a string. Has access to the `metrics` variable |
| `traces` | string | Yes | - | CEL expression to generate subject for trace events. Must return a string. Has access to the `traces` variable |

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
      "metrics": "'metrics.' + metrics.resource_metrics[0].resource.attributes.get_attr('service.name')",
      "traces": "'traces.default'"
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
