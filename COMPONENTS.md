# Components Documentation

This document provides detailed documentation for all implemented sources, stages, and sinks in Tram.

---

## 📡 Sources

Sources are the entry points of a pipeline that produce or receive data events.

### Available Sources

| Name | Type | Description |
|------|------|-------------|
| `generator` | `generator` | Generates synthetic events at a specified interval for testing |
| `otlp` | `otlp` | Receives OpenTelemetry Protocol data via HTTP or gRPC |

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

---

## 🔧 Stages

Stages process, transform, or filter data between sources and sinks.

### Available Stages

| Name | Type | Description |
|------|------|-------------|
| `cel_filter` | `cel_filter` | Filters events using Common Expression Language (CEL) |
| `delay` | `delay` | Delays events by a specified duration |
| `noop` | `noop` | Pass-through stage that does no processing (useful for testing) |

### CEL Filter Stage

Filters incoming events using Common Expression Language (CEL) expressions. Since an event contains only one of logs, metrics, or traces, separate expressions can be specified for each signal type. Events can be either dropped or rejected based on the evaluation result.

**Type:** `cel_filter`

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
  "type": "cel_filter",
  "from": "source",
  "spec": {
    "logs": "resource_logs.all(resLog, resLog.scope_logs.all(scopeLog, scopeLog.log_records.exists(record, record.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test')))",
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

---

## 📝 Notes

- **Component Type vs Name:** The `type` field in configuration refers to the component implementation type (e.g., `generator`, `cel_filter`), while the `name` field is a user-defined identifier for the component instance within a stream.
- **Connections:** The `from` field in stages and sinks specifies which component they receive data from.
- **Tree Structure:** Pipelines form a tree with the source as root, stages as intermediate nodes, and sinks as leaves.
