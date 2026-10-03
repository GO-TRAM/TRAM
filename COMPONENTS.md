# Components Documentation

This document provides detailed documentation for all implemented sources, stages, and sinks in Tram.

---

## 📡 Sources

Sources are the entry points of a pipeline that produce or receive data events.

### Available Sources

| Name | Type | Description |
|------|------|-------------|
| `generator` | `generator` | Emits empty events at a fixed interval, for testing pipelines |
| `nats` | `nats` | Consumes events published by Tram's `nats` sink from NATS JetStream |
| `otlp` | `otlp` | Receives protobuf-encoded OpenTelemetry Protocol (OTLP) data via HTTP |

### Generator Source

Emits an event at a regular interval. The events are empty: they contain no logs, metrics or traces. Useful for testing how a pipeline is wired; stages that count or inspect records, like `batch` or `cel_filter`, ignore them.

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

Receives OpenTelemetry Protocol data over HTTP (OTLP/HTTP). OTLP over gRPC is not supported.

**Type:** `otlp`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `http_port` | int | Yes | - | Port to listen on, on all network interfaces |

**Example:**

```json
{
  "name": "otlp-receiver",
  "type": "otlp",
  "spec": {
    "http_port": 4318
  }
}
```

**Behavior Notes:**
- Accepts `POST` requests on `/v1/logs`, `/v1/metrics` and `/v1/traces` with a protobuf-encoded body. JSON-encoded OTLP is not supported
- Each request becomes one event and is passed on as a whole
- Responses: `200` with an empty body once all sinks have accepted the data, `400` for a malformed body or data rejected by a stage (e.g. `cel_validator` with `on_mismatch: reject`), `413` for bodies larger than 20 MiB, `500` if passing the data on failed
- The port is opened when the stream is created, so a port that is already in use fails the stream creation

### NATS Source

Consumes events from a NATS JetStream stream that were published by Tram's [NATS Sink](#nats-sink).

**Type:** `nats`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | string | Yes | - | NATS server URL (e.g., `"nats://localhost:4222"`) |
| `stream` | string | Yes | - | JetStream stream to consume from. It must exist when the Tram stream is created; otherwise the source logs an error and stays idle until the Tram stream is updated |
| `subject` | string | Yes | - | Subject filter of the consumer; wildcards like `logs.*` are allowed |
| `durable` | string | No | - | Name of a durable consumer. Without it, a temporary consumer is created |
| `workers` | int | No | `1` | Number of messages processed concurrently (1-100) |

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
    "workers": 5
  }
}
```

**Behavior Notes:**
- Only messages with a `Tram-Signal` header, which Tram's NATS sink sets, can be decoded. Other messages are terminated and not redelivered
- Each message becomes one event
- A message is acknowledged once all sinks have accepted it. If passing it on fails, it is redelivered after 5 seconds; data rejected by a stage is terminated instead
- Delivery is at-least-once: when one branch of a stream fails, the redelivered message reaches all branches again
- The consumer starts at the beginning of the stream. A durable consumer continues where it stopped; a temporary consumer starts from the beginning again whenever the source is created, i.e. when the Tram stream is created or its source is changed. Updates that only change stages or sinks keep the consumer
- Each worker processes one message at a time; use several workers to let a `batch` stage merge messages
- Stopping or updating the source interrupts waiting for new messages right away; it only waits for messages that are being processed, which are still delivered and acknowledged

---

## 🔧 Stages

Stages process, transform, or filter data between sources and sinks.

### Available Stages

| Name | Type | Description |
|------|------|-------------|
| `batch` | `batch` | Merges concurrently arriving events into larger batches |
| `cel_filter` | `cel_filter` | Keeps only the records that match a Common Expression Language (CEL) expression |
| `cel_validator` | `cel_validator` | Accepts, drops or rejects whole events using Common Expression Language (CEL) |
| `delay` | `delay` | Holds every event back for a fixed duration |
| `noop` | `noop` | Pass-through stage that does no processing (useful for testing) |

### CEL Filter Stage

Keeps only the records for which a Common Expression Language (CEL) expression returns true, and passes on a batch containing just those. It decides per log record, metric data point and span, so records with different attribute values that arrive in the same request, resource or scope can be separated. To accept or reject whole events instead, use the [CEL Validator Stage](#cel-validator-stage).

**Type:** `cel_filter`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `logs` | string | One of `logs`, `metrics`, `traces` | - | CEL expression evaluated per log record. Must return a boolean |
| `metrics` | string | One of `logs`, `metrics`, `traces` | - | CEL expression evaluated per metric data point. Must return a boolean |
| `traces` | string | One of `logs`, `metrics`, `traces` | - | CEL expression evaluated per span. Must return a boolean |

Signal types without an expression are passed on unfiltered. Use `"false"` to remove a signal type completely.

**CEL Expression Context:**

| Expression | Variables |
|------------|-----------|
| `logs` | `resource` (Resource), `scope` (InstrumentationScope), `record` (LogRecord) |
| `metrics` | `resource`, `scope`, `metric` (Metric), `datapoint` (the data point; its type depends on the metric type) |
| `traces` | `resource`, `scope`, `span` (Span) |

`resource` and `scope` are empty messages when the payload has none, so their fields can be accessed without checks. Field names follow the OTLP protobuf definitions, e.g. `record.severity_text`, `span.name`, `datapoint.attributes`.

**Example:** split logs by customer into two branches:

```json
"stages": [
  {
    "name": "customer-a",
    "type": "cel_filter",
    "from": "otlp-in",
    "spec": {
      "logs": "record.attributes.exists(a, a.key == 'customer' && a.value.string_value == 'A')"
    }
  },
  {
    "name": "other-customers",
    "type": "cel_filter",
    "from": "otlp-in",
    "spec": {
      "logs": "!record.attributes.exists(a, a.key == 'customer' && a.value.string_value == 'A')"
    }
  }
]
```

**Additional Examples:**

Keep only ERROR logs:
```json
{ "logs": "record.severity_text == 'ERROR'" }
```

Keep only the data points of one metric:
```json
{ "metrics": "metric.name == 'http.server.request.duration'" }
```

Keep only spans of one service:
```json
{ "traces": "resource.attributes.exists(a, a.key == 'service.name' && a.value.string_value == 'checkout')" }
```

**Behavior Notes:**
- Kept records are passed on in their original resource and scope; nothing is copied, and containers without remaining records are removed. If no record is kept, nothing is passed on
- When splitting into branches, write the last branch as the negation of the others, so every record ends up in exactly one branch
- An expression that fails for a record, e.g. by accessing a missing list index, rejects the whole event. The OTLP source answers with HTTP 400, the NATS source terminates the message instead of redelivering it
- Place a batch stage before the filters to merge many small requests first; each branch then receives one filtered batch

### CEL Validator Stage

Checks each incoming event as a whole with Common Expression Language (CEL) expressions and passes it on, drops it, or rejects it. An event is one delivery from the stage's parent: one OTLP request, one NATS message, or one batch. The stage never removes individual records; to keep only matching records, use the [CEL Filter Stage](#cel-filter-stage).

**Type:** `cel_validator`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `logs` | string | No | `"true"` | CEL expression evaluated once for all logs of an event. Must return a boolean. Empty string accepts all logs |
| `metrics` | string | No | `"true"` | CEL expression evaluated once for all metrics of an event. Must return a boolean. Empty string accepts all metrics |
| `traces` | string | No | `"true"` | CEL expression evaluated once for all traces of an event. Must return a boolean. Empty string accepts all traces |
| `on_mismatch` | string | Yes | - | Action when an expression returns false: `"drop"` or `"reject"` |

**CEL Expression Context:**
- Each expression receives the complete payload of its signal type: `logs` (LogsData), `metrics` (MetricsData), or `traces` (TracesData)
- An event can contain several signal types, e.g. after a batch stage. Each type present is checked with its own expression
- Use the full protobuf structure, e.g. `logs.resource_logs.all(...)`, and write the expression as a condition that every accepted delivery has to fulfil

**On Mismatch Behavior:**
- `drop` - Removes all data of the signal type whose expression returned false. Other signal types of the event are still passed on; if nothing is left, nothing is passed on
- `reject` - Rejects the whole event with an error. The OTLP source answers with HTTP 400, the NATS source terminates the message instead of redelivering it

**Example:** reject log deliveries in which a resource has no `service.name`:

```json
{
  "name": "require-service-name",
  "type": "cel_validator",
  "from": "source",
  "spec": {
    "logs": "logs.resource_logs.all(r, r.resource.attributes.exists(a, a.key == 'service.name'))",
    "on_mismatch": "reject"
  }
}
```

**Additional Examples:**

Drop metric deliveries that contain a metric without a name:
```json
{
  "metrics": "metrics.resource_metrics.all(r, r.scope_metrics.all(s, s.metrics.all(m, m.name != '')))",
  "on_mismatch": "drop"
}
```

Reject trace deliveries that contain a span without a valid trace ID:
```json
{
  "traces": "traces.resource_spans.all(r, r.scope_spans.all(s, s.spans.all(span, size(span.trace_id) == 16)))",
  "on_mismatch": "reject"
}
```

**Behavior Notes:**
- The decision applies to the whole event. An expression like `log_records.exists(record, record.severity_text == 'ERROR')` passes on a delivery with one ERROR and many INFO records in full, including the INFO records
- Place validators with `on_mismatch: reject` before a batch stage, so one invalid delivery does not reject the deliveries batched with it

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

Holds every event back for a fixed duration before passing it on. The caller waits during that time, so a source that sends one event at a time is slowed down accordingly; concurrent events are delayed in parallel. Useful for testing and for simulating slow downstream systems.

**Type:** `delay`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `delay` | string | Yes | - | Duration to hold each event back (e.g., `"100ms"`, `"1s"`, `"5m"`) |

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
| `debug` | `debug` | Writes events to the log (stderr) for debugging and development |
| `nats` | `nats` | Publishes OTLP-encoded events to NATS JetStream with dynamic subject mapping |

### Debug Sink

Writes every event to the log on stderr as a `Debug event` record with the attributes `path` (the path the event took through the stream, e.g. `in -> filter -> out`), `records` (the number of log records, data points and spans) and `event` (Go's representation of the OTLP data, which is verbose but complete). Useful for development, testing, and debugging pipelines.

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
- Each expression is evaluated once per log record, metric and span. It receives a payload of its signal type (`logs`, `metrics` or `traces`) that contains only that one entry with its resource and scope, so `[0]` indexes address it, e.g. `logs.resource_logs[0].scope_logs[0].log_records[0]`
- Metrics are routed per metric, not per data point: all data points of a metric go to the same subject
- The custom function `get_attr(key)` on an attribute list returns the string value of an attribute, or `""` if it is missing, e.g. `logs.resource_logs[0].resource.attributes.get_attr('service.name')`
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
- The sink creates the JetStream stream or updates it with `register`, `retention` and `size` when the stream is created in Tram. Settings changed elsewhere are overwritten
- Records are grouped by their resolved subject and published as one OTLP message per subject and signal type, so different signal types may share a subject. Each message has a `Tram-Signal` header with the signal type, which the [NATS Source](#nats-source) needs to decode it
- Records keep their resource and scope. If all records of a signal type resolve to the same subject, the payload is published unchanged. Resources that appear in several entries of an event, e.g. after a `batch` stage, stay separate entries in the message, which OTLP allows
- The `register` subjects configure the stream, while the CEL expressions determine the subject of each record. Records whose subject is not covered by `register` cannot be published
- If a CEL expression fails to evaluate or returns a non-string value, the whole event is dropped with an error log and nothing of it is published
- If publishing fails, the error is reported back to the source, e.g. the NATS source redelivers the message

---


## 📝 Notes

- **Component Type vs Name:** The `type` field in configuration refers to the component implementation type (e.g., `generator`, `cel_validator`), while the `name` field is a user-defined identifier for the component instance within a stream.
- **Connections:** The `from` field in stages and sinks specifies which component they receive data from.
- **Tree Structure:** Pipelines form a tree with the source as root, stages as intermediate nodes, and sinks as leaves.
