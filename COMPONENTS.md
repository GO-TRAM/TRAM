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
| `delivery_timeout` | string | No | `"8s"` | Maximum time to pass a request on to the stages and sinks, as a Go duration. A request that takes longer is cancelled and answered with `503`, which clients retry |
| `retry_after` | string | No | `"5s"` | How long clients should wait before retrying a `503`, as a Go duration. Sent as the `Retry-After` header in whole seconds, rounded up |

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
- Accepts `POST` requests on `/v1/logs`, `/v1/metrics` and `/v1/traces` with a protobuf (`application/x-protobuf`) or JSON (`application/json`) body. Requests without a content type are read as protobuf
- Bodies may be compressed with `Content-Encoding: gzip`
- Each request becomes one event and is passed on as a whole
- A request in which a key appears twice in the same key-value list is refused with `422`: the attributes of a resource, scope, log record, span, span event, span link, data point or exemplar, a metric's metadata, and key-value lists nested in values or log bodies. OTLP requires unique keys; with a key twice, a stage could check one value while a sink routes by the other. The same key in different lists, e.g. on the resource and on a record, is allowed
- Responses: `200` with an empty response message in the request's encoding once all sinks have accepted the data, `422` for a body that cannot be read or parsed, contains a duplicate key, or holds data rejected by a stage or sink (e.g. `cel_validator` with `on_mismatch: reject`), `413` for bodies larger than 20 MiB (also after decompression), `415` for other content types or encodings, `503` with a `Retry-After` header if passing the data on failed or took longer than `delivery_timeout`. Only the `503` is retried by OTLP clients: it is the only answer the request itself did not cause
- A rejection is answered with `422` wherever it comes from, including a sink whose destination refuses the data permanently, e.g. an [OTLP Sink](#otlp-sink) whose destination answers `401`, `403` or `404` because of its own configuration. The client then drops data it did not cause to be refused
- The default `delivery_timeout` is below the 10s that OpenTelemetry SDK exporters wait by default, so they get the `503` before they give up. `batch` and `delay` stages have to leave time for it, see [Hold Times](#hold-times). Raise it for pipelines that take longer, e.g. with a `delay` stage or slow sinks, and for clients that wait longer, like the OpenTelemetry Collector (30s by default). A batch that is already being sent when the timeout expires is still awaited, since it holds data of other requests too
- A request may take up to 1 minute to send, which leaves time for large bodies on slow connections
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
| `workers` | int | No | `1` | Number of messages processed concurrently (1-10000) |
| `partition` | object | No | - | Reads the stream of a key from streams partitioned by the NATS sink: `count` (1-1024) as in the sink, and the required `key` |

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
- Up to `workers` messages are processed at the same time, each on its own. The source fetches only as many messages as workers are free, so no fetched message waits for a worker while its acknowledgement deadline runs
- Messages are not processed in the order of the stream, unless `workers` is 1
- A `batch` stage after the source holds up to `workers` messages, since every message waits for its batch to be delivered before it is acknowledged. To let batches fill up to `max_records` before their `timeout`, set `workers` to at least `max_records` divided by the records per message
- The source sets the consumer's `MaxAckPending`, the unacknowledged messages JetStream hands out, to twice `workers` and at least 1,000, the JetStream default. The limit applies across all Tram replicas that share a durable consumer
- The source asks for as many messages as it has `workers`, so with large messages and many workers NATS can have a lot of data to send to it at once. If that exceeds what the NATS server buffers for one connection (`max_pending`, 64 MB by default), the server disconnects the source as a slow consumer, and the messages it had sent come again after their acknowledgement deadline. The server logs `Slow Consumer Detected`; lower `workers`, or raise `max_pending` above `workers` × message size
- Stopping or updating the source interrupts waiting for new messages right away; it only waits for messages that are being processed, which are still delivered and acknowledged
- A lost connection to NATS is retried every 2 seconds, with up to 1 second of jitter, for as long as the source runs, so the source continues after any outage. Acknowledgements that fail while disconnected are not repeated; those messages are redelivered after their ack wait
- With `partition`, the source computes the partition of `key` like the [NATS sink](#partitioning) and replaces `{partition}` in `stream` and `subject` with it, so a source per key reads only the stream that holds the key. `stream` must contain `{partition}`; without `partition`, neither field may contain it
- `count` must match the sink's. The source refuses a stream whose metadata names another count or partition and stays idle like with a missing stream. With a wrong count, the key's stream may also not exist at all

**Example with partitioning by tenant:**

```json
{
  "name": "tenant-123",
  "type": "nats",
  "spec": {
    "url": "nats://localhost:4222",
    "stream": "audit-{partition}",
    "subject": "audit.{partition}.tenant-123",
    "durable": "tenant-123",
    "partition": {
      "count": 8,
      "key": "tenant-123"
    }
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
- An expression that fails for a record, e.g. by accessing a missing list index, rejects the whole event. The OTLP source answers with HTTP 422, the NATS source terminates the message instead of redelivering it
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
- `reject` - Rejects the whole event with an error. The OTLP source answers with HTTP 422, the NATS source terminates the message instead of redelivering it

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

Reject log deliveries that contain a record whose `tenant.id` is not a lowercase UUID. `matches` takes an RE2 regular expression and finds it anywhere in the value, so anchor it with `^` and `$`. Checking a value that a [NATS Sink](#nats-sink) puts into its subjects keeps out dots, whitespace and wildcards, which would make the subject invalid or change its meaning:
```json
{
  "logs": "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(record, record.attributes.exists(a, a.key == 'tenant.id' && a.value.string_value.matches('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')))))",
  "on_mismatch": "reject"
}
```

**Behavior Notes:**
- The decision applies to the whole event. An expression like `log_records.exists(record, record.severity_text == 'ERROR')` passes on a delivery with one ERROR and many INFO records in full, including the INFO records
- An expression that fails for an event, e.g. by accessing a missing list index, rejects the whole event regardless of `on_mismatch`. The OTLP source answers with HTTP 422, the NATS source terminates the message instead of redelivering it
- Place validators with `on_mismatch: reject` before a batch stage, so one invalid delivery does not reject the deliveries batched with it

### Batch Stage

Merges events that arrive concurrently into one event, so downstream components handle fewer, larger batches. It batches whatever it receives; use a filter stage before it to batch only some of the data.

**Type:** `batch`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `max_records` | int | No | `8192` | Flushes once the batch holds at least this many records (log records, metric data points and spans). Events are never split, so a batch can exceed it by the size of the last event |
| `max_bytes` | int | No | - | Flushes a batch before the next event would take it over this many bytes of encoded OTLP data. Without it, the size is not limited |
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
- Behind an OTLP source, `timeout` has to stay below the source's `delivery_timeout` (8s by default), together with the other `batch` and `delay` stages on the way to a sink; otherwise the stream is rejected, see [Hold Times](#hold-times)
- If a caller's context is cancelled (e.g. the OTLP client disconnects or the OTLP source's `delivery_timeout` expires) before the batch flushes, its event is withdrawn and not delivered. Once the flush has started, the event is delivered and the caller waits for the result
- A delivery error, including a rejection by a later stage, applies to every event in the batch. Place `cel_validator` stages with `on_mismatch: reject` before the batch, so one invalid event does not reject the others
- `max_bytes` keeps batches below a limit downstream, e.g. the NATS server's `max_payload` (1 MB by default) or the request size a destination accepts. It counts the protobuf encoding of the data, which is what the NATS and OTLP sinks send uncompressed. An event that is larger by itself is sent alone, since events are never split, so the limit downstream can still be exceeded by single large requests. Measuring costs a pass over every event, so it is only done with `max_bytes` set
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

**Behavior Notes:**
- Behind an OTLP source, the delay counts toward the source's `delivery_timeout` (8s by default). A stream in which the `delay` and `batch` stages on the way to a sink add up to the `delivery_timeout` or more is rejected, see [Hold Times](#hold-times). Raise the `delivery_timeout` for longer delays

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
| `otlp` | `otlp` | Sends OTLP-encoded events via HTTP to an OpenTelemetry endpoint |

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

### OTLP Sink

Sends OpenTelemetry Protocol data via HTTP to an OTLP-compatible endpoint. Sends logs to `/v1/logs`, metrics to `/v1/metrics`, and traces to `/v1/traces` with a protobuf-encoded body.

**Type:** `otlp`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | string | Without `url_expression` | - | OTLP HTTP endpoint URL (e.g., `"http://localhost:4318"`) |
| `url_expression` | object | No | - | Sends each entry to the URL a CEL expression returns, see [Routing by URL](#routing-by-url) |
| `headers` | object | No | - | HTTP headers to send with each request |
| `timeout` | string | No | `"10s"` | HTTP request timeout as a Go duration (e.g., `"5s"`, `"30s"`) |
| `insecure` | boolean | No | `false` | Allow connections to servers with invalid TLS certificates |

**Example:**

```json
{
  "name": "otlp-sink",
  "type": "otlp",
  "from": "source",
  "spec": {
    "url": "http://telemetry-collector:4318"
  }
}
```

**Example with authentication and custom timeout:**

```json
{
  "name": "secure-otlp-sink",
  "type": "otlp",
  "from": "filter-stage",
  "spec": {
    "url": "https://telemetry-collector:4318",
    "timeout": "30s",
    "headers": {
      "Authorization": "Bearer your-api-key-here"
    }
  }
}
```

**Example with invalid certificate acceptance (for testing):**

```json
{
  "name": "dev-otlp-sink",
  "type": "otlp",
  "from": "source",
  "spec": {
    "url": "https://localhost:4318",
    "insecure": true
  }
}
```

**Behavior Notes:**
- Each signal type of an event is sent as a separate HTTP request to its endpoint. An event can hold several signal types, e.g. after a batch stage; if one request fails, the source retries the whole event, so the others may be sent twice
- Uses protobuf encoding (`application/x-protobuf` content type) as required by OTLP/HTTP
- The sink handles URL construction properly, so both `"http://host:4318"` and `"http://host:4318/"` work correctly
- Non-2xx answers fail the delivery. Client errors that refuse the data itself (4xx except 408 and 429) reject the event, so the OTLP source answers with 422 and the NATS source terminates the message. Other errors, including 408, 429 and 5xx, can be retried by the source: the OTLP source answers with 503, the NATS source redelivers the message
- Proxy settings are taken from the `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` environment variables
- Connection pooling is used for better performance with multiple requests
- Timeout applies to the entire request lifecycle, including connection establishment and reading the response
- The requests of an event are sent at the same time, at most 16 at once. If one fails in a way that may go away, e.g. with 503, the event fails retriably even if another destination rejected its data, so the source retries all of it. Only if every failed request was rejected is the event rejected

#### Routing by URL

With `url_expression`, the sink sends each log record, metric data point and span to the URL its expression returns, e.g. to a tenant's own endpoint. It groups the entries by URL and sends one request per URL and signal type.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `logs` | string | One of `logs`, `metrics`, `traces` | - | CEL expression returning the URL of a log record |
| `metrics` | string | One of `logs`, `metrics`, `traces` | - | CEL expression returning the URL of a metric data point |
| `traces` | string | One of `logs`, `metrics`, `traces` | - | CEL expression returning the URL of a span |

- The expressions have the same variables and `get_attr` function as the subject expressions of the [NATS Sink](#nats-sink), e.g. `record`, `resource` and `datapoint`. Metrics are split per data point the same way
- A URL is the base URL like `url`; the sink appends `/v1/logs`, `/v1/metrics` or `/v1/traces`. It must be `http` or `https` and have a host. An expression that fails, does not return a string, or returns another URL rejects the event. `get_attr` returns `""` for a missing attribute, which is not a valid URL
- Signals without an expression are sent to `url`. Without `url`, they are not sent
- `headers`, `timeout` and `insecure` apply to every destination
- The data decides where it is sent. Make sure only trusted values can reach the expression, e.g. by deriving the URL from a tenant ID that a `cel_validator` checks, rather than taking a URL from an attribute

**Example with an endpoint per tenant:**

```json
{
  "name": "tenant-otlp",
  "type": "otlp",
  "from": "validator",
  "spec": {
    "url_expression": {
      "logs": "'https://' + record.attributes.get_attr('tenant.id') + '.otlp.example.com'"
    }
  }
}
```

### NATS Sink

Publishes OTLP-encoded events to NATS JetStream. Uses dynamic subject mapping with CEL expressions to route different telemetry data to appropriate subjects. Creates the specified stream if it does not exist, and otherwise only adds what the sink needs to it.

**Type:** `nats`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | string | Yes | - | NATS server URL (e.g., `"nats://localhost:4222"`) |
| `stream` | string | Yes | - | JetStream stream name to publish to |
| `subject` | object | Yes | - | Subject configuration for dynamic subject mapping |
| `retention` | string | No | `"limits"` | Stream message retention policy: `"limits"` (retain until limits are reached), `"interest"` (remove when all consumers acknowledge), or `"workqueue"` (remove when first consumer acknowledges) |
| `max_age` | string | No | - | Maximum age for messages in the stream as a Go duration (e.g., `"24h"`, `"168h"`; days are not supported) |
| `size` | int | No | - | Maximum stream size in bytes |
| `compression` | string | No | - | How JetStream compresses the stream on disk: `"s2"` or `"none"` |
| `replicas` | int | No | - | How many NATS servers store the stream, from 1 to 5. A new stream without it gets the server's default, 1 |
| `discard` | string | No | `"old"` | What JetStream does when the stream reaches `size` or another limit: `"old"` deletes the oldest messages, `"new"` refuses new ones |
| `storage` | string | No | `"file"` | Where JetStream stores the stream: `"file"` or `"memory"`. Fixed once the stream exists |
| `partition` | object | No | - | Spreads records over several streams, see [Partitioning](#partitioning) |

**Subject Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `register` | array[string] | Yes | - | List of subject patterns to register with the stream |
| `logs` | string | One of `logs`, `metrics`, `traces` | - | CEL expression to generate subject for log events. Must return a string. Has access to the `logs` variable |
| `metrics` | string | One of `logs`, `metrics`, `traces` | - | CEL expression to generate subject for metric events. Must return a string. Has access to the `metrics` variable |
| `traces` | string | One of `logs`, `metrics`, `traces` | - | CEL expression to generate subject for trace events. Must return a string. Has access to the `traces` variable |

Signals without an expression are not published by this sink.

**CEL Expression Context:**

| Expression | Variables |
|------------|-----------|
| `logs` | `logs` (payload), `resource` (Resource), `scope` (InstrumentationScope), `record` (LogRecord) |
| `metrics` | `metrics` (payload), `resource`, `scope`, `metric` (Metric), `datapoint` (the data point; its type depends on the metric type) |
| `traces` | `traces` (payload), `resource`, `scope`, `span` (Span) |

With [partitioning](#partitioning), subject expressions also have `partition` (int), the partition of the record, data point or span.

- Each expression is evaluated once per log record, metric data point and span. The payload variable holds only the current log record, metric or span with its resource and scope, so `[0]` indexes address it, e.g. `logs.resource_logs[0].scope_logs[0].log_records[0]`. For metrics it holds the whole metric with all its data points
- `resource` and `scope` are empty messages when the payload has none. Field names follow the OTLP protobuf definitions, e.g. `record.severity_text`, `datapoint.attributes`
- Metrics are routed per data point: a metric whose data points resolve to different subjects is split into one metric per subject, each with the metric's name, description, unit, metadata and type-specific fields like a sum's temporality. A metric whose data points all resolve to the same subject is published as it is, so an expression that does not use `datapoint`, e.g. `metric.gauge.data_points[0].attributes.get_attr('team')`, keeps every metric whole
- A metric without data points is evaluated once without `datapoint`; an expression that reads it fails for that metric and rejects the event
- The custom function `get_attr(key)` on an attribute list returns the string value of an attribute, or `""` if it is missing, e.g. `logs.resource_logs[0].resource.attributes.get_attr('service.name')` or `datapoint.attributes.get_attr('tenant.id')`
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

**Example with retention and max_age limits:**

```json
{
  "name": "nats_sink_with_limits",
  "type": "nats",
  "from": "filter_stage",
  "spec": {
    "url": "nats://localhost:4222",
    "stream": "tram-data",
    "retention": "interest",
    "max_age": "24h",
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
- When the stream is created in Tram, the sink creates a missing JetStream stream from `register`, `retention`, `max_age`, `size`, `compression`, `replicas`, `discard` and `storage`. An existing stream is only extended: `register` subjects it does not cover yet are added, and `retention`, `max_age`, `size`, `compression`, `replicas` and `discard` are applied if set. Other settings, limits that are not set, and the subjects of other sinks or writers are kept, so several sinks can share a stream and it can be configured outside Tram
- JetStream cannot change the storage of an existing stream. If `storage` is set and the stream has another one, the sink is not created and the Tram stream fails with an error, instead of the sink writing to a stream that does not keep its messages the way it asks for
- `replicas` above 1 needs a NATS cluster with at least that many JetStream servers; otherwise creating or updating the stream fails. Changing it on an existing stream makes JetStream add or remove copies
- Subjects are never removed from a stream; removing one from `register` leaves it on the stream. Set `retention`, `size`, `compression`, `replicas` and `discard` in one sink only, since sinks with different values replace each other's when they start
- With `discard: "old"`, a stream that reaches a limit deletes its oldest messages without telling anyone. With `"new"`, JetStream refuses the publish instead; the event fails and the source retries it, e.g. the OTLP source answers with `503`. Use `"new"` where no message may be lost to a limit, like audit logs
- The `retention` policy controls when acknowledged messages are removed: `"limits"` (default) keeps all messages until stream limits like `max_age`, `max_bytes` or `max_msgs` are reached, `"interest"` removes messages when all consumers have acknowledged them, and `"workqueue"` removes messages when the first consumer acknowledges them. The `max_age` setting applies to all messages regardless of acknowledgment status
- Compression saves disk space and costs CPU on the NATS server; how much it saves depends on the data. Changing it affects only data written afterwards. `size` counts the messages uncompressed
- An event becomes one message per subject. The sink encodes and sends them one after another without waiting for acknowledgements in between, so JetStream stores the first ones while the rest are still being encoded; at most 256 messages per sink are unacknowledged at a time, and sending waits while that many are. It reports success once JetStream has acknowledged every message; if one cannot be sent or is not acknowledged within 10 seconds, the event fails and the source retries it, so messages already stored are stored again. A message that cannot be sent stops the rest of the event from being sent
- Each sink has its own connection to NATS, which all events of the sink share
- Records are grouped by their resolved subject and published as one OTLP message per subject and signal type, so different signal types may share a subject. Each message has a `Tram-Signal` header with the signal type, which the [NATS Source](#nats-source) needs to decode it
- Records keep their resource and scope. If all records of a signal type resolve to the same subject, the payload is published unchanged. Resources that appear in several entries of an event, e.g. after a `batch` stage, stay separate entries in the message, which OTLP allows
- The `register` subjects configure the stream, while the CEL expressions determine the subject of each record. JetStream stores each message in the stream whose subjects cover its subject, which need not be the sink's `stream`: a subject covered by another existing stream is stored there, and one covered by no stream fails to publish
- JetStream reserves `size` on the NATS server for the whole stream when the stream is created, against the server's `max_file_store`. Large sizes on many streams can leave no room for more: creating a stream then fails with `insufficient storage`
- An event is rejected if a subject expression fails for one of its records, or resolves to an invalid subject: one with an empty token, whitespace or a wildcard. `get_attr` returns `""` for a missing attribute, so `'logs.' + ...get_attr('team')` gives the invalid `logs.` for records without it. The OTLP source answers with HTTP 422, the NATS source terminates the message instead of redelivering it
- If a CEL expression fails to evaluate or returns a non-string value, the whole event is dropped with an error log and nothing of it is published
- If publishing fails, the error is reported back to the source, e.g. the NATS source redelivers the message
- A lost connection to NATS is retried every 2 seconds, with up to 1 second of jitter, for as long as the sink runs. While disconnected, publishing fails at once instead of being buffered, so the OTLP source answers with `503` and clients retry until NATS is back

#### Partitioning

A stream slows down with many subjects, and all its writes go through one NATS server. With `partition`, the sink spreads records over `count` streams by a key, e.g. the tenant, so each stream holds a part of the subjects and the streams can be led by different servers.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `count` | int | Yes | - | Number of streams, from 1 to 1024 |
| `logs` | string | With `subject.logs` | - | CEL expression returning the key of a log record. Has the same variables as the subject expression, without `partition` |
| `metrics` | string | With `subject.metrics` | - | CEL expression returning the key of a metric data point |
| `traces` | string | With `subject.traces` | - | CEL expression returning the key of a span |

- The partition of a key is its 32-bit FNV-1a hash modulo `count`. This never changes, so consumers can compute which stream holds a key. The [NATS Source](#nats-source) does so with its `partition` field
- `{partition}` in `stream` and in every `register` subject is replaced by the partition number, so the sink sets up one stream per partition, e.g. `audit-0` to `audit-7` with the subjects `audit.0.>` to `audit.7.>`. Without `partition`, `{partition}` must not appear. All other stream settings, like `replicas` and `max_age`, apply to every stream
- JetStream stores a message in the stream whose subjects cover it, so the subject expression has to put the partition into the subject, e.g. `'audit.' + string(partition) + '.' + ...`. An event with a subject that is not covered by its partition's `register` is rejected
- An expression for the key that fails or does not return a string rejects the event. A missing attribute gives the key `""`, which is a valid key like any other
- Each stream gets the metadata `tram.partition.count` and `tram.partition.index`. A sink refuses to start if a stream has metadata of another count or index, so streams of different partitionings are never mixed
- Changing `count` moves keys to other streams. Data already stored stays in its old stream, where consumers of the new partitioning do not read it, so changing it is a migration. Choose `count` with room to grow

**Example with partitioning by tenant:**

```json
{
  "name": "audit_sink",
  "type": "nats",
  "from": "validator",
  "spec": {
    "url": "nats://localhost:4222",
    "stream": "audit-{partition}",
    "replicas": 3,
    "partition": {
      "count": 8,
      "logs": "record.attributes.get_attr('tenant.id')"
    },
    "subject": {
      "register": ["audit.{partition}.>"],
      "logs": "'audit.' + string(partition) + '.' + record.attributes.get_attr('tenant.id')"
    }
  }
}
```

---


## 📝 Notes

- **Component Type vs Name:** The `type` field in configuration refers to the component implementation type (e.g., `generator`, `cel_validator`), while the `name` field is a user-defined identifier for the component instance within a stream.
- **Connections:** The `from` field in stages and sinks specifies which component they receive data from.
- **Tree Structure:** Pipelines form a tree with the source as root, stages as intermediate nodes, and sinks as leaves.
- **Delivery Guarantee:** Delivery is at-least-once, and Tram does not deduplicate. A sink can receive the same data again, e.g. when a client retries after a `503` although some sinks had already accepted the data, when a NATS message is redelivered after one branch of its stream failed, or when a publish succeeded but its acknowledgement was lost. Deduplicate at the destination where duplicates matter.

### Hold Times

A source like OTLP waits for a delivery only for a limited time, its `delivery_timeout`, and then answers with `503`. `batch` stages can hold an event back for up to their `timeout`, `delay` stages for their `delay`. When a stream is created or updated, Tram adds up these hold times on the way from the source to each sink and rejects the stream if the sum reaches the source's limit: every event that does not fill its batch would be cancelled, so the stream would fail as soon as traffic gets low. The error names the stages:

```
stream spec is invalid: on the way to sink openobserve-traces, batch-traces (10s) can hold an event for 10s, but source otlp cancels deliveries after 8s
```

An update that is rejected leaves the running stream unchanged. Shorten the `timeout` or `delay`, or raise the source's `delivery_timeout`, and keep that below the time the clients wait. Sources without such a limit, like NATS, are not checked.
