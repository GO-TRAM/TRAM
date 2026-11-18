# Components Documentation

This document provides detailed documentation for all implemented sources, stages, and sinks in Tram.

---

## 📡 Sources

Sources are the entry points of a pipeline that produce or receive data events.

### Available Sources

| Name | Type | Description |
|------|------|-------------|
| `broker` | `broker` | Receives OTLP data from a message broker through a driver (NATS JetStream, Kafka) |
| `generator` | `generator` | Emits empty events at a fixed interval, for testing pipelines |
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

### Broker Source

Receives OTLP data from a message broker. The broker is chosen by `driver`, and the driver's options are under a key of the same name, e.g. `nats:`. Every message is one serialized OTLP `LogsData`, `MetricsData` or `TracesData`, the plain body that the [Broker Sink](#broker-sink) writes, without headers, so any producer can feed the source. The broker is set up outside Tram: the source creates or updates only its consumer.

**Type:** `broker`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `driver` | string | Yes | - | The broker: `nats` (NATS JetStream) or `kafka` (Apache Kafka and compatible brokers) |
| `signal` | string | Yes | - | What every message holds: `logs`, `metrics` or `traces`. Messages name no signal themselves, so a source reads one signal; read several with one source each |
| `workers` | int | No | `1` | Number of messages processed concurrently (1-10000) |
| `partition` | object | No | - | Reads the partition of a key from data a Broker Sink spread over partitions: `count` (1-1024) as in the sink, and the required `key`. `{partition}` in the driver's options is replaced by the key's partition |
| `nats` | object | With `driver: nats` | - | Options of the NATS driver, see below |
| `kafka` | object | With `driver: kafka` | - | Options of the Kafka driver, see below |

**NATS driver options (`nats`):**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | string | Yes | - | NATS server URL, e.g. `nats://localhost:4222` |
| `subject` | string | Yes | - | Subject filter of the consumer; wildcards like `logs.*` are allowed |
| `stream` | string | No | - | Stream that holds `subject`. Without it, the stream is looked up by `subject` when the source starts |
| `durable` | string | No | - | Name of a durable consumer. Without it, a temporary consumer is created whenever the source is created |

**Kafka driver options (`kafka`):**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `brokers` | array[string] | Yes | - | Seed brokers, `host:port` |
| `topic` | string | Yes | - | Topic to read, from all its partitions |
| `group` | string | Yes | - | Consumer group. Sources that share it split the partitions; the group's committed offsets are where a source continues |
| `tls` | object | No | - | Connect with TLS and the system's certificate authorities; `insecure_skip_verify: true` skips the verification, for tests only |
| `sasl` | object | No | - | Authenticate: `mechanism` (`plain`, `scram-sha-256` or `scram-sha-512`), `username`, `password`. In a `Stream` resource, take them from a secret with `valueFrom.secretKeyRef` |

**Example:**

```json
{
  "name": "audit-in",
  "type": "broker",
  "spec": {
    "driver": "nats",
    "signal": "logs",
    "workers": 64,
    "nats": {
      "url": "nats://localhost:4222",
      "subject": "audit.logs",
      "durable": "audit"
    }
  }
}
```

**Behavior Notes:**
- A message that is not valid OTLP data of `signal` is rejected and not redelivered. Protobuf does not name its type, so data of another signal may decode without error; keep one signal per subject
- Each message becomes one event. A message is acknowledged once all sinks have accepted it. If passing it on fails, it is redelivered after 5 seconds; data rejected by a stage or sink is rejected for good
- Delivery is at-least-once, and messages are not processed in order unless `workers` is 1
- Up to `workers` messages are processed at the same time. The driver buffers only as many messages as there are workers, so no message waits long for a worker while its acknowledgement deadline runs
- A `batch` stage after the source holds up to `workers` messages, since every message waits for its batch to be delivered before it is acknowledged. To let batches fill up to `max_records` before their `timeout`, set `workers` to at least `max_records` divided by the records per message
- Stopping or updating the source interrupts waiting for new messages right away; messages being processed are still delivered and acknowledged
- With `partition`, the source computes the partition of `key` like the [Broker Sink](#broker-partitioning) and replaces `{partition}` in every option of the driver. Without `partition`, the options must not contain it; with it, at least one must
- **NATS:** the consumer starts at the beginning of the stream. A durable consumer continues where it stopped. Its `MaxAckPending` is twice `workers`, at least 1,000, the JetStream default; the limit applies across all Tram replicas that share a durable consumer
- **NATS:** with large messages and many workers, NATS can have a lot of data to send to the source at once. If that exceeds what the server buffers for one connection (`max_pending`, 64 MB by default), the server disconnects the source as a slow consumer, and the messages it had sent come again after their acknowledgement deadline. Lower `workers`, or raise `max_pending` above `workers` × message size
- **NATS:** a lost connection is retried every 2 seconds, with up to 1 second of jitter, for as long as the source runs. A stream that does not exist when the source starts fails the Tram stream, which is retried
- **Kafka:** offsets are committed per partition, every second and when the source stops, and only up to the first message that is not done yet, so messages finished out of order are never skipped by a restart. A message that is retried comes again after the delay from the same source; its offset stays uncommitted until it is done, so a restart reads it again. A rejected message counts as done; Tram writes no dead-letter topic
- **Kafka:** a topic that does not exist when the source starts fails the Tram stream, which is retried. A group rebalance hands partitions to other sources; messages of those partitions that were not done yet are read again by their new owner

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
| `value_map` | `value_map` | Copies an attribute value to another attribute by looking it up in a table |

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
- An expression that fails for a record, e.g. by accessing a missing list index, rejects the whole event. The OTLP source answers with HTTP 422, the Broker Source rejects the message instead of redelivering it
- Place a batch stage before the filters to merge many small requests first; each branch then receives one filtered batch

### CEL Validator Stage

Checks each incoming event as a whole with Common Expression Language (CEL) expressions and passes it on, drops it, or rejects it. An event is one delivery from the stage's parent: one OTLP request, one broker message, or one batch. The stage never removes individual records; to keep only matching records, use the [CEL Filter Stage](#cel-filter-stage).

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
- `reject` - Rejects the whole event with an error. The OTLP source answers with HTTP 422, the Broker Source rejects the message instead of redelivering it

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

Reject log deliveries that contain a record whose `tenant.id` is not a lowercase UUID. `matches` takes an RE2 regular expression and finds it anywhere in the value, so anchor it with `^` and `$`. Checking a value that a [Broker Sink](#broker-sink) puts into its NATS subjects keeps out dots, whitespace and wildcards, which would make the subject invalid or change its meaning:
```json
{
  "logs": "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(record, record.attributes.exists(a, a.key == 'tenant.id' && a.value.string_value.matches('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')))))",
  "on_mismatch": "reject"
}
```

**Behavior Notes:**
- The decision applies to the whole event. An expression like `log_records.exists(record, record.severity_text == 'ERROR')` passes on a delivery with one ERROR and many INFO records in full, including the INFO records
- An expression that fails for an event, e.g. by accessing a missing list index, rejects the whole event regardless of `on_mismatch`. The OTLP source answers with HTTP 422, the Broker Source rejects the message instead of redelivering it
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
- Batching only helps when events arrive concurrently, e.g. parallel OTLP requests or a Broker Source with several `workers`. A source that sends one event at a time only adds `timeout` to its latency
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

### Value Map Stage

Looks up the value of a source attribute in a table for every log record, metric data point and span, and sets a target attribute to the value the table maps it to. On a miss - the source attribute is missing, or its value has no table entry - the target attribute is set to a default value instead. Every record is passed on; nothing is filtered, dropped or held back. The stage is generic: it knows nothing about specific attributes beyond its configuration.

**Type:** `value_map`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `source_attr` | string | Yes | - | Attribute whose value is looked up |
| `target_attr` | string | Yes | - | Attribute set to the result of the lookup. An existing attribute of that name is replaced |
| `default` | string | Yes | - | Value set when the source attribute is missing or its value has no table entry |
| `level` | string | No | `"record"` | Level the source attribute is read from: `record` (the record's own attributes) or `resource` (the attributes of the record's resource) |
| `table` | object | One of `table`, `table_ref` | - | Embedded lookup table, source value to target value. In a Kubernetes `Stream` resource, the table can come from config maps selected by label, which keeps large tables out of the resource; see [Config Maps](internal/adapters/k8scrd/README.md#config-maps) |
| `table_ref` | string | One of `table`, `table_ref` | - | Path to a JSON file that holds the lookup table as an object of string values, e.g. `{"value-a": "target-a"}`. Read once when the stage is created; an unreadable file or invalid JSON is rejected. Exactly one of `table` and `table_ref` has to be set |

The target attribute is always set on the record itself, whatever `level` the source attribute is read from. A source value that is not a string cannot have a table entry and receives the default value.

**Example:** translate a `team` attribute into a `team.id` attribute:

```json
{
  "name": "team-ids",
  "type": "value_map",
  "from": "otlp-in",
  "spec": {
    "source_attr": "team",
    "target_attr": "team.id",
    "default": "unassigned",
    "table": {
      "team-0": "cart",
      "team-1": "checkout"
    }
  }
}
```

**Metrics:** besides the node metrics of its stream, the stage records its lookup outcomes, with the `node` of the stage and the `signal` as labels:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `tram_stage_value_map_mapped_total` | Counter | `node`, `signal` | Records whose source value the table maps, which received the mapped value |
| `tram_stage_value_map_lookup_missing_attr_total` | Counter | `node`, `signal` | Records without the source attribute, which received the default value |
| `tram_stage_value_map_lookup_no_entry_total` | Counter | `node`, `signal` | Records whose source value has no table entry, which received the default value |

The node metrics framework counts records and durations only, not lookup outcomes, which is why the stage adds its own counters. Unlike the node metrics, they cannot carry the stream name, so they are series per stage name; keep stage names unique per stream to keep the series apart.

**Behavior Notes:**
- A lookup miss is a defined state, not an error: the record is still passed on, with the target attribute set to `default`
- Every record is annotated, so the stage always passes on a copy of the event: the containers it annotates (records of each signal) are rebuilt, resources, scopes and metric metadata are shared
- The table is fixed while the stream runs. To change it, update the stream with a changed spec, through the API, the config sync or a `Stream` resource, or change the config maps a `Stream` resource selects its table from; the usual stream update applies: an update with an identical spec changes nothing, an update with just a changed spec replaces the stage without touching the source
- The stage holds no events back and does not count toward the [Hold Times](#hold-times)

---

## 📤 Sinks

Sinks are the terminal points of a pipeline that consume or export data.

### Available Sinks

| Name | Type | Description |
|------|------|-------------|
| `broker` | `broker` | Sends OTLP data to a message broker through a driver (NATS JetStream, Kafka), with the destination of each record computed by CEL |
| `debug` | `debug` | Writes events to the log (stderr) for debugging and development |
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
- Non-2xx answers fail the delivery. Client errors that refuse the data itself (4xx except 408 and 429) reject the event, so the OTLP source answers with 422 and the Broker Source rejects the message. Other errors, including 408, 429 and 5xx, can be retried by the source: the OTLP source answers with 503, the Broker Source has the message redelivered
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

- The expressions have the same variables and `get_attr` function as the destination expressions of the [Broker Sink](#broker-sink), e.g. `record`, `resource` and `datapoint`. Metrics are split per data point the same way
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

### Broker Sink

Sends OTLP data to a message broker. The broker is chosen by `driver`, and the driver's options are under a key of the same name, e.g. `nats:`. A CEL expression per signal computes the destination of every record; the records of one signal and destination become one message whose body is the serialized OTLP data, without headers, so any consumer can read it. The broker is set up outside Tram: the sink creates no streams, topics or queues.

**Type:** `broker`

**Configuration:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `driver` | string | Yes | - | The broker: `nats` (NATS JetStream) or `kafka` (Apache Kafka and compatible brokers) |
| `destination` | object | Yes | - | CEL expressions per signal (`logs`, `metrics`, `traces`, at least one) that return the destination of each record, e.g. the NATS subject. Signals without an expression are not sent |
| `max_pending` | int | No | `256` | How many messages the sink sends before it waits for their acknowledgements. The broker has to store them as they arrive |
| `partition` | object | No | - | Spreads records over partitions, see [Partitioning](#broker-partitioning) |
| `nats` | object | With `driver: nats` | - | Options of the NATS driver: `url` (required) |
| `kafka` | object | With `driver: kafka` | - | Options of the Kafka driver: `brokers` (required), `tls` and `sasl` as for the [Broker Source](#broker-source), and `max_message_bytes` (default 1 MiB), the largest message sent; keep it at or below the broker's `message.max.bytes` |

**CEL Expression Context:**

| Expression | Variables |
|------------|-----------|
| `logs` | `logs` (payload), `resource` (Resource), `scope` (InstrumentationScope), `record` (LogRecord) |
| `metrics` | `metrics` (payload), `resource`, `scope`, `metric` (Metric), `datapoint` (the data point; its type depends on the metric type) |
| `traces` | `traces` (payload), `resource`, `scope`, `span` (Span) |

With partitioning, the expressions also have `partition` (int), the partition of the record, data point or span.

- Each expression is evaluated once per log record, metric data point and span and must return a string. The payload variable holds only the current log record, metric or span with its resource and scope, so `[0]` indexes address it, e.g. `logs.resource_logs[0].scope_logs[0].log_records[0]`. For metrics it holds the whole metric with all its data points
- `resource` and `scope` are empty messages when the payload has none. Field names follow the OTLP protobuf definitions, e.g. `record.severity_text`, `datapoint.attributes`
- Metrics are routed per data point: a metric whose data points resolve to different destinations is split into one metric per destination, each with the metric's name, description, unit, metadata and type-specific fields. A metric whose data points all resolve to the same destination is sent as it is
- A metric without data points is evaluated once without `datapoint`; an expression that reads it fails for that metric and rejects the event
- The custom function `get_attr(key)` on an attribute list returns the string value of an attribute, or `""` if it is missing, e.g. `resource.attributes.get_attr('service.name')`

**Example:**

```json
{
  "name": "to-routers",
  "type": "broker",
  "from": "map-router",
  "spec": {
    "driver": "nats",
    "nats": { "url": "nats://localhost:4222" },
    "destination": {
      "logs": "'router.audit.' + record.attributes.get_attr('example.router.id') + '.logs'"
    },
    "max_pending": 256
  }
}
```

**Behavior Notes:**
- **One signal per destination.** Messages name no signal, so a consumer can only decode a destination that holds one signal. Give each signal its own destinations, e.g. by a signal token in the subject
- Records keep their resource and scope. If all records of a signal resolve to the same destination, the payload is sent unchanged. Resources that appear in several entries of an event, e.g. after a `batch` stage, stay separate entries in the message, which OTLP allows
- An event is rejected if a destination expression fails for one of its records, does not return a string, or returns a destination the driver refuses, e.g. a NATS subject with an empty token, whitespace or a wildcard. `get_attr` returns `""` for a missing attribute, so `'logs.' + ...get_attr('team')` gives the invalid `logs.` for records without it. The OTLP source answers with HTTP 422, the Broker Source rejects the message instead of redelivering it
- A message larger than the broker accepts (NATS: the server's `max_payload`) rejects the event instead of failing on every retry. Messages of the event sent before it stay sent. Keep batches small enough that the largest message per destination stays below the limit
- The sink encodes and sends the messages of an event one after another without waiting in between, and reports success once the broker has acknowledged every message. If one cannot be sent or is not acknowledged within 10 seconds, the event fails and the source retries it, so messages already stored are stored again
- **NATS:** JetStream stores each message in the stream whose subjects cover its subject; a subject no stream covers fails the publish (`503` at the OTLP source). Set the streams up before the sink sends, e.g. with NACK or the `nats` CLI
- **NATS:** each sink has its own connection. A lost connection is retried every 2 seconds, with up to 1 second of jitter, for as long as the sink runs. While disconnected, publishing fails at once instead of being buffered, so the OTLP source answers with `503` and clients retry until NATS is back
- **Kafka:** destinations are topics, which may contain letters, digits, `.`, `_` and `-`, at most 249 characters. A topic that does not exist fails the send after a few metadata retries (`503` at the OTLP source); Tram creates no topics. Messages have no key, so Kafka spreads them over the topic's partitions; with partitioning, put the partition into the topic name
- **Kafka:** `max_pending` bounds the records buffered and in flight; a record not acknowledged within 10 seconds, retries included, fails the event

#### Broker Partitioning

With `partition`, the sink computes a partition per record from a key, e.g. the tenant, and gives it to the destination expressions as `partition`. Put it into the destination, e.g. `'audit.' + string(partition) + '.' + ...`, and set up one stream per partition, so each stream holds a part of the data and the streams can be led by different servers.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `count` | int | Yes | - | Number of partitions, from 1 to 1024 |
| `logs` | string | With `destination.logs` | - | CEL expression returning the key of a log record. Has the same variables as the destination expression, without `partition` |
| `metrics` | string | With `destination.metrics` | - | CEL expression returning the key of a metric data point |
| `traces` | string | With `destination.traces` | - | CEL expression returning the key of a span |

- The partition of a key is its 32-bit FNV-1a hash modulo `count`, the same as the former NATS sink's. It never changes, so consumers can compute which partition holds a key; the [Broker Source](#broker-source) does so with its `partition` field
- An expression for the key that fails or does not return a string rejects the event. A missing attribute gives the key `""`, which is a valid key like any other
- Changing `count` moves keys to other partitions. Data already stored stays where it is, so changing it is a migration

---


## 📝 Notes

- **Component Type vs Name:** The `type` field in configuration refers to the component implementation type (e.g., `generator`, `cel_validator`), while the `name` field is a user-defined identifier for the component instance within a stream.
- **Connections:** The `from` field in stages and sinks specifies which component they receive data from.
- **Tree Structure:** Pipelines form a tree with the source as root, stages as intermediate nodes, and sinks as leaves.
- **Delivery Guarantee:** Delivery is at-least-once, and Tram does not deduplicate. A sink can receive the same data again, e.g. when a client retries after a `503` although some sinks had already accepted the data, when a broker message is redelivered after one branch of its stream failed, or when a publish succeeded but its acknowledgement was lost. Deduplicate at the destination where duplicates matter.

### Hold Times

A source like OTLP waits for a delivery only for a limited time, its `delivery_timeout`, and then answers with `503`. `batch` stages can hold an event back for up to their `timeout`, `delay` stages for their `delay`. When a stream is created or updated, Tram adds up these hold times on the way from the source to each sink and rejects the stream if the sum reaches the source's limit: every event that does not fill its batch would be cancelled, so the stream would fail as soon as traffic gets low. The error names the stages:

```
stream spec is invalid: on the way to sink openobserve-traces, batch-traces (10s) can hold an event for 10s, but source otlp cancels deliveries after 8s
```

An update that is rejected leaves the running stream unchanged. Shorten the `timeout` or `delay`, or raise the source's `delivery_timeout`, and keep that below the time the clients wait. Sources without such a limit, like the Broker Source, are not checked.
