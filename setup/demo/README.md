# Tram Demo: Logs, Metrics and Traces into OpenObserve

This demo runs Tram between OTLP clients and [OpenObserve](https://openobserve.ai), with Grafana on top. Synthetic clients send logs, metrics and traces to Tram; Tram filters each signal, batches them and forwards them to OpenObserve, where Grafana dashboards show what arrived. Tram's own metrics go to OpenObserve too, so a fourth dashboard shows how Tram handles the data.

## Quick Start

From the repository root:

```sh
task demo:up      # or: docker compose -f setup/demo/compose.yaml up -d --build
```

Then open:

| URL | What |
|-----|------|
| http://localhost:3000 | Grafana, without login. Opens the **Logs through Tram** dashboard; **Metrics through Tram**, **Traces through Tram** and **Tram internals** are linked at the top |
| http://localhost:5080 | OpenObserve UI, login `root@example.com` / `Complexpass#123` |
| http://localhost:8080 | Tram HTTP API, e.g. `curl localhost:8080/v1/streams` |

Data shows up in the dashboards a few seconds after start. Stop the demo with:

```sh
task demo:down    # or: docker compose -f setup/demo/compose.yaml down
```

Task uses Podman if it is installed and Docker otherwise; set `CONTAINER_TOOL=docker` to choose.

## Services

| Service | Image | Role |
|---------|-------|------|
| `tram` | built from the repository | Runs the stream from [`config/otlp_to_openobserve.json`](config/otlp_to_openobserve.json) through the file sync |
| `checkout` | telemetrygen | Sends INFO logs of the `checkout` service, 5 workers at 1 log/s each |
| `debug-noise` | telemetrygen | Sends DEBUG logs of the `debug-noise` service, 50 workers at 5 logs/s each |
| `checkout-metrics` | telemetrygen | Sends the cumulative counter `checkout.orders`, 1 data point/s |
| `debug-noise-metrics` | telemetrygen | Sends the gauge `debug.cache.size`, 5 data points/s |
| `checkout-traces` | telemetrygen | Sends traces of the `checkout` service, 1 trace/s with a 100ms root span `lets-go` and two 50ms child spans |
| `debug-noise-traces` | telemetrygen | Sends traces of the `debug-noise` service, 5 traces/s |
| `openobserve` | OpenObserve | Stores logs, metrics and traces. Data is kept in the container only and is lost when it is removed |
| `prometheus` | Prometheus in agent mode | Scrapes Tram's own metrics from `http://tram:8080/metrics` every 5s and writes them to OpenObserve with remote write, see [`prometheus/prometheus.yml`](prometheus/prometheus.yml). In agent mode, it does not store or query them itself |
| `grafana` | Grafana with the OpenObserve and Infinity plugins, see [`grafana/Dockerfile`](grafana/Dockerfile) | Dashboards from [`grafana/provisioning`](grafana/provisioning) |

All clients send OTLP/HTTP to Tram's source on port 4318, which is also published on localhost, so you can send your own data.

## The Stream

```
                 ┌─ logs ──── batch-logs ──── openobserve-logs     → stream tram_logs
otlp (:4318) ────┼─ metrics ─ batch-metrics ─ openobserve-metrics  → one stream per metric
                 └─ traces ── batch-traces ── openobserve-traces   → stream tram_traces
```

- **`logs`** is a `cel_filter` that keeps log records with `severity_number >= 9` (INFO and above), so the DEBUG logs of `debug-noise` are dropped. Its `metrics` and `traces` expressions are `"false"`, so only logs go down this branch.
- **`metrics`** is a `cel_filter` that keeps metrics whose name does not start with `debug.`, so `debug.cache.size` is dropped. Its `logs` and `traces` expressions are `"false"`, so only metrics go down this branch.
- **`traces`** is a `cel_filter` that drops spans whose resource has the `service.name` `debug-noise`. Its `logs` and `metrics` expressions are `"false"`, so only traces go down this branch.
- **`batch-logs`**, **`batch-metrics`** and **`batch-traces`** merge concurrent requests, with a timeout of 5s, 1s and 1s. A batch stage answers a request only after its batch has been delivered, so the clients wait for OpenObserve to accept their data and slow down when it cannot keep up. A client that sends one request at a time therefore gets one request through per timeout: the 5 `checkout` workers deliver about 1 log/s in total.
- **`openobserve-logs`**, **`openobserve-metrics`** and **`openobserve-traces`** are `otlp` sinks that send to OpenObserve's OTLP endpoint `http://openobserve:5080/api/default` with Basic auth. The `stream-name` header puts logs into the OpenObserve stream `tram_logs` and traces into `tram_traces`.

The file sync reads the configuration at startup. After editing the stream, restart Tram with `docker compose -f setup/demo/compose.yaml restart tram`.

## Grafana

Three datasources read from OpenObserve:

- **OpenObserve** uses the [OpenObserve plugin](https://github.com/openobserve/openobserve-grafana-plugin) and queries logs with SQL. The log charts use it. In dashboards, the plugin only returns aggregations, not single log lines, and it cannot query traces.
- **OpenObserve Metrics** is a Prometheus datasource on OpenObserve's PromQL API, `http://openobserve:5080/api/default/prometheus`. OpenObserve stores metric names with underscores, so `checkout.orders` is queried as `checkout_orders`.
- **OpenObserve SQL** uses the [Infinity plugin](https://grafana.com/grafana/plugins/yesoreyeram-infinity-datasource/) to send SQL queries to OpenObserve's search API, `POST /api/default/_search?type=logs` or `?type=traces`, for what the OpenObserve plugin cannot do: log lines and traces. The request body passes Grafana's time range as `${__from}000` and `${__to}000`, since OpenObserve expects microseconds.

The dashboards:

- **Logs through Tram**: log records by service and by severity, and the latest 200 log lines. Only `checkout` and `INFO` show up.
- **Metrics through Tram**: the rate and value of `checkout.orders`, and a panel for `debug.cache.size` that stays empty because Tram drops it.
- **Tram internals**: Tram's own metrics, see [Metrics](../../README.md#metrics). Records the sources accepted per signal, the records each node received, the share each stage drops per signal, how many records a sink sends per request, the p95 duration per node, errors and rejections, and Tram's goroutines, heap and CPU. The p95 duration shows the backpressure: the batch stages wait for their flush, 5s for logs and 1s for metrics and traces, while the sinks answer within milliseconds.
- **Traces through Tram**: spans by service and the p95 span duration by operation, both showing only `checkout`, a table of the latest traces, and Grafana's trace view for the trace selected at the top. It defaults to the latest trace; click a trace ID in the table to show that one.

The OpenObserve plugin is unsigned and not in the Grafana plugin catalog. OpenObserve only publishes its latest build, so its version in the Grafana image is not pinned. Infinity is pinned to 3.7.1, the last version that supports Grafana 11.5.

## Credentials

The OpenObserve root user is set in `compose.yaml`. Tram's sinks and Grafana's datasources use the same credentials, the sinks as a Base64-encoded `Authorization` header. If you change them, update [`config/otlp_to_openobserve.json`](config/otlp_to_openobserve.json), [`prometheus/prometheus.yml`](prometheus/prometheus.yml) and [`grafana/provisioning/datasources/openobserve.yaml`](grafana/provisioning/datasources/openobserve.yaml) too. The credentials and Grafana's anonymous admin access are meant for a local demo only.
