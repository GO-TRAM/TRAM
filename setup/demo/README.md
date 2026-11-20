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
| `checkout` | telemetrygen | Sends INFO logs of the `checkout` service, one log per request, 200 workers at 10 requests/s each: about 2,000 requests/s |
| `debug-noise` | telemetrygen | Sends DEBUG logs of the `debug-noise` service, 100 workers at 50 logs/s each, batched into large requests like an OpenTelemetry SDK |
| `checkout-metrics` | telemetrygen | Sends the cumulative counter `checkout.orders`, 10 workers at 10 data points/s each |
| `debug-noise-metrics` | telemetrygen | Sends the gauge `debug.cache.size`, 10 workers at 100 data points/s each, batched |
| `checkout-traces` | telemetrygen | Sends traces of the `checkout` service, 20 workers at 5 traces/s each, every trace with a 100ms root span `lets-go` and two 50ms child spans |
| `debug-noise-traces` | telemetrygen | Sends traces of the `debug-noise` service, 20 workers at 50 traces/s each, batched |
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

- **`logs`** is a `filter` stage with the CEL driver that keeps log records with `severity_number >= 9` (INFO and above), so the DEBUG logs of `debug-noise` are dropped. Its `metrics` and `traces` CEL expressions are `"false"`, so only logs go down this branch.
- **`metrics`** is a `filter` stage with the CEL driver that keeps metrics whose name does not start with `debug.`, so `debug.cache.size` is dropped. Its `logs` and `traces` CEL expressions are `"false"`, so only metrics go down this branch.
- **`traces`** is a `filter` stage with the CEL driver that drops spans whose resource has the `service.name` `debug-noise`. Its `logs` and `metrics` CEL expressions are `"false"`, so only traces go down this branch.
- **`batch-logs`**, **`batch-metrics`** and **`batch-traces`** merge concurrent requests, flushing at 100, 10 and 5 records or after 5s. A batch stage answers a request only after its batch has been delivered, so the clients wait for OpenObserve to accept their data and slow down when it cannot keep up. A client that sends one request at a time gets one request through per flush, so `max_records` has to fit the number of requests that arrive at the same time: with 200 `checkout` workers, the batches of `batch-logs` fill up long before the timeout. The 5s stay below the OTLP source's `delivery_timeout` of 8s, which Tram requires, see [Hold Times](../../COMPONENTS.md#hold-times).
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
- **Tram internals**: Tram's own metrics, see [Metrics](../../README.md#metrics). Records the sources accepted per signal, the records each node received, the share each stage drops per signal, how many records a sink sends per request, the p95 duration per node, errors and rejections, and Tram's goroutines, heap and CPU. The p95 duration shows the backpressure: the batch stages wait for their flush, at most 5s, while the sinks answer within milliseconds.
- **Traces through Tram**: spans by service and the p95 span duration by operation, both showing only `checkout`, a table of the latest traces, and Grafana's trace view for the trace selected at the top. It defaults to the latest trace; click a trace ID in the table to show that one.

The OpenObserve plugin is unsigned and not in the Grafana plugin catalog. OpenObserve only publishes its latest build, so its version in the Grafana image is not pinned. Infinity is pinned to 3.7.1, the last version that supports Grafana 11.5.

## Resources

The services have memory limits in `compose.yaml`, measured with Tram limited to one CPU and up to 16 `checkout` instances in a VM with 5 CPUs and 6 GB of memory:

| Requests/s (one log each) | Records/s accepted | Tram CPU | Tram memory (RSS) | p95 at the source | OpenObserve memory |
|---|---|---|---|---|---|
| 2,200 | 9,100 | 0.27 | 63 MB | 90 ms | 0.55 GB |
| 8,200 | 15,100 | 0.32 | 82 MB | 31 ms | 0.70 GB |
| 16,200 | 23,100 | 0.57 | 110 MB | 17 ms | 0.99 GB |
| 24,200 | 31,100 | 0.87 | 148 MB | 25 ms | 1.01 GB |
| 24,400 | 31,100 | 1.00 | 225 MB | 153 ms | 1.01 GB |

On one CPU, Tram handles about 24,000 requests/s. Beyond that it slows the clients down instead of failing or buffering: the latency grows, and no request fails. Tram's CPU cost is per request rather than per record, so batched clients get far more records through. The limits leave room above these values: 512 MB for Tram, 2 GB for OpenObserve, whose memtables and memory cache are capped at 512 MB each, 512 MB for Grafana and 256 MB for Prometheus.

The limits add up to about 3.3 GB, plus about 30 MB per generator, so give the Docker or Podman VM at least 4 GB; Podman's default of 2 GB is not enough under high load. If OpenObserve is killed for lack of memory anyway, it restarts on its own, and Tram answers with `503` meanwhile, which the clients retry; the data OpenObserve held is lost.

To test the load yourself, start more `checkout` instances, e.g. `docker compose -f setup/demo/compose.yaml up -d --scale checkout=8`, and watch the **Tram internals** dashboard.

## Credentials

The OpenObserve root user is set in `compose.yaml`. Tram's sinks and Grafana's datasources use the same credentials, the sinks as a Base64-encoded `Authorization` header. If you change them, update [`config/otlp_to_openobserve.json`](config/otlp_to_openobserve.json), [`prometheus/prometheus.yml`](prometheus/prometheus.yml) and [`grafana/provisioning/datasources/openobserve.yaml`](grafana/provisioning/datasources/openobserve.yaml) too. The credentials and Grafana's anonymous admin access are meant for a local demo only.
