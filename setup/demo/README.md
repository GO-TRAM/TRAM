# Tram Demo: Logs and Metrics into OpenObserve

This demo runs Tram between OTLP clients and [OpenObserve](https://openobserve.ai), with Grafana on top. Synthetic clients send logs and metrics to Tram; Tram filters both, batches them and forwards them to OpenObserve, where Grafana dashboards show what arrived.

## Quick Start

From the repository root:

```sh
task demo:up      # or: docker compose -f setup/demo/compose.yaml up -d --build
```

Then open:

| URL | What |
|-----|------|
| http://localhost:3000 | Grafana, without login. Opens the **Logs through Tram** dashboard; **Metrics through Tram** is linked at the top |
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
| `openobserve` | OpenObserve | Stores logs and metrics. Data is kept in the container only and is lost when it is removed |
| `grafana` | Grafana with the OpenObserve plugin, see [`grafana/Dockerfile`](grafana/Dockerfile) | Dashboards from [`grafana/provisioning`](grafana/provisioning) |

All clients send OTLP/HTTP to Tram's source on port 4318, which is also published on localhost, so you can send your own data.

## The Stream

```
                 ┌─ logs ──── batch-logs ──── openobserve-logs     → stream tram_logs
otlp (:4318) ────┤
                 └─ metrics ─ batch-metrics ─ openobserve-metrics  → one stream per metric
```

- **`logs`** is a `cel_filter` that keeps log records with `severity_number >= 9` (INFO and above), so the DEBUG logs of `debug-noise` are dropped. Its `metrics` and `traces` expressions are `"false"`, so only logs go down this branch.
- **`metrics`** is a `cel_filter` that keeps metrics whose name does not start with `debug.`, so `debug.cache.size` is dropped. Its `logs` and `traces` expressions are `"false"`, so only metrics go down this branch.
- **`batch-logs`** and **`batch-metrics`** merge concurrent requests, with a timeout of 5s and 1s. A batch stage answers a request only after its batch has been delivered, so the clients wait for OpenObserve to accept their data and slow down when it cannot keep up. A client that sends one request at a time therefore gets one request through per timeout: the 5 `checkout` workers deliver about 1 log/s in total.
- **`openobserve-logs`** and **`openobserve-metrics`** are `otlp` sinks that send to OpenObserve's OTLP endpoint `http://openobserve:5080/api/default` with Basic auth. The `stream-name` header puts the logs into the OpenObserve stream `tram_logs`.

The file sync reads the configuration at startup. After editing the stream, restart Tram with `docker compose -f setup/demo/compose.yaml restart tram`.

## Grafana

Two datasources read from OpenObserve:

- **OpenObserve** uses the [OpenObserve plugin](https://github.com/openobserve/openobserve-grafana-plugin) and queries logs with SQL. In dashboards, the plugin only returns aggregations, not single log lines; to read the log lines, use Grafana's Explore view with this datasource or the OpenObserve UI.
- **OpenObserve Metrics** is a Prometheus datasource on OpenObserve's PromQL API, `http://openobserve:5080/api/default/prometheus`. OpenObserve stores metric names with underscores, so `checkout.orders` is queried as `checkout_orders`.

The dashboards:

- **Logs through Tram**: log records by service and by severity. Only `checkout` and `INFO` show up.
- **Metrics through Tram**: the rate and value of `checkout.orders`, and a panel for `debug.cache.size` that stays empty because Tram drops it.

The OpenObserve plugin is unsigned and not in the Grafana plugin catalog. OpenObserve only publishes its latest build, so the plugin version in the Grafana image is not pinned.

## Credentials

The OpenObserve root user is set in `compose.yaml`. Tram's sinks and Grafana's datasources use the same credentials, the sinks as a Base64-encoded `Authorization` header. If you change them, update [`config/otlp_to_openobserve.json`](config/otlp_to_openobserve.json) and [`grafana/provisioning/datasources/openobserve.yaml`](grafana/provisioning/datasources/openobserve.yaml) too. The credentials and Grafana's anonymous admin access are meant for a local demo only.
