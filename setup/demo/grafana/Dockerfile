# Grafana with the OpenObserve datasource plugin, which is not in the Grafana
# plugin catalog. OpenObserve only publishes the latest build of the plugin,
# so its version is not pinned.
FROM docker.io/grafana/grafana:11.5.2

ARG OPENOBSERVE_PLUGIN_URL=https://zincsearch-releases.s3.us-west-2.amazonaws.com/zo_gp/zo_gp.tar.gz

RUN mkdir -p /var/lib/grafana/plugins \
    && wget -qO- "${OPENOBSERVE_PLUGIN_URL}" | tar -xz -C /var/lib/grafana/plugins
