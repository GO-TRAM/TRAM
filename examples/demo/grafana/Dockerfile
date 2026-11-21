# Grafana with two datasource plugins for OpenObserve:
#
# - The OpenObserve plugin, for logs. It is not in the Grafana plugin catalog,
#   and OpenObserve only publishes its latest build, so its version is not
#   pinned.
# - Infinity, for traces, which the OpenObserve plugin cannot query. 3.7.1 is
#   the last version that supports this Grafana version.
FROM docker.io/grafana/grafana:13.3.1

ARG OPENOBSERVE_PLUGIN_URL=https://zincsearch-releases.s3.us-west-2.amazonaws.com/zo_gp/zo_gp.tar.gz
ARG INFINITY_PLUGIN_VERSION=3.7.1

RUN mkdir -p /var/lib/grafana/plugins \
    && wget -qO- "${OPENOBSERVE_PLUGIN_URL}" | tar -xz -C /var/lib/grafana/plugins \
    && grafana cli --pluginsDir /var/lib/grafana/plugins \
        plugins install yesoreyeram-infinity-datasource "${INFINITY_PLUGIN_VERSION}"
