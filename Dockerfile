FROM       gcr.io/distroless/static-debian12

# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH

COPY       exporter_${TARGETOS}_${TARGETARCH} /bin/exporter
EXPOSE     8080
ENTRYPOINT [ "/bin/exporter" ]

ARG REVISION
LABEL org.opencontainers.image.title="prometheus-cosmosdb-diagnostic-exporter" \
      org.opencontainers.image.source="https://github.com/grafana/prometheus-cosmosdb-diagnostic-exporter/tree/main/cmd" \
      org.opencontainers.image.revision="${REVISION}"
