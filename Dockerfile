FROM       gcr.io/distroless/static-debian12@sha256:a9fcaedd4c9b59e12dd65d954f0b5044f19b0647a8a3712e77205df9e7b102cd

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
