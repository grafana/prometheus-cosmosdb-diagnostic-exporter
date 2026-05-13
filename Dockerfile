FROM       gcr.io/distroless/static-debian12@sha256:20bc6c0bc4d625a22a8fde3e55f6515709b32055ef8fb9cfbddaa06d1760f838

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
