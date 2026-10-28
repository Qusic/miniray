FROM gcr.io/distroless/static
LABEL org.opencontainers.image.source=https://github.com/Qusic/miniray
ARG TARGETOS TARGETARCH
COPY tmp/miniray-${TARGETOS}-${TARGETARCH} /miniray
ENTRYPOINT ["/miniray"]
