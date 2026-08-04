# Coral server image. Built by repackaging a published release tarball, so the
# runtime stage is COPY-only: every RUN lives in the $BUILDPLATFORM prep stage
# and no target-architecture code ever executes during the build (zero QEMU).
# Build context is the repo root, with the release binary extracted to
# dist/${TARGETARCH}/coral beforehand.
FROM --platform=$BUILDPLATFORM debian:trixie-slim AS prep
ARG TARGETARCH
ARG TINI_VERSION=v0.19.0
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.54
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN set -eu; \
    case "$TARGETARCH" in \
    amd64) tini_sha=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c; \
           probe_sha=e25b2f7e50176a909b9036e2c7598d8997b524a87d24af81d92b796e6dfb4897 ;; \
    arm64) tini_sha=07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81; \
           probe_sha=bc77dba74822d2b00c2585d225b77048cdb044407a0541ebf4c12711c2fdf779 ;; \
    *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tini "https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TARGETARCH}"; \
    echo "${tini_sha}  /tini" | sha256sum -c -; \
    curl -fsSL -o /grpc_health_probe "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${TARGETARCH}"; \
    echo "${probe_sha}  /grpc_health_probe" | sha256sum -c -; \
    chmod 0755 /tini /grpc_health_probe
COPY dist/${TARGETARCH}/coral /coral
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod 0755 /coral /entrypoint.sh && mkdir -p /out/var/lib/coral

FROM debian:trixie-slim
COPY --from=prep /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=prep /tini /tini
COPY --from=prep /grpc_health_probe /usr/local/bin/grpc_health_probe
COPY --from=prep /coral /usr/local/bin/coral
COPY --from=prep --chown=0:0 --chmod=0755 /entrypoint.sh /usr/local/bin/entrypoint.sh
# g=u (0775) with GID 0 so any uid running with --user <uid>:0 can write the volume.
COPY --from=prep --chown=0:0 --chmod=0775 /out/var/lib/coral /var/lib/coral
ENV CORAL_CONFIG_DIR=/var/lib/coral/config \
    HOME=/var/lib/coral
USER 65532:0
EXPOSE 14555
HEALTHCHECK CMD ["/usr/local/bin/grpc_health_probe", "-addr=127.0.0.1:14555"]
ENTRYPOINT ["/tini", "--", "/usr/local/bin/entrypoint.sh"]
