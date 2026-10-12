# syntax=docker/dockerfile:1.7
# Coral runtime image: the `coral` executable behind docker/entrypoint.sh.
#
# The default build compiles this checkout. A release build repackages a
# published binary instead: extract it to dist/${TARGETARCH}/coral and pass
# --build-arg CORAL_BINARY=prebuilt. The final stage is COPY-only, so a
# prebuilt build never executes target-architecture code.
ARG CORAL_BINARY=source

FROM --platform=$BUILDPLATFORM debian:trixie-slim AS prep
ARG TARGETARCH
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.54
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN set -eu; \
    case "$TARGETARCH" in \
    amd64) probe_sha=e25b2f7e50176a909b9036e2c7598d8997b524a87d24af81d92b796e6dfb4897 ;; \
    arm64) probe_sha=bc77dba74822d2b00c2585d225b77048cdb044407a0541ebf4c12711c2fdf779 ;; \
    *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /grpc_health_probe "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${TARGETARCH}"; \
    echo "${probe_sha}  /grpc_health_probe" | sha256sum -c -; \
    chmod 0755 /grpc_health_probe; \
    mkdir -p /state/coral; \
    chmod 0775 /state/coral

FROM rust:1.98-trixie AS source
ARG TARGETARCH
ARG CORAL_GIT_SHA
WORKDIR /src
COPY . .
RUN --mount=type=cache,id=coral-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=coral-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=coral-cargo-target-${TARGETARCH},target=/src/target,sharing=locked \
    test -n "$CORAL_GIT_SHA" \
    && cargo build --locked --release -p coral --bin coral \
    && install -D -m 0755 target/release/coral /coral

FROM scratch AS prebuilt
ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/coral /coral

FROM ${CORAL_BINARY} AS binary

FROM debian:trixie-slim
COPY --from=prep /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=prep /grpc_health_probe /usr/local/bin/grpc_health_probe
COPY --from=binary /coral /usr/local/bin/coral
COPY --chown=0:0 --chmod=0755 docker/entrypoint.sh /usr/local/bin/entrypoint.sh
# g=u (0775) with GID 0 so any uid running with --user <uid>:0 can write the volume.
# Copying the parent keeps the directory's own mode under Docker and Podman;
# COPY --chmod on an empty directory does not apply to it under Podman.
COPY --from=prep --chown=0:0 /state/ /var/lib/
ENV CORAL_CONFIG_DIR=/var/lib/coral/config \
    HOME=/var/lib/coral
USER 65532:0
# Healthy means ready: `coral.readiness` answers from the database, while the
# empty service name only reports that the process is alive. The MCP and auth
# listeners serve their own /healthz and /readyz.
HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=6 \
    CMD /usr/local/bin/grpc_health_probe -service coral.readiness -addr="${CORAL_HEALTHCHECK_ADDR:-127.0.0.1:14555}"
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
