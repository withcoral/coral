# syntax=docker/dockerfile:1.7
# Coral runtime image: the `coral` executable behind docker/entrypoint.sh.
# It is the released binary, built from ee/crates/coral-ee (ADR 0033).
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
# The search model, in the read-only folder where Coral looks first. The
# values repeat coral_embed::MANIFEST. A test keeps them equal:
# crates/coral-embed/tests/server_image.rs. A file with another digest stops
# the build.
#
# Both downloads stay in a cache mount. A builder that keeps its mounts, as
# CI's does, rebuilds these layers without GitHub's release downloads, which
# fail at times. A kept file passes the same digest check as a new one.
RUN --mount=type=cache,id=coral-downloads,target=/var/cache/coral-downloads,sharing=locked \
    set -eu; \
    url=https://github.com/withcoral/coral-releases/releases/download/model-bge-small-en-v1.5-5c38ec7; \
    dir=/opt/coral/models/bge-small-en-v1.5; \
    kept="/var/cache/coral-downloads/${url##*/}"; \
    check() { \
        printf '%s\n' \
            '828e1496d7fabb79cfa4dcd84fa38625c0d3d21da474a00f08db0f559940cf35  model.onnx' \
            'd241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66  tokenizer.json' \
            | sha256sum -c -; \
    }; \
    mkdir -p "$dir" "$kept"; \
    cd "$kept"; \
    check >/dev/null 2>&1 || for file in model.onnx tokenizer.json; do \
        curl -fsSL --retry 3 -o "$file" "$url/$file"; \
    done; \
    check; \
    install -m 0644 model.onnx tokenizer.json "$dir/"
RUN --mount=type=cache,id=coral-downloads,target=/var/cache/coral-downloads,sharing=locked \
    set -eu; \
    case "$TARGETARCH" in \
    amd64) probe_sha=e25b2f7e50176a909b9036e2c7598d8997b524a87d24af81d92b796e6dfb4897 ;; \
    arm64) probe_sha=bc77dba74822d2b00c2585d225b77048cdb044407a0541ebf4c12711c2fdf779 ;; \
    *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    kept="/var/cache/coral-downloads/grpc_health_probe-${GRPC_HEALTH_PROBE_VERSION}-linux-${TARGETARCH}"; \
    echo "${probe_sha}  ${kept}" | sha256sum -c - >/dev/null 2>&1 \
        || curl -fsSL -o "$kept" "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${TARGETARCH}"; \
    echo "${probe_sha}  ${kept}" | sha256sum -c -; \
    install -m 0755 "$kept" /grpc_health_probe; \
    mkdir -p /state/coral; \
    chmod 0775 /state/coral

FROM rust:1.98-trixie AS source
ARG TARGETARCH
ARG CORAL_GIT_SHA
# The release workflow passes 1: only a release build sends usage data.
ARG CORAL_RELEASE_BUILD=
WORKDIR /src
COPY . .
RUN --mount=type=cache,id=coral-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=coral-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=coral-cargo-target-${TARGETARCH},target=/src/target,sharing=locked \
    test -n "$CORAL_GIT_SHA" \
    && cargo build --locked --release -p coral-ee --bin coral-ee \
    && install -D -m 0755 target/release/coral-ee /coral

FROM scratch AS prebuilt
ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/coral /coral

FROM ${CORAL_BINARY} AS binary

FROM debian:trixie-slim
COPY --from=prep /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=prep /grpc_health_probe /usr/local/bin/grpc_health_probe
COPY --from=binary /coral /usr/local/bin/coral
COPY --from=prep --chown=0:0 /opt/coral/ /opt/coral/
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
