# syntax=docker/dockerfile:1.7
FROM docker.io/library/node:24-trixie-slim AS node
FROM docker.io/library/rust:1.98-trixie AS reel
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
 && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
 && apt-get update && apt-get install -y --no-install-recommends \
    clang cmake ninja-build pkg-config libssl-dev protobuf-compiler ffmpeg \
    openssl libnss3-tools postgresql tini xvfb xauth libgtk-3-0t64 \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace/apps/reef
COPY apps/reef/package*.json ./
RUN --mount=type=cache,target=/root/.npm npm ci
RUN npx playwright install --with-deps chromium
# The desktop suite packages Electron. The install keeps Electron's download in
# its cache, where packaging finds it. npm may skip the install script.
WORKDIR /workspace/apps/desktop
COPY apps/desktop/package*.json ./
RUN --mount=type=cache,target=/root/.npm npm ci && node node_modules/electron/install.js
WORKDIR /workspace
COPY . .
ARG TARGETARCH
ARG CORAL_GIT_SHA
ENV CORAL_GIT_SHA=$CORAL_GIT_SHA
RUN --mount=type=cache,id=reel-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=reel-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=reel-target-${TARGETARCH},target=/workspace/target,sharing=locked \
    cargo build --locked -p coral \
 && install -D target/debug/coral /opt/reel/coral \
 && cargo test --locked -p coral-outbound --features transport --test container_network --no-run --message-format=json > /tmp/network-build.json \
 && node -e "const fs = require('node:fs'); const built = fs.readFileSync('/tmp/network-build.json', 'utf8').trim().split('\n').map(JSON.parse).find(x => x.reason === 'compiler-artifact' && x.target.name === 'container_network' && x.executable); if (!built) throw Error('Missing network acceptance binary'); fs.copyFileSync(built.executable, '/opt/reel/network-tests'); fs.chmodSync('/opt/reel/network-tests', 0o755)" \
 && rm /tmp/network-build.json
WORKDIR /workspace/apps/reef
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "bash", "e2e/container/entry.sh"]
