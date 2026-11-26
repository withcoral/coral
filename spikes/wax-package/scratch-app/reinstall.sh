#!/bin/sh
# SPIKE (withcoral/coral#3541). Rebuilds the tarball and installs it here afresh.
# npm pins a tarball by its digest, so a rebuilt one needs a new lockfile.
set -eu
cd "$(dirname "$0")"
node ../build.ts "$@"
cp ../out/withcoral-wax-0.0.0-spike.*.tgz ../out/withcoral-wax.tgz
rm -rf node_modules/@withcoral node_modules/.vite package-lock.json
npm install --no-audit --no-fund
