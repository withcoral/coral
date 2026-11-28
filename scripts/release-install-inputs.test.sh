#!/usr/bin/env bash
# Tries each case scripts/release-install-inputs.sh must handle, against a
# made-up release artifact. A stand-in `docker` on PATH logs its arguments and
# fails a pull of the image named in DOCKER_FAIL, so the test needs no registry.
#
#   scripts/release-install-inputs.test.sh
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir "$work/bin"
cat >"$work/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >>"$DOCKER_LOG"
if [[ "$1" == pull && -n "${DOCKER_FAIL:-}" && "$*" == *"$DOCKER_FAIL"* ]]; then
  echo "docker stand-in: cannot pull $DOCKER_FAIL" >&2
  exit 1
fi
# A real pull prints the digest on stdout, which must not reach $GITHUB_ENV.
if [[ "$1" == pull ]]; then echo "sha256:pulled"; fi
EOF
chmod +x "$work/bin/docker"
export PATH="$work/bin:$PATH"
export DOCKER_LOG="$work/docker.log"

coral_digest="sha256:$(printf 'a%.0s' {1..64})"
web_digest="sha256:$(printf 'b%.0s' {1..64})"
envoy="docker.io/envoyproxy/envoy@sha256:$(printf 'c%.0s' {1..64})"
coral="ghcr.io/withcoral/coral:1.2.3@$coral_digest"
web="ghcr.io/withcoral/coral-web:1.2.3@$web_digest"

failures=0

# release CHART... -- LINE...: a fresh release artifact holding the CHART
# files and an images.txt of LINEs.
release() {
  rm -rf "$work/release" && mkdir "$work/release"
  while [[ "$1" != -- ]]; do
    : >"$work/release/$1"
    shift
  done
  shift
  printf '%s\n' "$@" >"$work/release/images.txt"
}

# expect NAME STDOUT DOCKER [ARG...]: the script, given ARGs, prints exactly
# STDOUT and calls docker exactly as DOCKER says, one call to a line.
expect() {
  local name="$1" stdout="$2" docker="$3" out status
  shift 3
  : >"$DOCKER_LOG"
  set +e
  out="$(scripts/release-install-inputs.sh "$@" 2>"$work/stderr")"
  status=$?
  set -e
  if [[ $status -eq 0 && "$out" == "$stdout" && "$(cat "$DOCKER_LOG")" == "$docker" ]]; then
    echo "ok: $name"
  else
    echo "FAIL: $name (exit $status)" >&2
    printf 'stdout:\n%s\ndocker:\n%s\nstderr:\n%s\n' "$out" "$(cat "$DOCKER_LOG")" "$(cat "$work/stderr")" >&2
    failures=1
  fi
}

# refuse NAME TEXT [ARG...]: the script, given ARGs, fails, says TEXT, and
# prints nothing for $GITHUB_ENV.
refuse() {
  local name="$1" text="$2" out status
  shift 2
  set +e
  out="$(scripts/release-install-inputs.sh "$@" 2>"$work/stderr")"
  status=$?
  set -e
  if [[ $status -ne 0 && -z "$out" && "$(cat "$work/stderr")" == *"$text"* ]]; then
    echo "ok: $name"
  else
    echo "FAIL: $name (exit $status)" >&2
    printf 'stdout:\n%s\nstderr:\n%s\n' "$out" "$(cat "$work/stderr")" >&2
    failures=1
  fi
}

release coral-1.2.3.tgz -- "$envoy" "$web" "$coral"
expect 'loads the images images.txt pins' \
  "REEL_INSTALL_CHART=$work/release/coral-1.2.3.tgz
REEL_INSTALL_CORAL_IMAGE=coral:release
REEL_INSTALL_WEB_IMAGE=coral-web:release" \
  "docker pull --quiet ghcr.io/withcoral/coral@$coral_digest
docker tag ghcr.io/withcoral/coral@$coral_digest coral:release
docker pull --quiet ghcr.io/withcoral/coral-web@$web_digest
docker tag ghcr.io/withcoral/coral-web@$web_digest coral-web:release" \
  "$work/release"
expect 'air-gapped, leaves images.txt to the suite' \
  "REEL_INSTALL_CHART=$work/release/coral-1.2.3.tgz
REEL_INSTALL_IMAGES_TXT=$work/release/images.txt" \
  '' \
  "$work/release" --air-gapped

release -- "$envoy" "$web" "$coral"
refuse 'no packaged chart' 'one packaged chart' "$work/release"
release coral-1.2.3.tgz coral-1.2.4.tgz -- "$envoy" "$web" "$coral"
refuse 'two packaged charts' 'one packaged chart' "$work/release"
release coral-1.2.3.tgz -- "$envoy" "$coral"
refuse 'an image missing from images.txt' 'coral-web' "$work/release"
release coral-1.2.3.tgz -- "$envoy" "ghcr.io/withcoral/coral-web:1.2.3" "$coral"
refuse 'an image not pinned by digest' 'coral-web' "$work/release"
release coral-1.2.3.tgz -- "$envoy" "$web" "$coral" "$web"
refuse 'an image pinned twice' 'coral-web' "$work/release"
release coral-1.2.3.tgz -- "$envoy" "$web" "$coral"
rm "$work/release/images.txt"
refuse 'no images.txt' 'images.txt' "$work/release" --air-gapped
release coral-1.2.3.tgz -- "$envoy" "$web" "$coral"
DOCKER_FAIL=coral-web refuse 'a pull that fails' 'cannot pull' "$work/release"
refuse 'an unknown option' 'Usage' "$work/release" --airgapped

exit "$failures"
