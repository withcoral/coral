#!/usr/bin/env bash
# Readies Reel's helm-install suite to install a release: its packaged chart
# and its images. Prints the suite's settings as NAME=VALUE lines, for
# $GITHUB_ENV, and nothing else on stdout.
#
# Usage: scripts/release-install-inputs.sh RELEASE [--air-gapped]
#
# RELEASE is a folder holding the release workflow's chart artifact: one
# packaged coral-<version>.tgz and images.txt. By default the script pulls
# Coral's two images by the digests images.txt pins, and tags them
# coral:release and coral-web:release for kind to load. With --air-gapped it
# pulls nothing: the suite copies images.txt into its own registry.
#
# Needs docker, unless --air-gapped.
set -euo pipefail

usage() {
  echo "Usage: $0 RELEASE [--air-gapped]" >&2
  exit 2
}
[[ $# -eq 1 || ($# -eq 2 && "$2" == --air-gapped) ]] || usage
release="$1"
air_gapped="${2:-}"

shopt -s nullglob
charts=("$release"/coral-*.tgz)
if [[ ${#charts[@]} -ne 1 ]]; then
  echo "Expected one packaged chart in ${release}, found ${#charts[@]}" >&2
  exit 1
fi
images="$release/images.txt"
if [[ ! -f "$images" ]]; then
  echo "No images.txt in ${release}" >&2
  exit 1
fi

# Collect the output first, so that a failure prints nothing for $GITHUB_ENV.
settings=("REEL_INSTALL_CHART=${charts[0]}")
if [[ -n "$air_gapped" ]]; then
  settings+=("REEL_INSTALL_IMAGES_TXT=$images")
else
  for name in coral coral-web; do
    lines="$(grep -E "^ghcr\.io/withcoral/${name}:[^@]+@sha256:[0-9a-f]{64}$" "$images" || true)"
    if [[ -z "$lines" || "$lines" == *$'\n'* ]]; then
      echo "images.txt must pin ghcr.io/withcoral/${name} once, by tag and digest" >&2
      exit 1
    fi
    # Pull by digest alone: before promote, the release's tag does not exist.
    image="${lines%:*@*}@${lines##*@}"
    docker pull --quiet "$image" >&2
    docker tag "$image" "${name}:release" >&2
  done
  settings+=("REEL_INSTALL_CORAL_IMAGE=coral:release" "REEL_INSTALL_WEB_IMAGE=coral-web:release")
fi
printf '%s\n' "${settings[@]}"
