#!/usr/bin/env bash
# Checks a release's images.txt against its packaged chart. images.txt lists
# every image the chart renders with default values, each pinned by digest:
# a line is the rendered reference with @<digest> added when the render has
# none, such as ghcr.io/withcoral/coral:1.12.0@sha256:...
#
# Fails unless:
# - the images the chart renders are exactly images.txt's, once each line
#   drops a digest the render does not have, and
# - each rendered reference, by tag or by digest, resolves in its registry to
#   the line's digest.
#
# Usage: scripts/check-images-txt.sh CHART IMAGES_TXT
#
# Needs helm, yq and docker buildx, signed in to any registry that needs it.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 CHART IMAGES_TXT" >&2
  exit 2
fi
chart="$1"
images_txt="$2"
scripts="$(cd "$(dirname "$0")" && pwd)"

status=0
rendered="$("$scripts/chart-images.sh" "$chart")"
listed=""
while read -r line; do
  [[ -n "$line" ]] || continue
  if [[ "$line" != *@sha256:* ]]; then
    echo "images.txt: ${line} is not pinned by digest" >&2
    status=1
    continue
  fi
  ref="${line%@*}"
  # A reference the chart renders by digest keeps its digest.
  [[ "${ref##*/}" == *:* ]] || ref="$line"
  listed+="${ref}"$'\n'

  # A manifest's digest is the hash of its raw bytes.
  digest="sha256:$(docker buildx imagetools inspect --raw "$ref" | sha256sum | cut -d' ' -f1)"
  if [[ "$digest" != "${line##*@}" ]]; then
    echo "${ref} resolves to ${digest}, not images.txt's ${line##*@}" >&2
    status=1
  fi
done <"$images_txt"

if ! diff <(echo "$rendered") <(sort <<<"${listed%$'\n'}") >&2; then
  echo "The chart's images (<) differ from images.txt's (>)" >&2
  status=1
fi
exit "$status"
