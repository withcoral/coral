#!/usr/bin/env bash
# Prints each image the Coral chart renders with default values, one per
# line, sorted. "Default values" means the chart's own values plus the least a
# render needs (charts/coral/ci/default-values.yaml), which sets no image.
#
# Usage: scripts/chart-images.sh [CHART]
# CHART is a chart folder or a packaged .tgz; it defaults to charts/coral.
#
# Needs helm and yq (mikefarah).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
chart="${1:-$root/charts/coral}"

helm template coral "$chart" -f "$root/charts/coral/ci/default-values.yaml" \
  | yq -N '.spec.template.spec | (.initContainers // [], .containers // []) | .[] | .image | select(. != null)' \
  | sort -u
