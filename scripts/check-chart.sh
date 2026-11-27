#!/usr/bin/env bash
# Checks the Helm chart in charts/coral: lint, the values schema, a render of
# each values file in ci/, kubeconform on the oldest and newest Kubernetes
# minors the chart supports, and the helm-unittest suites.
#
# Needs helm, kubeconform and helm-unittest. HELM_UNITTEST names the command
# that runs the suites: by default the helm plugin, `helm unittest`.
set -euo pipefail

chart=charts/coral
# The oldest and newest of the last three upstream minors, which the chart
# supports.
kube_versions=(1.35.0 1.37.0)
read -r -a unittest <<<"${HELM_UNITTEST:-helm unittest}"

cd "$(dirname "$0")/.."

for values in "$chart"/ci/*.yaml; do
  echo "== $values"
  # Lint also checks the values against values.schema.json.
  helm lint --strict "$chart" -f "$values"
  rendered="$(helm template coral "$chart" -f "$values")"
  if grep -qx 'kind: Secret' <<<"$rendered"; then
    echo "$values renders a Secret: the chart never renders one" >&2
    exit 1
  fi
  for version in "${kube_versions[@]}"; do
    helm template coral "$chart" -f "$values" --kube-version "$version" \
      | kubeconform -strict -summary -kubernetes-version "$version" -
  done
done

"${unittest[@]}" "$chart"
