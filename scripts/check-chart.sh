#!/usr/bin/env bash
# Checks the Helm chart in charts/coral: lint, the values schema, a render of
# each values file in ci/ and each example in examples/, kubeconform on
# the oldest and newest Kubernetes minors the chart supports, and the
# helm-unittest suites.
#
# Needs helm, kubeconform and helm-unittest. HELM_UNITTEST names the command
# that runs the suites: by default the helm plugin, `helm unittest`.
# KUBECONFORM_CACHE, when set, names a directory where kubeconform keeps the
# schemas it downloads.
set -euo pipefail

chart=charts/coral
# The oldest and newest of the last three upstream minors, which the chart
# supports.
kube_versions=(1.35.0 1.37.0)
read -r -a unittest <<<"${HELM_UNITTEST:-helm unittest}"
cache=()
if [[ -n "${KUBECONFORM_CACHE:-}" ]]; then
  mkdir -p "$KUBECONFORM_CACHE"
  # Absolute, since the script changes directory below.
  cache=(-cache "$(cd "$KUBECONFORM_CACHE" && pwd)")
fi
# Kubernetes' own schemas don't cover HTTPRoute, a CRD: this catalog does. It
# is pinned to a commit so that a change upstream can't fail a check.
crd_schemas='https://raw.githubusercontent.com/datreeio/CRDs-catalog/b50f3fbc45169a14b6d2d3f767b32fd96ef4a3fa/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'

cd "$(dirname "$0")/.."

# Lints, renders and validates the chart with the values files it is given.
check() {
  local args=()
  for values in "$@"; do args+=(-f "$values"); done
  echo "== $*"
  # Lint also checks the values against values.schema.json.
  helm lint --strict "$chart" "${args[@]}"
  rendered="$(helm template coral "$chart" "${args[@]}")"
  if grep -qx 'kind: Secret' <<<"$rendered"; then
    echo "$* renders a Secret: the chart never renders one" >&2
    exit 1
  fi
  for version in "${kube_versions[@]}"; do
    helm template coral "$chart" "${args[@]}" --kube-version "$version" \
      | kubeconform -strict -summary -kubernetes-version "$version" \
        -schema-location default -schema-location "$crd_schemas" ${cache[@]+"${cache[@]}"} -
  done
}

for values in "$chart"/ci/*.yaml; do
  check "$values"
done
# Each example holds only its own keys, so it goes on top of the least values.
for example in "$chart"/examples/*.yaml; do
  check "$chart/ci/default-values.yaml" "$example"
done

"${unittest[@]}" "$chart"
