#!/usr/bin/env bash
# Tries each case scripts/check-images-txt.sh must judge, against a copy of
# this checkout's chart. A stand-in `docker` on PATH answers each registry
# lookup from a table, so the test needs no registry. The copy pins Envoy to
# the digest of the stand-in's text for it, since no text has the real
# digest. Needs helm and yq.
#
#   scripts/check-images-txt.test.sh
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# The stand-in prints the table's text for a reference as its raw manifest.
mkdir "$work/bin"
cat >"$work/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" != "buildx imagetools inspect --raw "* ]]; then
  echo "docker stand-in: unexpected arguments: $*" >&2
  exit 2
fi
manifest="$(awk -v ref="$5" '$1 == ref { print $2 }' "$REGISTRY_TABLE")"
if [[ -z "$manifest" ]]; then
  echo "docker stand-in: no image ${5}" >&2
  exit 1
fi
printf '%s' "$manifest"
EOF
chmod +x "$work/bin/docker"
export PATH="$work/bin:$PATH"
export REGISTRY_TABLE="$work/registry"

digest() { printf 'sha256:%s' "$(printf '%s' "$1" | sha256sum | cut -d' ' -f1)"; }

chart="$work/coral"
cp -R charts/coral "$chart"
envoy_digest="$(digest envoy-manifest)"
yq -i ".web.envoy.image.digest = \"$envoy_digest\"" "$chart/values.yaml"

version="$(yq '.appVersion' "$chart/Chart.yaml")"
coral="ghcr.io/withcoral/coral:$version"
web="ghcr.io/withcoral/coral-web:$version"
envoy="docker.io/envoyproxy/envoy@$envoy_digest"
cat >"$REGISTRY_TABLE" <<EOF
$coral coral-manifest
$web web-manifest
$envoy envoy-manifest
ghcr.io/withcoral/other:$version other-manifest
EOF

# The images.txt that a release writes for the copy.
good=(
  "$envoy"
  "$web@$(digest web-manifest)"
  "$coral@$(digest coral-manifest)"
)
if [[ "$(scripts/chart-images.sh "$chart" | sort)" != "$(printf '%s\n' "$coral" "$web" "$envoy" | sort)" ]]; then
  echo "FAIL: the chart renders other images than this test expects:" >&2
  scripts/chart-images.sh "$chart" >&2
  exit 1
fi

failures=0

# expect NAME TEXT LINE...: the checker fails on an images.txt of LINEs and
# prints TEXT. An empty TEXT expects a clean pass.
expect() {
  local name="$1" text="$2" out status
  shift 2
  printf '%s\n' "$@" >"$work/images.txt"
  set +e
  out="$(scripts/check-images-txt.sh "$chart" "$work/images.txt" 2>&1)"
  status=$?
  set -e
  if [[ -z "$text" && $status -eq 0 ]] || [[ -n "$text" && $status -ne 0 && "$out" == *"$text"* ]]; then
    echo "ok: $name"
  else
    echo "FAIL: $name (exit $status)" >&2
    echo "$out" >&2
    failures=1
  fi
}

expect 'every image, pinned' '' "${good[@]}"
expect 'a digest the tag does not resolve to' "$coral resolves to" \
  "$envoy" "$web@$(digest web-manifest)" "$coral@$(digest other-manifest)"
expect 'an image the chart renders is missing' 'differ' \
  "$web@$(digest web-manifest)" "$coral@$(digest coral-manifest)"
expect 'a line without a digest' 'not pinned by digest' \
  "$envoy" "$web" "$coral@$(digest coral-manifest)"
expect 'a line twice' 'differ' "${good[@]}" "$envoy"
expect 'an image the chart does not render' 'differ' \
  "${good[@]}" "ghcr.io/withcoral/other:$version@$(digest other-manifest)"

exit "$failures"
