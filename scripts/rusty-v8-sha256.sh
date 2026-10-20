#!/usr/bin/env bash
# Prints the SHA-256 that RUSTY_V8_ARCHIVE_SHA256 pins for the prebuilt V8
# archive the v8 crate downloads. With it set, v8's build script rejects an
# archive that differs from the asset denoland/rusty_v8 published.
#
#   scripts/rusty-v8-sha256.sh [target]  prints the pin (default: rustc's host)
#   scripts/rusty-v8-sha256.sh --update  pins the v8 version in Cargo.lock,
#                                        from the release's asset digests
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
pins="$root/crates/coral-v8/rusty_v8.sha256"
# The targets Coral builds v8 for.
targets=(
  aarch64-apple-darwin
  aarch64-unknown-linux-gnu
  x86_64-pc-windows-msvc
  x86_64-unknown-linux-gnu
)

# Windows checkouts can hold CRLF line endings.
version=$(tr -d '\r' < "$root/Cargo.lock" \
  | awk '$0 == "name = \"v8\"" { getline; gsub(/"/, "", $3); print $3; exit }')
if [[ -z "$version" ]]; then
  echo "error: Cargo.lock does not lock v8" >&2
  exit 1
fi

archive() {
  if [[ "$1" == *-windows-* ]]; then
    echo "rusty_v8_release_$1.lib.gz"
  else
    echo "librusty_v8_release_$1.a.gz"
  fi
}

if [[ "${1:-}" == --update ]]; then
  digests=$(gh release view "v$version" --repo denoland/rusty_v8 --json assets \
    --jq '.assets[] | "\(.name) \(.digest)"')
  trap 'rm -f "$pins.tmp"' EXIT
  {
    echo "# SHA-256 of the prebuilt V8 archives, from the asset digests of"
    echo "# https://github.com/denoland/rusty_v8/releases/tag/v$version"
    echo "# Regenerate with scripts/rusty-v8-sha256.sh --update."
    for target in "${targets[@]}"; do
      name=$(archive "$target")
      digest=$(awk -v name="$name" '$1 == name { print $2 }' <<< "$digests")
      if [[ "$digest" != sha256:* ]]; then
        echo "error: v$version has no SHA-256 digest for $name" >&2
        exit 1
      fi
      echo "${digest#sha256:}  v$version/$name"
    done
  } > "$pins.tmp"
  mv "$pins.tmp" "$pins"
  exit 0
fi

target=${1:-$(rustc -vV | tr -d '\r' | sed -n 's/^host: //p')}
key="v$version/$(archive "$target")"
sha256=$(tr -d '\r' < "$pins" | awk -v key="$key" '$2 == key { print $1 }')
if [[ -z "$sha256" ]]; then
  echo "error: ${pins#"$root/"} pins no $key." >&2
  echo "Run scripts/rusty-v8-sha256.sh --update after a v8 upgrade, or add $target to its targets." >&2
  exit 1
fi
echo "$sha256"
