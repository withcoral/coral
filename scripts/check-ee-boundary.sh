#!/usr/bin/env bash
# Fails when code outside ee/ reaches into ee/, or licence headers cross the
# boundary (ADR 0033):
#
#   - a package outside ee/ depends on a package inside it;
#   - a default workspace member sits inside ee/, so a plain `cargo build`
#     would build enterprise code;
#   - code outside ee/ has a compile-time enterprise switch or names an ee/
#     crate;
#   - a source file in ee/ lacks the enterprise SPDX header, or a file
#     outside ee/ carries it.
#
#   scripts/check-ee-boundary.sh
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
root="$PWD"
failed=0
fail() {
  echo "check-ee-boundary: $*" >&2
  failed=1
}

# Built from parts so this script does not carry the header it looks for.
marker="SPDX-License-Identifier: LicenseRef-Coral-""Enterprise"

metadata="$(cargo metadata --format-version 1 --no-deps --locked)"

while IFS= read -r line; do
  fail "$line"
done < <(jq -r --arg ee "$root/ee/" '
  .packages[]
  | select(.manifest_path | startswith($ee) | not)
  | .name as $name
  | .dependencies[]
  | select(.path != null and (.path + "/" | startswith($ee)))
  | "\($name) depends on \(.name) in ee/; dependencies only point from ee/ to the rest of Coral"
' <<<"$metadata")

while IFS= read -r line; do
  fail "$line"
done < <(jq -r --arg ee "$root/ee/" '
  .workspace_default_members as $defaults
  | .packages[]
  | select(.id as $id | $defaults | index($id))
  | select(.manifest_path | startswith($ee))
  | "\(.name) in ee/ is a default workspace member; a plain cargo build must not build ee/"
' <<<"$metadata")

# Every tracked or new file, minus deleted ones, as paths from the root.
files() {
  git ls-files --cached --others --exclude-standard -- "$@" |
    while IFS= read -r path; do [[ -f "$path" && ! -L "$path" ]] && printf '%s\n' "$path"; done
}

outside_ee=(':!ee/')

while IFS= read -r hit; do
  fail "$hit: compile-time enterprise switch outside ee/; check the Edition at runtime instead"
done < <(files "${outside_ee[@]}" '*.rs' '*Cargo.toml' |
  tr '\n' '\0' | xargs -0 grep -nE 'feature *= *"(ee|enterprise)"|cfg\((ee|enterprise)\b|^ *(ee|enterprise) *= *\[' /dev/null || true)

while IFS= read -r hit; do
  fail "$hit: Rust outside ee/ names an ee/ crate"
done < <(files "${outside_ee[@]}" '*.rs' | tr '\n' '\0' | xargs -0 grep -nE '\bcoral_ee' /dev/null || true)

while IFS= read -r hit; do
  fail "${hit%%:*}: carries the enterprise licence header outside ee/"
done < <(files "${outside_ee[@]}" | tr '\n' '\0' | xargs -0 grep -lF "$marker" /dev/null || true)

while IFS= read -r path; do
  case "$path" in
    ee/LICENSE | *.md) continue ;;
  esac
  if ! head -n 3 "$path" | grep -qF "$marker"; then
    fail "$path: put \"$marker\" in its first three lines"
  fi
done < <(files ee/)

exit "$failed"
