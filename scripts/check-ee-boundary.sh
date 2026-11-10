#!/usr/bin/env bash
# Fails when code outside ee/ reaches into ee/, or licence headers cross the
# boundary (ADR 0033):
#
#   - a package outside ee/ depends on a package inside it;
#   - a default workspace member sits inside ee/, so a plain `cargo build`
#     would build enterprise code;
#   - code outside ee/ has a compile-time enterprise switch, or Rust outside
#     ee/ names an ee/ crate or a path into ee/;
#   - ee/ holds a symlink, or a symlink outside ee/ points into it;
#   - ee/ holds a migration;
#   - a source file in ee/ lacks the enterprise SPDX header, or a file
#     outside ee/ carries it.
#
# scripts/check-ee-boundary.test.sh tries each breach.
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

# Each query runs in a plain assignment, so a failed jq stops the script
# under `set -e` instead of passing with no findings.
report() {
  if [[ -n "$1" ]]; then
    while IFS= read -r line; do fail "$line"; done <<<"$1"
  fi
}

outward="$(jq -r --arg ee "$root/ee/" '
  .packages[]
  | select(.manifest_path | startswith($ee) | not)
  | .name as $name
  | .dependencies[]
  | select(.path != null and (.path + "/" | startswith($ee)))
  | "\($name) depends on \(.name) in ee/; dependencies only point from ee/ to the rest of Coral"
' <<<"$metadata")"

defaults="$(jq -r --arg ee "$root/ee/" '
  .workspace_default_members as $defaults
  | .packages[]
  | select(.id as $id | $defaults | index($id))
  | select(.manifest_path | startswith($ee))
  | "\(.name) in ee/ is a default workspace member; a plain cargo build must not build ee/"
' <<<"$metadata")"
report "$outward"
report "$defaults"

# Every tracked or new path, minus deleted ones, relative to the root.
paths() {
  git ls-files --cached --others --exclude-standard -- "$@"
}

# The regular files among those paths.
files() {
  paths "$@" | while IFS= read -r path; do
    if [[ -f "$path" && ! -L "$path" ]]; then printf '%s\n' "$path"; fi
  done
}

outside_ee=(':!ee/')

# A symlink would let a file escape the header and name scans: one in ee/
# could point anywhere, and one outside could compile an ee/ file into core.
while IFS= read -r path; do
  if [[ ! -L "$path" ]]; then continue; fi
  case "$path" in
    ee/*) fail "$path: symlink in ee/; put the file itself here" ;;
    *)
      # A dangling link resolves to nothing and compiles nothing.
      target="$(realpath "$path" 2>/dev/null || true)"
      if [[ -n "$target" && "$target/" == "$root/ee/"* ]]; then
        fail "$path: symlink outside ee/ points into ee/"
      fi
      ;;
  esac
done < <(paths)

while IFS= read -r path; do
  fail "$path: migration in ee/; put the general schema in crates/coral-db/migrations"
done < <(paths ee/ | grep -E '\.sql$|(^|/)migrations/' || true)

while IFS= read -r hit; do
  fail "$hit: compile-time enterprise switch outside ee/; check the Edition at runtime instead"
done < <(files "${outside_ee[@]}" '*.rs' '*Cargo.toml' |
  tr '\n' '\0' | xargs -0 grep -nE 'features? *= *\[?[^]]*"(ee|enterprise)"|cfg!? *\( *(ee|enterprise)\b|^ *(ee|enterprise) *= *\[' /dev/null || true)

# A string that holds a path into ee/ catches include!, #[path] and the
# paths that concat!, env! or a build script would compose.
while IFS= read -r hit; do
  fail "$hit: Rust outside ee/ names an ee/ crate or path"
done < <(files "${outside_ee[@]}" '*.rs' | tr '\n' '\0' |
  xargs -0 grep -nE '\bcoral_ee|"([^"]*/)?ee/' /dev/null || true)

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
