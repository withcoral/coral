#!/usr/bin/env bash
# Tries each breach that scripts/check-ee-boundary.sh must catch, one at a
# time, in a throwaway worktree of HEAD. It runs this checkout's checker, so
# an uncommitted change to the checker is what gets tested.
#
#   scripts/check-ee-boundary.test.sh
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
src="$PWD"
work="$(mktemp -d)"
tree="$work/tree"
checker="$work/check-ee-boundary.sh"
cleanup() {
  git -C "$src" worktree remove --force "$tree" >/dev/null 2>&1 || true
  rmdir "$work" 2>/dev/null || true
}
trap cleanup EXIT

git worktree add --quiet --detach "$tree" HEAD
cp scripts/check-ee-boundary.sh "$checker"

marker="SPDX-License-Identifier: LicenseRef-Coral-""Enterprise"
failures=0

reset() {
  git -C "$tree" checkout --quiet -- .
  git -C "$tree" clean -fdq
}

# expect NAME TEXT [ENV...]: the checker, run with ENV, fails and prints
# TEXT. An empty TEXT expects a clean pass.
expect() {
  local name="$1" text="$2" out status
  shift 2
  set +e
  out="$(cd "$tree" && env "$@" bash "$checker" 2>&1)"
  status=$?
  set -e
  if [[ -z "$text" && $status -eq 0 ]] || [[ -n "$text" && $status -ne 0 && "$out" == *"$text"* ]]; then
    echo "ok: $name"
  else
    echo "FAIL: $name (exit $status)" >&2
    echo "$out" >&2
    failures=1
  fi
  reset
}

in_tree() { (cd "$tree" && "$@"); }
write() { mkdir -p "$(dirname "$tree/$1")" && printf '%s\n' "$2" >"$tree/$1"; }
append() { printf '%s\n' "$2" >>"$tree/$1"; }

core=crates/coral-edition
ee=ee/crates/coral-ee

expect "a clean tree passes" ""

append "$core/Cargo.toml" $'[dependencies]\ncoral-ee = { path = "../../ee/crates/coral-ee" }'
expect "a dependency into ee/" "depends on coral-ee in ee/"

in_tree perl -pi -e 's|^default-members = \[(.*)\]|default-members = [$1, "ee/crates/*"]|' Cargo.toml
expect "an ee/ default member" "is a default workspace member"

write "$core/src/breach.rs" '#[cfg(feature = "enterprise")] fn a() {}'
expect "a cfg feature switch" "compile-time enterprise switch"

append "$core/Cargo.toml" $'[dependencies]\nserde = { workspace = true, features = ["derive", "enterprise"] }'
expect "a features list switch" "compile-time enterprise switch"

write "$core/src/breach.rs" 'const ON: bool = cfg!(enterprise);'
expect "a cfg! switch" "compile-time enterprise switch"

write "$core/src/breach.rs" 'use coral_ee as _;'
expect "an ee/ crate name" "names an ee/ crate or path"

write "$core/src/breach.rs" 'include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../ee/crates/coral-ee/src/main.rs"));'
expect "a composed include! into ee/" "names an ee/ crate or path"

write "$core/src/breach.rs" '#[path = "../../../ee/crates/coral-ee/src/main.rs"] mod m;'
expect "a #[path] into ee/" "names an ee/ crate or path"

write "$core/src/breach.rs" '#[cfg(windows)] include!("..\\..\\..\\ee\\crates\\coral-ee\\src\\main.rs");'
expect "a Windows path into ee/" "names an ee/ crate or path"

append "$core/Cargo.toml" $'[[bin]]\nname = "breach"\npath = "../../ee/crates/coral-ee/src/main.rs"'
expect "a Cargo target built from ee/" "target from"

write "$core/src/breach.rs" "// $marker"
expect "the enterprise header outside ee/" "carries the enterprise licence header outside ee/"

write "$ee/src/bare.rs" 'fn a() {}'
expect "an ee/ file without the header" "in its first three lines"

in_tree ln -s ../README.md ee/link.md
expect "a symlink in ee/" "symlink in ee/"

in_tree ln -s ../../../ee/crates/coral-ee/src/main.rs "$core/src/linked.rs"
expect "a symlink into ee/" "points into ee/"

write "$ee/migrations/0001_add.sql" "-- $marker"
expect "a migration in ee/" "migration in ee/"

mkdir -p "$work/bin"
printf '#!/bin/sh\nexit 3\n' >"$work/bin/jq"
chmod +x "$work/bin/jq"
set +e
(cd "$tree" && PATH="$work/bin:$PATH" bash "$checker" >/dev/null 2>&1)
status=$?
set -e
if [[ $status -eq 0 ]]; then
  echo "FAIL: a failed jq passed the check" >&2
  failures=1
else
  echo "ok: a failed jq fails the check"
fi
rm "$work/bin/jq"
rmdir "$work/bin"

exit "$failures"
