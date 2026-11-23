#!/usr/bin/env bash
# Tries each case scripts/check-protocol-level.sh must judge, one at a time,
# in a throwaway repository whose one release holds this checkout's link
# protocol and globals. It runs this checkout's checker, so an uncommitted
# change to the checker is what gets tested. Needs Buf on PATH.
#
#   scripts/check-protocol-level.test.sh
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
repo="$work/repo"
checker="$work/check-protocol-level.sh"
cp scripts/check-protocol-level.sh "$checker"

module='crates/coral-run-executor'
proto="$module/proto/coral/run_executor/v1/link.proto"
level_file="$module/src/link.rs"
globals='crates/coral-v8/tests/program-runtime-surface/globals.json'

mkdir -p "$repo"
for file in "$module/buf.yaml" "$proto" "$level_file" "$globals"; do
  mkdir -p "$repo/$(dirname "$file")"
  cp "$file" "$repo/$file"
done
git -C "$repo" init --quiet
git -C "$repo" add .
git -C "$repo" -c user.name=test -c user.email=test@example.invalid \
  -c commit.gpgsign=false -c core.hooksPath=/dev/null commit --quiet -m release
git -C "$repo" tag v9.9.9
level="$(sed -nE 's/^pub const PROTOCOL_LEVEL: u32 = ([0-9]+);$/\1/p' "$level_file")"

failures=0

in_repo() { (cd "$repo" && "$@"); }
set_level() { in_repo sed -i.bak -E "s/^(pub const PROTOCOL_LEVEL: u32 = )[0-9]+;$/\1$1;/" "$level_file"; }
add_field() { in_repo sed -i.bak -E 's/^message Room \{$/&\n  \/\/ A field the test adds.\n  uint32 test_only = 9999;/' "$proto"; }

# expect NAME TEXT: the checker fails and prints TEXT. An empty TEXT expects
# a clean pass.
expect() {
  local name="$1" text="$2" out status
  set +e
  out="$(cd "$repo" && bash "$checker" 2>&1)"
  status=$?
  set -e
  if [[ -z "$text" && $status -eq 0 ]] || [[ -n "$text" && $status -ne 0 && "$out" == *"$text"* ]]; then
    echo "ok: $name"
  else
    echo "FAIL: $name (exit $status)" >&2
    echo "$out" >&2
    failures=1
  fi
  in_repo git checkout --quiet -- .
  in_repo git clean -fdq
}

expect 'nothing changed' ''

in_repo sed -i.bak -E 's/^package .*;$/\/\/ A comment the test adds.\n&/' "$proto"
expect 'a comment alone is no change' ''

add_field
expect 'a new field without a raise' "to $((level + 1)), not $level"

add_field
set_level $((level + 1))
expect 'a new field with a raise of one' ''

add_field
set_level $((level + 2))
expect 'a new field with a raise of two' "to $((level + 1)), not $((level + 2))"

in_repo sed -i.bak -E 's/^\[/["testOnlyGlobal",/' "$globals"
expect 'a new global without a raise' "Program Runtime Surface"

set_level $((level + 1))
expect 'a raise with no change' 'nothing it covers changed'

exit "$failures"
