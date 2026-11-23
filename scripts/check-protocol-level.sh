#!/usr/bin/env bash
# Fails when the Run Executor link's protocol level breaks ADR 0027's version
# rule against the newest release in HEAD's history: a change to the link's
# messages or to the Program Runtime Surface's globals must raise the level
# by exactly one, and the level moves only with such a change.
#
# The messages are compared as Buf builds them without source info, so a
# comment or a format change is no change. Needs Buf on PATH
# (`make lint-proto` installs it).
#
#   scripts/check-protocol-level.sh [release-tag]   # default: the newest release
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

module='crates/coral-run-executor'
level_file="$module/src/link.rs"
globals='crates/coral-v8/tests/program-runtime-surface/globals.json'

# A release is a `vX.Y.Z` tag, as scripts/check-migrations.sh reads one: a
# pre-release or a tag of another shape is none.
release="${1:-$(git tag --merged HEAD --list 'v[1-9]*' --sort=-version:refname |
  { grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' || true; } | head -n 1)}"
if [ -z "$release" ]; then
  echo "check-protocol-level: no release tag in HEAD's history; fetch the tags and retry" >&2
  exit 1
fi

for file in "$level_file" "$module/buf.yaml"; do
  if ! git cat-file -e "$release:$file" 2>/dev/null; then
    echo "check-protocol-level: $release has no $file; name a release that has the link" >&2
    exit 1
  fi
done

# The level that the link.rs on stdin declares.
level_in() {
  sed -nE 's/^pub const PROTOCOL_LEVEL: u32 = ([0-9]+);$/\1/p' | head -n 1
}
released_level="$(git show "$release:$level_file" | level_in)"
level="$(level_in <"$level_file")"
if [ -z "$released_level" ] || [ -z "$level" ]; then
  echo "check-protocol-level: no PROTOCOL_LEVEL in $level_file at $release or in this checkout" >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
git archive "$release" -- "$module/buf.yaml" "$module/proto" | tar -x -C "$work"
buf build "$work/$module" --exclude-source-info -o "$work/released.binpb"
buf build "$module" --exclude-source-info -o "$work/current.binpb"

changed=()
cmp -s "$work/released.binpb" "$work/current.binpb" || changed+=("the messages in $module/proto")
git diff --quiet "$release" -- "$globals" || changed+=("the Program Runtime Surface's globals ($globals)")

if [ ${#changed[@]} -gt 0 ]; then
  wanted=$((released_level + 1))
  if [ "$level" -ne "$wanted" ]; then
    echo "check-protocol-level: since $release (level $released_level), this checkout changes:" >&2
    printf '  %s\n' "${changed[@]}" >&2
    echo "Set PROTOCOL_LEVEL in $level_file to $wanted, not $level. AGENTS.md has the steps a change takes." >&2
    exit 1
  fi
elif [ "$level" -ne "$released_level" ]; then
  echo "check-protocol-level: PROTOCOL_LEVEL is $level, but nothing it covers changed since $release (level $released_level); set it back to $released_level" >&2
  exit 1
fi
echo "check-protocol-level: level $level against $release (level $released_level)"
