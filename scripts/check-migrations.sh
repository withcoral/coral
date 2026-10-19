#!/usr/bin/env bash
# Fails when this checkout edits, renames or deletes a migration that is on
# main, adds one whose version does not sort after every version on main, or
# adds one where no ledger reads it. Databases record applied migrations by
# checksum, so either change breaks a database created from an earlier main.
#
#   scripts/check-migrations.sh [base-ref]   # default: origin/main
set -euo pipefail

base_ref="${1:-origin/main}"
cd "$(git rev-parse --show-toplevel)"

if ! git rev-parse --verify --quiet "$base_ref^{commit}" >/dev/null; then
  echo "check-migrations: $base_ref not found; fetch it and retry" >&2
  exit 1
fi
if ! fork_point="$(git merge-base HEAD "$base_ref")"; then
  echo "check-migrations: no common history with $base_ref; fetch more history (git fetch --unshallow) and retry" >&2
  exit 1
fi

# Coral 0.x kept another schema, and Coral 1.x refuses to open the state it
# wrote. No database created from a 0.x main runs these migrations, so a
# checkout that forks from one has nothing on main to keep.
if git show "$fork_point:Cargo.toml" 2>/dev/null | grep -qE '^version = "0\.'; then
  echo "check-migrations: $base_ref forks from Coral 0.x, whose state Coral 1.x rejects; nothing to compare" >&2
  exit 0
fi

# One version sequence per ledger: the shared SQLx chain, then each search stream.
# The chain's database-specific halves sit in subfolders of its directory.
chain='crates/coral-db/migrations'
ledgers=("$chain" 'crates/coral-search/streams/postgres' 'crates/coral-search/streams/sqlite')
pathspecs=()
for ledger in "${ledgers[@]}"; do pathspecs+=(":(glob)$ledger/**/*.sql"); done

status=0

# The migration plan reads only the chain's directory, so a migration added to
# another crate's folder would never run.
crate_migrations=':(glob)crates/*/migrations/**/*.sql'
stray="$({
  git diff --no-renames --name-only --diff-filter=A "$fork_point" -- "$crate_migrations"
  git ls-files --others --exclude-standard -- "$crate_migrations"
} | grep -v "^$chain/" | sort -u || true)"
if [ -n "$stray" ]; then
  echo "check-migrations: these migrations never run; add them to $chain instead:" >&2
  echo "$stray" | sed 's/^/  /' >&2
  status=1
fi

changed="$(git diff --no-renames --name-only --diff-filter=MDT "$fork_point" -- "${pathspecs[@]}")"
if [ -n "$changed" ]; then
  echo "check-migrations: these migrations are on $base_ref and must not change; add a new migration instead:" >&2
  echo "$changed" | sed 's/^/  /' >&2
  status=1
fi

for ledger in "${ledgers[@]}"; do
  pathspec=":(glob)$ledger/**/*.sql"
  on_base="$(git ls-tree -r --name-only "$base_ref" -- "$ledger" | grep -E "^$ledger/([^/]+/)*[^/]+\.sql$" || true)"
  max="$(echo "$on_base" | sed -nE 's#^.*/0*([0-9]+)_[^/]*$#\1#p' | sort -n | tail -1)"
  # A ledger with no migrations on main yet accepts any version, 0000 included.
  max="${max:--1}"

  added="$({
    git diff --no-renames --name-only --diff-filter=A "$fork_point" -- "$pathspec"
    git ls-files --others --exclude-standard -- "$pathspec"
  } | sort -u)"
  while IFS= read -r file; do
    [ -n "$file" ] || continue
    if ! [[ "$(basename "$file")" =~ ^0*([0-9]+)_ ]]; then
      echo "check-migrations: $file must start with a numeric version, like $(printf '%04d' $((max + 1)))_name.sql" >&2
      status=1
    elif [ "${BASH_REMATCH[1]}" -le "$max" ]; then
      echo "check-migrations: $file must use a version above $max, the highest on $base_ref" >&2
      status=1
    fi
  done <<<"$added"
done

exit "$status"
