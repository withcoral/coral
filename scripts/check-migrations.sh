#!/usr/bin/env bash
# Fails when this checkout edits, renames or deletes a migration that is on
# main, adds one whose version does not sort after every version on main, or
# adds one where no ledger reads it. Databases record applied migrations by
# checksum, so either change breaks a database created from an earlier main.
#
# Also fails when a migration it adds can break an older Coral that still runs
# during a rolling upgrade, unless the migration declares why that Coral keeps
# working. scripts/migration-compatibility.mjs has the rules.
#
#   scripts/check-migrations.sh [base-ref]   # default: origin/main
set -euo pipefail

base_ref="${1:-origin/main}"
scripts="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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

# One version sequence, the shared SQLx chain. Its database-specific halves sit
# in subfolders of its directory.
chain='crates/coral-db/migrations'
# SQLx reads the chain's directory and each database's half, nothing deeper.
chain_runs="^$chain/((postgres|sqlite)/)?[^/]+\.sql$"
pathspec=":(glob)$chain/**/*.sql"

# The versions of the chain's migrations in `ref`, one per line.
chain_versions() {
  git ls-tree -r --name-only "$1" -- "$chain" | { grep -E "$chain_runs" || true; } |
    sed -nE 's#^.*/0*([0-9]+)_[^/]*$#\1#p'
}

status=0

# The migration plan reads only the chain's files, so a migration added to
# another crate's folder, or to another subfolder of the chain, would never run.
crate_migrations=':(glob)crates/*/migrations/**/*.sql'
stray="$({
  git diff --no-renames --name-only --diff-filter=A "$fork_point" -- "$crate_migrations"
  git ls-files --others --exclude-standard -- "$crate_migrations"
} | grep -vE "$chain_runs" | sort -u || true)"
if [ -n "$stray" ]; then
  echo "check-migrations: these migrations never run; add them to $chain, or its postgres/ or sqlite/ folder, instead:" >&2
  echo "$stray" | sed 's/^/  /' >&2
  status=1
fi

changed="$(git diff --no-renames --name-only --diff-filter=MDT "$fork_point" -- "$pathspec")"
if [ -n "$changed" ]; then
  echo "check-migrations: these migrations are on $base_ref and must not change; add a new migration instead:" >&2
  echo "$changed" | sed 's/^/  /' >&2
  status=1
fi

max="$(chain_versions "$base_ref" | sort -n | tail -1)"
# A chain with no migrations on main yet accepts any version, 0000 included.
max="${max:--1}"

added="$({
  git diff --no-renames --name-only --diff-filter=A "$fork_point" -- "$pathspec"
  git ls-files --others --exclude-standard -- "$pathspec"
} | { grep -E "$chain_runs" || true; } | sort -u)"
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

# With `[search] semantic_search_enabled = false`, Coral starts on a Postgres
# without pgvector, and the chain must too. The vector table lives outside the
# chain: coral_search::prepare_vector_table creates it at boot. A bare
# `vector` counts only as a column type: search migrations also use the word
# for a text vector, as in `unnest(vector)`.
pgvector="$(echo "$added" | { grep -v '^$' || true; } | while IFS= read -r file; do
  grep -qiE \
    -e '(^|[^[:alnum:]_])(halfvec|sparsevec)([^[:alnum:]_]|$)' \
    -e '(^|[^[:alnum:]_])vector[[:space:]]*\(' \
    -e '[[:alnum:]_"][[:space:]]+vector[[:space:]]*([,);[]|$|(not|null|default|primary|references|unique|check|constraint|generated|collate)([^[:alnum:]_]|$))' \
    -e '::[[:space:]]*vector([^[:alnum:]_]|$)' \
    -e 'extension[[:space:]]+(if[[:space:]]+not[[:space:]]+exists[[:space:]]+)?"?vector([^[:alnum:]_]|$)' \
    "$file" && echo "$file"
done || true)"
if [ -n "$pgvector" ]; then
  echo "check-migrations: these migrations name pgvector, which a Postgres without it cannot run; change coral_search::prepare_vector_table instead:" >&2
  echo "$pgvector" | sed 's/^/  /' >&2
  status=1
fi

# A rolling upgrade runs the previous Coral while this one migrates, so an
# older Coral must keep working on the schema a new migration leaves. The
# SQLite half of a step is the same step, often written as a table rebuild,
# so the shared files and the Postgres halves stand for both.
# A removal names the step of the change that stopped using what it removes.
# A release user upgrades from one release to the next, so that step must be
# in a release: the release then no longer uses what is removed, and does not
# remove it yet. A step that any release has, the newest release has.
release="$(git tag --merged "$fork_point" --list 'v[1-9]*' --sort=-version:refname |
  { grep -v -- '-' || true; } | head -1 || true)"
released=""
if [ -n "$release" ]; then
  released="$(chain_versions "$release" | sort -un | tr '\n' ' ')"
fi
compatibility=()
while IFS= read -r file; do
  if [ -n "$file" ] && [[ "$file" != "$chain/sqlite/"* ]]; then
    compatibility+=("$file")
  fi
done <<<"$added"
if [ "${#compatibility[@]}" -gt 0 ] &&
  ! node "$scripts/migration-compatibility.mjs" --released "$released" "${compatibility[@]}"; then
  status=1
fi

exit "$status"
