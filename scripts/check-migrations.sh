#!/usr/bin/env bash
# Fails when this checkout edits, renames or deletes a migration that is on
# main, adds one whose version does not sort after every version on main, or
# leaves one where no database runs it. Databases record applied migrations by
# checksum, so any of these breaks a database created from an earlier main.
# Also fails when a crate migration exists for one database but not the other.
#
# Each database has its own chain of migrations: every crate's
# migrations/postgres and migrations/sqlite, then each search stream. Before
# the chains split, one file in a crate's migrations/ served both databases;
# moved unchanged into both folders, it keeps its checksum.
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

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export LC_ALL=C

# Prints "<blob> <path>" for each migration file in a commit, or in the working
# tree when no commit is given.
files() {
  if [ -n "${1:-}" ]; then
    git ls-tree -r "$1" -- crates | awk -F'\t' '{ split($1, f, " "); print f[3], $2 }'
  else
    git ls-files --cached --others --exclude-standard -- crates | sort -u |
      while IFS= read -r path; do [ -f "$path" ] && echo "$path"; done >"$tmp/paths"
    paste -d' ' <(git hash-object --stdin-paths <"$tmp/paths") "$tmp/paths"
  fi | grep -E ' crates/[^/]+/(migrations|streams)/' || true
}

# Prints "<key> <blob>" for each migration of one chain, sorted by key. The key
# names a migration wherever its file lives: crate and file name in a crate's
# chain, file name in a search stream.
chain() {
  local name="$1" database="${1#*:}" blob path
  while read -r blob path; do
    case "$name" in
      crates:*)
        if [[ "$path" =~ ^crates/([^/]+)/migrations/($database/)?([^/]+\.sql)$ ]]; then
          echo "${BASH_REMATCH[1]}/${BASH_REMATCH[3]} $blob"
        fi
        ;;
      search:*)
        if [[ "$path" =~ ^crates/coral-search/streams/$database/([^/]+\.sql)$ ]]; then
          echo "${BASH_REMATCH[1]} $blob"
        fi
        ;;
    esac
  done <"$2" | sort
}

# The path a chain's migration has in this checkout.
path_of() {
  case "$1" in
    crates:*) echo "crates/${2%%/*}/migrations/${1#*:}/${2#*/}" ;;
    search:*) echo "crates/coral-search/streams/${1#*:}/$2" ;;
  esac
}

files "$fork_point" >"$tmp/fork-files"
files "$base_ref" >"$tmp/base-files"
files >"$tmp/head-files"

status=0

loose="$(awk '{ print $2 }' "$tmp/head-files" | grep -E '^crates/[^/]+/migrations/[^/]+\.sql$' || true)"
if [ -n "$loose" ]; then
  echo "check-migrations: no database runs these migrations; move each into migrations/postgres and migrations/sqlite:" >&2
  echo "$loose" | sed 's/^/  /' >&2
  status=1
fi

for name in crates:postgres crates:sqlite search:postgres search:sqlite; do
  chain "$name" "$tmp/fork-files" >"$tmp/fork"
  chain "$name" "$tmp/head-files" >"$tmp/head"

  changed="$(join -a 1 -e missing -o 1.1,1.2,2.2 "$tmp/fork" "$tmp/head" | awk '$2 != $3 { print $1 }')"
  if [ -n "$changed" ]; then
    echo "check-migrations: these migrations are on $base_ref and must not change; add a new migration instead:" >&2
    while IFS= read -r key; do echo "  $(path_of "$name" "$key")" >&2; done <<<"$changed"
    status=1
  fi

  max="$(chain "$name" "$tmp/base-files" | sed -nE 's#^([^ ]*/)?0*([0-9]+)_[^ ]* .*$#\2#p' | sort -n | tail -1)"
  max="${max:-0}"
  while IFS= read -r key; do
    [ -n "$key" ] || continue
    file="$(path_of "$name" "$key")"
    if ! [[ "${key##*/}" =~ ^0*([0-9]+)_ ]]; then
      echo "check-migrations: $file must start with a numeric version, like $(printf '%04d' $((max + 1)))_name.sql" >&2
      status=1
    elif [ "${BASH_REMATCH[1]}" -le "$max" ]; then
      echo "check-migrations: $file must use a version above $max, the highest on $base_ref" >&2
      status=1
    fi
  done < <(join -v 2 "$tmp/fork" "$tmp/head" | awk '{ print $1 }')
done

# Each database needs its own copy of every crate migration, under the same
# name. Search streams differ on purpose and are exempt.
chain crates:postgres "$tmp/head-files" | awk '{ print $1 }' >"$tmp/postgres"
chain crates:sqlite "$tmp/head-files" | awk '{ print $1 }' >"$tmp/sqlite"
while IFS= read -r line; do
  database="${line%% *}"
  echo "check-migrations: $(path_of "crates:$database" "${line#* }") is missing; each database needs its own copy" >&2
  status=1
done < <(
  comm -23 "$tmp/postgres" "$tmp/sqlite" | sed 's/^/sqlite /'
  comm -13 "$tmp/postgres" "$tmp/sqlite" | sed 's/^/postgres /'
)

exit "$status"
