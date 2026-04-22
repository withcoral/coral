#!/bin/zsh

set -euo pipefail

usage() {
  cat <<'EOF' >&2
Usage:
  ./sources/confluence/confluence-auth.sh <email> [api_token]

Examples:
  ./sources/confluence/confluence-auth.sh you@example.com
  ./sources/confluence/confluence-auth.sh you@example.com atlassian_api_token

You can also pass the token via CONFLUENCE_API_TOKEN:
  CONFLUENCE_API_TOKEN=... ./sources/confluence/confluence-auth.sh you@example.com
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

email="$1"
token="${2:-${CONFLUENCE_API_TOKEN:-}}"

if [[ -z "$token" ]]; then
  printf 'Confluence API token: ' >&2
  trap 'stty echo >/dev/null 2>&1 || true' EXIT
  stty -echo
  read -r token
  stty echo
  trap - EXIT
  printf '\n' >&2
fi

printf '%s' "${email}:${token}" | base64 | tr -d '\n'
printf '\n'
