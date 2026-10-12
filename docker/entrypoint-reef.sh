#!/bin/sh
set -eu

fatal() {
  echo "coral-web-entrypoint: FATAL: $1" >&2
  echo "coral-web-entrypoint: $2" >&2
  exit 1
}

trap 'exit 143' TERM

[ -n "${CORAL_ENDPOINT:-}" ] || fatal \
  'CORAL_ENDPOINT is required.' \
  'Use loopback in a shared network namespace, HTTPS, or explicit cleartext h2c with CORAL_WEB_ALLOW_INSECURE_CORAL_ENDPOINT=1.'

[ -n "${CORAL_WEB_AUTH_MODE:-}" ] || fatal \
  'CORAL_WEB_AUTH_MODE is required.' \
  'Set CORAL_WEB_AUTH_MODE=required or CORAL_WEB_AUTH_MODE=disabled.'

CORAL_WEB_AUTH_MODE=$(
  printf '%s' "$CORAL_WEB_AUTH_MODE" \
    | tr '[:upper:]' '[:lower:]' \
    | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'
)
export CORAL_WEB_AUTH_MODE

case "$CORAL_WEB_AUTH_MODE" in
  required)
    [ -n "${CORAL_WEB_SESSION_SECRET:-}" ] || fatal \
      'CORAL_WEB_SESSION_SECRET is required when CORAL_WEB_AUTH_MODE=required.' \
      'Set a secret of at least 32 characters; Coral web validates its shape at startup.'
    [ -n "${CORAL_WEB_AUTH_ISSUER:-}" ] || fatal \
      'CORAL_WEB_AUTH_ISSUER is required when CORAL_WEB_AUTH_MODE=required.' \
      'Set the HTTPS or explicit-loopback OAuth issuer; Coral web validates it at startup.'
    [ -n "${CORAL_WEB_PUBLIC_URL:-}" ] || fatal \
      'CORAL_WEB_PUBLIC_URL is required when CORAL_WEB_AUTH_MODE=required.' \
      'Set the externally reachable HTTPS or explicit-loopback Coral web origin.'
    ;;
  disabled)
    echo 'coral-web-entrypoint: WARNING: CORAL_WEB_AUTH_MODE=disabled serves Coral with no sign-in.' >&2
    echo 'coral-web-entrypoint: Anyone who can reach this port acts as the organization owner.' >&2
    echo 'coral-web-entrypoint: Set CORAL_WEB_AUTH_MODE=required unless only you can reach this port.' >&2
    ;;
  *)
    fatal 'CORAL_WEB_AUTH_MODE must be required or disabled.' \
      'Set one of the documented values.'
    ;;
esac

trap - TERM
exec node server.mjs
