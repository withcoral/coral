#!/bin/sh
set -eu

fatal() {
  echo "coral-ui-entrypoint: FATAL: $1" >&2
  echo "coral-ui-entrypoint: $2" >&2
  exit 1
}

warn() {
  echo "coral-ui-entrypoint: WARNING: $1" >&2
  echo "coral-ui-entrypoint: $2" >&2
}

trap 'exit 143' TERM

[ -n "${CORAL_ENDPOINT:-}" ] || fatal \
  'CORAL_ENDPOINT is required.' \
  'Use loopback in a shared network namespace, HTTPS, or explicit cleartext h2c with CORAL_UI_ALLOW_INSECURE_CORAL_ENDPOINT=1.'

[ -n "${CORAL_UI_AUTH_MODE:-}" ] || fatal \
  'CORAL_UI_AUTH_MODE is required.' \
  'Set CORAL_UI_AUTH_MODE=required or CORAL_UI_AUTH_MODE=disabled.'

CORAL_UI_AUTH_MODE=$(
  printf '%s' "$CORAL_UI_AUTH_MODE" \
    | tr '[:upper:]' '[:lower:]' \
    | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'
)
export CORAL_UI_AUTH_MODE

case "$CORAL_UI_AUTH_MODE" in
  required)
    [ -n "${CORAL_UI_SESSION_SECRET:-}" ] || fatal \
      'CORAL_UI_SESSION_SECRET is required when CORAL_UI_AUTH_MODE=required.' \
      'Set a secret of at least 32 characters; Coral UI validates its shape at startup.'
    [ -n "${CORAL_UI_AUTH_ISSUER:-}" ] || fatal \
      'CORAL_UI_AUTH_ISSUER is required when CORAL_UI_AUTH_MODE=required.' \
      'Set the HTTPS or explicit-loopback OAuth issuer; Coral UI validates it at startup.'
    [ -n "${CORAL_UI_PUBLIC_URL:-}" ] || fatal \
      'CORAL_UI_PUBLIC_URL is required when CORAL_UI_AUTH_MODE=required.' \
      'Set the externally reachable HTTPS or explicit-loopback Coral UI origin.'
    ;;
  disabled)
    echo 'coral-ui-entrypoint: WARNING: CORAL_UI_AUTH_MODE=disabled serves the Coral console with no' >&2
    echo 'coral-ui-entrypoint: login at all — anyone who can reach this port can read every source' >&2
    echo 'coral-ui-entrypoint: and POST new source credentials.' >&2
    echo 'coral-ui-entrypoint: Set CORAL_UI_AUTH_MODE=required unless this port is reachable only' >&2
    echo 'coral-ui-entrypoint: from a trusted network.' >&2
    ;;
  *)
    fatal 'CORAL_UI_AUTH_MODE must be required or disabled.' \
      'Set one of the documented values.'
    ;;
esac

if [ -n "${CORAL_DESKTOP_APP:-}" ] || [ -n "${VITE_CORAL_DESKTOP_APP:-}" ]; then
  warn 'Desktop build markers are set but have no effect at container runtime.' \
    'CORAL_UI_AUTH_MODE is the runtime authentication control; rebuild the image to change compiled Desktop behavior.'
fi

trap - TERM
exec node server.js
