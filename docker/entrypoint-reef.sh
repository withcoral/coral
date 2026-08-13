#!/bin/sh
set -eu

fatal() {
  echo "reef-entrypoint: FATAL: $1" >&2
  echo "reef-entrypoint: $2" >&2
  exit 1
}

warn() {
  echo "reef-entrypoint: WARNING: $1" >&2
  echo "reef-entrypoint: $2" >&2
}

trap 'exit 143' TERM

[ -n "${CORAL_ENDPOINT:-}" ] || fatal \
  'CORAL_ENDPOINT is required.' \
  'Use loopback in a shared network namespace, HTTPS, or explicit cleartext h2c with REEF_ALLOW_INSECURE_CORAL_ENDPOINT=1.'

[ -n "${REEF_AUTH_MODE:-}" ] || fatal \
  'REEF_AUTH_MODE is required.' \
  'Set REEF_AUTH_MODE=required or REEF_AUTH_MODE=disabled.'

REEF_AUTH_MODE=$(
  printf '%s' "$REEF_AUTH_MODE" \
    | tr '[:upper:]' '[:lower:]' \
    | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'
)
export REEF_AUTH_MODE

case "$REEF_AUTH_MODE" in
  required)
    [ -n "${REEF_SESSION_SECRET:-}" ] || fatal \
      'REEF_SESSION_SECRET is required when REEF_AUTH_MODE=required.' \
      'Set a secret of at least 32 characters; Reef validates its shape at startup.'
    [ -n "${REEF_AUTH_ISSUER:-}" ] || fatal \
      'REEF_AUTH_ISSUER is required when REEF_AUTH_MODE=required.' \
      'Set the HTTPS or explicit-loopback OAuth issuer; Reef validates it at startup.'
    [ -n "${REEF_PUBLIC_URL:-}" ] || fatal \
      'REEF_PUBLIC_URL is required when REEF_AUTH_MODE=required.' \
      'Set the externally reachable HTTPS or explicit-loopback Reef origin.'
    ;;
  disabled)
    echo 'reef-entrypoint: WARNING: REEF_AUTH_MODE=disabled serves the Coral console with no' >&2
    echo 'reef-entrypoint: login at all — anyone who can reach this port can read every source' >&2
    echo 'reef-entrypoint: and POST new source credentials. It ALSO selects Reef’s gRPC-Web' >&2
    echo 'reef-entrypoint: transport, while standalone `coral server` serves native gRPC.' >&2
    echo 'reef-entrypoint: Without a gRPC-Web-capable proxy in front of Coral,' >&2
    echo 'reef-entrypoint: every page this container serves will fail to load data.' >&2
    echo 'reef-entrypoint: Set REEF_AUTH_MODE=required unless you know you have both' >&2
    echo 'reef-entrypoint: a trusted network and such a proxy.' >&2
    ;;
  *)
    fatal 'REEF_AUTH_MODE must be required or disabled.' \
      'Set one of the documented values.'
    ;;
esac

if [ -n "${CORAL_DESKTOP_APP:-}" ] || [ -n "${VITE_CORAL_DESKTOP_APP:-}" ]; then
  warn 'Desktop build markers are set but have no effect at container runtime.' \
    'REEF_AUTH_MODE is the runtime authentication control; rebuild the image to change compiled Desktop behavior.'
fi

trap - TERM
exec node server.js
