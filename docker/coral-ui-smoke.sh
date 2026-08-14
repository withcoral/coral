#!/bin/sh
# Image-level checks for the built Coral UI image: that each accepted runtime
# configuration boots and stays healthy, that each rejected one fails fast with
# a named cause, and that the container runs unprivileged on a read-only root.
#
# No Coral peer takes part. Coral UI probes its own /healthz before listening
# (apps/coral-ui/server.js), so readiness against a live peer is a separate
# concern, covered by the mocked health client in
# apps/coral-ui/app/routes/readyz.server.test.ts.
set -eu

CORAL_UI_IMAGE=${CORAL_UI_IMAGE:-coral-ui:local}
prefix="coral-ui-smoke-$$"
coral_ui="$prefix-coral-ui"
secret=coral-ui-smoke-session-secret-0123456789abcdef

cleanup() {
    docker rm -f "$coral_ui" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_health() {
    container=$1
    attempts=0
    while [ "$attempts" -lt 30 ]; do
        status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")
        [ "$status" = healthy ] && return 0
        [ "$status" = exited ] && break
        attempts=$((attempts + 1))
        sleep 1
    done
    docker logs "$container" >&2 || true
    return 1
}

wait_for_exit() {
    container=$1
    attempts=0
    while [ "$attempts" -lt 10 ]; do
        status=$(docker inspect --format '{{.State.Status}}' "$container")
        [ "$status" = exited ] && return 0
        attempts=$((attempts + 1))
        sleep 1
    done
    docker logs "$container" >&2 || true
    return 1
}

assert_identity() {
    container=$1
    identity=$(docker exec "$container" node -e 'process.stdout.write(`${process.getuid()}:${process.getgid()}`)')
    [ "$identity" = '1000:1000' ] || {
        echo "expected non-root identity 1000:1000, got $identity" >&2
        return 1
    }
}

run_required() {
    endpoint=$1
    shift
    docker run -d --read-only --name "$coral_ui" "$@" \
        -e CORAL_ENDPOINT="$endpoint" \
        -e CORAL_UI_AUTH_MODE=required \
        -e CORAL_UI_SESSION_SECRET="$secret" \
        -e CORAL_UI_AUTH_ISSUER=http://127.0.0.1:9080 \
        -e CORAL_UI_PUBLIC_URL=http://127.0.0.1:3000 \
        "$CORAL_UI_IMAGE" >/dev/null
    wait_for_health "$coral_ui"
    assert_identity "$coral_ui"
}

# A: explicit-loopback cleartext is accepted without an opt-in.
run_required http://127.0.0.1:14555
docker rm -f "$coral_ui" >/dev/null

# B: cleartext on an operator-controlled network needs the explicit opt-in.
run_required http://coral:14555 -e CORAL_UI_ALLOW_INSECURE_CORAL_ENDPOINT=1
docker rm -f "$coral_ui" >/dev/null

# C: the same non-loopback endpoint without the opt-in fails before listening
# and reports the named cause.
docker run -d --read-only --name "$coral_ui" \
    -e CORAL_ENDPOINT=http://coral.invalid:14555 \
    -e CORAL_UI_AUTH_MODE=required \
    -e CORAL_UI_SESSION_SECRET="$secret" \
    -e CORAL_UI_AUTH_ISSUER=http://127.0.0.1:9080 \
    -e CORAL_UI_PUBLIC_URL=http://127.0.0.1:3000 \
    "$CORAL_UI_IMAGE" >/dev/null
wait_for_exit "$coral_ui" || {
    echo 'invalid cleartext config unexpectedly stayed running' >&2
    exit 1
}
[ "$(docker inspect --format '{{.State.ExitCode}}' "$coral_ui")" -ne 0 ]
docker logs "$coral_ui" 2>&1 | grep -F 'CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP' >/dev/null
docker rm "$coral_ui" >/dev/null

# D: auth-disabled mode is legal and noisy, and the container stays live.
docker run -d --read-only --name "$coral_ui" \
    -e CORAL_ENDPOINT=http://127.0.0.1:14555 \
    -e CORAL_UI_AUTH_MODE=' DISABLED ' \
    "$CORAL_UI_IMAGE" >/dev/null
wait_for_health "$coral_ui"
docker logs "$coral_ui" 2>&1 | grep -F 'WARNING: CORAL_UI_AUTH_MODE=disabled' >/dev/null
assert_identity "$coral_ui"

echo 'Coral UI image smoke matrix passed'
