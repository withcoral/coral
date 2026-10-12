#!/bin/sh
# Image tests for the Coral runtime and Reef images, on fresh state only.
#
#   docker/image-test.sh [runtime] [reef] [pair]
#
# With no argument it runs all three parts. A workflow that holds one image
# names the part it can run:
#
#   runtime  needs CORAL_IMAGE: the entrypoint contract and a serving runtime.
#   reef     needs CORAL_WEB_IMAGE: the accepted and rejected configurations.
#   pair     needs both: a first start, with Reef in the runtime's network
#            namespace and every listener answering health and readiness.
#
# Product journeys belong to Reel, which runs the same binaries. Each probe
# runs inside a container, so nothing is published on the host and the script
# works with Docker or Podman.
set -eu

ENGINE=${CONTAINER_ENGINE:-docker}
CORAL_IMAGE=${CORAL_IMAGE:-coral:local}
CORAL_WEB_IMAGE=${CORAL_WEB_IMAGE:-coral-web:local}
prefix="coral-image-test-$$"
config_path=/var/lib/coral/config/config.toml
secret=coral-web-test-session-secret-0123456789abcdef
containers=
volumes=
# Set by new_volume/start for the caller. These helpers record what they create
# so cleanup can reach it, which means they must run in this shell.
volume=
container=

cleanup() {
    for name in $containers; do
        "$ENGINE" rm -f "$name" >/dev/null 2>&1 || true
    done
    for name in $volumes; do
        "$ENGINE" volume rm "$name" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

fail() {
    echo "image-test: $1" >&2
    exit 1
}

new_volume() {
    volume="$prefix-$1"
    "$ENGINE" volume create "$volume" >/dev/null
    volumes="$volumes $volume"
}

# start <name> <image> [run options...]
start() {
    container="$prefix-$1"
    image=$2
    shift 2
    containers="$containers $container"
    "$ENGINE" run -d --name "$container" "$@" "$image" >/dev/null
}

remove() {
    "$ENGINE" rm -f "$1" >/dev/null
}

logs() {
    "$ENGINE" logs "$1" 2>&1
}

state() {
    "$ENGINE" inspect --format '{{.State.Status}}' "$1"
}

# Runs the image's declared HEALTHCHECK inside the container. An engine's own
# health status depends on its scheduler; the declared command does not. exec
# bypasses the HEALTHCHECK timeout, so the probe gets its own deadline.
healthcheck() {
    check=$("$ENGINE" inspect --format '{{index .Config.Healthcheck.Test 1}}' "$1")
    "$ENGINE" exec "$1" timeout 5 /bin/sh -c "$check" >/dev/null 2>&1
}

wait_healthy() {
    attempts=0
    while [ "$attempts" -lt 60 ]; do
        healthcheck "$1" && return 0
        [ "$(state "$1")" = running ] || break
        attempts=$((attempts + 1))
        sleep 1
    done
    logs "$1" >&2 || true
    fail "$1 did not pass its health check"
}

wait_exited() {
    attempts=0
    while [ "$attempts" -lt 30 ]; do
        [ "$(state "$1")" = exited ] && return 0
        attempts=$((attempts + 1))
        sleep 1
    done
    logs "$1" >&2 || true
    fail "$1 did not exit"
}

assert_failed() {
    wait_exited "$1"
    [ "$("$ENGINE" inspect --format '{{.State.ExitCode}}' "$1")" -ne 0 ] ||
        fail "$1 exited successfully"
}

assert_log() {
    logs "$1" | grep -Fq "$2" || {
        logs "$1" >&2
        fail "$1 did not log: $2"
    }
}

# http_status <reef container> <url>: Reef's image carries Node, the runtime
# image carries no HTTP client, and both share one network namespace.
http_status() {
    "$ENGINE" exec "$1" node -e \
        'fetch(process.argv[1], { signal: AbortSignal.timeout(5000) }).then((r) => console.log(r.status), () => console.log("unreachable"))' "$2"
}

assert_status() {
    status=$(http_status "$1" "$2")
    [ "$status" = "$3" ] || fail "$2 answered $status, expected $3"
}

assert_identity() {
    identity=$("$ENGINE" exec "$1" /bin/sh -c 'echo "$(id -u):$(id -g)"')
    [ "$identity" = "$2" ] || fail "$1 runs as $identity, expected $2"
}

# Compare by digest: command substitution strips trailing newlines, so a
# string compare cannot prove a verbatim write.
config_digest() {
    "$ENGINE" run --rm -v "$1:/var/lib/coral" --entrypoint /bin/sh "$CORAL_IMAGE" \
        -c "sha256sum < $config_path | cut -d' ' -f1"
}

digest_of_stdin() {
    "$ENGINE" run --rm -i --entrypoint /bin/sh "$CORAL_IMAGE" -c "sha256sum | cut -d' ' -f1"
}

# start_reef <name> <auth mode> <endpoint> [run options...]
start_reef() {
    name=$1
    mode=$2
    endpoint=$3
    shift 3
    start "$name" "$CORAL_WEB_IMAGE" --read-only \
        -e CORAL_ENDPOINT="$endpoint" \
        -e CORAL_WEB_AUTH_MODE="$mode" \
        -e CORAL_WEB_SESSION_SECRET="$secret" \
        -e CORAL_WEB_AUTH_ISSUER=http://127.0.0.1:9080 \
        -e CORAL_WEB_PUBLIC_URL=http://127.0.0.1:3000 \
        "$@"
}

test_runtime() {
    # A first start on an empty volume writes the starter config and serves.
    new_volume starter
    starter=$volume
    start coral "$CORAL_IMAGE" -v "$starter:/var/lib/coral"
    wait_healthy "$container"
    assert_log "$container" 'wrote starter'
    assert_identity "$container" 65532:0
    remove "$container"

    # A restart without CORAL_CONFIG reuses the state and never rewrites the config.
    before=$(config_digest "$starter")
    start coral-restart "$CORAL_IMAGE" -v "$starter:/var/lib/coral"
    wait_healthy "$container"
    assert_log "$container" 'using existing'
    [ "$(config_digest "$starter")" = "$before" ] || fail 'a restart rewrote an existing config'
    remove "$container"

    # CORAL_CONFIG is written verbatim, trailing bytes and all, and the
    # runtime serves from it.
    supplied='# operator supplied
[listeners.grpc]
bind = "127.0.0.1:14999"

[listeners.web]
public_url = "http://127.0.0.1:3000"
'
    supplied_digest=$(printf '%s' "$supplied" | digest_of_stdin)
    new_volume operator
    operator=$volume
    start coral-config "$CORAL_IMAGE" -v "$operator:/var/lib/coral" \
        -e CORAL_CONFIG="$supplied" -e CORAL_HEALTHCHECK_ADDR=127.0.0.1:14999
    wait_healthy "$container"
    assert_log "$container" "wrote $config_path from CORAL_CONFIG"
    [ "$(config_digest "$operator")" = "$supplied_digest" ] || fail 'CORAL_CONFIG was not written verbatim'
    remove "$container"

    # CORAL_CONFIG is the file's source of truth: a changed value replaces
    # the existing config on the next start, and the runtime serves from it.
    changed='# operator changed
[listeners.grpc]
bind = "127.0.0.1:14998"

[listeners.web]
public_url = "http://127.0.0.1:3000"
'
    changed_digest=$(printf '%s' "$changed" | digest_of_stdin)
    start coral-config-changed "$CORAL_IMAGE" -v "$operator:/var/lib/coral" \
        -e CORAL_CONFIG="$changed" -e CORAL_HEALTHCHECK_ADDR=127.0.0.1:14998
    wait_healthy "$container"
    assert_log "$container" "wrote $config_path from CORAL_CONFIG"
    [ "$(config_digest "$operator")" = "$changed_digest" ] || fail 'a changed CORAL_CONFIG did not replace the config'
    remove "$container"

    # Unsetting CORAL_CONFIG keeps the file it last wrote.
    start coral-config-kept "$CORAL_IMAGE" -v "$operator:/var/lib/coral" \
        -e CORAL_HEALTHCHECK_ADDR=127.0.0.1:14998
    wait_healthy "$container"
    assert_log "$container" 'using existing'
    [ "$(config_digest "$operator")" = "$changed_digest" ] || fail 'a start without CORAL_CONFIG rewrote the config'
    remove "$container"

    # CORAL_CONFIG never replaces a mounted config: a directory or symlink at
    # the path fails fast and stays as it was.
    new_volume mounted
    mounted=$volume
    "$ENGINE" run --rm -v "$mounted:/var/lib/coral" --entrypoint /bin/sh "$CORAL_IMAGE" \
        -c "mkdir -p $config_path"
    start coral-config-mounted "$CORAL_IMAGE" -v "$mounted:/var/lib/coral" \
        -e CORAL_CONFIG="$supplied"
    assert_failed "$container"
    assert_log "$container" 'CORAL_CONFIG is set but'
    "$ENGINE" run --rm -v "$mounted:/var/lib/coral" --entrypoint /bin/sh "$CORAL_IMAGE" \
        -c "[ -d $config_path ]" || fail 'CORAL_CONFIG replaced a directory at the config path'

    # An unwritable volume fails fast with one actionable message.
    new_volume readonly
    start coral-readonly "$CORAL_IMAGE" -v "$volume:/var/lib/coral:ro"
    assert_failed "$container"
    assert_log "$container" 'coral-entrypoint: FATAL'
    assert_log "$container" "run with '--user <uid>:0'"
}

# Reef's own /healthz needs no Coral peer, so each configuration runs alone.
test_reef() {
    # Sign-in off is legal and noisy.
    start_reef reef-local ' DISABLED ' http://127.0.0.1:14555
    wait_healthy "$container"
    assert_log "$container" 'WARNING: CORAL_WEB_AUTH_MODE=disabled'
    assert_identity "$container" 1000:1000
    remove "$container"

    # With sign-in on, loopback cleartext needs no opt-in, and cleartext to
    # another host needs an explicit one.
    start_reef reef-loopback required http://127.0.0.1:14555
    wait_healthy "$container"
    remove "$container"
    start_reef reef-cleartext required http://coral:14555 -e CORAL_WEB_ALLOW_INSECURE_CORAL_ENDPOINT=1
    wait_healthy "$container"
    remove "$container"

    # The same endpoint without the opt-in fails before listening and names the cause.
    start_reef reef-rejected required http://coral.invalid:14555
    assert_failed "$container"
    assert_log "$container" 'CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP'
}

# A first start: the runtime on its starter config, and Reef in its network
# namespace with sign-in off. Reef's readiness follows its Coral dependency,
# so its /readyz proves the pair is wired.
test_pair() {
    new_volume pair
    start pair-coral "$CORAL_IMAGE" -v "$volume:/var/lib/coral"
    coral=$container
    wait_healthy "$coral"
    start_reef pair-reef disabled http://127.0.0.1:14555 --network "container:$coral"
    reef=$container
    wait_healthy "$reef"
    for origin in http://127.0.0.1:3000 http://127.0.0.1:14556 http://127.0.0.1:8779; do
        assert_status "$reef" "$origin/healthz" 200
        assert_status "$reef" "$origin/readyz" 200
    done
    remove "$reef"
    remove "$coral"
}

[ "$#" -gt 0 ] || set -- runtime reef pair
for part in "$@"; do
    case "$part" in
        runtime) test_runtime ;;
        reef) test_reef ;;
        pair) test_pair ;;
        *) fail "unknown part: $part (expected runtime, reef, or pair)" ;;
    esac
    echo "image-test: $part passed"
done
