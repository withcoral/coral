#!/bin/sh
# Exercises docker/entrypoint.sh inside the built Coral image. Every branch the
# entrypoint takes runs before it execs the server binary, so this matrix is
# valid against a stub binary (pull requests) and the real one alike. It
# deliberately never waits for HEALTHCHECK, which needs a real gRPC server.
set -eu

CORAL_IMAGE=${CORAL_IMAGE:-coral:local}
prefix="coral-smoke-$$"
config_path=/var/lib/coral/config/config.toml
containers=
volumes=
# Set by new_volume/start for the caller. These helpers record what they create
# so cleanup can reach it, which means they must run in this shell -- calling
# them through $(...) would lose both the name and the bookkeeping.
volume=
container=

cleanup() {
    for name in $containers; do
        docker rm -f "$name" >/dev/null 2>&1 || true
    done
    for name in $volumes; do
        docker volume rm "$name" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

new_volume() {
    volume="$prefix-$1"
    docker volume create "$volume" >/dev/null
    volumes="$volumes $volume"
}

start() {
    container="$prefix-$1"
    shift
    containers="$containers $container"
    docker run -d --name "$container" "$@" "$CORAL_IMAGE" >/dev/null
}

wait_for_log() {
    needle=$1
    attempts=0
    while [ "$attempts" -lt 30 ]; do
        if docker logs "$container" 2>&1 | grep -Fq "$needle"; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    docker logs "$container" >&2 || true
    echo "expected a log line containing: $needle" >&2
    return 1
}

wait_for_exit() {
    attempts=0
    while [ "$attempts" -lt 15 ]; do
        [ "$(docker inspect --format '{{.State.Status}}' "$container")" = exited ] && return 0
        attempts=$((attempts + 1))
        sleep 1
    done
    docker logs "$container" >&2 || true
    echo "container $container did not exit" >&2
    return 1
}

# Read the config back through the image itself, so this works no matter which
# uid owns the volume.
config_text() {
    docker run --rm -v "$1:/var/lib/coral" --entrypoint /bin/sh "$CORAL_IMAGE" \
        -c "cat $config_path"
}

# Compare by digest rather than by shell string: command substitution strips
# trailing newlines, so a string compare cannot see them and "verbatim" would
# go unverified. Both sides are hashed by the same sha256sum in the same image.
config_digest() {
    docker run --rm -v "$1:/var/lib/coral" --entrypoint /bin/sh "$CORAL_IMAGE" \
        -c "sha256sum < $config_path | cut -d' ' -f1"
}

digest_of_stdin() {
    docker run --rm -i --entrypoint /bin/sh "$CORAL_IMAGE" \
        -c "sha256sum | cut -d' ' -f1"
}

# 1. First start on an empty volume seeds the built-in starter config.
new_volume starter
starter=$volume
start starter -v "$starter:/var/lib/coral"
wait_for_log 'seeded starter'
config_text "$starter" | grep -Fq 'bind_addr = "0.0.0.0:14555"'
docker rm -f "$container" >/dev/null

# 2. A restart reuses the existing config and never rewrites it.
before=$(config_digest "$starter")
start reuse -v "$starter:/var/lib/coral"
wait_for_log 'using existing'
[ "$(config_digest "$starter")" = "$before" ] || {
    echo 'restart rewrote an existing config' >&2
    exit 1
}
docker rm -f "$container" >/dev/null

# 3. CORAL_SEED_CONFIG is written verbatim on a first start, trailing bytes and
# all.
seed='# operator supplied
[server]
bind_addr = "0.0.0.0:14999"
'
seed_digest=$(printf '%s' "$seed" | digest_of_stdin)
new_volume seeded
seeded=$volume
start seeded -v "$seeded:/var/lib/coral" -e CORAL_SEED_CONFIG="$seed"
wait_for_log "seeded $config_path from CORAL_SEED_CONFIG"
[ "$(config_digest "$seeded")" = "$seed_digest" ] || {
    echo 'CORAL_SEED_CONFIG was not written verbatim' >&2
    exit 1
}
docker rm -f "$container" >/dev/null

# 4. The seed applies only once: an existing config wins and says so.
start seeded-again -v "$seeded:/var/lib/coral" -e CORAL_SEED_CONFIG='[server]'
wait_for_log 'CORAL_SEED_CONFIG ignored'
[ "$(config_digest "$seeded")" = "$seed_digest" ] || {
    echo 'CORAL_SEED_CONFIG overwrote an existing config' >&2
    exit 1
}
docker rm -f "$container" >/dev/null

# 5. An unwritable volume fails fast with one actionable message.
new_volume readonly
start readonly -v "$volume:/var/lib/coral:ro"
wait_for_exit
[ "$(docker inspect --format '{{.State.ExitCode}}' "$container")" -ne 0 ]
docker logs "$container" 2>&1 | grep -Fq 'coral-entrypoint: FATAL'
docker logs "$container" 2>&1 | grep -Fq "run with '--user <uid>:0'"

echo 'Coral image entrypoint matrix passed'
