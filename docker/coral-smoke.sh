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

cleanup() {
    for container in $containers; do
        docker rm -f "$container" >/dev/null 2>&1 || true
    done
    for volume in $volumes; do
        docker volume rm "$volume" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT HUP INT TERM

new_volume() {
    name="$prefix-$1"
    docker volume create "$name" >/dev/null
    volumes="$volumes $name"
    printf '%s' "$name"
}

start() {
    name="$prefix-$1"
    shift
    containers="$containers $name"
    docker run -d --name "$name" "$@" "$CORAL_IMAGE" >/dev/null
    printf '%s' "$name"
}

wait_for_log() {
    container=$1
    needle=$2
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
    container=$1
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

# Read the seeded config back through the image itself, so this works no matter
# which uid owns the volume.
config_of() {
    docker run --rm -v "$1:/var/lib/coral" --entrypoint /bin/sh "$CORAL_IMAGE" \
        -c "cat $config_path"
}

# 1. First start on an empty volume seeds the built-in starter config.
starter=$(new_volume starter)
container=$(start starter -v "$starter:/var/lib/coral")
wait_for_log "$container" 'seeded starter'
config_of "$starter" | grep -Fq 'bind_addr = "0.0.0.0:14555"'
docker rm -f "$container" >/dev/null

# 2. A restart reuses the existing config and never rewrites it.
before=$(config_of "$starter")
container=$(start reuse -v "$starter:/var/lib/coral")
wait_for_log "$container" 'using existing'
[ "$(config_of "$starter")" = "$before" ] || {
    echo 'restart rewrote an existing config' >&2
    exit 1
}
docker rm -f "$container" >/dev/null

# 3. CORAL_SEED_CONFIG is written verbatim on a first start.
seed='# operator supplied
[server]
bind_addr = "0.0.0.0:14999"'
seeded=$(new_volume seeded)
container=$(start seeded -v "$seeded:/var/lib/coral" -e CORAL_SEED_CONFIG="$seed")
wait_for_log "$container" "seeded $config_path from CORAL_SEED_CONFIG"
[ "$(config_of "$seeded")" = "$seed" ] || {
    echo 'CORAL_SEED_CONFIG was not written verbatim' >&2
    exit 1
}
docker rm -f "$container" >/dev/null

# 4. The seed applies only once: an existing config wins and says so.
container=$(start seeded-again -v "$seeded:/var/lib/coral" -e CORAL_SEED_CONFIG='[server]')
wait_for_log "$container" 'CORAL_SEED_CONFIG ignored'
[ "$(config_of "$seeded")" = "$seed" ] || {
    echo 'CORAL_SEED_CONFIG overwrote an existing config' >&2
    exit 1
}
docker rm -f "$container" >/dev/null

# 5. An unwritable volume fails fast with one actionable message.
readonly_volume=$(new_volume readonly)
container=$(start readonly -v "$readonly_volume:/var/lib/coral:ro")
wait_for_exit "$container"
[ "$(docker inspect --format '{{.State.ExitCode}}' "$container")" -ne 0 ]
docker logs "$container" 2>&1 | grep -Fq 'coral-entrypoint: FATAL'
docker logs "$container" 2>&1 | grep -Fq "run with '--user <uid>:0'"

echo 'Coral image entrypoint matrix passed'
