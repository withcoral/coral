#!/bin/sh
set -eu

REEF_IMAGE=${REEF_IMAGE:-reef:local}
CORAL_IMAGE=${CORAL_IMAGE:-coral:local}
prefix="reef-smoke-$$"
reef="$prefix-reef"
coral="$prefix-coral"
network="$prefix-net"
secret=reef-smoke-session-secret-0123456789abcdef

cleanup() {
  docker rm -f "$reef" "$coral" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
}

wait_for_coral() {
  attempts=0
  while [ "$attempts" -lt 60 ]; do
    docker exec "$coral" grpc_health_probe -addr=127.0.0.1:14555 >/dev/null 2>&1 && return 0
    attempts=$((attempts + 1))
    sleep 1
  done
  docker logs "$coral" >&2 || true
  return 1
}

assert_ready() {
  published_by=$1
  address=$(docker port "$published_by" 3000/tcp)
  attempts=0
  while [ "$attempts" -lt 30 ]; do
    curl -fsS "http://$address/readyz" | grep -F '"coral":"reachable"' >/dev/null && return 0
    attempts=$((attempts + 1))
    sleep 1
  done
  docker logs "$reef" >&2 || true
  return 1
}
trap cleanup EXIT HUP INT TERM

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
  name=$1
  endpoint=$2
  shift 2
  docker run -d --read-only --name "$name" "$@" \
    -e CORAL_ENDPOINT="$endpoint" \
    -e REEF_AUTH_MODE=required \
    -e REEF_SESSION_SECRET="$secret" \
    -e REEF_AUTH_ISSUER=http://127.0.0.1:9080 \
    -e REEF_PUBLIC_URL=http://127.0.0.1:3000 \
    "$REEF_IMAGE" >/dev/null
  wait_for_health "$name"
  assert_identity "$name"
}

# A: Reef and Coral share one network namespace and Reef uses explicit loopback.
docker run -d --name "$coral" -p 127.0.0.1::3000 "$CORAL_IMAGE" >/dev/null
wait_for_coral
run_required "$reef" http://127.0.0.1:14555 --network "container:$coral"
assert_ready "$coral"
docker rm -f "$reef" "$coral" >/dev/null

# B: cleartext h2c on an operator-controlled network requires the explicit opt-in.
docker network create "$network" >/dev/null
docker run -d --name "$coral" --network "$network" --network-alias coral "$CORAL_IMAGE" >/dev/null
wait_for_coral
run_required "$reef" http://coral:14555 --network "$network" -p 127.0.0.1::3000 \
  -e REEF_ALLOW_INSECURE_CORAL_ENDPOINT=1
assert_ready "$reef"
docker stop "$coral" >/dev/null
[ "$(docker inspect --format '{{.State.Health.Status}}' "$reef")" = healthy ]
! curl -fsS "http://$(docker port "$reef" 3000/tcp)/readyz" >/dev/null 2>&1
docker rm -f "$reef" "$coral" >/dev/null
docker network rm "$network" >/dev/null

# Invalid cleartext config fails before listening and reports the named cause.
docker run -d --read-only --name "$reef" \
  -e CORAL_ENDPOINT=http://coral.invalid:14555 \
  -e REEF_AUTH_MODE=required \
  -e REEF_SESSION_SECRET="$secret" \
  -e REEF_AUTH_ISSUER=http://127.0.0.1:9080 \
  -e REEF_PUBLIC_URL=http://127.0.0.1:3000 \
  "$REEF_IMAGE" >/dev/null
wait_for_exit "$reef" || {
  echo 'invalid cleartext config unexpectedly stayed running' >&2
  exit 1
}
[ "$(docker inspect --format '{{.State.ExitCode}}' "$reef")" -ne 0 ]
docker logs "$reef" 2>&1 | grep -F 'CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP' >/dev/null
docker rm "$reef" >/dev/null

# Auth-disabled mode is legal and noisy, and the container stays live.
docker run -d --read-only --name "$reef" \
  -e CORAL_ENDPOINT=http://127.0.0.1:14555 \
  -e REEF_AUTH_MODE=' DISABLED ' \
  "$REEF_IMAGE" >/dev/null
wait_for_health "$reef"
docker logs "$reef" 2>&1 | grep -F 'WARNING: REEF_AUTH_MODE=disabled' >/dev/null
assert_identity "$reef"

echo 'Reef image smoke matrix passed'
