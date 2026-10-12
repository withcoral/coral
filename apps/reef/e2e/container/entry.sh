#!/usr/bin/env bash
set -euo pipefail
# All address, CA and browser trust changes happen in this disposable container.
export E2E_TLS_DIRECTORY=/tmp/reel-tls
mkdir -p "$E2E_TLS_DIRECTORY"
chmod 700 "$E2E_TLS_DIRECTORY"
for name in trusted untrusted; do
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj "/CN=Reel $name CA" -keyout "$E2E_TLS_DIRECTORY/$name-ca.key" \
    -out "$E2E_TLS_DIRECTORY/$name-ca.pem" >/dev/null 2>&1
  openssl req -new -newkey rsa:2048 -nodes -subj /CN=sidecar \
    -keyout "$E2E_TLS_DIRECTORY/$name.key" -out "$E2E_TLS_DIRECTORY/$name.csr" >/dev/null 2>&1
  printf 'subjectAltName=DNS:sidecar\nextendedKeyUsage=serverAuth\n' > "$E2E_TLS_DIRECTORY/extensions"
  openssl x509 -req -days 1 -in "$E2E_TLS_DIRECTORY/$name.csr" \
    -CA "$E2E_TLS_DIRECTORY/$name-ca.pem" -CAkey "$E2E_TLS_DIRECTORY/$name-ca.key" \
    -CAcreateserial -extfile "$E2E_TLS_DIRECTORY/extensions" \
    -out "$E2E_TLS_DIRECTORY/$name.pem" >/dev/null 2>&1
done
cp "$E2E_TLS_DIRECTORY/trusted-ca.pem" /usr/local/share/ca-certificates/reel.crt
update-ca-certificates >/dev/null
mkdir -p "$HOME/.pki/nssdb"
certutil -N -d "sql:$HOME/.pki/nssdb" --empty-password
certutil -A -d "sql:$HOME/.pki/nssdb" -n reel -t 'C,,' -i "$E2E_TLS_DIRECTORY/trusted-ca.pem"
export E2E_CORAL_BINARY=/opt/reel/coral
export E2E_FIXTURE_BIND_HOST=0.0.0.0
export E2E_FIXTURE_PUBLIC_HOST=sidecar
# Public classification without an external request. This is container-local DNS
# evidence for consent rejection, never an address assigned on the host.
printf '\n8.8.8.8 public-consent.test\n' >> /etc/hosts
# Database storage and credentials are container-owned. LocalPostgres in run.ts
# consumes this explicit disposable URL; no daemon socket enters the container.
if [[ ${REEL_DATABASE:-sqlite} == postgres ]]; then
  pg_ctlcluster 17 main start
  runuser -u postgres -- psql -v ON_ERROR_STOP=1 -c "ALTER USER postgres PASSWORD 'reel';" >/dev/null
  runuser -u postgres -- createdb reel
  export E2E_POSTGRES_URL=postgres://postgres:reel@127.0.0.1/reel
fi
cleanup() {
  # Only this invocation's final run directories are mounted here.
  # Docker may run as root; return their evidence to the host owner.
  if [[ -n ${REEL_UID:-} && -n ${REEL_GID:-} ]]; then
    chown -R "$REEL_UID:$REEL_GID" e2e/runs
  fi
}
trap cleanup EXIT
node e2e/container/network.ts
if [[ ${REEL_SELF_CHECK:-0} == 1 ]]; then
  node e2e/scripts/self-check.ts
fi
# The desktop suite opens Electron windows on a virtual display.
xvfb-run -a --server-args='-screen 0 1280x800x24' node e2e/scripts/run.ts "$@"
