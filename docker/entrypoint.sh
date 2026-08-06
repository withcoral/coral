#!/bin/sh
# Coral container entrypoint. On first start with an empty volume it seeds a
# starter config.toml — from $CORAL_SEED_CONFIG when set (verbatim), else a
# built-in default — and it NEVER modifies an existing one. All configuration
# is editing that file. See https://withcoral.com/docs/guides/self-host-with-docker
set -eu

# PID 1 ignores default-action signals on Linux. Install handlers before any
# filesystem preparation so a stop during startup cannot strand the container.
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

CONFIG_FILE="$CORAL_CONFIG_DIR/config.toml"

fatal() {
    echo "coral-entrypoint: FATAL: $1" >&2
    echo "coral-entrypoint: the volume mounted at /var/lib/coral must be writable by uid $(id -u):" \
         "run with '--user <uid>:0' (GID 0), set a Kubernetes fsGroup, or pre-chown the volume;" \
         "read-only mounts are not supported (Coral locks and writes its state root at startup)." >&2
    exit 1
}

# Everything we create is private to the runtime uid. mktemp creates 0600
# regardless; this governs mkdir (0700), matching coral's own hardening
# (ensure_private_dir chmods 0700, storage/fs.rs:22-27).
umask 077

# 1. The config dir is a SUBDIRECTORY of the volume root: docker creates named-
#    volume roots as root:root, and coral unconditionally chmods its config dir
#    to 0700 at startup — EPERM on a dir the runtime uid does not own (spike-1
#    correction 1). A subdir created here is owned by the runtime uid.
mkdir -p "$CORAL_CONFIG_DIR" 2>/dev/null || fatal "cannot create $CORAL_CONFIG_DIR"

# 2. Fail fast with ONE actionable message on read-only or wrongly-owned
#    volumes, before the binary hits a less obvious flock/DB error. Runs every
#    start: a volume that became read-only after first boot fails here too.
probe="$(mktemp "$CORAL_CONFIG_DIR/.write-probe.XXXXXX" 2>/dev/null)" \
    || fatal "$CORAL_CONFIG_DIR is not writable"
rm -f "$probe"

# 3. Seed only when NOTHING exists at the path — no file, no directory, no
#    symlink (dangling included: a dangling symlink is user intent, e.g. a
#    not-yet-mounted secret; coral itself stats via symlink_metadata,
#    server_config.rs:53). Atomic: mktemp in the same directory, then hard-link
#    the completed temporary file into place. link(2) never overwrites an
#    existing path, so concurrent starts preserve the first complete config.
if [ ! -e "$CONFIG_FILE" ] && [ ! -L "$CONFIG_FILE" ]; then
    seed="$(mktemp "$CORAL_CONFIG_DIR/.config.toml.seed.XXXXXX")"
    trap 'rm -f "$seed"' EXIT
    if [ -n "${CORAL_SEED_CONFIG:-}" ]; then
        # Whole-file seed supplied by the operator: written VERBATIM (no
        # templating, no parsing) through the same atomic path. Once-only:
        # this branch is unreachable when any config exists.
        printf '%s' "$CORAL_SEED_CONFIG" > "$seed"
        seed_source="CORAL_SEED_CONFIG"
    else
        cat > "$seed" <<'EOF'
# Generated once by the Coral Docker image on first start. The image will
# never modify this file again; edit it and restart the container.
# Reference: https://withcoral.com/docs/reference/configuration

[server]
bind_addr = "0.0.0.0:14555"
EOF
        seed_source="starter"
    fi

    if ln -T "$seed" "$CONFIG_FILE" 2>/dev/null; then
        rm -f "$seed"
        trap - EXIT
        if [ "$seed_source" = "CORAL_SEED_CONFIG" ]; then
            echo "coral-entrypoint: seeded $CONFIG_FILE from CORAL_SEED_CONFIG" >&2
        else
            echo "coral-entrypoint: seeded starter $CONFIG_FILE (bind_addr 0.0.0.0:14555)" >&2
        fi
    elif [ -e "$CONFIG_FILE" ] || [ -L "$CONFIG_FILE" ]; then
        rm -f "$seed"
        trap - EXIT
        if [ -n "${CORAL_SEED_CONFIG:-}" ]; then
            echo "coral-entrypoint: $CONFIG_FILE exists; CORAL_SEED_CONFIG ignored (seed applies only to first start)" >&2
        fi
        echo "coral-entrypoint: using existing $CONFIG_FILE" >&2
    else
        fatal "cannot atomically create $CONFIG_FILE"
    fi
else
    if [ -n "${CORAL_SEED_CONFIG:-}" ]; then
        echo "coral-entrypoint: $CONFIG_FILE exists; CORAL_SEED_CONFIG ignored (seed applies only to first start)" >&2
    fi
    echo "coral-entrypoint: using existing $CONFIG_FILE" >&2
fi

exec /usr/local/bin/coral server "$@"
