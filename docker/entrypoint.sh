#!/bin/sh
# Coral container entrypoint. It prepares config.toml, then starts Coral.
#
#   CORAL_CONFIG set    Writes config.toml from it, verbatim, on EVERY start.
#                       The variable is the file's source of truth: an edit
#                       to the file on the volume lasts until the next start.
#   CORAL_CONFIG unset  Keeps an existing config.toml. On a first start with
#                       an empty volume it writes the starter below. To change
#                       a setting, edit the file and restart the container.
#
# Coral never writes config.toml itself, so neither path loses state.
#
# The starter runs Coral with sign-in off, so every listener is on loopback.
# Run Reef in this container's network namespace to reach it, or run both on
# the host network. To serve a network, set CORAL_CONFIG to a config with
# [auth] instead.
set -eu

probe=
staged=

cleanup() {
    if [ -n "$probe" ]; then
        rm -f "$probe" 2>/dev/null || :
    fi
    if [ -n "$staged" ]; then
        rm -f "$staged" 2>/dev/null || :
    fi
}

# PID 1 ignores default-action signals on Linux. Install cleanup and signal
# handlers before any filesystem preparation so a stop during startup cannot
# strand the container or leave temporary files in its persistent state.
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

CONFIG_FILE="$CORAL_CONFIG_DIR/config.toml"

fatal() {
    echo "coral-entrypoint: FATAL: $1" >&2
    exit 1
}

unwritable() {
    echo "coral-entrypoint: FATAL: $1" >&2
    echo "coral-entrypoint: the volume mounted at /var/lib/coral must be writable by uid $(id -u):" \
         "run with '--user <uid>:0' (GID 0), set a Kubernetes fsGroup, or pre-chown the volume;" \
         "read-only mounts are not supported (Coral locks and writes its state at startup)." >&2
    exit 1
}

# Everything created here is private to the runtime uid, as Coral keeps its
# own state. mktemp creates 0600 regardless; this governs mkdir (0700).
umask 077

# 1. The config dir is a subdirectory of the volume root. The engine creates a
#    named volume's root as root:root, and Coral sets its config dir to 0700 at
#    startup, which fails on a directory the runtime uid does not own.
mkdir -p "$CORAL_CONFIG_DIR" 2>/dev/null || unwritable "cannot create $CORAL_CONFIG_DIR"

# 2. Fail fast with ONE actionable message on read-only or wrongly-owned
#    volumes, before the binary hits a less obvious flock/DB error. Runs every
#    start: a volume that became read-only after first boot fails here too.
probe="$(mktemp "$CORAL_CONFIG_DIR/.write-probe.XXXXXX" 2>/dev/null)" \
    || unwritable "$CORAL_CONFIG_DIR is not writable"
rm -f "$probe"
probe=

# 3. Prepare config.toml. Every write stages a complete file in the same
#    directory first, so a reader never sees a partial config and a stop
#    mid-write leaves nothing behind. The staged name is cleaned up on exit
#    because Coral refuses a fresh state directory that holds a foreign file.
if [ -n "${CORAL_CONFIG:-}" ]; then
    # Operator-owned config: written VERBATIM (no templating, no parsing) on
    # every start. A directory or symlink at the path is a different operator
    # intent (a mounted secret, say) that this script must not replace, so the
    # two cannot be combined.
    if [ -d "$CONFIG_FILE" ] || [ -L "$CONFIG_FILE" ]; then
        fatal "CORAL_CONFIG is set but $CONFIG_FILE is a directory or symlink; unset CORAL_CONFIG or remove it"
    fi
    staged="$(mktemp "$CORAL_CONFIG_DIR/.config.toml.XXXXXX")"
    printf '%s' "$CORAL_CONFIG" > "$staged"
    # rename(2) replaces a regular file atomically, so concurrent starts with
    # the same value leave one complete config.
    mv -f "$staged" "$CONFIG_FILE" || unwritable "cannot replace $CONFIG_FILE"
    staged=
    echo "coral-entrypoint: wrote $CONFIG_FILE from CORAL_CONFIG" >&2
elif [ ! -e "$CONFIG_FILE" ] && [ ! -L "$CONFIG_FILE" ]; then
    # Starter only when NOTHING exists at the path — no file, no directory, no
    # symlink (dangling included: a dangling symlink is operator intent, such
    # as a secret not yet mounted). link(2) never overwrites an existing path,
    # so concurrent first starts preserve the first complete config.
    staged="$(mktemp "$CORAL_CONFIG_DIR/.config.toml.XXXXXX")"
    cat > "$staged" <<'EOF'
# Generated once by the Coral image on a first start without CORAL_CONFIG.
# The image does not modify this file again: edit it and restart the
# container, or set CORAL_CONFIG to replace it on every start.
#
# Sign-in is off, so Coral accepts only loopback listeners. Add [auth] before
# you bind a listener to another address.

[listeners.grpc]
bind = "127.0.0.1:14555"

[listeners.mcp]
bind = "127.0.0.1:14556"

[listeners.auth]
bind = "127.0.0.1:8779"

[listeners.web]
public_url = "http://127.0.0.1:3000"
EOF
    if ln -T "$staged" "$CONFIG_FILE" 2>/dev/null; then
        rm -f "$staged"
        staged=
        echo "coral-entrypoint: wrote starter $CONFIG_FILE (sign-in off, loopback listeners)" >&2
    elif [ -e "$CONFIG_FILE" ] || [ -L "$CONFIG_FILE" ]; then
        rm -f "$staged"
        staged=
        echo "coral-entrypoint: using existing $CONFIG_FILE" >&2
    else
        unwritable "cannot atomically create $CONFIG_FILE"
    fi
else
    echo "coral-entrypoint: using existing $CONFIG_FILE" >&2
fi

exec /usr/local/bin/coral "$@"
