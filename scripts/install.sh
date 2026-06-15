#!/bin/sh

set -eu

REPO="${CORAL_REPO:-withcoral/coral}"
INSTALL_DIR="${CORAL_INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${CORAL_VERSION:-}"

detect_target() {
    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Linux) os_part="unknown-linux-gnu" ;;
        Darwin) os_part="apple-darwin" ;;
        *)
            echo "Error: unsupported OS: $os" >&2
            exit 1
            ;;
    esac

    case "$arch" in
        x86_64|amd64) arch_part="x86_64" ;;
        aarch64|arm64) arch_part="aarch64" ;;
        *)
            echo "Error: unsupported architecture: $arch" >&2
            exit 1
            ;;
    esac

    echo "${arch_part}-${os_part}"
}

fetch_latest_version() {
    token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
    set -- \
        -H "Accept: application/vnd.github+json" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        -H "User-Agent: withcoral-install"
    if [ -n "$token" ]; then
        set -- "$@" -H "Authorization: Bearer ${token}"
    fi

    curl -fsSL "$@" \
        "https://api.github.com/repos/${REPO}/releases/latest" |
        sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' |
        head -n 1
}

download() {
    url="$1"
    output="$2"
    curl -fsSL "$url" -o "$output"
}

archive_for_target() {
    target="$1"

    case "$target" in
        *-apple-darwin) echo "coral-${target}.zip" ;;
        *) echo "coral-${target}.tar.gz" ;;
    esac
}

legacy_archive_for_target() {
    target="$1"

    case "$target" in
        *-apple-darwin) echo "coral-${target}.tar.gz" ;;
        *) echo "" ;;
    esac
}

extract_archive() {
    archive="$1"

    case "$archive" in
        *.zip)
            if ! command -v unzip >/dev/null 2>&1; then
                echo "Error: unzip is required to extract ${archive}." >&2
                exit 1
            fi
            unzip -q "$archive"
            ;;
        *.tar.gz)
            tar xzf "$archive"
            ;;
        *)
            echo "Error: unsupported archive format: ${archive}" >&2
            exit 1
            ;;
    esac
}

main() {
    target="$(detect_target)"
    if [ -z "$VERSION" ]; then
        VERSION="$(fetch_latest_version)"
    fi

    if [ -z "$VERSION" ]; then
        echo "Error: could not determine a Coral release version." >&2
        echo "Set CORAL_VERSION explicitly or use Homebrew instead:" >&2
        echo "  brew install withcoral/tap/coral" >&2
        exit 1
    fi

    archive="$(archive_for_target "$target")"
    base_url="https://github.com/${REPO}/releases/download/${VERSION}"
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT

    echo "Installing Coral ${VERSION} for ${target}..."

    if ! download "${base_url}/${archive}" "${tmpdir}/${archive}"; then
        legacy_archive="$(legacy_archive_for_target "$target")"
        if [ -z "$legacy_archive" ]; then
            echo "Error: could not download ${archive}." >&2
            exit 1
        fi

        echo "Could not download ${archive}; trying legacy ${legacy_archive}..."
        rm -f "${tmpdir:?}/${archive}"
        archive="$legacy_archive"
        download "${base_url}/${archive}" "${tmpdir}/${archive}"
    fi
    download "${base_url}/checksums.sha256" "${tmpdir}/checksums.sha256"

    cd "$tmpdir"
    checksum_line="$(grep -F "$archive" checksums.sha256 || true)"
    if [ -z "$checksum_line" ]; then
        echo "Error: checksum entry for ${archive} not found." >&2
        exit 1
    fi

    if command -v sha256sum >/dev/null 2>&1; then
        printf '%s\n' "$checksum_line" | sha256sum -c -
    elif command -v shasum >/dev/null 2>&1; then
        printf '%s\n' "$checksum_line" | shasum -a 256 -c -
    else
        echo "Error: sha256sum or shasum is required for checksum verification." >&2
        exit 1
    fi

    extract_archive "$archive"
    mkdir -p "$INSTALL_DIR"
    mv coral "$INSTALL_DIR/coral"
    chmod +x "$INSTALL_DIR/coral"

    echo
    echo "Installed Coral to ${INSTALL_DIR}/coral"
    if ! printf '%s' ":${PATH}:" | grep -q ":${INSTALL_DIR}:"; then
        echo
        echo "Add ${INSTALL_DIR} to your PATH:"
        echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
    fi
    echo
    echo "Verify:"
    echo "  coral --help"
    echo "Next:"
    echo "  coral onboard"
    echo "Optional agent usage:"
    echo "  withcoral.com/docs/guides/agent-usage"
    echo
    echo "To upgrade a direct install, re-run this script."
}

main
