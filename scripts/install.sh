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
    curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" |
        sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' |
        head -n 1
}

download() {
    url="$1"
    output="$2"
    curl -fsSL "$url" -o "$output"
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

    archive="coral-${target}.tar.gz"
    base_url="https://github.com/${REPO}/releases/download/${VERSION}"
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT

    echo "Installing Coral ${VERSION} for ${target}..."

    download "${base_url}/${archive}" "${tmpdir}/${archive}"
    download "${base_url}/checksums.sha256" "${tmpdir}/checksums.sha256"

    cd "$tmpdir"
    if command -v sha256sum >/dev/null 2>&1; then
        grep "  ${archive}$" checksums.sha256 | sha256sum -c -
    elif command -v shasum >/dev/null 2>&1; then
        grep "  ${archive}$" checksums.sha256 | shasum -a 256 -c -
    else
        echo "Error: sha256sum or shasum is required for checksum verification." >&2
        exit 1
    fi

    tar xzf "$archive"
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
