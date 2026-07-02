#!/bin/sh
# shellcheck shell=sh
# Installer for comet-enrich: downloads a release binary, verifies its
# checksum, installs it, and sets up shell completions.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/cometadata/comet-enrich/main/install.sh | sh
#   curl -fsSL .../install.sh | sh -s -- --version v0.1.0-rc1
#   sh install.sh --version v0.1.0-rc1 --bin-dir ~/bin
#
# Options (flags override the matching environment variable):
#   --version <tag>       Release tag to install        [COMET_ENRICH_VERSION, default: latest]
#   --bin-dir <dir>       Install directory             [COMET_ENRICH_BIN_DIR, default: ~/.local/bin]
#   --target <suffix>     Release asset suffix; skips platform detection
#                                                       [COMET_ENRICH_TARGET, default: detected]
#   --base-url <url>      Base download URL             [COMET_ENRICH_BASE_URL]
#   --no-completions      Skip shell completion setup
#   -h, --help            Show this help

set -eu

REPO="cometadata/comet-enrich"
BIN_NAME="comet-enrich"
KNOWN_TARGETS="x86_64-v3-unknown-linux-musl x86_64-unknown-linux-musl aarch64-unknown-linux-musl aarch64-apple-darwin"

version="${COMET_ENRICH_VERSION:-}"
bin_dir="${COMET_ENRICH_BIN_DIR:-$HOME/.local/bin}"
target="${COMET_ENRICH_TARGET:-}"
base_url="${COMET_ENRICH_BASE_URL:-https://github.com/$REPO/releases/download}"
completions=1
tmpdir=""

info() { printf '%s\n' "$BIN_NAME install: $*" >&2; }
warn() { printf '%s\n' "$BIN_NAME install: warning: $*" >&2; }
die() {
    printf '%s\n' "$BIN_NAME install: error: $*" >&2
    exit 1
}

usage() {
    cat <<EOF
Installer for $BIN_NAME: downloads a release binary, verifies its checksum,
installs it, and sets up shell completions.

Usage:
  curl -fsSL https://raw.githubusercontent.com/$REPO/main/install.sh | sh
  curl -fsSL .../install.sh | sh -s -- --version v0.1.0-rc1
  sh install.sh --version v0.1.0-rc1 --bin-dir ~/bin

Options (flags override the matching environment variable):
  --version <tag>     Release tag to install     [COMET_ENRICH_VERSION, default: latest]
  --bin-dir <dir>     Install directory          [COMET_ENRICH_BIN_DIR, default: ~/.local/bin]
  --target <suffix>   Release asset suffix; skips platform detection
                      one of: $KNOWN_TARGETS
                                                 [COMET_ENRICH_TARGET, default: detected]
  --base-url <url>    Base download URL          [COMET_ENRICH_BASE_URL]
  --no-completions    Skip shell completion setup
  -h, --help          Show this help
EOF
}

fetch() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$1"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "$1"
    else
        die "need curl or wget to download files"
    fi
}

download() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL -o "$2" "$1"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$2" "$1"
    else
        die "need curl or wget to download files"
    fi
}

detect_platform() {
    if [ -n "$target" ]; then
        case " $KNOWN_TARGETS " in
            *" $target "*) ;;
            *) die "unknown --target '$target'; expected one of: $KNOWN_TARGETS" ;;
        esac
        return
    fi
    os=$(uname -s)
    arch=$(uname -m)
    case "$os,$arch" in
        Linux,x86_64)
            if grep -qi avx2 /proc/cpuinfo 2>/dev/null; then
                target="x86_64-v3-unknown-linux-musl"
                info "detected Linux x86_64 with AVX2; using the x86-64-v3 build"
            else
                target="x86_64-unknown-linux-musl"
                info "detected Linux x86_64 without AVX2; using the baseline build"
            fi
            ;;
        Linux,aarch64 | Linux,arm64)
            target="aarch64-unknown-linux-musl"
            ;;
        Darwin,arm64)
            target="aarch64-apple-darwin"
            ;;
        Darwin,x86_64)
            die "no prebuilt binary for Intel macOS; build from source (see docs/installation.md)"
            ;;
        *)
            die "no prebuilt binary for $os/$arch; build from source (see docs/installation.md)"
            ;;
    esac
}

resolve_version() {
    if [ -n "$version" ]; then
        case "$version" in
            v*) ;;
            *) version="v$version" ;;
        esac
        return
    fi
    info "looking up the latest release"
    version=$(fetch "https://api.github.com/repos/$REPO/releases/latest" 2>/dev/null |
        sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1) || version=""
    if [ -z "$version" ]; then
        die "could not determine the latest release. If only pre-releases exist, pin one, e.g.:
  --version v0.1.0-rc1   (or COMET_ENRICH_VERSION=v0.1.0-rc1)
GitHub API rate limiting can also cause this; retry later or pin a version."
    fi
}

download_and_verify() {
    asset="$BIN_NAME-$version-$target.tar.gz"
    url="$base_url/$version/$asset"
    info "downloading $url"
    download "$url" "$tmpdir/$asset" ||
        die "download failed; check that release $version exists and includes $asset"
    download "$url.sha256" "$tmpdir/$asset.sha256" ||
        die "download failed for the checksum file $asset.sha256"
    if command -v sha256sum >/dev/null 2>&1; then
        (cd "$tmpdir" && sha256sum -c "$asset.sha256" >/dev/null 2>&1) ||
            die "checksum verification failed for $asset"
    elif command -v shasum >/dev/null 2>&1; then
        (cd "$tmpdir" && shasum -a 256 -c "$asset.sha256" >/dev/null 2>&1) ||
            die "checksum verification failed for $asset"
    else
        die "need sha256sum or shasum to verify the download"
    fi
    info "checksum verified"
    tar -xzf "$tmpdir/$asset" -C "$tmpdir"
    [ -f "$tmpdir/$BIN_NAME" ] || die "$asset did not contain the $BIN_NAME binary"
}

install_binary() {
    mkdir -p "$bin_dir"
    # Stage inside the destination dir, then rename: atomic, and replacing a
    # running binary this way avoids "text file busy" errors.
    cp "$tmpdir/$BIN_NAME" "$bin_dir/.$BIN_NAME.tmp.$$"
    chmod 755 "$bin_dir/.$BIN_NAME.tmp.$$"
    mv -f "$bin_dir/.$BIN_NAME.tmp.$$" "$bin_dir/$BIN_NAME"
    info "installed $bin_dir/$BIN_NAME ($("$bin_dir/$BIN_NAME" --version 2>/dev/null || echo "$version"))"
    case ":$PATH:" in
        *":$bin_dir:"*) ;;
        *)
            warn "$bin_dir is not on your PATH. Add it, e.g. append to ~/.bashrc:"
            warn "  export PATH=\"$bin_dir:\$PATH\""
            ;;
    esac
}

# Generate a completion script into $2, only replacing it if generation succeeds
# (older releases lack the completions subcommand).
generate_completion() {
    mkdir -p "$(dirname "$2")"
    if "$bin_dir/$BIN_NAME" completions "$1" >"$2.tmp.$$" 2>/dev/null; then
        mv -f "$2.tmp.$$" "$2"
    else
        rm -f "$2.tmp.$$"
        return 1
    fi
}

install_completions() {
    shell_name=$(basename "${SHELL:-}")
    case "$shell_name" in
        bash)
            dest="${XDG_DATA_HOME:-$HOME/.local/share}/bash-completion/completions/$BIN_NAME"
            generate_completion bash "$dest" || return 1
            info "installed bash completions to $dest (open a new shell to use them)"
            ;;
        fish)
            dest="${XDG_CONFIG_HOME:-$HOME/.config}/fish/completions/$BIN_NAME.fish"
            generate_completion fish "$dest" || return 1
            info "installed fish completions to $dest"
            ;;
        zsh)
            dest="$HOME/.zsh/completions/_$BIN_NAME"
            generate_completion zsh "$dest" || return 1
            info "installed zsh completions to $dest"
            info "ensure ~/.zshrc contains, before compinit:"
            info "  fpath=(~/.zsh/completions \$fpath)"
            info "  autoload -Uz compinit && compinit"
            ;;
        *)
            info "no completions installed for shell '${shell_name:-unknown}'; see '$BIN_NAME completions --help'"
            ;;
    esac
}

main() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --version) version="${2:?--version needs a value}" && shift ;;
            --version=*) version="${1#--version=}" ;;
            --bin-dir) bin_dir="${2:?--bin-dir needs a value}" && shift ;;
            --bin-dir=*) bin_dir="${1#--bin-dir=}" ;;
            --target) target="${2:?--target needs a value}" && shift ;;
            --target=*) target="${1#--target=}" ;;
            --base-url) base_url="${2:?--base-url needs a value}" && shift ;;
            --base-url=*) base_url="${1#--base-url=}" ;;
            --no-completions) completions=0 ;;
            -h | --help)
                usage
                exit 0
                ;;
            *) die "unknown option '$1' (see --help)" ;;
        esac
        shift
    done

    detect_platform
    resolve_version
    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT INT TERM
    download_and_verify
    install_binary
    if [ "$completions" -eq 1 ]; then
        if ! install_completions; then
            warn "completion setup failed; run '$BIN_NAME completions --help' to set them up manually"
        fi
    fi
    info "done"
}

main "$@"
