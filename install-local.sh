#!/bin/sh
# shellcheck shell=sh
# Local installer for comet-enrich: builds the release binary from source,
# installs it, and sets up shell completions. This is the from-source
# counterpart of install.sh, which downloads a prebuilt release instead.
#
# Usage:
#   ./install-local.sh
#   ./install-local.sh --bin-dir ~/bin --no-completions
#   ./install-local.sh --target-cpu native
#
# Options (flags override the matching environment variable):
#   --bin-dir <dir>     Install directory        [COMET_ENRICH_BIN_DIR, default: ~/.local/bin]
#   --target-cpu <cpu>  cargo -C target-cpu=<cpu> [RUST_TARGET_CPU, default: none]
#   --no-completions    Skip shell completion setup
#   -h, --help          Show this help

set -eu

BIN_NAME="comet-enrich"

bin_dir="${COMET_ENRICH_BIN_DIR:-$HOME/.local/bin}"
target_cpu="${RUST_TARGET_CPU:-}"
completions=1
script_dir=""
built=""

info() { printf '%s\n' "$BIN_NAME install: $*" >&2; }
warn() { printf '%s\n' "$BIN_NAME install: warning: $*" >&2; }
die() {
    printf '%s\n' "$BIN_NAME install: error: $*" >&2
    exit 1
}

usage() {
    cat <<EOF
Local installer for $BIN_NAME: builds the release binary from source, installs
it, and sets up shell completions. The from-source counterpart of install.sh.

Usage:
  ./install-local.sh
  ./install-local.sh --bin-dir ~/bin --no-completions
  ./install-local.sh --target-cpu native

Options (flags override the matching environment variable):
  --bin-dir <dir>     Install directory         [COMET_ENRICH_BIN_DIR, default: ~/.local/bin]
  --target-cpu <cpu>  cargo -C target-cpu=<cpu>  [RUST_TARGET_CPU, default: none]
  --no-completions    Skip shell completion setup
  -h, --help          Show this help
EOF
}

build_release() {
    command -v make >/dev/null 2>&1 ||
        die "make not found; install it to build from source (see docs/installation.md)"
    command -v cargo >/dev/null 2>&1 ||
        die "cargo not found; install Rust to build from source (see docs/installation.md)"
    info "building release binary (this may take a while)"
    # Delegate to the Makefile so the RUSTFLAGS/target-cpu build command lives in
    # one place (the same target CI uses). RUST_TARGET and CARGO_TARGET_DIR are
    # pinned so inherited values can't redirect the build output elsewhere.
    CARGO_TARGET_DIR="$script_dir/target" make build-release RUST_TARGET_CPU="$target_cpu" RUST_TARGET=
    built="$script_dir/target/release/$BIN_NAME"
    [ -x "$built" ] || die "expected the built binary at $built, but it is not there"
}

install_binary() {
    mkdir -p "$bin_dir"
    # Stage inside the destination dir, then rename: atomic, and replacing a
    # running binary this way avoids "text file busy" errors.
    cp "$built" "$bin_dir/.$BIN_NAME.tmp.$$"
    chmod 755 "$bin_dir/.$BIN_NAME.tmp.$$"
    mv -f "$bin_dir/.$BIN_NAME.tmp.$$" "$bin_dir/$BIN_NAME"
    info "installed $bin_dir/$BIN_NAME ($("$bin_dir/$BIN_NAME" --version 2>/dev/null || echo "unknown version"))"
    case ":$PATH:" in
        *":$bin_dir:"*) ;;
        *)
            warn "$bin_dir is not on your PATH. Add it, e.g. append to ~/.bashrc:"
            warn "  export PATH=\"$bin_dir:\$PATH\""
            ;;
    esac
}

# Generate a completion script into $2, only replacing it if generation succeeds.
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
    shell_name=${SHELL:-}
    shell_name=${shell_name##*/}
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
            --bin-dir) bin_dir="${2:?--bin-dir needs a value}"; shift ;;
            --bin-dir=*) bin_dir="${1#--bin-dir=}" ;;
            --target-cpu) target_cpu="${2:?--target-cpu needs a value}"; shift ;;
            --target-cpu=*) target_cpu="${1#--target-cpu=}" ;;
            --no-completions) completions=0 ;;
            -h | --help)
                usage
                exit 0
                ;;
            *) die "unknown option '$1' (see --help)" ;;
        esac
        shift
    done

    # Resolve the repo root from the script location so the build works from any CWD.
    script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
    cd "$script_dir"

    build_release
    install_binary
    if [ "$completions" -eq 1 ]; then
        if ! install_completions; then
            warn "completion setup failed; run '$BIN_NAME completions --help' to set them up manually"
        fi
    fi
    info "done"
}

main "$@"
