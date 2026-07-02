# Installation

## Quick install

Install the latest release binary and shell completions with:

```bash
curl -fsSL https://raw.githubusercontent.com/cometadata/comet-enrich/main/install.sh | sh
```

The script detects your platform, downloads the matching release tarball, verifies its
SHA-256 checksum, installs the binary to `~/.local/bin` (no sudo), and installs tab
completions for your shell.

Supported platforms:

- Linux x86-64: CPUs with AVX2 automatically get the faster `x86-64-v3` build; older CPUs
  get the baseline build.
- Linux arm64.
- macOS Apple silicon.
- Everything else: [build from source](#build).

Options can be set via environment variables or flags (pass flags through a pipe with
`sh -s --`):

```bash
# Pin a version. Required while only pre-releases exist, as "latest" resolves
# only to stable releases.
curl -fsSL .../install.sh | COMET_ENRICH_VERSION=v0.1.0-rc1 sh

# Same, using flags; also choose the install directory and skip completions.
curl -fsSL .../install.sh | sh -s -- --version v0.1.0-rc1 --bin-dir ~/bin --no-completions
```

Run `sh install.sh --help` for the full list of options.

### Docker

Pin the version and the target for reproducible, portable images:

```dockerfile
RUN curl -fsSL https://raw.githubusercontent.com/cometadata/comet-enrich/main/install.sh \
      | sh -s -- --version v0.1.0 --bin-dir /usr/local/bin --no-completions \
                 --target x86_64-unknown-linux-musl
```

Pass `--target x86_64-unknown-linux-musl` (or `aarch64-unknown-linux-musl` for arm64
images) rather than letting the script detect the CPU: detection runs on the *build* host,
and an `x86-64-v3` binary baked in there would crash on a runtime host without AVX2. The
Linux binaries are fully static (musl), so any base image works, including Alpine and
distroless.

## Prerequisites

The rest of this page covers building from source, which requires:

- [Rust 1.85+](https://rustup.rs/) (edition 2024) with `cargo`, installed via rustup.
- `git`.

`comet-enrich` is a Cargo workspace: the command line tool crate lives in `crates/cli` and the enrichment
methods live in the other crates.

> These docs assume macOS or Linux. The `make` targets below are convenience wrappers around
> `cargo`. You can also run the underlying commands directly, such as `cargo build` and
> `cargo test`, on any platform with the Rust toolchain installed.

## Build

Clone the repository and build from the workspace root:

```bash
git clone https://github.com/cometadata/comet-enrich.git
cd comet-enrich

make build           # debug build (target/debug/comet-enrich)
make build-release   # optimised build (target/release/comet-enrich)
```

Use the release binary for processing real data; it is faster.

### Target CPU

`make build-release` builds for the machine you are using by default.

You can set `RUST_TARGET_CPU` when you want to tune the binary for a specific CPU:

```bash
make build-release RUST_TARGET_CPU=native      # this machine's exact CPU
make build-release RUST_TARGET_CPU=x86-64-v3   # AVX2-class x86-64 baseline
RUST_TARGET_CPU=znver3 make build-release      # or set it in the environment
```

## Prebuilt binaries

Tagged releases include prebuilt binaries on the
[releases page](https://github.com/cometadata/comet-enrich/releases).

Available builds:

- Linux x86-64-v3 (AVX2): `comet-enrich-<tag>-x86_64-v3-unknown-linux-musl.tar.gz`
- Linux x86-64 baseline: `comet-enrich-<tag>-x86_64-unknown-linux-musl.tar.gz`
- Linux arm64: `comet-enrich-<tag>-aarch64-unknown-linux-musl.tar.gz`
- macOS Apple silicon: `comet-enrich-<tag>-aarch64-apple-darwin.tar.gz`

## Shell completions

`comet-enrich completions <shell>` prints a tab-completion script for `bash`, `zsh`, `fish`,
`powershell`, or `elvish` (`comet-enrich completions --help` shows these instructions too).

The simplest option is to generate the script at shell startup, so it always matches the
installed binary:

```bash
# bash: add to ~/.bashrc
source <(comet-enrich completions bash)

# zsh: add to ~/.zshrc (after compinit)
source <(comet-enrich completions zsh)

# fish: add to ~/.config/fish/config.fish
comet-enrich completions fish | source
```

Or install the script once into your shell's completions directory (regenerate after
upgrading):

```bash
# bash
comet-enrich completions bash > ~/.local/share/bash-completion/completions/comet-enrich

# zsh: place on your $fpath, e.g. (may need sudo)
comet-enrich completions zsh > /usr/local/share/zsh/site-functions/_comet-enrich

# fish
comet-enrich completions fish > ~/.config/fish/completions/comet-enrich.fish
```

## Test

```bash
make test       # run the workspace test suite
make coverage   # run the tests under instrumentation and print a coverage summary
```

`make coverage-html` writes an HTML report under `target/llvm-cov/html/`, and
`make coverage-lcov` writes `lcov.info` for editors or CI. `make lint` runs Clippy and
`cargo deny`; `make fmt` runs the formatter; `make setup-rust` installs the Rust tooling
used by the workspace (including `cargo-llvm-cov`, used for coverage).

## Verify

```bash
target/debug/comet-enrich --help
```

You should see the three methods listed — `resource-type-general`, `affiliations`, and
`funders` — plus the `completions` subcommand.
See [Usage](usage.md) next.
