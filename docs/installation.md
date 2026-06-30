# Installation

## Prerequisites

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

- Linux x86-64-v3: `comet-enrich-<tag>-x86_64-v3-unknown-linux-musl.tar.gz`
- Linux arm64: `comet-enrich-<tag>-aarch64-unknown-linux-musl.tar.gz`
- macOS Apple silicon: `comet-enrich-<tag>-aarch64-apple-darwin.tar.gz`

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

You should see the three methods listed: `resource-type-general`, `affiliations`, and `funders`.
See [Usage](usage.md) next.
