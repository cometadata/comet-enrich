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

`make build-release` sets `-C target-cpu` from `RUST_TARGET_CPU`. The default is `x86-64-v3`.
Override it on the command line or in the environment:

```bash
make build-release RUST_TARGET_CPU=native    # native means build for this machine's CPU
RUST_TARGET_CPU=znver3 make build-release     # or set it in the environment
```

On non-x86-64 hosts, such as Apple silicon, use a value for your architecture, such as `native`
or `apple-m4`.

## Test

```bash
make test
```

`make lint` runs Clippy and `cargo deny`; `make fmt` runs the formatter; `make setup-rust`
installs the Rust tooling used by the workspace.

## Verify

```bash
target/debug/comet-enrich --help
```

You should see the three methods listed: `resource-type-general`, `affiliations`, and `funders`.
See [Usage](usage.md) next.
