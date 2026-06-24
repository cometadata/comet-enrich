.RECIPEPREFIX := >

SHELL=/bin/bash

# Default CPU target for release build. Override on the command line or in the
# environment, e.g. `make build-release RUST_TARGET_CPU=native`.
RUST_TARGET_CPU ?= x86-64-v3

setup-rust:
> rustup component add rustfmt clippy
> @command -v cargo-deny >/dev/null 2>&1 || cargo install cargo-deny
> @command -v rumdl >/dev/null 2>&1 || cargo install rumdl

fmt:
> cargo fmt --all
> rumdl fmt

# Check formatting without modifying files
fmt-ci:
> cargo fmt --all -- --check

lint:
> cargo clippy --workspace --all-targets --all-features
> cargo deny check
> rumdl check

# Lint without modifying files, failing on warnings
lint-ci:
> cargo clippy --workspace --all-targets --all-features -- -D warnings
> cargo deny check
> rumdl check

test:
> cargo test --workspace

build:
> cargo build --workspace

build-release:
> RUSTFLAGS="-C target-cpu=$(RUST_TARGET_CPU)" cargo build --release --workspace

clean:
> cargo clean
