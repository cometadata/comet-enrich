.RECIPEPREFIX := >

SHELL=/bin/bash

# build-release builds for the current system by default. Set RUST_TARGET_CPU for a
# micro architecture and RUST_TARGET to cross-compile. CI sets both per target.
RUST_TARGET_CPU ?=
RUST_TARGET ?=

setup-rust:
> rustup component add rustfmt clippy llvm-tools-preview
> @command -v cargo-deny >/dev/null 2>&1 || cargo install cargo-deny
> @command -v rumdl >/dev/null 2>&1 || cargo install rumdl
> @command -v cargo-llvm-cov >/dev/null 2>&1 || cargo install cargo-llvm-cov

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

# Print a coverage summary for the workspace test suite
coverage:
> cargo llvm-cov --workspace

# Write an HTML coverage report under target/llvm-cov/html/.
coverage-html:
> cargo llvm-cov --workspace --html
> @echo "HTML report: target/llvm-cov/html/index.html"

# Write lcov.info for CI upload
coverage-lcov:
> cargo llvm-cov --workspace --lcov --output-path lcov.info

build:
> cargo build --workspace

build-release:
> RUSTFLAGS="$(if $(RUST_TARGET_CPU),-C target-cpu=$(RUST_TARGET_CPU))" cargo build --release --workspace $(if $(RUST_TARGET),--target $(RUST_TARGET))

clean:
> cargo clean
