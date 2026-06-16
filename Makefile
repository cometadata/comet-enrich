.RECIPEPREFIX := >

SHELL=/bin/bash

setup-rust:
> rustup component add rustfmt clippy
> @command -v cargo-deny >/dev/null 2>&1 || cargo install cargo-deny

fmt:
> cargo fmt --all

lint:
> cargo clippy --workspace --all-targets --all-features
> cargo deny check

test:
> cargo test --workspace

build:
> cargo build --workspace

clean:
> cargo clean
