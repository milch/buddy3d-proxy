set shell := ["bash", "-uc"]
set dotenv-load := true
set dotenv-required := false

# List available recipes.
default:
    @just --list --unsorted

# Run the proxy binary (pass extra args after `--`, e.g. `just run -- list-cameras`).
run *args:
    cargo run --bin buddy3d-proxy {{args}}

# Build the main crate in debug mode.
build:
    cargo build

# Build the main crate optimized.
release:
    cargo build --release

# Type-check without producing artifacts (fast feedback).
check:
    cargo check --all-targets

# Run the unit + integration test suite (excludes #[ignore]'d real-Prusa smoke tests).
test:
    cargo test --all-targets

# Run the real-Prusa smoke test. Requires PRUSA_EMAIL and PRUSA_PASSWORD in the environment.
smoke:
    cargo test --test manual_smoke -- --ignored --nocapture

# cargo fmt across workspace.
fmt:
    cargo fmt --all

# cargo fmt --check (CI-style, no writes).
fmt-check:
    cargo fmt --all -- --check

# Lint with clippy. -D warnings makes any warning fail.
lint:
    cargo clippy --all-targets -- -D warnings

# Regenerate src/proto/buddy3d.rs from proto/*.proto.
gen-proto:
    cargo xtask gen-proto

# Format + lint + test, in that order. Run before pushing.
ci: fmt-check lint test
