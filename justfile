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

# Run the real-Prusa streaming smoke test (auth + signaling + WebRTC + RTP).
smoke-stream:
    cargo test --test manual_smoke real_prusa_stream_smoke -- --ignored --nocapture

# Run the RTSP proxy. Listens on RTSP_PORT (default 8554) and serves
# rtsp://localhost:8554/<camera-name>. WebRTC stays idle until a client
# connects, then is torn down IDLE_TIMEOUT_SECONDS after the last viewer
# disconnects. Open the stream in another terminal with:
#     vlc rtsp://localhost:8554/<your-camera-slug>
serve:
    cargo run --bin buddy3d-proxy -- serve

# Send a CameraTrigger reboot command to the camera. Useful after the camera
# has degraded its stream quality from many reconnects. The protobuf field
# number for start_device_reboot is unknown; default to 3 and probe.
# Usage: just restart-camera [field=3]
restart-camera field="3":
    cargo run --bin buddy3d-proxy -- restart-camera --field {{field}}

# Format + lint + test, in that order. Run before pushing.
ci: fmt-check lint test
