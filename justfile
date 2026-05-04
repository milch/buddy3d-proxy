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

# Send a CameraTrigger reboot command (start_device_reboot=1).
restart-camera:
    cargo run --bin buddy3d-proxy -- restart-camera

# Set the camera's video resolution.
# 1 = SD, 2 = HD, 3 = FHD (1080p, default).
# Use this to bring the camera back to 1080p after it auto-degrades.
# Usage: just set-quality [quality=3]
set-quality quality="3":
    cargo run --bin buddy3d-proxy -- set-quality --quality {{quality}}

# Set the camera's IR / day-night mode.
# 1 = Auto (default), 2 = Day, 3 = Night.
# Usage: just set-mode [mode=1]
set-mode mode="1":
    cargo run --bin buddy3d-proxy -- set-mode --mode {{mode}}

# Format + lint + test, in that order. Run before pushing.
ci: fmt-check lint test

# Hit the local /healthz endpoint (default HEALTH_PORT=8080).
healthz:
    curl -i http://localhost:8080/healthz

# Build the local Docker image (single-arch, host platform).
docker-build:
    docker build -t buddy3d-proxy:local .

# Run the local Docker image, hot-mounting .env and a local /data volume.
# Use this to validate the container actually starts; for full streaming
# tests, use `just serve` directly (no Docker round-trip).
docker-run: docker-build
    docker run --rm \
        --env-file .env \
        -v "$(pwd)/data:/data" \
        -p 8554:8554 \
        -p 8080:8080 \
        buddy3d-proxy:local
