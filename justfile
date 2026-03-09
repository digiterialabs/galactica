# Galactica monorepo build orchestration

default:
    @just --list

# === Rust ===

# Build all Rust crates
build:
    cargo build --workspace

# Run all Rust tests
test:
    cargo test --workspace

# Run clippy lints
check:
    cargo clippy --workspace -- -D warnings

# Format Rust code
fmt:
    cargo fmt --all

# Check Rust formatting
fmt-check:
    cargo fmt --all -- --check

# === Python ===

# Install Python dependencies
py-install:
    cd python && uv sync

# Run Python tests
py-test:
    cd python && uv run pytest

# Run Python linter
py-lint:
    cd python && uv run ruff check

# Format Python code
py-fmt:
    cd python && uv run ruff format

# Check Python formatting
py-fmt-check:
    cd python && uv run ruff format --check

# === Dashboard (TypeScript) ===

# Install dashboard dependencies
dash-install:
    cd dashboard && npm install

# Build dashboard
dash-build:
    cd dashboard && npm run build

# Run dashboard dev server
dash-dev:
    cd dashboard && npm run dev

# Lint dashboard
dash-lint:
    cd dashboard && npm run lint

# === Proto ===

# Generate Rust proto stubs (handled by build.rs, this is a convenience alias)
proto-gen-rust:
    cargo build -p galactica-common

# Generate Python proto stubs from runtime.proto and common.proto
proto-gen-python:
    python -m grpc_tools.protoc \
        -Iproto \
        --python_out=python/galactica_runtime/proto \
        --grpc_python_out=python/galactica_runtime/proto \
        proto/galactica/common/v1/common.proto \
        proto/galactica/runtime/v1/runtime.proto

# Generate all proto stubs
proto-gen: proto-gen-rust proto-gen-python

# === All ===

# Build everything
build-all: build py-install dash-install dash-build

# Test everything
test-all: test py-test dash-build

# Lint everything
lint-all: check py-lint dash-lint

# Format everything
fmt-all: fmt py-fmt

# Check all formatting
fmt-check-all: fmt-check py-fmt-check

# === Git Hooks (Husky) ===

# Install Husky and register local Git hooks
precommit-install:
    npm install
    npm --prefix dashboard install

# Run the Husky pre-commit checks manually
precommit-run:
    npm run hooks:pre-commit

# Run the Husky pre-push checks manually
prepush-run:
    npm run hooks:pre-push

# Clean all build artifacts
clean:
    cargo clean
    rm -rf python/.venv
    rm -rf dashboard/node_modules dashboard/.next

# === Dev Cluster Helpers ===

# Detect the current host profile and recommended runtime
dev-detect:
    cargo run -p galactica-cli -- detect

# Check local prerequisites for the current host
dev-doctor:
    cargo run -p galactica-cli -- doctor

# Prepare local state and print the next commands for this host
dev-install:
    cargo run -p galactica-cli -- install

# Install galactica-cli into a user-local bin directory
dev-self-install:
    cargo run -p galactica-cli -- self-install

# Start the dev control plane using checked-in defaults
dev-control-plane:
    cargo run -p galactica-cli -- up control-plane

# Mint and save a bootstrap enrollment token to var/dev/enrollment-token.txt
dev-token:
    cargo run -p galactica-cli -- token mint

# Start the dev gateway using checked-in defaults
dev-gateway:
    cargo run -p galactica-cli -- up gateway

# Start the current machine as a node using auto-detected runtime/config
dev-node:
    cargo run -p galactica-cli -- up node
