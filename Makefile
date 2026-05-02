.PHONY: help check build release run test clippy fmt fmt-check doc clean \
        audit deny udeps machete outdated ci ci-strict tools

help:
	@echo "Common:"
	@echo "  check       - cargo check --all-targets"
	@echo "  build       - cargo build --all-targets"
	@echo "  release     - cargo build --release"
	@echo "  run         - cargo run"
	@echo "  test        - cargo test --workspace --all-features"
	@echo "  clippy      - clippy with -D warnings"
	@echo "  fmt         - cargo +nightly fmt --all"
	@echo "  fmt-check   - cargo +nightly fmt --all -- --check"
	@echo "  doc         - cargo doc --no-deps --open (with -D warnings)"
	@echo "  clean       - cargo clean"
	@echo ""
	@echo "Strict (require extra tools — see 'make tools'):"
	@echo "  audit       - cargo audit (security advisories)"
	@echo "  deny        - cargo deny check (licenses, sources, bans, advisories)"
	@echo "  udeps       - cargo +nightly udeps (unused deps)"
	@echo "  machete     - cargo machete (unused deps, faster)"
	@echo "  outdated    - cargo outdated"
	@echo ""
	@echo "Pipelines:"
	@echo "  ci          - fmt-check + clippy + test"
	@echo "  ci-strict   - ci + audit + deny + machete + doc"
	@echo "  tools       - install all auxiliary cargo tools"

check:
	cargo check --workspace --all-targets --all-features

build:
	cargo build --workspace --all-targets

release:
	cargo build --workspace --release

run:
	cargo run

test:
	cargo test --workspace --all-features

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

fmt:
	cargo +nightly fmt --all

fmt-check:
	cargo +nightly fmt --all -- --check

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --open

clean:
	cargo clean

audit:
	@command -v cargo-audit >/dev/null 2>&1 || { echo "cargo-audit not installed. Run: make tools"; exit 1; }
	cargo audit --deny warnings

deny:
	@command -v cargo-deny >/dev/null 2>&1 || { echo "cargo-deny not installed. Run: make tools"; exit 1; }
	cargo deny check

udeps:
	@command -v cargo-udeps >/dev/null 2>&1 || { echo "cargo-udeps not installed. Run: make tools"; exit 1; }
	cargo +nightly udeps --workspace --all-targets

machete:
	@command -v cargo-machete >/dev/null 2>&1 || { echo "cargo-machete not installed. Run: make tools"; exit 1; }
	cargo machete

outdated:
	@command -v cargo-outdated >/dev/null 2>&1 || { echo "cargo-outdated not installed. Run: make tools"; exit 1; }
	cargo outdated --workspace --root-deps-only

ci: fmt-check clippy test

ci-strict: fmt-check clippy test audit deny machete doc

tools:
	cargo install --locked cargo-audit cargo-deny cargo-machete cargo-outdated cargo-udeps
