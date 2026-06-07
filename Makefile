.PHONY: install ui-build rust-checks perf-check architecture-check removed-contract-check license-check lint-proto docs-generate docs-check schema-generate schema-check

install: ui-build
	cargo install --path crates/coral-cli --locked

ui-build:
	npm ci --prefix ui
	npm run build --prefix ui
	test -s ui/dist/index.html

rust-checks:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
	cargo nextest run --workspace --all-targets --all-features --locked --no-fail-fast
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked

perf-check:
	cargo build --locked -p coral-cli --release
	cargo run --locked -p xtask --release -- perf-check

architecture-check:
	cargo run --locked -p xtask -- architecture-check

removed-contract-check:
	cargo run --locked -p xtask -- removed-contract-check

# ----------------------------------------------------------------------------
# Dependency license scan
# ----------------------------------------------------------------------------
# Fails if any workspace dependency uses a license outside the allow-list in
# deny.toml. Requires `cargo-deny`.
#
#   make license-check

license-check:
	cargo deny --version >/dev/null 2>&1 || cargo install --locked cargo-deny
	cargo deny check licenses

# ----------------------------------------------------------------------------
# Protobuf API linting
# ----------------------------------------------------------------------------
# Lints crates/coral-api/proto with Buf.
#
#   make lint-proto   # check protobuf style and API-shape rules

lint-proto:
	cd crates/coral-api && buf lint

# ----------------------------------------------------------------------------
# Source docs generation
# ----------------------------------------------------------------------------
# Regenerates the source catalog pages and Mintlify navigation from
# SourceSpec manifests via the xtask binary. docs-check intentionally skips the
# community source catalog so PRs do not fail on aggregate community source
# catalog drift.
#
#   make docs-generate   # write/refresh the generated files in docs/
#   make docs-check      # CI freshness check: non-zero exit if stale

docs-generate:
	cargo run --locked -p xtask -- generate-docs \
	  --sources-dir sources/core \
	  --index docs/reference/bundled-sources.mdx \
	  --community-sources-dir sources/community \
	  --community-index docs/reference/community-sources.mdx \
	  --docs-json docs/docs.json

docs-check:
	cargo run --locked -p xtask -- generate-docs \
	  --sources-dir sources/core \
	  --index docs/reference/bundled-sources.mdx \
	  --docs-json docs/docs.json \
	  --skip-community-sources \
	  --check

# ----------------------------------------------------------------------------
# JSON schema generation
# ----------------------------------------------------------------------------
# Regenerates source manifest schemas that are generated from Rust types.
#
#   make schema-generate   # write/refresh generated schemas
#   make schema-check      # CI freshness check: non-zero exit if stale

schema-generate:
	cargo run --locked -p xtask -- generate-schemas

schema-check:
	cargo run --locked -p xtask -- generate-schemas --check
