install:
	cargo install --path crates/coral-cli --locked

rust-checks:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
	cargo test --workspace --all-targets --all-features --locked
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked

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
# Source manifest linting
# ----------------------------------------------------------------------------
# Lints sources/ with ryl (Rust-native yamllint port).
#
#   make lint-sources   # check only — run before pushing changes
#   make fix-sources    # apply ryl's safe auto-fixes in place

lint-sources:
	ryl sources

fix-sources:
	ryl --fix sources

# ----------------------------------------------------------------------------
# Source docs generation
# ----------------------------------------------------------------------------
# Regenerates the bundled-sources index and Mintlify navigation from
# sources/core/*/manifest.y{a,}ml via the xtask binary.
#
#   make docs-generate   # write/refresh the generated files in docs/
#   make docs-check      # CI freshness check: non-zero exit if stale

docs-generate:
	cargo run --locked -p xtask -- generate-docs \
	  --sources-dir sources/core \
	  --index docs/reference/bundled-sources.mdx \
	  --docs-json docs/docs.json

docs-check:
	cargo run --locked -p xtask -- generate-docs \
	  --sources-dir sources/core \
	  --index docs/reference/bundled-sources.mdx \
	  --docs-json docs/docs.json \
	  --check
