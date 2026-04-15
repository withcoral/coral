rust-checks:
	cargo fmt --all -- --check
	cargo check --workspace --all-targets --all-features --locked
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo test --workspace --all-targets --all-features --locked
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

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
# Generates the per-source MDX pages, the bundled-sources index, and the
# Mintlify navigation from sources/*/manifest.y{a,}ml.
#
#   make docs-generate   # write/refresh the generated files in docs/
#   make docs-check      # CI freshness check: non-zero exit if stale

docs-generate:
	cargo run -p coral-docgen -- \
	  --sources-dir sources \
	  --index docs/reference/bundled-sources.mdx \
	  --docs-json docs/docs.json

docs-check:
	cargo run -p coral-docgen -- \
	  --sources-dir sources \
	  --index docs/reference/bundled-sources.mdx \
	  --docs-json docs/docs.json \
	  --check
