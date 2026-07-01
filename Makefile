.PHONY: install ui-build rust-checks virtual-graph-checks virtual-graph-tck virtual-graph-tck-report virtual-graph-upstream-tck-report virtual-graph-graphql virtual-graph-graphql-report virtual-graph-graphql-schema-coverage perf-check license-check lint-proto lint-sources fix-sources docs-generate docs-check schema-generate schema-check

OPENCYPHER_TCK_TAG ?= 2024.3
OPENCYPHER_TCK_REVISION ?= 677cbafabb8c3c5eed458fd3b1ec0daec8d67d23

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

virtual-graph-checks:
	cargo fmt --all -- --check
	cargo clippy -p coral-engine --all-targets --locked -- -D warnings
	cargo test -p coral-engine virtual_graph --locked
	cargo test -p coral-engine opencypher_tck --locked
	cargo test -p coral-engine graphql_read_baseline --locked

virtual-graph-tck:
	cargo test -p coral-engine opencypher_tck --locked

virtual-graph-tck-report:
	cargo run --locked -p xtask -- virtual-graph-tck-report --json

virtual-graph-upstream-tck-report:
	tmp_dir="$$(mktemp -d)"; \
	trap 'rm -rf "$$tmp_dir"' EXIT; \
	git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$(OPENCYPHER_TCK_TAG)" \
	  https://github.com/opencypher/openCypher.git "$$tmp_dir/openCypher"; \
	test "$$(git -C "$$tmp_dir/openCypher" rev-parse HEAD)" = "$(OPENCYPHER_TCK_REVISION)"; \
	cargo run --locked -p xtask -- virtual-graph-upstream-tck-report \
	  --features-dir "$$tmp_dir/openCypher/tck/features" \
	  --json

virtual-graph-graphql:
	cargo test -p coral-engine graphql_read_baseline --locked

virtual-graph-graphql-report:
	cargo run --locked -p xtask -- virtual-graph-baseline-report \
	  --fixture crates/coral-engine/tests/fixtures/virtual_graph/graphql_read_baseline.json \
	  --json

virtual-graph-graphql-schema-coverage:
	cargo run --locked -p xtask -- virtual-graph-graphql-schema-coverage \
	  --fixture crates/coral-engine/tests/fixtures/virtual_graph/graphql_read_baseline.json \
	  --json

perf-check:
	cargo build --locked -p coral-cli --release
	cargo run --locked -p xtask --release -- perf-check --coral-bin target/release/coral

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
# Regenerates the source catalog pages and Mintlify navigation from
# sources/core/*/manifest.y{a,}ml and sources/community/*/manifest.y{a,}ml
# via the xtask binary. docs-check intentionally skips the community source
# catalog so PRs do not fail on aggregate community source catalog drift.
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
