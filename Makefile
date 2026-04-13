rust-checks:
	cargo fmt --all -- --check
	cargo check --workspace --all-targets --all-features --locked
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo test --workspace --all-targets --all-features --locked
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

# ----------------------------------------------------------------------------
# YAML source manifest linting
# ----------------------------------------------------------------------------
# Lints sources/*/manifest.yaml with ryl (Rust-native yamllint port).
#
#   make yaml-lint   # check — run before pushing changes to sources/
#   make yaml-fix    # apply safe fixes in place

RYL_VERSION := 0.6.0
YAMLLINT_FILES := $(wildcard sources/*/manifest.yaml sources/*/manifest.yml)

ryl-ensure:
	@if ! command -v ryl >/dev/null 2>&1 || \
	  [ "$$(ryl --version 2>&1 | awk '{print $$NF}')" != "$(RYL_VERSION)" ]; then \
		echo "Installing ryl $(RYL_VERSION)..."; \
		cargo install ryl --locked --version $(RYL_VERSION); \
	fi

yaml-lint: ryl-ensure
	ryl $(YAMLLINT_FILES)

yaml-fix: ryl-ensure
	ryl --fix $(YAMLLINT_FILES)
