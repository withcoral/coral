validate:
	cargo fmt --all -- --check
	cargo check --workspace --all-targets --all-features --locked
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

test:
	mkdir -p .context/coverage
	cargo llvm-cov --workspace --all-targets --all-features --locked --json --summary-only --output-path .context/coverage/llvm-cov.json
	./scripts/coverage-summary.sh .context/coverage/llvm-cov.json .context/coverage/summary.md
	cat .context/coverage/summary.md
