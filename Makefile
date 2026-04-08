validate:
	cargo fmt --all -- --check
	cargo check --workspace --all-targets --all-features --locked
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

test:
	cargo test -p coral-engine --test engine --locked
	cargo test --workspace --all-targets --all-features --locked
