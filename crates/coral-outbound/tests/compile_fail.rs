//! Proofs that a URL under an untrusted policy cannot start a request on a raw
//! HTTP client. Each case in `compile_fail/` must fail to compile with exactly
//! the error in its `.stderr` file, so a case that fails for another reason
//! fails this test.
//!
//! The `.stderr` files quote the compiler. After a toolchain change, rewrite
//! them and review the difference:
//!
//! ```text
//! TRYBUILD=overwrite cargo nextest run -p coral-outbound --features transport --test compile_fail
//! ```
//!
//! trybuild checks the dependencies in `target/tests/trybuild`, apart from the
//! workspace's own build, so the first run takes minutes.
#![cfg(feature = "transport")]
#![allow(
    unused_crate_dependencies,
    reason = "integration tests inherit library dependencies"
)]

#[test]
fn untrusted_policies_cannot_use_a_raw_client() {
    trybuild::TestCases::new().compile_fail("tests/compile_fail/*.rs");
}
