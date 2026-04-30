//! Build hints for optional CLI assets.

#![allow(
    clippy::disallowed_methods,
    reason = "Cargo build scripts read build-time environment variables directly."
)]

fn main() {
    if std::env::var_os("CARGO_FEATURE_EMBEDDED_UI").is_some() {
        println!("cargo:rerun-if-changed=../../ui/dist");
        println!("cargo:rerun-if-changed=../../ui/dist/index.html");
    }
}
