//! The composed server's sign-in journeys through the upstream OIDC
//! provider. They run Programs, so they live beside the `coral` binary that
//! the local Run Executors run.
#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration tests inherit the package's dependency set and exercise only a subset of it."
)]

#[path = "oidc/oidc_login_tests.rs"]
mod oidc_login_tests;
#[path = "oidc/refresh_journey_tests.rs"]
mod refresh_journey_tests;
