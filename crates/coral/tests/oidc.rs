//! The composed server's sign-in journeys through the upstream OIDC
//! provider. The refresh journey runs a Program, so it lives beside the
//! `coral` binary that the local Run Executors run, with the login journey
//! whose fixtures it shares.
#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration tests inherit the package's dependency set and exercise only a subset of it."
)]

#[path = "oidc/oidc_login_tests.rs"]
mod oidc_login_tests;
#[path = "oidc/refresh_journey_tests.rs"]
mod refresh_journey_tests;
